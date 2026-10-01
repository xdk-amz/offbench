"""Task = one cell family; Queue = one JSON file per task, FIFO by id.

Task ids are timestamps, so directory order is submission order. Import is
tmp + fsync + rename so a half-written task never heads the queue. A task the
runner cannot parse is parked as ``.unsupported`` instead of blocking forever.
"""

from __future__ import annotations

import itertools
import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

from .schema import Bin, Cell

TASK_KINDS = ("matrix", "sweep", "canary")


def new_task_id(now: Optional[datetime] = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%S.%f")[:-3] + "Z"


@dataclass
class Task:
    kind: str                                  # matrix | sweep | canary
    bins: list[Bin]
    workloads: list[str] = field(default_factory=lambda: ["get", "set", "mixed"])
    pipelines: list[int] = field(default_factory=lambda: [1, 16])
    conns: list[int] = field(default_factory=lambda: [2400, 7000])
    io_threads: list[int] = field(default_factory=lambda: [16])
    reps: int = 3
    order: str = "interleaved"                 # interleaved (A B A B per cell) | grouped
    perf: bool = False                         # perf stat on the main thread per rep
    keys: int = 100000
    value_size: int = 64
    acl: int = 0
    pin: int = 0
    window_s: int = 6
    note: str = ""
    task_id: str = field(default_factory=new_task_id)
    submitted_by: str = ""
    priority: int = 0
    parent_task_id: Optional[str] = None       # set on subtasks produced by partition()
    part: Optional[str] = None                 # "k/n" for the k-th of n subtasks

    def __post_init__(self) -> None:
        if self.kind not in TASK_KINDS:
            raise ValueError(f"unknown task kind {self.kind!r}, expected one of {TASK_KINDS}")
        if not self.bins:
            raise ValueError("a task needs at least one bin")
        if self.reps < 1:
            raise ValueError("reps must be >= 1")
        if self.order not in ("interleaved", "grouped"):
            raise ValueError("order must be interleaved or grouped")
        self.bins = [b if isinstance(b, Bin) else Bin(**b) for b in self.bins]

    # ---- expansion ------------------------------------------------------
    def cells(self) -> list[Cell]:
        # Omitting a P/c/iot pair in the reporter's matrix is done by the caller
        # passing explicit lists; the product is the whole space asked for.
        return [
            Cell(w, p, c, iot, self.keys, self.value_size, self.acl, self.pin)
            for w, p, c, iot in itertools.product(self.workloads, self.pipelines, self.conns, self.io_threads)
        ]

    def plan(self) -> list[tuple[Cell, Bin, int]]:
        """Execution order. Interleaved: per cell, rep1 of every bin, then rep2...
        The bin order rotates by one position each rep, so every bin runs in
        every slot once per len(bins) reps and order bias cannot favour a bin."""
        out: list[tuple[Cell, Bin, int]] = []
        n = len(self.bins)
        for cell in self.cells():
            if self.order == "interleaved":
                for rep in range(1, self.reps + 1):
                    k = (rep - 1) % n if n else 0
                    for b in self.bins[k:] + self.bins[:k]:
                        out.append((cell, b, rep))
            else:
                for b in self.bins:
                    for rep in range(1, self.reps + 1):
                        out.append((cell, b, rep))
        return out

    def estimated_seconds(self, per_rep_s: float = 25.0) -> float:
        return len(self.plan()) * per_rep_s

    # ---- partitioning -----------------------------------------------------
    # Axes we may split, in preference order. Splitting an axis hands each
    # subtask a disjoint subset of that axis' values, so every cell (and all of
    # its interleaved A/B reps) stays whole on one host. acl is scalar on a Task
    # so it can only split a two-value acl span (acl<0 sentinel meaning "both");
    # in the common single-acl case it has one value and we fall through to
    # io_threads, then workload.
    _SPLIT_AXES = ("acl", "io_threads", "workloads")

    def _axis_values(self, axis: str) -> list:
        if axis == "acl":
            # A Task carries a single acl; there is nothing to split unless the
            # task explicitly spans both (encoded as acl == -1).
            return [0, 1] if self.acl < 0 else [self.acl]
        return list(getattr(self, axis))

    def _with_axis(self, axis: str, values: list) -> "Task":
        d = self.to_dict()
        if axis == "acl":
            d["acl"] = values[0] if len(values) == 1 else -1
        else:
            d[axis] = list(values)
        d.pop("task_id", None)                 # fresh id per subtask
        return Task.from_dict(d)

    def partition(self, n: int) -> list["Task"]:
        """Split this task into ``n`` subtasks with the most even per-host runtime.

        Chooses the first axis in preference order (acl, io_threads, workload)
        that has at least ``n`` distinct values, and greedily bins its values by
        estimated runtime (longest-first into the currently-lightest subtask) so
        the ``n`` subtasks balance. A single cell (all reps + interleaved A/B
        pairs) is never split across subtasks. Each subtask is a full Task
        carrying ``parent_task_id`` and ``part='k/n'``.
        """
        if n < 1:
            raise ValueError("n must be >= 1")
        if n == 1:
            only = self._with_axis(self._SPLIT_AXES[1], self._axis_values(self._SPLIT_AXES[1]))
            only.parent_task_id = self.task_id
            only.part = "1/1"
            return [only]

        # Per-value estimated seconds along a candidate axis: the cost of one
        # value = cost of the whole task scaled by (1 / number of axis values),
        # since cells() is a Cartesian product and every value contributes an
        # equal slice of cells. We balance the actual value groups below.
        chosen_axis = None
        for axis in self._SPLIT_AXES:
            if len(self._axis_values(axis)) >= n:
                chosen_axis = axis
                break
        if chosen_axis is None:
            raise ValueError(
                f"cannot split into {n}: no axis (acl/io_threads/workloads) has >= {n} "
                f"distinct values without splitting a cell"
            )

        values = self._axis_values(chosen_axis)
        total = self.estimated_seconds()
        per_value = total / len(values)

        # Longest-processing-time greedy bin packing into n buckets. All values
        # cost the same slice here, but keep the weighted form so a future
        # non-uniform axis balances correctly too.
        buckets: list[list] = [[] for _ in range(n)]
        loads = [0.0] * n
        for v in sorted(values, key=lambda _v: -per_value):
            i = min(range(n), key=lambda j: loads[j])
            buckets[i].append(v)
            loads[i] += per_value

        subtasks: list[Task] = []
        non_empty = [g for g in buckets if g]
        parts = len(non_empty)
        for k, group in enumerate(non_empty, start=1):
            sub = self._with_axis(chosen_axis, group)
            sub.parent_task_id = self.task_id
            sub.part = f"{k}/{parts}"
            subtasks.append(sub)
        return subtasks

    def short(self) -> str:
        bins = ",".join(b.name for b in self.bins)
        return (
            f"{self.kind} [{bins}] w={','.join(self.workloads)} P={','.join(map(str, self.pipelines))} "
            f"c={','.join(map(str, self.conns))} iot={','.join(map(str, self.io_threads))} "
            f"reps={self.reps} cells={len(self.cells())} runs={len(self.plan())}"
            + (f' "{self.note}"' if self.note else "")
        )

    # ---- (de)serialisation ------------------------------------------------
    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Task":
        return cls(**d)


class Queue:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.pending = self.root / "queue"
        self.running = self.root / "running"
        self.done = self.root / "done"
        self.failed = self.root / "failed"
        for d in (self.pending, self.running, self.done, self.failed):
            d.mkdir(parents=True, exist_ok=True)

    def _path(self, task_id: str, where: Optional[Path] = None) -> Path:
        return (where or self.pending) / f"task_{task_id}.json"

    def submit(self, task: Task) -> Path:
        return self.import_document(task.to_dict())

    def import_document(self, doc: dict) -> Path:
        task = Task.from_dict(doc)  # validate before anything touches disk
        target = self._path(task.task_id)
        if target.exists():
            if json.loads(target.read_text()) != doc:
                raise ValueError(f"task {task.task_id} exists with different content")
            return target
        fd, tmp = tempfile.mkstemp(dir=self.pending, prefix=f".task_{task.task_id}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(doc, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, target)
            dfd = os.open(self.pending, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        return target

    def list(self) -> list[Task]:
        out = []
        for p in sorted(self.pending.glob("task_*.json")):
            try:
                out.append(Task.from_dict(json.loads(p.read_text())))
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
        return out

    def running_task(self) -> Optional[Task]:
        for p in sorted(self.running.glob("task_*.json")):
            return Task.from_dict(json.loads(p.read_text()))
        return None

    def next(self) -> Optional[Task]:
        for p in sorted(self.pending.glob("task_*.json")):
            try:
                return Task.from_dict(json.loads(p.read_text()))
            except json.JSONDecodeError:
                p.unlink(missing_ok=True)      # corrupt: drop
            except (ValueError, TypeError) as exc:
                parked = p.with_name(p.name + ".unsupported")
                p.rename(parked)
                parked.with_suffix(".reason").write_text(str(exc))
        return None

    def claim(self, task: Task) -> Path:
        src, dst = self._path(task.task_id), self._path(task.task_id, self.running)
        os.replace(src, dst)
        return dst

    def finish(self, task: Task, ok: bool, error: Optional[str] = None) -> Path:
        src = self._path(task.task_id, self.running)
        dst = self._path(task.task_id, self.done if ok else self.failed)
        doc = task.to_dict()
        doc["finished"] = datetime.now(timezone.utc).isoformat()
        doc["ok"] = ok
        if error:
            doc["error"] = error
        dst.write_text(json.dumps(doc, indent=2))
        src.unlink(missing_ok=True)
        if not ok:
            with (self.root / "failed_tasks.jsonl").open("a") as f:
                f.write(json.dumps({"task_id": task.task_id, "error": error, "ts": doc["finished"]}) + "\n")
        return dst

    def remove(self, task_id: str) -> bool:
        p = self._path(task_id)
        if p.exists():
            p.unlink()
            return True
        return False

    def clear(self) -> int:
        n = 0
        for p in self.pending.glob("task_*.json"):
            p.unlink()
            n += 1
        return n

    def history(self) -> Iterator[dict]:
        for d in (self.done, self.failed):
            for p in sorted(d.glob("task_*.json")):
                yield json.loads(p.read_text())
