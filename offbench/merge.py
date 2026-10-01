"""Fold a per-host results.jsonl into the shared data/results.jsonl.

Each metal host runs its own subtask and writes its own results.jsonl. Merging
stamps every record's ``host`` field from the registry entry that produced it,
then appends into the shared file, skipping any record already present. The id
that decides "already present" is stable across re-runs, so re-merging the same
host file adds nothing. Pure stdlib.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from .hosts import Host
from .schema import Record, append_records, read_records


def record_id(rec: Record) -> str:
    """Stable identity for a merged record: task_id + bin + cell + rep + host.

    Two records with the same id are the same measurement and must collapse to
    one, so merging is idempotent. The host component is the registry name the
    record was stamped with.
    """
    return "|".join((
        rec.task_id or "",
        rec.bin.name,
        rec.cell.key(),
        str(rec.rep),
        rec.host.get("id", ""),
    ))


def stamp_host(rec: Record, host: Host) -> Record:
    """Set the record's host id/type from the registry entry, preserving any
    kernel string the host itself reported."""
    kernel = rec.host.get("kernel", "")
    rec.host = {"id": host.name, "type": host.ssh, "kernel": kernel}
    if host.az:
        rec.host["az"] = host.az
    return rec


@dataclass
class MergeResult:
    added: int
    skipped: int
    total: int          # records read from the host file

    @property
    def merged_any(self) -> bool:
        return self.added > 0


def merge_host_results(
    host_file: Path,
    host: Host,
    results_path: Path,
) -> MergeResult:
    """Ingest ``host_file`` (a per-host results.jsonl), stamp each record with
    ``host``, and append to ``results_path`` deduped on record_id.

    Idempotent: records whose id already exists in ``results_path`` (or earlier
    in the same host file) are skipped, so a second merge of the same file adds
    nothing.
    """
    host_file = Path(host_file)
    results_path = Path(results_path)

    seen = {record_id(r) for r in read_records(results_path)}

    to_add: list[Record] = []
    total = 0
    for rec in read_records(host_file):
        total += 1
        stamp_host(rec, host)
        rid = record_id(rec)
        if rid in seen:
            continue
        seen.add(rid)
        to_add.append(rec)

    added = append_records(results_path, to_add) if to_add else 0
    return MergeResult(added=added, skipped=total - added, total=total)
