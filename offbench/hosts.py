"""Host registry: the metal hosts a task can be dispatched to.

One JSON file (default ``hosts.json`` beside the package), a list of entries
keyed by a short ``name``. Writes go through the same tmp + fsync + rename dance
as queue.py so a half-written registry never replaces a good one. Pure stdlib.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

DEFAULT_REMOTE_ROOT = "~/offbench"


@dataclass
class Host:
    name: str                                  # short handle, e.g. metal-1
    ssh: str                                   # user@ip, e.g. ec2-user@192.0.2.10
    instance_id: str = ""                      # i-... (for start/stop, tagging)
    az: str = ""                               # availability zone, if known
    remote_root: str = DEFAULT_REMOTE_ROOT     # where the runner keeps its root
    tags: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("host needs a name")
        if not self.ssh:
            raise ValueError(f"host {self.name!r} needs an ssh target (user@ip)")

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "Host":
        return cls(**d)


class Registry:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.hosts: dict[str, Host] = {}
        self.load()

    # ---- (de)serialisation ------------------------------------------------
    def load(self) -> "Registry":
        self.hosts = {}
        if not self.path.exists():
            return self
        doc = json.loads(self.path.read_text())
        for entry in doc.get("hosts", []):
            h = Host.from_dict(entry)
            self.hosts[h.name] = h
        return self

    def save(self) -> Path:
        doc = {"hosts": [self.hosts[n].to_dict() for n in sorted(self.hosts)]}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=self.path.parent, prefix=f".{self.path.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(doc, f, indent=2)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            dfd = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)
        return self.path

    # ---- helpers ----------------------------------------------------------
    def add(self, host: Host, replace: bool = False) -> Host:
        if host.name in self.hosts and not replace:
            raise ValueError(f"host {host.name!r} already registered (use replace=True to overwrite)")
        self.hosts[host.name] = host
        self.save()
        return host

    def remove(self, name: str) -> bool:
        if name in self.hosts:
            del self.hosts[name]
            self.save()
            return True
        return False

    def get(self, name: str) -> Host:
        try:
            return self.hosts[name]
        except KeyError:
            raise KeyError(f"no host named {name!r}; known: {', '.join(sorted(self.hosts)) or '(none)'}")

    def resolve(self, names: list[str]) -> list[Host]:
        return [self.get(n) for n in names]

    def list(self) -> list[Host]:
        return [self.hosts[n] for n in sorted(self.hosts)]


def default_path() -> Path:
    # hosts.json lives at the package parent (repo root), beside README.md.
    return Path(__file__).resolve().parent.parent / "hosts.json"


def load_registry(path: Optional[Path] = None) -> Registry:
    return Registry(Path(path) if path else default_path())
