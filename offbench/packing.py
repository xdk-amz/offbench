"""Deterministic packing for the run archive.

A file or a directory tree becomes a gzip stream named by the sha256 of its uncompressed bytes. A tree is a tar
with sorted names, zero mtimes and owners, and modes reduced to 644/755, so the same files give the same sha256 on
any host. Runs on the orchestrator (harness snapshots) and on the hosts (binaries, build trees): pure stdlib, and
it imports nothing else from offbench.
"""

from __future__ import annotations

import fnmatch
import gzip
import hashlib
import io
import os
import stat
import tarfile
from pathlib import Path
from typing import Iterable, Optional

CHUNK = 1 << 20

# What a valkey build tree holds besides its sources: objects, archives, the binaries make leaves in src/, and
# test scratch. Packing the rest keeps the exact files that were compiled.
VALKEY_TREE_EXCLUDE = (
    "*.o", "*.a", "*.d", "*.so", "*.so.*", "*.lo", "*.la", "*.gcda", "*.gcno", ".git", "tests/tmp",
    "src/valkey-server", "src/valkey-cli", "src/valkey-benchmark", "src/valkey-check-aof", "src/valkey-check-rdb",
    "src/valkey-sentinel", "src/valkey-unit-tests", "src/valkey-unit-gtests", "src/redis-*",
)


class _HashingGzip(io.RawIOBase):
    """Write-only stream that hashes the bytes it is given and gzips them into out."""

    def __init__(self, out, level: int = 6):
        super().__init__()
        self.sha = hashlib.sha256()
        self.n = 0
        # No file name and a zero mtime in the gzip header, so equal input gives equal output.
        self.gz = gzip.GzipFile(filename="", mode="wb", compresslevel=level, fileobj=out, mtime=0)

    def writable(self) -> bool:
        return True

    def write(self, b) -> int:
        self.sha.update(b)
        self.n += len(b)
        self.gz.write(b)
        return len(b)

    def close(self) -> None:
        if not self.closed:
            self.gz.close()
        super().close()


def sha256_file(path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_entry(path) -> str:
    """A file's sha256, or for a symlink the sha256 of its target string."""
    if os.path.islink(path):
        return hashlib.sha256(("symlink:" + os.readlink(path)).encode()).hexdigest()
    return sha256_file(path)


def gunzip_sha256(path) -> str:
    """sha256 of a gzip file's uncompressed bytes."""
    h = hashlib.sha256()
    with gzip.open(path, "rb") as f:
        for chunk in iter(lambda: f.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def gzip_file(src, dest, level: int = 6) -> dict:
    """gzip src into dest. Returns the sha256 and size of src, and the size of dest."""
    with open(dest, "wb") as out:
        w = _HashingGzip(out, level)
        with open(src, "rb") as f:
            for chunk in iter(lambda: f.read(CHUNK), b""):
                w.write(chunk)
        w.close()
    return {"sha256": w.sha.hexdigest(), "bytes": w.n, "gz_bytes": os.path.getsize(dest)}


def gzip_bytes(data: bytes, dest, level: int = 6) -> dict:
    with open(dest, "wb") as out:
        w = _HashingGzip(out, level)
        w.write(data)
        w.close()
    return {"sha256": w.sha.hexdigest(), "bytes": w.n, "gz_bytes": os.path.getsize(dest)}


def tar_gz(entries: Iterable[tuple], dest, level: int = 6) -> dict:
    """Write a deterministic tar.gz. entries: (name, path, data) with either a filesystem path or bytes. Returns the
    sha256 and size of the uncompressed tar, the size of dest and the number of entries."""
    items = sorted(entries, key=lambda e: e[0])
    with open(dest, "wb") as out:
        w = _HashingGzip(out, level)
        with tarfile.open(fileobj=w, mode="w|", format=tarfile.GNU_FORMAT) as tar:
            for name, path, data in items:
                ti = tarfile.TarInfo(name)
                ti.mtime, ti.uid, ti.gid, ti.uname, ti.gname = 0, 0, 0, "", ""
                if data is not None:
                    ti.size, ti.mode = len(data), 0o644
                    tar.addfile(ti, io.BytesIO(data))
                    continue
                st = os.lstat(path)
                if stat.S_ISLNK(st.st_mode):
                    ti.type, ti.linkname, ti.mode = tarfile.SYMTYPE, os.readlink(path), 0o777
                    tar.addfile(ti)
                    continue
                ti.size, ti.mode = st.st_size, (0o755 if st.st_mode & 0o111 else 0o644)
                with open(path, "rb") as f:
                    tar.addfile(ti, f)
        w.close()
    return {"sha256": w.sha.hexdigest(), "bytes": w.n, "gz_bytes": os.path.getsize(dest), "files": len(items)}


def _excluded(rel: str, base: str, patterns: Iterable[str]) -> bool:
    return any(fnmatch.fnmatchcase(rel, p) or fnmatch.fnmatchcase(base, p) for p in patterns)


def walk(root, exclude: Iterable[str] = (), max_bytes: Optional[int] = None) -> tuple:
    """Regular files and symlinks under root, as sorted (relative posix path, path) pairs, without names matching
    exclude (glob against the relative path or the base name) and without regular files over max_bytes.
    Returns (entries, skipped), skipped being the relative paths of the oversized files."""
    root = Path(root)
    exclude = tuple(exclude)
    out, skipped = [], []
    for dirpath, dirnames, filenames in os.walk(root):
        here = Path(dirpath)
        rel_dir = here.relative_to(root)
        keep = []
        for d in sorted(dirnames):
            rel = (rel_dir / d).as_posix()
            if _excluded(rel, d, exclude):
                continue
            if (here / d).is_symlink():       # not descended into: kept as a link
                out.append((rel, here / d))
                continue
            keep.append(d)
        dirnames[:] = keep
        for f in sorted(filenames):
            rel = (rel_dir / f).as_posix()
            full = here / f
            if _excluded(rel, f, exclude):
                continue
            if max_bytes is not None and not full.is_symlink() and full.stat().st_size > max_bytes:
                skipped.append(rel)
                continue
            out.append((rel, full))
    return sorted(out), sorted(skipped)
