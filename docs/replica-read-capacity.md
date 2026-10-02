# Replica read capacity

At X writes/s and Y reads/s on the primary, how many reads/s can the replica serve before it falls out of
sync?

X, Y and the cores per server are the inputs. Connection counts, pipeline depth and io-threads are knobs for
reaching them: they are recorded with each rep, but they are not results.

## Setup

- Four hosts: primary, replica, one generator host for the primary's writers and readers, and a second
  for the replica's readers, which need a whole host at 16 cores (79 generator cores top out around 22M
  reads/s). `repl-run` measures every path first and refuses one above 0.15 ms (`--max-rtt-ms`).
- Equal cores per server. A build whose main thread shares a core with its first IO thread (the staged
  loop) runs one more IO thread on the same cores (`--share-bins`).
- Pinned servers with IRQs off their cores, 100k keys, 64-byte values, closed-loop generators at P16.
- Builds interleaved and rotated per rep, 3 reps per step, 6 s measurement window.

## Procedure

1. Choose X and Y for each core count. Every build's primary must be able to carry both at once; a
   workable default is the lowest write rate and the lowest read rate among the builds at a reference load
   (6 writer connections per core, 25 primary reader connections).
2. Calibrate each build with no replica readers: set its writer and primary reader connection counts so
   the primary delivers X and Y within 5%. The two interact, so alternate between them. If whole
   connections step past the target, lower that generator's pipeline depth for a finer step.
3. Raise the replica's offered read load step by step (50, 100, 200, 400, 800 reader connections, and
   1600 from 8 cores up) with the primary held at its calibrated counts.

## Validity

The replica is in sync when it applied at least 99% of the primary's writes and its replication lag grew by
no more than the larger of 0.5 MB/s and 1% of the replication stream over the window. A rep is invalid if
the replica was not in sync, the link dropped or resynced, a server asserted, fewer clients connected than
requested, a generator erred or timed out, a host exceeded its ENA allowance, or the primary delivered X or
Y more than 7% off target. A step needs a majority of valid reps; an invalid step is reported as invalid,
with no number.

Flags on valid steps:

- RTT: the round trip is over half of each request's latency, so the rate understates the server.
- GEN: a generator used 95% or more of its cores, so the rate is a lower bound.

## Result

For each core count and build: X and Y as delivered, the highest replica read rate over in-sync steps, and
what more offered reads did:

- stays in sync, reads level off: that rate is the replica's capacity at (X, Y);
- falls out of sync: the boundary lies between that step and the next ("at least" if the rate was still
  rising);
- not reached: a generator ran out of cores first.

| cores | X | Y | stock | rr | custom |
|---:|---:|---:|---|---|---|
| C | writes/s | reads/s | replica reads/s, what more reads did | ... | ... |

Per-step tables (offered load by reader connections) go underneath as raw data.

## Running

```
python3 -m offbench repl-run --primary metal-2 --replica metal-7 --gen metal-8 --reader-gen metal-4 \
    --bins "stock=<sha>,rr=<sha>,custom=<sha>|keyspace-shard-bits 10" --share-bins custom \
    --iot <C> --pipelines 16 --write-pipeline 16 \
    --bin-write-conns stock=<n>,rr=<n>,custom=<n> --p-conns 25 \
    --conns 0,50,100,200,400,800 --reps 3 --out data/repl/<name>.jsonl
```

Status (2026-10-02): `repl-run` takes per-build writer counts but not per-build primary reader counts, so Y
cannot be matched yet, and it does not check delivered X and Y against a target. The calibration loop and
result table live outside this repo (`../sweep4/driver.py`, `../sweep4/capacity.py`). The sweep4 run
matched X only.
