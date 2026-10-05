# Replica read capacity

At X writes/s and Y reads/s on the primary, how many reads/s can the replica serve before it falls out of
sync?

X, Y and the cores per server are the inputs. Connection counts, pipeline depth and io-threads are knobs for
reaching them: they are recorded with each rep, but they are not results.

## Setup

- Four hosts: primary, replica, one generator host for the primary's writers and readers, and a second for
  the replica's readers, which need a whole host at 16 cores (79 generator cores top out around 22M reads/s).
- Every generator-to-server path must measure 0.15 ms or less. `repl-calibrate` and `repl-run` ping both
  generator paths before the first rep and refuse a slower one (`--max-rtt-ms`). The primary-to-replica path
  is recorded, not gated.
- Equal cores per server. A build whose main thread shares a core with its first IO thread (the staged loop)
  runs one more IO thread on the same cores (`--share-bins`).
- Pinned servers with IRQs off their cores, 100k keys, 64-byte values, closed-loop generators.
- Builds interleaved and rotated per rep, 6 s measurement window.

### What the hosts need

Under the ssh user's home on each host:

- primary and replica: every build at `~/offbench/builds/<full sha>/src/valkey-server`,
  `~/base/src/valkey-cli`, and pintune applied (irqbalance stopped, loopback RPS steered);
- generator hosts: cachecannon built at `~/offbench-src/vendor/cachecannon/target/release/cachecannon`;
- every host: `~/offbench-src` with the same offbench code as the checkout you run from, since the host side
  of each rep is this package's `repl-agent`.

`repl-calibrate` and `repl-run` check all of this before the first rep and list what is missing. Step 1 of the
procedure syncs the code; nothing here builds valkey or cachecannon.

## Procedure

Run from the offbench checkout. Set the rig's ssh key and known-hosts file, the hosts and the builds:

```
export OFFBENCH_KEY=../rig/valkey-bench-09092304.pem OFFBENCH_KNOWN_HOSTS=../rig/known_hosts
H="--primary metal-2 --replica metal-7 --gen metal-8 --reader-gen metal-4"
B="stock=<sha>,rr=<sha>,custom=<sha>|keyspace-shard-bits 10"
```

Lease the four hosts for the whole procedure, so nobody takes them between commands, and release them at
the end. Each command also leases its hosts and leaves the ones you already hold alone. A command started
from another shell or as a detached job (tmux, nohup) counts as yours only with `HOSTS_LEASE` set to the
token acquire prints; without it, it can be refused as someone else's.

```
python3 ~/.kiro/crew/skills/hosts/hosts.py acquire metal-2,metal-7,metal-8,metal-4 --label "replica read capacity"
...
python3 ~/.kiro/crew/skills/hosts/hosts.py release metal-2,metal-7,metal-8,metal-4
```

1. Sync this checkout's code to the hosts (about a minute; rsync only, it starts nothing):

   ```
   python3 -m offbench deploy --source-only --hosts metal-2,metal-7,metal-8,metal-4
   ```

2. Calibrate X, Y and each build's primary load (about 12 min at 4 cores). `<C>` is the cores per server:

   ```
   python3 -m offbench repl-calibrate $H --bins "$B" --share-bins custom --iot <C> --out data/repl/<name>-calib.jsonl
   ```

   - Reference: every build at 6 writer connections per core and 25 primary reader connections, all P16,
     3 interleaved reps. X is the lowest median writes/s and Y the lowest median reads/s among the builds,
     so every build's primary can carry both. `--targets X,Y` (k/s) skips this.
   - Search: per build, one-rep probes with no replica readers. Requests in flight are adjusted until the
     primary delivers X and Y within 3%: the total against X + Y, the writer/reader split against X/Y. A
     generator below 10 connections has its pipeline depth halved until it has 10 (or reaches P1), for finer
     steps, and keeps that depth for the rest of the search. At most 12 probes per build.
   - Confirm: 3 interleaved reps per build at its chosen load. The median must be within 5% of X and Y, or
     the build is searched again (3 rounds at most).
   - Output: the records in `<name>-calib.jsonl`, a `.log` beside them, and the calibration in
     `<name>-calib.json`: targets, hosts, builds, each build's `shape` (`w` writer and `p` primary reader
     connections, `wp` and `pp` their pipelines), and under `points` every measurement per build, labelled
     reference, probe or confirm. It exits 1 if a build did not converge, and `repl-run` refuses that
     calibration.

3. Raise the replica's offered read load step by step (about 25 min at 4 cores, 54 reps; add 1600 from 8
   cores up):

   ```
   python3 -m offbench repl-run --calibration data/repl/<name>-calib.json \
       --conns 0,50,100,200,400,800 --pipelines 16 --reps 3 --out data/repl/<name>.jsonl
   ```

   The calibration sets the hosts, builds, cores and each build's primary load, so `--bins`, `--iot`,
   `--share-bins`, `--bin-write-conns` and `--bin-p-conns` are refused alongside it. Every rep is judged
   against its targets.

4. Report:

   ```
   python3 -m offbench repl-capacity data/repl/<name>.jsonl
   ```

## Validity

`repl-run` judges every rep, and the rules are applied again whenever records are loaded. The replica is in
sync when it applied at least 99% of the primary's writes and its replication lag grew by no more than the
larger of 0.5 MB/s and 1% of the replication stream over the window. A rep is invalid if:

- the replica was not in sync;
- the link dropped or resynced;
- a server asserted or closed clients for their output buffers;
- fewer clients connected than requested;
- a generator died, timed out or reported errors;
- a host exceeded its ENA allowance;
- the primary delivered X or Y more than 7% off target.

`repl-capacity` judges each step. It is out of sync if the replica fell behind in any of its reps, since the
replica cannot be relied on at that load, and invalid if fewer than a majority of its reps are valid (harness
errors count as reps). Either way it is reported as invalid, with no number.

Flags on valid steps with replica readers:

- RTT: the replica readers' mean request latency (connections x pipeline / replica read rate) is under twice
  the minimum ping from their generator to the replica, so the round trip is over half of each request and
  the rate understates the server. An idle ping overstates the loaded round trip, so the flag errs toward
  firing.
- GEN: the replica readers' generator used 95% or more of its threads (one per core: the connection count,
  at most 79 on its own host), so the rate is a lower bound.

## Result

`repl-capacity` prints, per core count and build: X and Y as delivered (medians over the sweep's valid reps),
the highest replica read rate over in-sync steps below the first step where the replica fell behind, and what
more offered reads did:

- in sync at every step, reads level off: the rate is the replica's capacity at (X, Y). Levelling off means
  the top step is within 5% of the in-sync step below it, or a later step read less.
- in sync at every step, reads still rising: "at least"; extend the sweep.
- falls out of sync between N and M readers: M fell behind and no step above it was in sync. "At least" if
  the reads were still rising at N.
- unstable: an out-of-sync step sits below an in-sync one. The out-of-sync steps are listed with how many
  reps fell behind, then the in-sync steps above the first of them, which do not raise the rate.
- not reached: the replica readers' generator ran out of cores (GEN) at the top step. "At least".

Under it, the per-step raw table: build (writers/primary readers, with pipelines when not P16), replica reader
connections, valid reps, writes/s, primary reads/s, replica reads/s, reader p99, and a note (flags, how many
reps fell behind, or how many reps were invalid and the first reason).

Example, 4 cores (2026-10-05, `data/repl/capacity-test2-4c.jsonl`):

| build | X writes/s | Y reads/s | replica reads/s in sync | more offered reads |
|---|---:|---:|---|---|
| stock | 771k | 995k | at least 2.31M at 50 readers | falls out of sync between 50 and 100 readers |
| rr | 778k | 1.02M | 6.65M at 100 readers | unstable: out of sync at 200 (1/3 fell behind), 800 (2/3 fell behind), in sync at 400 |
| custom | 802k | 1.00M | 6.85M at 800 readers | in sync at every step; reads level off |

A run on 2026-10-02 (`capacity-test-4c.jsonl`) gave the same outcomes with rates within 2%; there rr fell
behind at 200 and 400 readers.

## repl-run reference

- `--conns` and `--pipelines` are the replica readers'; both are lists, and every combination runs.
- `--write-conns 0` (the default) means the same as `--conns`, so without a calibration or `--bin-write-conns`
  a `--conns 0` step gets no writers.
- `--p-conns` is one count for every build; `--bin-p-conns` or the calibration sets it per build.
  `--p-pipeline` is the primary readers' pipeline (default: `--pipelines`).
- In `--bins`, each `|config value` goes to the replica only.
- `--out` appends, and writes a `.log` beside it.
- One record is one rep. Rates are k/s: `primary.write_kcmd_s` and `primary.read_kcmd_s` are X and Y as
  delivered, `replica.read_kcmd_s` the replica's reads (its total minus the SETs it applied),
  `replica.set_kcmd_s` the writes it applied, `lag_growth_mb_s` its lag growth, `valid` and `invalid` the
  verdict and reasons, `rtt_ms` the ping per path, and `targets` and `calibration` the calibration it ran
  under.
