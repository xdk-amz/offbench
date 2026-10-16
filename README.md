# offbench

Benchmark queue, runner, compare and dashboard for the Valkey main-thread-offload
work. Shape borrowed from [valkey-conductress](https://github.com/rainsupreme/valkey-conductress)
(file-per-task FIFO queue, one worker, restart-per-rep, Welch compare, static
JSON dashboard), specialised for A/B across the cell matrix on the c8g.metal-24xl
cachecannon loopback rig. Pure stdlib Python 3.9+.

```
offbench/
  offbench/schema.py     Record = one rep: bin, cell, metrics (+perf), validity gate (main >= 98%)
  offbench/queue.py      Task (cell family, Cartesian expansion, interleaved plan) + Queue dirs
  offbench/cell.py       one rep on the rig: port of rig/cccell.sh, contamination guards, perf stat
  offbench/runner.py     worker loop + Builder (git archive + plain make server, cargo vendored cachecannon)
  offbench/analysis.py   Welch t-test, CI95, acceptance (cand P1 vs stock P16), pipeline invariance, layout hint
  offbench/backfill.py   parse existing cccell/metalcell/ccsweep/batchdiag logs into records
  offbench/export.py     records + queue state -> dash/data/dashboard.json
  offbench/hosts.py      host registry: the shared one the hosts skill keeps (read-only here)
  offbench/lease.py      host leases for repl-run / dispatch / deploy / push via hosts.py claim
  offbench/dispatch.py   split one task across hosts, push a subtask to each host queue
  offbench/deploy.py     rsync source + start the worker in tmux on a host
  offbench/sync.py       pull each host's results/status/queue back; merge (deduped)
  offbench/merge.py      fold a per-host results.jsonl into the shared file, idempotent on record_id
  offbench/repl.py       primary/replica reps across hosts: orchestrator, host agent, validity
  offbench/replcap.py    replica read capacity: calibration search and the result table
  offbench/replview.py   replica read capacity page data from the runs in repl_view.json
  offbench/archive.py    run archive: one sealed directory per repl run (manifest, records, log, objects)
  offbench/hostfacts.py  what a host is and runs (instance, OS, packages, tools, tuning, binaries), for the archive
  offbench/packing.py    deterministic tar/gzip named by sha256, on the orchestrator and the hosts
  offbench/reproduce.py  a run's REPRODUCE.md, from its manifest
  offbench/cli.py        offbench queue|run|backfill|compare|export|publish|serve|hosts|push|sync|status|dispatch|deploy
                         |repl-calibrate|repl-run|repl-capacity|repl-summary|export-repl|runs
  offbench/__main__.py   package entrypoint (python3 -m offbench -> cli.main)
  __main__.py            project-root entrypoint (python3 -m offbench from the parent dir)
  vendor/cachecannon/    owned cachecannon source, pinned + auth baked in; built on the host
  dash/index.html        static dashboard (Chart.js from jsdelivr)
  dash/replica/          replica read capacity page
  bins.json              rig directory name -> commit / branch / role
  repl_view.json         builds and runs the replica read capacity page shows
  hosts.example.json     host registry format, for a hosts.json when there is no shared registry
  known_hosts.example    how to build the known-hosts file the ssh commands check
  data/                  default root: queue/ running/ done/ failed/ results.jsonl status.json hosts/
```

Pure stdlib Python 3.9+ throughout; the only non-Python component is the vendored
Rust cachecannon under `vendor/cachecannon`, which the worker's Builder compiles
on the host on first task.

## Local (this host)

```
cd offbench
python3 -m offbench backfill ../rig/c8g --replace     # rig logs -> data/results.jsonl
python3 -m offbench export                             # -> dash/data/dashboard.json
python3 -m offbench serve --port 8765                  # http://127.0.0.1:8765/
python3 -m offbench compare ccbase cccandidate --session clientcontrol-ab
python3 -m pytest -q tests
```

## Rig (runner lives on the metal host)

`offbench deploy` does the setup: it rsyncs this project root (the `offbench/`
package and its sibling `vendor/cachecannon` source) to `~/offbench-src`, makes
sure a Valkey clone with the fork remote exists at `~/valkey`, and starts the
worker in a tmux session `offbench-worker`. cachecannon is owned in-tree (auth
support baked into the source), so there is no external checkout and no
deploy-time patch; the worker's Builder compiles `vendor/cachecannon` on the
host on first task and caches the binary.

```
OFFBENCH_KEY=../rig/valkey-bench-09092304.pem OFFBENCH_KNOWN_HOSTS=../rig/known_hosts \
    python3 -m offbench deploy --hosts metal-2,metal-3
```

`deploy --source-only` does the rsync alone, with no clone, worker or 12 h lease: that is all the
`repl-*` commands need on a host, and they refuse to start while a host's copy differs from this checkout.

## Hosts and leases

Hosts come from the shared registry the hosts skill keeps (`~/.kiro/crew/hosts/registry.json`;
`hosts.py add` registers one). `OFFBENCH_HOSTS_FILE` or `--hosts-file` points offbench at a local
file instead, as does a `hosts.json` beside this README when there is no shared registry, and then
nothing is leased. `hosts.example.json` shows the format: each host needs a `name`, which `--hosts`
and the `repl-*` role flags take, and an `ssh` target (`user@address`); `instance_id`, `az`,
`remote_root` (default `~/offbench`) and `tags` are optional.

ssh logs in with the private key named by `OFFBENCH_KEY` or `--key` and checks host keys against the
file named by `OFFBENCH_KNOWN_HOSTS` or `--known-hosts`; `known_hosts.example` says how to build it.
The `repl-*` commands need both and refuse a host whose key is not in the file. `deploy`, `dispatch`
and `sync` check against the file when given one, and `push` does not check. `hosts.json`,
`known_hosts` and `*.pem` are gitignored.

The commands that use hosts lease them through `hosts.py claim`, and stop with exit 2 when another
session holds one:

- `repl-run` holds its hosts for its own run and gives back the ones it took when it exits.
- `dispatch`, `deploy` and `push` take 12 h session leases, since the work outlives the command.
- Hosts the caller already holds are left as they are, and a host it does not hold is refused when
  work is already running there.

A driver that calls repl-run several times should hold the hosts across the gaps between runs:
`hosts.py run --hosts metal-2,metal-7,metal-8 --label "..." -- python3 driver.py`.

To bring the worker up by hand instead (source already on the host):

```
cd ~/offbench-src && python3 -m offbench --root ~/offbench run \
    --repo ~/valkey \
    --cachecannon ~/offbench-src/vendor/cachecannon/target/release/cachecannon \
    --cli ~/base/src/valkey-cli
```

`--cachecannon` is optional: omit it and the Builder builds and invokes the
vendored copy itself.

Queue from anywhere (bins are `name=commit@branch`, `:stock` marks side A; a bare
name uses `bins.json` or the legacy `~/<name>/src/valkey-server` on the host):

```
python3 -m offbench queue add --bins base:stock cc=a8f50bd2c@main-thread-offload-client-control \
    --workloads get,set,mixed --pipelines 1,16 --conns 2400,7000 --reps 3 --perf \
    --note "ClientControl A/B" --emit /tmp/t.json
OFFBENCH_KEY=../rig/valkey-bench-09092304.pem OFFBENCH_KNOWN_HOSTS=../rig/known_hosts python3 -m offbench push /tmp/t.json --host ec2-user@<address>
...
python3 -m offbench sync --all && python3 -m offbench export   # pull results + status, refresh dashboard
```

Across several hosts, `offbench dispatch <task.json> --hosts metal-2,metal-3`
partitions one task so no cell is split across hosts and pushes a subtask to
each; `offbench status --all` shows per-host pending/running/done, and
`offbench sync --all` merges every host's results into the shared
`data/results.jsonl` (deduped on record id, so re-syncing is a no-op).

The runner writes `status.json` only at task boundaries and never touches the host
during a measurement window. Every rep starts and ends with zero `valkey-server` /
`cachecannon` processes (matched by full command line) or the task fails.

## Validity

For queue runs, a rep is stored always, but counts toward a mean only when `main_active >= 98%`,
`asserts == 0`, clients reached the requested count and the generator reported no
timeouts. `compare` prints no delta for a cell with fewer than two valid reps on a
side; the dashboard hatches it. This encodes the saturation rule so nobody has to
remember it. Primary/replica runs (`repl-*`) have their own rules, in the scenario doc below.

## Scenarios

- [Replica read capacity](docs/replica-read-capacity.md): at X writes/s and Y reads/s on the primary, how
  many reads/s the replica serves before it falls out of sync (`repl-calibrate`, `repl-run`, `repl-capacity`).

## Run archive

Every `repl-calibrate` and `repl-run` is archived under `~/offbench-runs` (or `OFFBENCH_ARCHIVE`) as a sealed run:
scenario, exact command and config, harness snapshot, every binary by sha256 with its build settings and source,
tool versions, host facts, records, log, result and a `REPRODUCE.md`. `python3 -m offbench runs list` and
`runs show <run>` answer what ran; [docs/run-archive.md](docs/run-archive.md) has the rest.
