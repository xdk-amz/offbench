# Run archive

Every `repl-calibrate` and `repl-run` invocation is archived as one run: a directory that says what ran, on
what, with what, and how to run it again, plus the binaries and code it ran. A run never changes after it ends.

The archive is local. `--archive` or `OFFBENCH_ARCHIVE` names it (default `~/offbench-runs`); `off` turns it off
for one command. It records host names, addresses and instance ids, so it is not for publishing as it is.

## Asking a run questions

```
python3 -m offbench runs list [--scenario S] [--step calibrate|sweep] [--commit C] [--binary SHA] [--cores N]
                              [--tag T] [--status S] [--label L] [--since 2026-10-16]
python3 -m offbench runs show <run id, unique prefix or label> [--json]
python3 -m offbench runs reproduce <run>     # its REPRODUCE.md
```

`runs show` answers, from the manifest:

| question | where it comes from |
|---|---|
| what scenario, defined where | `scenario`: name, version, step, question, the defining doc and its sha256 in the harness snapshot |
| what command and config | `command`: the exact argv, cwd, every parsed argument and the `OFFBENCH_*` environment; `config`: cores, loads per build, steps, reps, window, targets; `constants`: the CPU layout and tolerances in the code |
| what harness | `harness`: offbench commit, branch, dirty flag, remote, and a snapshot of every file that ran (uncommitted changes and untracked files included, keys and host files never) |
| what binary | `builds`: per build the valkey commit and server arguments, then each binary by sha256 with the hosts that ran it, its version line, build id, `.text` size, toolchain (from the binary's `.comment`), shared libraries, make settings, release header, and the source tree it was compiled from |
| what tools, which versions | `tools`: cachecannon (vendored source, upstream commit, Cargo version and lock hash, binary sha256, toolchain), valkey-cli; `rig.roles.*.facts.tools` and `packages`: gcc, glibc, kernel, python, ethtool, util-linux and the rest, as installed |
| on what hosts | `rig.roles`: per role the instance type, id, AZ, placement group, AMI, OS release, kernel, CPU model and count, memory, NICs and drivers, sysctls, THP, open-file limits, the pintune layout and chrony's offset; `rig.rtt_ms`: every path's round trip |
| what came in and out | `inputs.calibration` (the file, its sha256 and the run that made it); `outputs`: record counts and the calibration a calibrate run produced; `result.txt` |
| what is missing | `gaps`: anything not captured, in words; `warnings`: things to check, such as a dirty harness or two hosts running different binaries for one commit |

Each record in `records.jsonl` carries `archive_run`, and `launch`: every server's argv, the commands run on it
after start and where each thread was pinned, and every generator's argv and config (`launch/<sha256>.toml`).

## Layout

```
~/offbench-runs/
  runs/<UTC start>-<command>-<label>-<4 hex>/
    manifest.json  records.jsonl  run.log  calibration.json  launch/  result.txt  REPRODUCE.md
  objects/<aa>/<sha256>.gz     harness snapshots, binaries, build trees: named by the sha256 of what they hold
  index/                       which source tree a binary came from (saves packing it again)
  notes/<run id>.jsonl         notes added later; not part of the sealed run
```

A binary's object name is the sha256 the host reported for it, so the same binary is stored once however many
runs use it. The first run that uses a binary copies it from a host (cachecannon is about 180 MB, 50 MB packed);
`--no-archive-binaries` records sha256s only.

## Sealing and corrections

When a run ends (complete, failed or aborted, including by SIGTERM) its manifest gets every file's sha256 and
the files become read-only. `runs verify` checks the files and the objects they rely on. A run killed outright
stays `running`; it still holds everything it wrote up to then.

A correction is a new run. Link the two with a note, which both runs show:

```
python3 -m offbench runs note <new> --supersedes <old> --text "one reader host was the limit"
```

## Reproducing a run

`REPRODUCE.md` in each run is written for someone who has only the archive: the hosts to launch, the packages
and their versions, how to unpack the exact harness, the hosts file, how to put each archived binary where the
run expects it (or rebuild it from the archived source), pintune, the command with this checkout's paths, and
what to compare. To hand a run to another machine:

```
python3 -m offbench runs bundle <run> run.tar     # the run plus every object it names; unpack anywhere
```

## Runs from before the archive

```
python3 -m offbench runs import data/repl/<name>.jsonl --calibration data/repl/<name>-calib.json \
    --log data/repl/<name>.log --harness-commit <sha> --harness-repo <offbench clone> --evidence "<where the sha comes from>"
```

An imported run keeps its records as they were and lists what was never captured (binaries, host facts, tool
versions) under gaps. With `--harness-repo` its harness snapshot is taken from that commit.

## Not archived yet

Queue runs (`queue`, `run`, `dispatch`) still write only `results.jsonl`. The replica page cites no run id.
