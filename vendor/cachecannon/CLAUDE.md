# cachecannon

A load generator. That framing is the point of most of what follows: when a
number looks bad, the first question is whether this process produced it.

## Before trusting a measurement

**Check the generator is not the bottleneck.** A load generator that cannot keep
up does not error — it reports latency, and time spent waiting to read a
response that already arrived is indistinguishable from server time in the
benchmark's own numbers. `docs/guide.md` → "Is the generator the bottleneck?"
has the procedure; the short version is to compare client-side
`tcp_packet_latency` (socket-readable → userspace-read) against reported
latency, and to treat per-worker CPU near 1.00 core as disqualifying.

Since #151 the generator's own runtime counters reach `/metrics` and the parquet
snapshot: `ringline/pool{op="recv_parked"}`, `ringline/bytes{op="fallback_received"}`,
`ringline/ring{op="sqe_submit_failures"}`. Non-zero means the client is the
story.

**A metric pinned at its ceiling cannot show an improvement.** Per-worker CPU
reading 1.00 in both arms of an A/B means saturated, not unchanged — the gain,
if any, shows up as more work done per unit of the pinned resource. Pick a
metric the change can move.

## Two regimes, not one setting

`idle_sleep` (`src/worker.rs`) returns `IDLE_SLEEP_MIN` *unchanged* when
`rate == 0`, and `rate` is 0 whenever there is no ratelimiter:

```rust
let ratelimiter = if initial_rate > 0 || config.workload.saturation_search.is_some() {
```

- **Rate-limited** (`workload.rate_limit`, *or* `[workload.saturation_search]` —
  either creates a ratelimiter): the sleep scales as
  `connections / (IDLE_WAKEUPS_PER_TOKEN * rate)`.
- **Closed-loop** (neither key): the sleep is a constant, and the fire loop
  always has budget so the idle branch is rarely reached at all.

These are different code paths. Do not reason about one from the other — that
error was made twice, in both directions, during the effort recorded in
`docs/journal/2026-09-18-generator-saturation.md`. Classifying a spec by
grepping `rate_limit` alone misses the saturation-search case.

Known open issue: `IDLE_WAKEUPS_PER_TOKEN = 64` means the fleet wakes 64x per
token granted by construction — millions of wakeups/s/worker at four-figure
connection counts. See the journal entry.

## ringline

**Per-worker pools default to 256 and are sized independently of the workload.**
Three matter: `standalone_task_capacity` and `timer_slots` are scaled by
connections-per-worker in `src/runner.rs` (one was silently capping runs at 2040
connections, the other panicking workers at 4096); `recv_buffer.ring_size` is
deliberately left at ringline's default and exposed as `general.recv_ring_size`
instead. Adding a fourth pool user means checking whether it needs the same
treatment.

**Recv buffers are not held per connection.** A buffer is held only between a
completion and the client draining it, so ring depth divided by connection count
is not a meaningful ratio. Measured: 10,000 connections, 56 KiB responses, four
buffers per response against a 256-buffer ring — `parks=0` on every worker.

**Linking ringline registers its metrics globally** via metriken, whether or not
this crate mentions them. They arrive as `ShardedCounterGroup`, which surfaces as
`Value::CounterGroup` — a variant both exposition sites once dropped through a
catch-all. Metric names from foreign crates are not necessarily legal Prometheus
names (`ringline/connections/active`), which is why `src/admin/mod.rs` sanitizes
on the way out and a test scans the rendered body.

## Conventions

- `docs/journal/` is the narrative layer: one entry per non-trivial effort,
  `YYYY-MM-DD-slug.md`, every claim grounded in a real SHA, path or measured
  number. **Dead ends and refuted hypotheses are first-class entries** — record
  the mechanism and the condition to reopen. See its README.
- Code changes carry a CHANGELOG entry under `[Unreleased]`; docs-only commits
  do not.
- Released CHANGELOG sections are not rewritten. A correction to a shipped
  change goes under `[Unreleased]` describing both.
