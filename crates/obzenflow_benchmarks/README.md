# ObzenFlow Benchmarks

This crate is an internal implementation detail of the ObzenFlow project. It exists to benchmark end-to-end performance across the workspace and is not intended as a public API crate.

From an onion-architecture perspective, this crate sits on the outside of the onion:

- It depends on the other workspace crates (core/runtime/adapters/infra) to run realistic integration workloads.
- Nothing else in the project depends on this crate (it is a standalone/leaf crate), so benchmarking-only dependencies do not “leak” into production code.

## Benchmark Harness (Criterion)

Benchmarks in this crate use `criterion` (with `async_tokio`) rather than the default `libtest` harness:

- Each file in `benches/` is a standalone Criterion benchmark binary (`harness = false` in `Cargo.toml`).
- HTML reports are enabled via the `criterion/html_reports` feature.
- Benchmark configuration (sample sizes, measurement times, warmups) is defined in the benchmark sources themselves.

### Running Benchmarks

Run all benchmarks:

```bash
cargo bench -p obzenflow_benchmarks
```

Run a single benchmark target:

```bash
cargo bench -p obzenflow_benchmarks --bench pipeline_throughput
```

Pass Criterion CLI flags (after `--`):

```bash
cargo bench -p obzenflow_benchmarks --bench pipeline_throughput -- --help
```

Results are written under Cargo’s target directory, typically `target/criterion/`. With HTML reports enabled, open:

- `target/criterion/<group>/report/index.html`

This crate also calls `obzenflow_benchmarks::init_tracing()` inside most benchmarks to keep runs predictable:

- Installs a benchmark bootstrap config with metrics disabled by default.
- Best-effort bumps the process `nofile` limit on Unix to help deep disk-journal pipelines on platforms with low defaults.
- Configures `tracing_subscriber` using `RUST_LOG` (defaulting to `warn` to minimize overhead).

## Benchmark Suites

The benchmarks are organized by what they’re trying to measure.

### Latency (Per-Event)

These benchmarks measure *per-event* end-to-end latency by embedding an “emit timestamp” in the event payload at the source and computing the latency at the sink. Most report the median latency over a fixed number of test events (after a small warmup).

- `per_event_latency_1_stage`: Isolated 1-stage pipeline median latency.
- `per_event_latency_2_stage`, `per_event_latency_3_stage`, `per_event_latency_4_stage`, `per_event_latency_5_stage`, `per_event_latency_20_stage`: Isolated fixed-depth pipelines to avoid cross-benchmark ordering/warmup effects.
- `per_event_latency_100_stage`: Deep 100-stage pipeline on disk journals. Defaults are reduced because this is heavier; adjust with:
  - `OBZENFLOW_BENCH_100_STAGE_WARMUP_EVENTS`
  - `OBZENFLOW_BENCH_100_STAGE_TEST_EVENTS`
  - `OBZENFLOW_BENCH_100_STAGE_TIMEOUT_SECS`
- `per_event_latency_100_stage_memory`: Same 100-stage benchmark but using in-memory journals to help isolate disk I/O overhead.

### Throughput (Sustained Rate)

These benchmarks measure sustained processing rate (events/sec) after an explicit warmup phase.

- `pipeline_throughput`: Measures throughput across multiple pipeline depths (currently 1/3/5/10 stages). Also includes:
  - `time_per_event`: Converts measured throughput into an “inverse” representation.
  - `relative_throughput`: Compares each depth to a single-stage baseline.

### Integration (Total Execution Time)

These benchmarks measure overall “batch completion” time—how long it takes to process a fixed number of events through a full pipeline.

- `pipeline_execution`: Measures total execution time (and amortized time per event) for several pipeline depths (currently 1/3/5/10 stages).

### Runtime Characteristics (CPU / Scheduler Behavior)

These benchmarks are less about event throughput/latency and more about runtime behavior under specific conditions.

- `idle_cpu_usage`: Measures CPU usage while a pipeline is running but idle (no events flowing). Includes a “by depth” variant.
- `waiting_for_gun_cpu_usage`: Measures CPU usage while a pipeline is materialized but not started (targets “pure wait” busy-spin scenarios such as `WaitingForGun`). The benchmark installs a manual-start bootstrap config for that run.
- `tokio_worker_3_stage_experiment`: Experiments with Tokio worker-thread counts for a 3-stage pipeline to understand scheduler effects (plus a 5-stage control).

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md`, and `TRADEMARKS.md`.
