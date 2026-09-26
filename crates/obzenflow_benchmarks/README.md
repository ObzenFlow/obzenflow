# ObzenFlow benchmarks

This unpublished crate measures framework performance across the workspace.
It depends on the implementation layers so benchmarks can isolate runtime,
journal, and reporting costs.

## Run

```bash
# All suites.
cargo bench -p obzenflow_benchmarks

# One target.
cargo bench -p obzenflow_benchmarks --bench pipeline_throughput

# Criterion options.
cargo bench -p obzenflow_benchmarks --bench pipeline_throughput -- --help

# Prometheus rendering and publication costs.
cargo bench -p obzenflow_benchmarks --bench pipeline_execution -- metrics_reporting

# Causal validation, byte-budget accounting, and authored-event copies.
cargo bench -p obzenflow_benchmarks --bench pipeline_execution -- causal_record_costs
```

Each target uses Criterion with `async_tokio` and HTML reports.
Reports are written under `target/criterion/<group>/report/index.html` unless
Cargo's target directory is overridden. Sample sizes, warmups, and measurement
times are set in the benchmark sources.

## Suites

| Target | Measures |
| --- | --- |
| `per_event_latency_*` | Median source-to-sink latency at fixed pipeline depths, including disk and memory variants at 100 stages. |
| `pipeline_throughput` | Sustained event rate at 1, 3, 5, and 10 stages, plus time per event and relative throughput. |
| `pipeline_execution` | Batch completion time, metrics reporting, and record costs at several causal frontier widths. |
| `idle_cpu_usage` | CPU use while a running pipeline has no input. |
| `waiting_for_gun_cpu_usage` | CPU use while a materialised pipeline waits for manual start. |
| `tokio_worker_3_stage_experiment` | Worker-thread counts with a three-stage workload and five-stage control. |

The disk-backed 100-stage latency target accepts:

- `OBZENFLOW_BENCH_100_STAGE_WARMUP_EVENTS`
- `OBZENFLOW_BENCH_100_STAGE_TEST_EVENTS`
- `OBZENFLOW_BENCH_100_STAGE_TIMEOUT_SECS`

## Interpreting results

Latency suites timestamp inputs and measure their arrival at the sink.
Throughput suites measure sustained processing after warmup. Batch timings
measure total completion time; these answer different performance questions.

The metrics group measures rendering for 100 stages and application/infrastructure
publication with zero or four concurrent scrapers. Publication timings include
cloning, allocation, and retirement, but exclude worker startup and joining.
These are local cost measurements; `monitoring_injection_test` separately
checks flow settlement and journal totals during scrapes.

The causal record group measures commitment extraction, byte-budget accounting,
and authored-event conversion with 1, 32, and 1,024 witnesses. Its records come
from successful journal appends and include a local predecessor. Fixture
construction stays outside the measured loops. Use `--profile test` to measure
the unoptimized paths exercised by CI, and compare baselines built with the
same profile.

Most suites call `init_tracing()`, which installs benchmark defaults with
reporting disabled, attempts to raise the Unix open-file limit, and configures
tracing from `RUST_LOG` (default `warn`).

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md`, and `TRADEMARKS.md`.
