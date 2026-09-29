# ObzenFlow benchmarks

This unpublished crate measures framework performance across the workspace.
Use this README to choose a suite, run it and compare equivalent operations.
Detailed fixture matrices and investigation history live in the
[FLOWIP evidence](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-benchmark-measurement-contracts.md).

After the 145h scope reduction, causal fixtures retain the same input journals
and clock widths but no longer create witness lists. Hot-path dimensions now
name `advanced_inputs` separately from retained `clock` width. The former
`commitment` extraction case is retired; `journal_clock_restore` measures the
remaining append/recovery operation. Historical witness and proof-wrapper
results are not current acceptance baselines. The paired
[clock simplification comparison](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-clock-simplification-2026-09-27.md)
retains matching clock, payload and completed-work dimensions.

## Suites

| Target | Purpose | Required feature |
| --- | --- | --- |
| `per_event_latency_*` | Source-to-sink latency experiments at fixed pipeline depths, including disk and memory variants at 100 stages. | Default |
| `pipeline_throughput` | Throughput and time-per-event experiments at 1, 3, 5 and 10 stages. | Default |
| `pipeline_execution` | Batch completion, metrics rendering/publication and causal record costs. | Default |
| `journal_components` | 33 cases for causal operations, full decoding and ordinary reader dispatch. | `components` |
| `journal_hot_path` | 75 cases for ordinary record accounting, validation, reconstruction, dispatch and append attribution. | `journal-benchmarks` |
| `idle_cpu_usage` | Idle-runtime experiment. | Default |
| `waiting_for_gun_cpu_usage` | Manual-start waiting experiment. | Default |
| `tokio_worker_3_stage_experiment` | Worker-thread counts with a three-stage workload and five-stage control. | Default |

The legacy CPU, relative-throughput and three-stage latency measurements have
known measurement-validity gaps recorded in
[FLOWIP-143a](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/open/FLOWIP-143a-benchmark-measurement-integrity.md).
Their names alone do not establish valid CPU percentages or latency samples.

## Run

Run commands from the implementation repository root. Default suites run with
`cargo bench -p obzenflow_benchmarks`; select a target and filter for routine work:

```sh
cargo bench -p obzenflow_benchmarks --bench pipeline_execution -- metrics_reporting
cargo bench -p obzenflow_benchmarks --bench pipeline_execution -- causal_record_costs
cargo bench -p obzenflow_benchmarks --bench pipeline_throughput -- --help
```

The component suites lazily construct only selected fixtures. Check completed-work
assertions once before collecting a new baseline; `--test` collects no timings:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/journal-check.json cargo bench --locked --profile test -p obzenflow_benchmarks --features journal-benchmarks --bench journal_hot_path -- --test
```

Capture a reference with a new baseline name, then use that name for a candidate
comparison. This example selects one operation; omit the filter to capture the
whole suite:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo bench --locked --profile test -p obzenflow_benchmarks --features components --bench journal_components -- 'disk_components/reader_next/' --save-baseline reader-reference-test
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo bench --locked --profile test -p obzenflow_benchmarks --features components --bench journal_components -- 'disk_components/reader_next/' --baseline reader-reference-test
```

| Target | Work census environment variable | Capture script `--suite` |
| --- | --- | --- |
| `journal_components` | None | `components` |
| `journal_hot_path` | `OBZENFLOW_WORK_CENSUS=target/<name>-work.json` | `hot-path` |

For example, capture complete append timings and their work census:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/append-candidate-work.json cargo bench --locked --profile test -p obzenflow_benchmarks --features journal-benchmarks --bench journal_hot_path -- journal_append_cost --noplot --save-baseline append-candidate
```

The 100-stage disk latency target also accepts
`OBZENFLOW_BENCH_100_STAGE_WARMUP_EVENTS`, `OBZENFLOW_BENCH_100_STAGE_TEST_EVENTS`
and `OBZENFLOW_BENCH_100_STAGE_TIMEOUT_SECS`.

## Measurement boundaries

The component suites measure elapsed time for a complete named operation.
Throughput counts records for that operation, including discarded rows
when scanning. It does not represent full application throughput.

| Operation | Timed boundary and important exclusion |
| --- | --- |
| Causal/accounting primitives | The actual production operation on admitted records; fixture creation excluded. Validation, accounting and decoding timings can overlap and must not be added together. |
| Ordinary frame decoding | Production frame verification and reconstruction; preloaded frame cases exclude primary-file I/O. Warm/cold definition cases distinguish auxiliary carrier reads. |
| Actual readers | Physical reading, dispatch and admission. Opening is excluded from reader-iteration controls. |
| Append attribution | Encoding, preencoded writes and complete appends are distinct controls with overlapping work. They are not additive phases. |

Fresh cursors, frontiers, FSMs or destination journals isolate iterations.
The component harnesses check counts, identity/order, coverage and required
publications. Incomplete operations fail the benchmark instead of becoming a
zero-duration sample. Async operations have a 30-second invalid-sample deadline.

`journal-benchmarks` enables development-only production counters and a
process-wide allocation meter. Use identical instrumentation for comparisons.

Encoded-byte counters do not measure physical device traffic. Allocation requests
and incremental live heap do not measure RSS, page cache or a hard memory bound.
Files are OS-cache-warm; cold definitions mean fresh metadata state. Runtime limits are two async/two blocking workers. Compare matching limits.

The metrics-rendering group includes cloning, allocation and retirement but
excludes scraper startup/joining. Most older suites use `init_tracing()` with
reporting disabled and `RUST_LOG=warn`; the component suites install no subscriber.

## Preserve and compare evidence

The component defaults are 20 samples, 300 ms warm-up and a one-second requested
measurement window. Criterion extends slow cases. Inspect raw samples and
uncertainty; no automatic numerical regression threshold is established here.
Run comparative benchmarks without concurrent builds or tests.

`--profile test` measures unoptimised CI-profile code. The default bench profile
is a separate optimised series. Match profile, features, instrumentation,
fixture dimensions, runtime limits and measurement contract. Never overwrite
an accepted reference with a candidate. Saved results can be compared without
remeasuring using `--baseline <reference> --load-baseline <candidate>`; omit work
census output for that analysis-only invocation.

Reports and raw samples live under `target/criterion/` unless Cargo's target
directory is overridden. The existing capture script preserves estimates,
samples, work observations, source hashes and host/toolchain identity:

```sh
python3 crates/obzenflow_benchmarks/scripts/capture_component_baseline.py --help
```

Supply the baseline name, output path, `--profile`, exact successful `--command`
and the suite from the table above; instrumented suites also require `--work-json`.
It requires the complete suite and at least 20 samples per case. It performs no
new measurements. New captures hash this README; historical evidence retains the
original document paths and hashes.

Detailed contracts and historical results are collected in the
[145h benchmark evidence](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-benchmark-measurement-contracts.md).
Part 2 deletes selective reporting and parent reconciliation. Their benchmarks
are removed. Ordinary append and decoding fixtures remain useful; compare only
matching surviving operations, with completed append and encoded size as primary
evidence. Preparation and preencoded writes diagnose costs and overlap with append.

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md` and `TRADEMARKS.md`.
