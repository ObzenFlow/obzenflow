# ObzenFlow benchmarks

This unpublished crate measures framework performance across the workspace.
Use this README to choose a suite, run it and compare equivalent operations.
Detailed fixture matrices and investigation history live in the
[FLOWIP evidence](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-benchmark-measurement-contracts.md).

## Suites

| Target | Purpose | Required feature |
| --- | --- | --- |
| `per_event_latency_*` | Source-to-sink latency experiments at fixed pipeline depths, including disk and memory variants at 100 stages. | Default |
| `pipeline_throughput` | Throughput and time-per-event experiments at 1, 3, 5 and 10 stages. | Default |
| `pipeline_execution` | Batch completion, metrics rendering/publication and causal record costs. | Default |
| `journal_components` | 59 cases for causal operations, full decoding, dispatch, discovery, ready-report handoff and parent admission/publication. | `components` |
| `supervision_selection` | 30 cases for selective discovery, mixed groups, cold definition dependencies, work counters and write costs. | `supervision-benchmarks` |
| `journal_hot_path` | 138 cases for selected-report accounting, validation, reconstruction, dispatch, parent fan-in and append attribution. | `supervision-benchmarks` |
| `supervision_delivery` | 30 cases measuring first-report delivery and completed parent readiness with fixed report volume and varying business traffic. | `supervision-benchmarks` |
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
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_EXPECT_SELECTIVE_READS=1 OBZENFLOW_SUPERVISION_WORK_OUTPUT=target/supervision-selection-check.json cargo bench --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_selection -- --test
```

Capture a reference with a new baseline name, then use that name for a candidate
comparison. This example selects one operation; omit the filter to capture the
whole suite:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo bench --locked --profile test -p obzenflow_benchmarks --features components --bench journal_components -- 'report_discovery/disk/readers1_prefix1024_p256$' --save-baseline discovery-reference-test
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo bench --locked --profile test -p obzenflow_benchmarks --features components --bench journal_components -- 'report_discovery/disk/readers1_prefix1024_p256$' --baseline discovery-reference-test
```

| Target | Work census environment variable | Capture script `--suite` |
| --- | --- | --- |
| `journal_components` | None | `components` |
| `supervision_selection` | `OBZENFLOW_SUPERVISION_WORK_OUTPUT=target/<name>-work.json` | `supervision` |
| `journal_hot_path` | `OBZENFLOW_WORK_CENSUS=target/<name>-work.json` | `hot-path` |
| `supervision_delivery` | `OBZENFLOW_WORK_CENSUS=target/<name>-work.json` | `delivery` |

For example, capture delivery timings and their separate completed-work census:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/delivery-reference-test-work.json cargo bench --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_delivery -- --noplot --save-baseline delivery-reference-test
```

The constrained live-reader cases in `journal_hot_path` require an explicit
blocking-worker setting, which is included in their case names:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_LIVE_BLOCKING_THREADS=2 cargo bench --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench journal_hot_path -- 'supervisor_fan_in/live_per_journal_8/blocking_capacity_2/' --test
```

The 100-stage disk latency target also accepts
`OBZENFLOW_BENCH_100_STAGE_WARMUP_EVENTS`, `OBZENFLOW_BENCH_100_STAGE_TEST_EVENTS`
and `OBZENFLOW_BENCH_100_STAGE_TIMEOUT_SECS`.

## Measurement boundaries

The four component suites measure elapsed time for a complete named operation.
Throughput counts records/reports for that operation, including discarded rows
when scanning. It does not represent full application throughput.

| Operation | Timed boundary and important exclusion |
| --- | --- |
| Causal/accounting primitives | The actual production operation on admitted records; fixture creation excluded. Validation, accounting and decoding timings can overlap and must not be added together. |
| Full/selected frame decoding | Production frame verification and reconstruction; preloaded frame cases exclude primary-file I/O. Warm/cold definition cases distinguish auxiliary carrier reads. |
| Actual readers | Physical reading, dispatch and admission. Opening is excluded from reader-iteration controls and included in discovery cases. |
| Report discovery | Registration through exact report delivery and covered prefix, with a cheap consumer. History construction and reader teardown excluded. |
| Ready-report handoff | Consumption of already-ready reports; discovery and readiness waiting excluded. |
| Parent admission/publication | Admission uses prepared reports and the real FSM; publication measures committed journal output. Neither isolated case establishes combined service capacity. |
| Parent fan-in/delivery completion | Actual readers, parent FSM, required publication and complete coverage. Live variants also include concurrent child appends. |
| First-report delivery | Registration until the first report is returned, before application. Every sample still completes the whole fixture. This is not commit-to-application latency. |
| Append attribution | Encoding, preencoded writes and complete appends are distinct controls with overlapping work. They are not additive phases. |

Fresh cursors, frontiers, FSMs or destination journals isolate iterations.
The component harnesses check counts, identity/order, coverage and required
publications. Incomplete operations fail the benchmark instead of becoming a
zero-duration sample. Async operations have a 30-second invalid-sample deadline.

`supervision-benchmarks` enables development-only production counters and a
process-wide allocation meter. Use identical instrumentation for comparisons.
`OBZENFLOW_EXPECT_SELECTIVE_READS=1` additionally requires zero discarded business
payload decodes, constructions and accounting serialisations in selection fixtures;
positive full-reader controls verify the counters. Live writer work remains in
process-wide counters and must not be attributed entirely to reading.

Encoded-byte counters do not measure physical device traffic. Allocation requests
and incremental live heap do not measure RSS, page cache or a hard memory bound.
Files are OS-cache-warm; cold definitions mean fresh metadata state. Runtime limits
are two async/two blocking workers, except explicitly labelled hot-path live
fixtures, whose default blocking capacity is 512. Compare matching limits.

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
[Part 2](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/open/FLOWIP-145h-part-2-supervision-capacity.md)
owns tactical capacity improvements within the existing tapes and clocks;
[145j](../../../obzenflow-improvement-proposals/content/planning/obzenflow/backlog/open/FLOWIP-145j-message-delivery-for-supervisors.md)
explores future delivery. These benchmarks do not require additional report journals
or establish that live supervision must read zero business bytes.

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md` and `TRADEMARKS.md`.
