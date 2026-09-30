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
| `validation_boundaries` | Archive export/admission/audit, 10k replay comparison, metrics data/error refresh and actual Studio stream settlement. | `validation-benchmarks` |
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
Core owns causal/record probes; Infra owns frame, carrier and dispatch probes.
The benchmark crate's `src/support` owns session coordination, aggregation,
allocator activation and shared fixtures. Benchmark executables explicitly
install their allocator. Ordinary builds do not enable the probes.

Encoded-byte counters do not measure physical device traffic. Allocation requests
and incremental live heap do not measure RSS, page cache or a hard memory bound.
Files are OS-cache-warm; cold definitions mean fresh metadata state. Runtime limits are two async/two blocking workers. Compare matching limits.

The metrics-rendering group includes cloning, allocation and retirement but
excludes scraper startup/joining. Most older suites use `init_tracing()` with
reporting disabled and `RUST_LOG=warn`; the component suites install no subscriber.

## Preserve and compare evidence

The component defaults are 20 samples, 300 ms warm-up and a one-second requested
measurement window. Criterion extends slow cases. Inspect raw samples and
uncertainty. The required selected gate has its own explicit policy below.
Run comparative benchmarks without concurrent builds or tests.

`cargo xtask test --lane performance` builds the pinned reference revision and
the current checkout before measuring either one. For each selected case it runs
the reference, candidate and unchanged reference consecutively. The owner checks
exact case/workload coverage, complete raw samples and confidence intervals using
`.config/performance.toml`; every artifact is retained under `target/test-runs`.
Reference and candidate builds use separate target directories. Each executable's
hash and compile-time source directory are checked: Cargo cannot silently reuse
a reference artifact as the candidate. The content-identified reference cache
under `target/validation-reference` is verified before reuse; candidate builds use
`target/validation-candidate`.
A noisy, drifting or incomparable result remains incomplete. It never retries
for acceptance, relaxes a threshold or replaces the reference automatically.
The selected policy concerns median complete-operation time, not tail latency
or a production capacity promise. The six new validation operations use the same
identified measurement driver against the preserved production reference and the
candidate. `measurement-driver.json` lists the added benchmark files and thin
test-support Studio endpoint adapter; production implementation files are not
replaced. Fixture creation and output checking stay outside the measured interval.
Archive admission includes complete ordinary record reads, and the 10k comparison
uses real live/replayed archives without timing their application setup.

The same lane qualifies its decision with benchmark-only slow-reader and
missing-reader-output controls. The first must produce a measured regression;
the second must fail the actual output-completeness oracle. Normal measurements
clear `OBZENFLOW_BENCH_CONTROL`, and the controls' separate artifacts cannot stand
in for candidate evidence.

Before timing, both implementations must pass all 75 hot-path workload oracles
with identical case identities and input dimensions. Timing acceptance currently
covers four hot-path cases and six validation operations. Single-reader dispatch,
mixed-group and execution-fact-group append remain required correctness workloads and available
Criterion diagnostics. Their timing trials failed precision qualification; this
policy makes no performance acceptance claim for them. The rejection controls
use the qualified eight-reader workload. The historical incomplete
runs remain incomplete. Numeric regression, reference-drift and precision limits
were not relaxed to select the required gate.

### Supervision and observer capacity experiments

```sh
cargo bench --locked -p obzenflow_benchmarks --features capacity-benchmarks --bench supervision_capacity -- --test
cargo bench --locked -p obzenflow_benchmarks --features capacity-benchmarks --bench projection_capacity -- --test
OBZENFLOW_WORK_CENSUS=target/capacity-work.json cargo bench --locked -p obzenflow_benchmarks --features capacity-benchmarks --bench supervision_capacity -- --save-baseline capacity-baseline --noplot
```

The supervision target has 72 lifecycle cases plus eight publication-pressure
cases. Actual retained child results feed the production parent FSM, with
2/3/8 children, 32-child scaling, explicitly labelled 50/75/100-child stress,
and independent incoming/retained clock widths. Each child contributes its
initialisation, completion or failure once. Child-result retention, parent
application and physical terminal publication are separate boundaries.

Publication-pressure cases hold the first real ingress-refusal append behind
one or eight accepted host commands. The parent's existing admission pools,
reserved stop publications, FSM actions and retained receipts remain in use.
They check graceful stop, cancellation, child failure, rejection of a ninth
unadmitted host command when admission closes, and settlement after an accepted
command's receipt waiter is dropped. All committed facts, host order and child
causes are verified after recovery. Child command execution and the supervisor's
full dispatch loop are outside this isolated boundary.

The projection target has 18 finite-burst/window cases: 1/3/8 data journals,
32-journal scaling and 50/75/100-journal stress; offered rates of 100/1,000/10,000
records per second; zero/one/three Studio connections; metrics off/on; slow
client pause/resume; 256/8,192-byte payloads; ordinary/mixed groups and uneven
traffic. Each data journal also has an error journal, and there is one system
journal. Inputs record every actual reader kind and count. Ordinary readers,
Studio's HTTP endpoint/projection and metrics' actual 25-ms tail-refresh tasks
run concurrently. Metrics export publication and network transport are excluded.
Every case checks original record order, Studio checkpoints/completions and
final metrics accounting after arrivals stop. Missed arrival slots, actual
commit rates and post-arrival drain are retained; a slow producer is not evidence
of consumer capacity. A missing-reader-output control must fail the same oracle:

```sh
OBZENFLOW_CAPACITY_CONTROL=missing-reader-output cargo bench --locked -p obzenflow_benchmarks --features capacity-benchmarks --bench projection_capacity -- 'burst/baseline_journals_1$' --test
```

Detailed first-operation censuses record signed append-acknowledgement-to-apply
latencies, position backlog and Studio pending-frame/string retention. Physical
writes can become readable before the append future returns; these negative
latencies are retained rather than called physical-commit latency. Ordinary
timing runs omit Studio's bounded diagnostic probe; an otherwise identical
probe-enabled case measures its overhead. Reports also retain CPU time, requested
Rust heap and whole-process lifetime peak RSS, with the current file-descriptor
limit. They distinguish fixture/client retention and count shared metrics record
arrays once, including old consumer-held snapshots. RSS is not a per-case peak,
requested heap excludes allocator overhead, and full role attribution remains
open. Stress cases may expose resource exhaustion; they are not supported loads.
Case censuses are saved incrementally so a later failure cannot erase earlier
observations or mark the invocation complete.

B7 remains open. These are finite experiments, without a supported capacity
claim. Longer sustained and combined parent/observer workloads, complete memory
and phase/queue accounting, workload-based numeric limits and matched acceptance
verification are still required. Existing runtime APIs and overload policy are
unchanged.

Capacity captures use the existing artifact utility with `--suite
supervision-capacity` or `--suite projection-capacity`, `--work-json` and a
preserved `--executable`. Repeat `--case <exact-case-id>` to declare a selected
experiment. The utility checks all 40 samples and an exact census/case match;
it records a selected scope, never full capacity acceptance.

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
