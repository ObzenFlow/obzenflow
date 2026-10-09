# ObzenFlow benchmarks

This unpublished crate owns benchmark fixtures, measurement state and controls.
It calls existing framework capabilities without benchmark features, private
accessors or instrumentation in framework crates. Benchmarks require no network
listeners. Transport and host lifecycle correctness belong in integration tests.

## Suites

| Target | Purpose | Feature |
| --- | --- | --- |
| `journal_components` | 23 public causal and journal-reader operations | `components` |
| `journal_hot_path` | 75 record, clock, observed append/read, capture, metrics and concurrent-reader validity cases | `journal-benchmarks` |
| `validation_boundaries` | Archive export/admission, 10k replay comparison, metrics tail refresh and Studio projection | `validation-benchmarks` |
| `per_event_latency_*` | Fixed-depth pipeline latency, including disk/memory 100-stage variants | Default |
| `pipeline_throughput` | Six completed source/transform/sink cases: shallow/deep, memory/disk, constrained capacity and sparse arrivals | Default |
| `pipeline_execution` | Batch completion, metrics and causal-record operations | Default |
| `idle_cpu_usage`, `waiting_for_gun_cpu_usage`, `tokio_worker_3_stage_experiment` | Existing runtime experiments | Default |

Legacy CPU and three-stage latency validity gaps remain
owned by FLOWIP-143a. Their names do not establish valid CPU or latency samples.

Private-FSM capacity experiments and private codec timings are retired.
Historical results retain their original contracts; they are not comparable to
`public-operations-v1`.

## Run and validate

Performance is optional and runs only on explicit request. In GitHub, open
**Actions → Performance → Run workflow**. Set the optional `revision` input to a
version (`0.2.6` or `v0.2.6`) or a commit SHA (7–40 hex characters), or leave it blank
for the latest commit on `main`. Versions resolve through the corresponding `vX.Y.Z`
tag. The workflow resolves the selection once and checks out that full SHA; caching
and reports identify the measured commit. Unknown revisions fail without a fallback.
The separate `performance.yml` workflow invokes `cargo xtask test --lane performance`;
the same command runs locally. PRs, main pushes, ordinary CI dispatches, publishing
and release dry runs neither execute nor require performance measurements.
An explicitly requested run owns both the existing reference/current qualification
and all 16 Criterion targets declared in this crate's `Cargo.toml`.

Compilation is separate from timing. Targets with identical required features share
one Cargo invocation. Reference and candidate builds have distinct output directories
and share the four-job compilation budget. Timing binaries never enable allocation
census; their separate census binaries retain the existing validity checks.

The nine comparison cases and their controls run alone after all compilation finishes.
Then the full suite uses two stable queues from `.config/performance-suite.toml` on
Linux hosts with at least four allowed CPUs. Each queue is pinned to two distinct
logical CPUs; descendants inherit that assignment. The queues share memory, CPU caches
and filesystem bandwidth. These observations are not the regression gate and are not
directly comparable to the old separate-runner matrix. Authored workloads, sample sizes
and measurement windows are unchanged. Runtime defaults are two Tokio workers and two
Criterion analysis threads; explicitly authored runtime configurations remain intact.
Other platforms run serially and record a distinct execution mode.

The queue inventory must match the manifest exactly. Each executable lists its cases
before measurement, and missing/duplicate results fail completion. A comparison failure
still permits the full suite to finish; a failed target does not cancel later targets.

The manual workflow restores and saves dependency caches and intact compilation
directories, including the content-verified reference source, but never measurement evidence. This
job does not invoke generic Rust-cache cleanup, which can prune that source even with
target caching disabled. Every native run gets a fresh
`target/test-runs/<run-id>/` directory. Download `test-results-performance` for the
comparison JSON, logs, phase timings and `performance/suite/<target>/criterion/`
raw samples and HTML reports. Executable copies and source archives are excluded from
the uploaded evidence to avoid transferring build-sized artefacts.

Full performance runs can exceed 30 minutes and have no PR turnaround target.
`performance/phases.json` separates preparation,
compilation, qualification and full-suite wall time. Each target also records elapsed
time and CPU assignment. Assess cold and restored-cache runs separately.

### Copyable CI reports

Open the Performance run's **Summary** page and find **Performance report**. It shows rendered
tables and an expandable **Copy the complete report as Markdown** block. The same
content is uploaded directly as `performance-<run>-attempt-<attempt>.md`, following
the publishing dry-run pattern. A link to raw evidence accompanies the run identity,
source commit/content hash, reference revision, features, compiler, host, execution
mode, CPU assignments, phase durations and target outcomes.

Each operation category contains the applicable isolated comparison decisions and
full-suite observations. Comparison values come from the native `comparison.json`'s
identified before/candidate/after fields, never the mutable Criterion `new/` directory
used by reference and control trials. Observation rows retain the target, full case
ID, timed work, median in microseconds, confidence interval/level and sample count.
Medians are already per iteration; the formatter does not divide them again. There
are no category totals or new regression thresholds.

The current 148 observations across 16 targets fall into these categories:

| Operation | Cases | Included work |
| --- | ---: | --- |
| Reading and upstream consumption | 30 | Journal reads, reader creation/dispatch, reopened/fresh-process scans and concurrent scans |
| Journal appends | 11 | Complete appends and group appends, including encoding |
| Journal write/read interaction | 2 | Append after EOF and interleaved write/read workloads |
| Causal and record bookkeeping | 53 | Clock restoration/cloning, frontiers, append preparation and record byte accounting |
| Observations, metrics and reporting | 16 | Observation capture/validation/submission, metrics refresh, rendering, publication and Studio projection |
| Supervision and runtime scheduling | 11 | Idle/waiting lifecycle and Tokio worker experiments |
| Complete pipeline processing | 22 | Completed flows, execution wrappers and event-latency measurements |
| Archive and replay operations | 3 | Archive export, admission/read and streaming comparison |

Reading cases measure journal operations beneath upstream subscriptions; subscription
selection, merging, receipts and contract checks are not separately isolated. Likewise,
runtime experiments do not isolate supervisor dispatch. CPU-labelled cases expose
elapsed CPU-sampling routine time in Criterion output, not the calculated CPU percentage.
Per-event latency cases expose a per-run median, while completed-flow cases time a
whole batch. The row descriptions preserve those distinctions. Codec costs belong
inside reads/appends; canonical JSON byte accounting is a separate diagnostic operation,
not a measurement of the production disk codec.

Observation rows consume only this native attempt's per-target `new/` results. Failed or skipped measurement
commands produce an incomplete report with any available rows. Missing, malformed or
duplicate results are called out and fail report generation after the report is saved.
Unknown operations remain visible under **Uncategorised**. Raw output and Markdown
uploads run even after failures. Each run attempt has its own output directory so
cached results cannot appear as new measurements.

When adding or changing benchmark timing boundaries, update the operation rules in
`.github/scripts/criterion_report.py` and the audited case inventory in
`.github/scripts/test_criterion_report.py`. The existing CI policy job runs the focused
formatter tests; locally use:

```sh
python3 -m unittest discover -s .github/scripts -p 'test_criterion_report.py'
```

### Local measurements

Run from the implementation repository root. Correctness-only execution performs
each workload and checks its output without collecting a timing baseline:

```sh
cargo test --locked -p obzenflow_benchmarks --features journal-benchmarks,validation-benchmarks --bench journal_hot_path --bench journal_components --bench validation_boundaries -- --test
```

Use `cargo xtask test --lane performance` for the native comparison, the full suite
and their validity gates.

For a selected diagnostic measurement:

```sh
cargo bench --locked -p obzenflow_benchmarks --features components --bench journal_components -- 'disk_components/reader_next/' --save-baseline reader-reference
```

FLOWIP-080n's six families use `journal_hot_path` and `pipeline_throughput`.
Run those targets through `cargo bench` without `--test` to collect Criterion
baselines. The separate demo integration test and workload validity passes do
not supply Criterion timing evidence.

FLOWIP-080n A7 has a first-use measurement in the same target:

```sh
cargo bench --locked --profile dev -p obzenflow_benchmarks --features journal-benchmarks --bench journal_hot_path -- 'hotspots/first_process_open_and_scan/wide_observed' --save-baseline first-process-dev
cargo bench --locked -p obzenflow_benchmarks --features journal-benchmarks --bench journal_hot_path -- 'hotspots/first_process_open_and_scan/wide_observed' --save-baseline first-process-optimised
```

Every operation, including warm-up operations, executes a fresh copy of the same
benchmark binary. The parent prepares 1,024 observed records with 33 inherited
clock coordinates, retains their referenced journals, and saves independently
checked append receipts. The child starts timing before its first journal open
and stops after EOF. Opening, index reconstruction, definition resolution and
complete record reads are included. Process launch, runtime construction, receipt
loading, exact output verification and teardown are outside timing. `black_box`
barriers cover the child's input and the measured result.

Acceptance requires all 1,024 full records to equal their receipts in order,
including identities, payloads, provenance, observations, clocks and timestamps,
with final cursor 1,024. Child failure, an invalid result or the parent's 30-second
deadline fails the operation; the parent kills and reaps a timed-out child.
`OBZENFLOW_BENCH_CONTROL=missing-reader-output` deliberately removes one returned
record to prove this oracle rejects incomplete work. Use `--test` for that control.
Collect at least 20 samples and report the median with its 95% confidence interval
in each profile, without allocation instrumentation; compare profiles separately.
This case detects costs
moved into first-use reconstruction that warmed-process reads can miss. It is a
fresh-process measurement with uncontrolled filesystem cache, not cold disk I/O.

Set `OBZENFLOW_WORK_CENSUS=target/<name>-work.json` for hot-path or validation
workload evidence. Hot-path allocation censuses require a separate build with
`--features journal-benchmarks,allocation-census`; omit that feature and variable
for timing. The native comparison builds separate identified executables for these
two operations. Files include the measurement contract and compile-time source
directory. A missing output, wrong identity/order, failed operation or watchdog
expiry fails the workload. Async watchdogs protect completion, not speed claims.

## Measurement boundaries

| Operation | Included | Outside timing |
| --- | --- | --- |
| Record/clock primitives | Public accounting, clock restoration, cloning or serialisation | Fixture construction and output checks |
| `journal_record_read/open_and_read` | Ordinary reader creation and complete read/admission at all six record dimensions | Fixture construction and exact record comparison |
| Actual reader dispatch | Reading/admission by 1/8/32 concurrent real readers | Reader opening and identity/order checks |
| Complete append | Real append/group append of 64 records; observed cases include production cloning and causal preparation | Destination setup and ordinary-reader receipt verification |
| Hotspot reads | 1,024 complete observed records; sequential consumption, open-plus-read and reader creation are separate | Fixture construction and exact record/clock/timestamp checks |
| `hotspots/first_process_open_and_scan/wide_observed` | First journal open, reconstruction and complete 1,024-record read in a new process per operation | Archive creation, process launch, runtime setup, exact receipt/cursor checks and teardown |
| Observation capture/submission | Bound `capture_for_record`, validation and advancing live submission | Fixture setup and independent retained-family checks |
| Metrics refresh | Ordinary `read_metrics_tail` for 1/4 stages with independently stamped families | Journal setup/appends and exact committed-carrier checks |
| Completed flow | Start through completed publication, delivery and drain for 128 inputs | Flow construction and exact output identity/order checks |
| Archive export/admission | Public export, or opening and fully reading an archive | Live fixture execution and complete-record checks |
| Replay comparison | Public comparison of real 10k live/replayed archives | Both executions and verdict checks |
| Metrics refresh | Actual public tail refresh over data/error journals | Journal setup and accounting checks |
| Studio projection | Public projection of 69 committed records, measurements and final snapshots | Journal reads, projection creation and output checks |

Studio projection measures adapter work only. It does not claim SSE transport,
cursor settlement or host shutdown performance. No HTTP client, server, stream
test adapter or copied projection loop participates.

The benchmark binary owns the optional process allocation census. There are no
Core/Runtime/Infra work counters. Completed-work values describe checked outputs.
Requested heap is not RSS, allocator overhead, page cache or a memory limit.
Runtime fixtures use two async and two blocking workers. In-process cases reuse
OS-cache-warm files; the fresh-process case leaves filesystem cache uncontrolled.

## Comparison policy and retained evidence

Within an explicitly requested run, the gate uses `.config/performance.toml`: four
hot-path timings and five validation operations. Before timing, both implementations pass all 75 hot-path
oracles with identical case identities and input dimensions. Single-reader and
grouped-append timings remain diagnostic; their correctness cases are required.

The comparison owner installs the same outer benchmark crate against the pinned
reference and candidate. It changes no framework source or feature declaration.
`measurement-driver.json` identifies the driver and its files; only the benchmark
package lock entry is synchronised with the copied manifest. The pinned `854c04` reference
predates the explicit schema-version argument on `ChainEventFactory::data_event`.
The versioned `support/reference_854c04/data_event.rs` outer adapter calls that
reference's three-argument public constructor; the current fixture calls the four-argument
constructor with schema version 1. The adjacent `validation_sink.rs` adapter uses the
reference's explicit successful Noop delivery result, where the current inline-sink
API accepts `Ok(())`. Archive construction remains outside the timed operation; its
existing source and sink-delivery completeness assertions remain required. `measurement-driver.json` records the adapter and
its final file hash. Framework source, fixture payloads and workload dimensions remain
unchanged. A future product API change needs an explicit outer adapter or a qualified
new baseline.

Separate, content-identified builds prevent Cargo artifact aliasing. Each case
runs reference/candidate/unchanged-reference trials after compilation finishes.
The owner checks executable identity, contract, complete samples, workload
dimensions, precision and drift. No retries, automatic threshold relaxation or
silent baseline replacement can turn an inconclusive run into acceptance.

Three benchmark-owned rejection controls accompany ordinary measurements:
a slow reader must fail the regression rule; missing reader output and missing
Studio completion output must fail their real oracles. Ordinary runs clear
`OBZENFLOW_BENCH_CONTROL`. Controls never substitute for candidate evidence.

Component defaults are 20 samples, 300 ms warm-up and a one-second requested
window. The required gate has its own sampling policy. Compare identical
profiles, contracts, dimensions and runtime limits; run timings without concurrent
builds or tests. Test-profile and optimised bench-profile results are separate.

Direct `cargo bench` writes HTML and raw results under `target/criterion`. The native
lane isolates comparisons in `target/test-runs/<run-id>/performance/criterion-comparison`
and full-suite output in `performance/suite/<target>/criterion`.
Use `--save-baseline <name>` to retain a named baseline and `--baseline <name>`
to compare a later run of the same cases and build profile against it.
Required-gate artifacts live under `target/test-runs`.

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md` and `TRADEMARKS.md`.
