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
| `per_event_latency` | Per-run median latency through 1, 2, 3, 4, 5, 20 and 100-stage pipelines, including a memory-journal 100-stage control | Default |
| `pipeline_throughput` | Six completed source/transform/sink cases: shallow/deep, memory/disk, constrained capacity and sparse arrivals | Default |
| `pipeline_execution` | Batch completion, metrics and causal-record operations | Default |
| `idle_cpu_usage`, `waiting_for_gun_cpu_usage` | Process CPU time while a started flow is idle, or a built flow waits for its start command | Default |

Every case follows FLOWIP-080n's B1 contract, with a declared timed boundary and unit,
an independent check of completed work, and fail-closed samples. A timeout, failure,
missing, duplicate or empty run fails its sample instead of reporting a value. Flow
cases require every source-assigned input exactly once. Latency uses monotonic,
process-local stamps, and idle cases fail if the flow leaves its idle or waiting state.

A fix that only rejects invalid samples keeps a case ID. A change to the measured
quantity, unit or timed boundary takes a new ID and retires the old one (FLOWIP-080v
B5). Retired IDs are `execution_time_per_event/*` (build-to-teardown time divided by the
wrong input count), `tokio_worker_3_stage_experiment` (a Tokio scheduling experiment
whose premise did not hold under the runner), and `idle_cpu_usage/cpu_percentage`
(a duplicate topology). The CPU cases previously timed their whole sampling routine
and discarded the percentage. They are now `idle_process_cpu/window_2s/stages_*` and
`waiting_for_gun_process_cpu/window_2s`.

Private-FSM capacity experiments and private codec timings are retired.
Historical results retain their original contracts and are not comparable to
`public-operations-v1`.

## Benchmark levels

Three kinds of work use this crate, and each answers a different question
(FLOWIP-080v B8).

**Component cases** time one public operation, such as a journal read, an append or a
clock restoration, behind a declared boundary with an independent check of the work it
completed. They are black-box measurements. A case calls the operation the way the
runtime does and never reaches into framework internals. Only component cases supply
the gated comparisons, because only they attribute a change to one operation.

**End-to-end flow cases** (`per_event_latency`, `pipeline_throughput`,
`pipeline_execution` and the CPU-time cases) run whole flows with scheduling,
supervision, journaling, subscription and backpressure all active. They confirm that a
component improvement survives in a running flow. They cannot attribute a change,
because a flow's time mixes every stage's work with the runtime's scheduling.

**Profiling** explains where the time goes inside one case. Its flame graphs guide
investigation and supply no comparable numbers.

Tuning upstream subscription readers, journal append writers or any similar path
follows one loop. Start from a component case that reproduces the cost, adding one
when none exists. Profile it to see where the time goes, make the change, and compare
the component case against its reference. Then confirm the improvement in a flow case.
Treat a flow-case change that no component case explains as a lead to investigate.
Upstream subscription component cases arrive with FLOWIP-080s, through the public
`UpstreamSubscription`. Benchmarks never receive benchmark-only runtime constructors or
accessors.

### Profiling a named case

Every named case doubles as a profiling harness. The `profiling` Cargo profile inherits
`bench` and keeps line tables without stripping, so flame graphs of these fat-LTO
binaries retain inlined frames. Build the target once with frame pointers, in its own
target directory so the different compiler flags never invalidate ordinary builds.

```sh
cargo bench --locked -p obzenflow_benchmarks --profile profiling --features components \
  --bench journal_components --target-dir target/profiling --no-run \
  --config 'build.rustflags=["--cfg","tokio_unstable","-C","force-frame-pointers=yes"]'
```

Cargo prints the executable's path. Run it directly so the capture excludes Cargo.
Criterion's `--profile-time` repeats one case for the given seconds without analysis,
and `--exact` selects one full case ID.

```sh
perf record --call-graph fp -o target/profiling/perf.data -- \
  target/profiling/profiling/deps/journal_components-<hash> \
  --bench --profile-time 10 --exact 'causal_components/journal_clock_restore/w1_p256'
samply record target/profiling/profiling/deps/journal_components-<hash> \
  --bench --profile-time 10 --exact 'causal_components/journal_clock_restore/w1_p256'
```

`perf` works on Linux, and `samply` on Linux and macOS. Instruments can attach to the
same command on macOS. The standard library is precompiled without frame pointers, so
samples inside it can lose their callers. `perf record --call-graph dwarf` recovers them
at a much larger capture size. Captures stay local and ephemeral under FLOWIP-080n's
policy. They explain a measurement and never replace one.

## Run and validate

Performance is optional and runs only on explicit request. In GitHub, open
**Actions → Performance → Run workflow** and choose the branch or tag to run from
(`main` by default). Set the optional `revision` input to a version (`0.2.6` or
`v0.2.6`) or a commit SHA (7–40 hex characters) that the chosen ref contains, or leave
it blank for that ref's latest commit. Versions resolve through the corresponding
`vX.Y.Z` tag. Revisions resolve only among this repository's branches and tags, and
the chosen ref must contain the result. A run writes caches for its ref, so it only
executes code that ref already contains (FLOWIP-080v B4). Measure an unmerged branch
by running the workflow from that branch, and push a fork's commits to a branch first.
The workflow resolves the selection once and checks out that full SHA, and caching and
reports identify the measured commit. Unknown or uncontained revisions fail without a
fallback.

Before compiling benchmarks, the workflow checks that the selected checkout provides
the native performance stages and this report formatter (FLOWIP-080v B3). A revision
without them is reported as unsupported, with a summary and downloadable report, and
runs nothing. Malformed configuration or a probe that cannot run fails as itself.
PRs, main pushes, ordinary CI dispatches, publishing and release dry runs neither
execute nor require performance measurements. An explicitly requested run owns both
the reference/candidate qualification and all 8 Criterion targets declared in this
crate's `Cargo.toml`.

### Every PR runs each case once

Correctness CI runs `cargo xtask test --lane benchmarks` on every PR (FLOWIP-080v B9).
The lane builds every target in the test profile, checks each target's case
declarations against its listing, and runs every case once in Criterion's test mode.
Every completed-work check and fail-closed path therefore executes where a change
breaks it. The lane times nothing and makes no performance claim.

### Fanned-out requested runs

A requested run fans out across GitHub-hosted runners (FLOWIP-080v B7). One job plans
the run from `.config/performance-shards.toml`. Build jobs compile each group once and
publish its executables with a manifest of their hashes. Qualification and suite jobs
download those executables, verify every hash, and run one measurement at a time on
their own runner without a toolchain. A final job assembles the evidence and writes
the report. `cargo xtask test --lane performance` runs the same plan serially on one
machine. Any single stage can be reproduced locally under one run identity.

```sh
cargo xtask performance plan --run-id local-1
cargo xtask performance build --run-id local-1 --group candidate-default
cargo xtask performance qualify --run-id local-1 --shard gate
cargo xtask performance measure --run-id local-1 --shard journal
cargo xtask performance assemble --run-id local-1
```

`plan` prints its build groups and shards. Targets with identical required features
form one build group, compiled by one `cargo build --profile bench --keep-going`
invocation. The reference builds only the two gated targets. The group holding
`journal_hot_path` builds its allocation census in a separate binary, so timing
binaries never enable it. A rejected group leaves other groups unaffected, and
executables Cargo still identifies are kept. Qualification needs only its own timing
and census executables. A missing census leaves it incomplete, never substituted by a
timing binary. Unavailable targets record their build outcome and no samples.

Each qualification shard holds whole gated cases. It runs the 75 hot-path validity
cases when it holds hot-path cases, then adjacent reference-before, candidate and
reference-after trials for each case, then the rejection controls that reuse those
trials. Each suite shard names targets, optionally narrowed by an anchored Criterion
case filter. It lists each target's complete inventory and declarations, then measures
its selection with authored sample sizes and windows. Runtime defaults are two Tokio
workers and two Criterion analysis threads, and no target authors more than two
workers. Shards are packed from measured case times, longest first, and rebalanced from
recorded stage times.

Assembly accepts the run only when every planned stage reported on the planned source
with the published executables, and every gated and listed case appears exactly once.
A missing stage settles as incomplete. Overlapping or missing selections, a different
source and an executable that differs from its manifest fail. A failed shard never
cancels another, and partial evidence is always kept.

Only the planning and build jobs restore and save caches, holding dependencies and
intact compilation directories, including the content-verified reference source. They
never cache measurement evidence or invoke generic Rust-cache cleanup, which can prune
that source even with target caching disabled. Before saving, they prune what no later
run can reuse, namely other reference identities and workspace-crate artifacts, which
rebuild after every fresh checkout.

The run targets 15 minutes of wall time with warm caches. Download
`test-results-performance` for `plan.json`, `assembly.json`, each stage's record with
its runner's CPU, logical CPUs, image and duration, the comparison JSON, logs, and
`suite/<shard>/<target>/criterion/` raw samples and HTML reports. Executables travel
between jobs only as one-day intermediate artifacts, and the evidence keeps their
hashes. Locally, every run gets a fresh `target/test-runs/<run-id>/performance/`
directory with the same layout.

### Copyable CI reports

Open the Performance run's **Summary** page and find **Performance report**. It shows rendered
tables and an expandable **Copy the complete report as Markdown** block. The same
content is uploaded directly as `performance-<run>-attempt-<attempt>.md`, following
the publishing dry-run pattern. A link to raw evidence accompanies the run identity,
source commit and content hash, reference commit and the rule that chose it, compiler,
each stage's runner and duration, the end-to-end wall time and per-target completion
by shard.

Each operation category contains the applicable isolated comparison decisions and
full-suite observations. Comparison values come from the assembled `comparison.json`'s
identified before/candidate/after fields, never the mutable Criterion `new/` directory
used by reference and control trials. Observation rows retain the target, full case
ID, timed work, median in microseconds, confidence interval and level, and sample count.
Medians are already per iteration, and the formatter does not divide them again. There
are no category totals or new regression thresholds. Compare observations only across
runs on matching runner hardware.

The current 138 observations across 8 targets fall into these categories.

| Operation | Cases | Included work |
| --- | ---: | --- |
| Reading and upstream consumption | 30 | Journal reads, reader creation/dispatch, reopened/fresh-process scans and concurrent scans |
| Journal appends | 11 | Complete appends and group appends, including encoding |
| Journal write/read interaction | 2 | Append after EOF and interleaved write/read workloads |
| Causal and record bookkeeping | 53 | Clock restoration/cloning, frontiers, append preparation and record byte accounting |
| Observations, metrics and reporting | 16 | Observation capture/validation/submission, metrics refresh, rendering, publication and Studio projection |
| Supervision and runtime scheduling | 5 | Process CPU time of idle and waiting flows |
| Complete pipeline processing | 18 | Completed flows, execution wrappers and event-latency measurements |
| Archive and replay operations | 3 | Archive export, admission/read and streaming comparison |

Reading cases measure journal operations beneath upstream subscriptions, and they do
not separately isolate subscription selection, merging, receipts or contract checks.
Likewise, runtime cases do not isolate supervisor dispatch. They report process CPU
time used in a 2 s window, so 2,000,000 µs is one logical CPU fully busy for the whole
window. Per-event latency cases expose a per-run median, while completed-flow cases
time a whole batch. The row descriptions preserve those distinctions. Codec costs
belong inside reads and appends. Canonical JSON byte accounting is a separate
diagnostic operation that does not measure the production disk codec.

Observation rows consume only this run's per-shard `new/` results. Failed or skipped
stages produce an incomplete report with any available rows. Missing, malformed or
duplicate results are called out and fail report generation after the report is saved.
Cases without a valid declaration remain visible under **Uncategorised** and fail
report generation. Raw output and Markdown uploads run even after failures. Each run
has its own output directory so cached results cannot appear as new measurements.

### Adding a named case

Declare each case next to its registration, before `bench_function`, with its full
Criterion ID, operation category and timed boundary.

```rust
use obzenflow_benchmarks::case::{declare, Category};

declare("journal_refresh/append_after_eof", Category::ReadWrite, "64 append, read and EOF-check pairs");
group.bench_function("append_after_eof", |b| { /* ... */ });
```

The native lanes collect declarations while listing cases and reject a target whose
declarations are missing, duplicated or unlisted, before running anything. The report
reads them, so there is no separate rule table or case list to update. Meet B1 by
keeping fixture construction and checks outside timing, checking completed work
against an independent oracle, and failing the sample on timeout or incomplete work.
A new target also needs its `[[bench]]` entry and a place in one suite shard of
`.config/performance-shards.toml`. Run these before opening a PR.

```sh
cargo xtask test --lane benchmarks
cargo test --locked -p obzenflow_benchmarks --bench per_event_latency -- --test
OBZENFLOW_CASE_DECLARATIONS=target/declarations.jsonl cargo test --locked -p obzenflow_benchmarks --bench per_event_latency -- --list
python3 -m unittest discover -s .github/scripts -p 'test_*.py'
```

The lane checks every target. The next two commands run one target's cases once with
their checks, and write the declarations the native lanes collect.

### Local measurements

Run from the implementation repository root. Correctness-only execution performs
each workload and checks its output without collecting a timing baseline.

```sh
cargo test --locked -p obzenflow_benchmarks --features journal-benchmarks,validation-benchmarks --bench journal_hot_path --bench journal_components --bench validation_boundaries -- --test
```

Use `cargo xtask test --lane performance` for the native comparison, the full suite
and their validity gates. For a selected diagnostic measurement, run one target with a
case filter.

```sh
cargo bench --locked -p obzenflow_benchmarks --features components --bench journal_components -- 'disk_components/reader_next/' --save-baseline reader-reference
```

FLOWIP-080n's six families use `journal_hot_path` and `pipeline_throughput`.
Run those targets through `cargo bench` without `--test` to collect Criterion
baselines. The separate demo integration test and workload validity passes do
not supply Criterion timing evidence.

FLOWIP-080n A7 has a first-use measurement in the same target.

```sh
cargo bench --locked --profile dev -p obzenflow_benchmarks --features journal-benchmarks --bench journal_hot_path -- 'hotspots/first_process_open_and_scan/wide_observed' --save-baseline first-process-dev
cargo bench --locked -p obzenflow_benchmarks --features journal-benchmarks --bench journal_hot_path -- 'hotspots/first_process_open_and_scan/wide_observed' --save-baseline first-process-optimized
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
deadline fails the operation, and the parent kills and reaps a timed-out child.
`OBZENFLOW_BENCH_CONTROL=missing-reader-output` deliberately removes one returned
record to prove this oracle rejects incomplete work. Use `--test` for that control.
Collect at least 20 samples and report the median with its 95% confidence interval
in each profile, without allocation instrumentation, and compare profiles separately.
This case detects costs moved into first-use reconstruction that warmed-process reads
can miss. It is a fresh-process measurement with uncontrolled filesystem cache, which
differs from cold disk I/O.

Set `OBZENFLOW_WORK_CENSUS=target/<name>-work.json` for hot-path or validation
workload evidence. Hot-path allocation censuses require a separate build with
`--features journal-benchmarks,allocation-census`. Omit that feature and variable
for timing. The native comparison builds separate identified executables for these
two operations. Files include the measurement contract and compile-time source
directory. A missing output, wrong identity or order, failed operation or watchdog
expiry fails the workload. Async watchdogs protect completion, not speed claims.

## Measurement boundaries

| Operation | Included | Outside timing |
| --- | --- | --- |
| Record/clock primitives | Public accounting, clock restoration, cloning or serialization | Fixture construction and output checks |
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
Requested heap excludes RSS, allocator overhead, page cache and memory limits.
Runtime fixtures use two async and two blocking workers. In-process cases reuse
OS-cache-warm files, and the fresh-process case leaves filesystem cache uncontrolled.

## Comparison policy and retained evidence

Within an explicitly requested run, the gate uses `.config/performance-policy.toml`, with four
hot-path timings and five validation operations. Before timing, both implementations
pass all 75 hot-path oracles with identical case identities and input dimensions.
Single-reader and grouped-append timings remain diagnostic, and their correctness
cases are required.

The gate's reference is where the measured change started (FLOWIP-080v B10). A commit
that `main` does not contain is compared with its merge base with `main`, and a commit
on `main` with its first parent. Set `pin` in `.config/performance-policy.toml` only for a
deliberate re-qualification against a fixed commit. The plan resolves the reference
once and records the commit and the rule that chose it.

The comparison owner installs the candidate's outer benchmark crate against both the
reference and the candidate, so both sides run identical workloads. It changes no
framework source or feature declaration. `measurement-driver.json` identifies the
driver and its files, and only the benchmark package lock entry is synchronized with
the copied manifest. When a change alters a public API the benchmarks use, the
reference cannot compile the candidate's driver, and its build ends incomplete with
that diagnosis. Resolve it with a versioned adapter in
`src/support/reference_<first six digits of the reference>/`, whose files replace their
`src/support/` namesakes in the reference copy only, or with an explicit pin.
`measurement-driver.json` records any adapter and its file hashes. Framework source,
fixture payloads and workload dimensions remain unchanged. The adapters for the former
`854c04` pin retired with it.

Separate, content-identified builds prevent Cargo artifact aliasing. Each case runs
reference, candidate and unchanged-reference trials on a runner with no compilation or
other measurement. The owner checks executable identity, contract, complete samples,
workload dimensions, precision and drift. No retries, automatic threshold relaxation or
silent baseline replacement can turn an inconclusive run into acceptance.

Three benchmark-owned rejection controls accompany ordinary measurements.
A slow reader must fail the regression rule, and missing reader output and missing
Studio completion output must fail their real oracles. Ordinary runs clear
`OBZENFLOW_BENCH_CONTROL`. Controls never substitute for candidate evidence.

Component defaults are 20 samples, 300 ms warm-up and a one-second requested
window. The required gate has its own sampling policy. Compare identical
profiles, contracts, dimensions and runtime limits, and run timings without concurrent
builds or tests. Test-profile and optimized bench-profile results are separate.

Direct `cargo bench` writes HTML and raw results under `target/criterion`. The native
stages isolate comparisons in `performance/qualification/<shard>/criterion-comparison`
and full-suite output in `performance/suite/<shard>/<target>/criterion`, under
`target/test-runs/<run-id>/`. Use `--save-baseline <name>` to retain a named baseline
and `--baseline <name>` to compare a later run of the same cases and build profile
against it.

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md` and `TRADEMARKS.md`.
