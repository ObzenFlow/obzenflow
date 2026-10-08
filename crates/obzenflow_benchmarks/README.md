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

Run from the implementation repository root. Correctness-only execution performs
each workload and checks its output without collecting a timing baseline:

```sh
cargo test --locked -p obzenflow_benchmarks --features journal-benchmarks,validation-benchmarks --bench journal_hot_path --bench journal_components --bench validation_boundaries -- --test
cargo xtask test --lane performance
```

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

The required gate uses `.config/performance.toml`: four hot-path timings and five
validation operations. Before timing, both implementations pass all 75 hot-path
oracles with identical case identities and input dimensions. Single-reader and
grouped-append timings remain diagnostic; their correctness cases are required.

The comparison owner installs the same outer benchmark crate against the pinned
reference and candidate. It changes no framework source or feature declaration.
`measurement-driver.json` identifies the driver and its files; only the benchmark
package lock entry is synchronised with the copied manifest. A future product API
change needs an explicit outer compatibility adapter or a qualified new baseline.

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

Raw samples live under `target/criterion`; required-gate artifacts live under
`target/test-runs`. The capture utility preserves existing samples without
performing measurements:

```sh
python3 crates/obzenflow_benchmarks/scripts/capture_component_baseline.py --help
```

Full component captures require 23 cases; hot-path captures use the count in
`.config/performance.toml` and a matching `--work-json` census. For a declared
investigation subset, repeat `--case <exact-name>` for every required case. The
capture rejects absent cases and records the selection explicitly. Shared captures
omit the local compile-time directory. Old capacity captures remain historical artefacts.
Use `--suite flows` to capture all six `pipeline_throughput` Criterion cases.

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md` and `TRADEMARKS.md`.
