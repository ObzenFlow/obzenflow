# Journal hot-path baselines

`journal_hot_path` measures the remaining costs after early supervision selection.
It contains **138 Criterion cases in six groups**, without building a flow or
running business handlers. This is a measurement change, not an optimisation.
The existing `journal_components` and `supervision_selection` baselines remain
separate references.

The [accepted local baseline and investigation comparison](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-journal-hot-path-baseline-2026-09-27.md)
records all 138 cases, their work censuses, and the live-reader liveness defect
found during fixture validation.
The subsequent [report-reader locking comparison](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-report-reader-locking-comparison-2026-09-27.md)
preserves a fresh 37-case before/after reference and eight completed live cases
with two blocking workers. It records the liveness fix, its allocation cost and
the observed live-workload regression separately from sparse-scan improvements.
The separate [supervision delivery contract](SUPERVISION_DELIVERY.md) adds repeated
first-report measurements and live 50/75-child cases while keeping total report
volume fixed. It does not change this suite's 138-case inventory.

## Measurement contract: journal-hot-path-v2

| Group | Cases | One completed operation | Question |
| --- | ---: | --- | --- |
| `report_accounting` | 18 | Canonical byte count, runtime retained-byte count, or both passes on the same selected report | How much does each actual counting serialisation cost? |
| `causal_record_work` | 18 | Structural validation, clock clone, or clock JSON byte count | Which clock/witness operations account for the cost? |
| `record_reconstruction` | 36 | Frame integrity/routing; payload JSON decoding; compact provenance reconstruction; complete selected-frame decoding | What remains after business records are skipped? |
| `reader_dispatch` | 30 | Consume 64 ordinary report frames per cursor, with 1/8/32 concurrent cursors | How much comes from blocking dispatch and contention? |
| `supervisor_fan_in` | 24 | Actual `ReportReaders` feed one real parent FSM; all child reports are applied, coverage reaches the known end, and the required parent publication is committed and read back | How does the single parent scale with independent child journals? |
| `journal_append_cost` | 12 | Encode 64 committed records; write/flush their preencoded frames; or append 64 events through the complete provider | Where is the write cost? |

All timing results come from Criterion. Throughput is completed records or reports
for the declared operation, not application throughput. Partial work is never a
sample. `iter_custom` returns summed elapsed time for all requested iterations.

### Accounting, validation and reconstruction

Six fixed dimensions: `(clock components, external witnesses, payload string bytes)`
are `(1,0,256)`, `(33,0,256)`, `(33,4,256)`, `(33,32,256)`, `(1025,1024,256)` and
`(33,32,8192)`. These are valid selected `SourceCleanupFailed` reports. Payload size
means the error string, not the entire record. Fixtures use real appends: first
inherit all coordinates, then strictly advance exactly the specified witness set.
Every fixture asserts its exact dimensions, independently of random journal IDs.

Canonical accounting invokes `journal::limits::record_bytes`. Retention accounting
invokes the same counting writer used by runtime report buffering. Neither case
serialises into an allocated byte vector as a substitute for the production call.
The two-pass case demonstrates their combination; it is not a proposed fix.

Structural validation directly invokes the real causal validator. Clock cloning
and serialisation are separate. Clock serialisation includes its own nonzero
sequence scan and temporary entry vector. Validation timings are nested inside
accounting and complete decoding; **do not add these overlapping results together**.

Payload and provenance cases prepare the frame view and definition table before
timing, then call the same private functions as the production decoder. Provenance
includes compact clock/witness expansion; its predecessor is in the routing
section. The complete-frame control includes framing, table setup, reconstruction,
record validation and canonical accounting, but not primary-frame I/O or core reader admission.
All component outputs are checked; output destruction follows the timing boundary.

`compact_provenance_warm` and `complete_selected_frame_warm` retain the fixture's
real writer/definition store and assert **zero definition-carrier reads**.
The corresponding `_cold` cases use a fresh private definition store on every
operation and assert one real auxiliary carrier read. Their elapsed time includes
that carrier I/O, verification, table construction and expansion. OS pages remain
warm. This makes cold resolution visible without charging it to warm reconstruction.
The exploratory v1 fixture dropped its writer, unintentionally releasing the weakly
registered cache; its unlabeled reconstruction timings were cold. Only v2 is the
accepted baseline for this contract, and it adds explicit cache-state assertions.

### Dispatch

Controlled cases use the identical 64 ordinary frames, preloaded in memory, through
the production decoder and provider continuity checks. `frames_per_job_1`, `_8`
and `_64` change only the experimental blocking-job boundary. `inline` performs
the same work on the async workers as a diagnostic control, not a recommendation.
Full and selected decode paths have separate cases. These cases do not change
production dispatch, file layout, or atomic grouping.

`actual_reader` uses real disk readers, including file reads, current dispatch and
core admission. Reader opening is outside timing; consumption through EOF is
inside. The 1/8/32 cursors read the same warm archive so the dispatch experiment
keeps bytes constant. Independent archives are exercised by `supervisor_fan_in`.
Controlled cases observe first completed job latency; actual-reader cases observe
first delivered record latency. They are deliberately named differently.

### Parent fan-in

There are 1, 8, 32 or 100 independent child journals, with either zero or seven
business records before each report. Three workloads separate scaling effects:

- `fixed_total_800`: distribute exactly 800 reports across the journals.
- `per_journal_8`: eight reports per journal; total reports increase with fan-in.
- `live_per_journal_8`: concurrent child writers append the same per-journal work
  while the parent reads. Real tail polling and writer/reader contention are included.

Live cases explicitly use `blocking_capacity_512` with two async workers. Prepared
cases retain two blocking workers. This distinction is consequential: the live
eight-journal fixture with two blocking workers stalled twice before any parent
report admission in the original baseline. The selective reader called
`blocking_read` inside a blocking job, while an appender could hold the write lock
awaiting its own queued blocking job. Occupied reader workers prevented the writer
that released their lock from running. Those failed operations have no elapsed-time
baseline.

The reader now retains an asynchronous lock acquisition before submitting its
blocking work. After each copied frame it releases the guard; contention before
another frame returns verified progress without declaring a tail. Sparse scans
still process multiple frames per job when uncontended. The constrained cases now
complete, including their report-order, processed-coverage and committed-parent
publication checks. The default 512-worker cases remain unchanged for comparability.
These component results do not establish the cause of the earlier CI timeouts.
Run the complete constrained live group explicitly:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_LIVE_BLOCKING_THREADS=2 cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench journal_hot_path -- 'supervisor_fan_in/live_per_journal_8/blocking_capacity_2/' --test
```

Each child ends with `Running`; earlier reports are passing contract status facts.
The parent starts in `AwaitingStageReadiness`, applies every report through the
real FSM and executes its actual publication action. The sample ends after the
parent reads its committed `ReadyForRun`, reaches that state, covers every expected
child position and settles publications/writers. It asserts exactly one required
parent publication, preserving each child's last reported causal coordinate.
The fixture uses inert stage handles: no stage processing, metrics service, server
or complete flow is involved. This is the report-read/FSM/publication component,
not the complete supervisor loop's external controls, shutdown or metrics workload.

Reader registration/opening is timed. Child history, topology and parent journal
construction are not. Read tasks are shut down and joined before another sample;
teardown closes the handoffs and awaits bounded provider reads and blocking jobs
outside elapsed time. Every live iteration gets fresh child journals.
Identity/order/coverage checks and lightweight service timestamps are included in
this component workload. Coverage cannot pass an expected unapplied report.
Development observations include first report latency, per-journal service gaps
including initial wait, and the sum of retained-report byte high-water marks.
Those gap quantiles describe one operation census, not Criterion confidence intervals.

Business decoding must remain zero in all fan-in cases. For prepared histories,
business construction/accounting must also remain zero. Live cases legitimately
construct and account business records on the writer; their process-wide counts
must not be attributed to the reader. Live writers retain append receipts until
joined; process-wide heap observations include that harness output retention.

### Append attribution

Fixtures contain 64 ordinary business frames (256/8192-byte body), one mixed group
of 64 (one report per eight members), or one report-only group of 64.
`prepare_encoding` measures the production codec's preparation and definition
publication from existing committed records, resetting its definition store every
iteration. It checks byte-for-byte agreement with the original frames.
`write_preencoded` calls the provider's actual seek/write/flush/rollback boundary
through its blocking pool, without record preparation. It checks every output byte.
`complete_append` includes commitment preparation, encoding, dispatch, write/flush,
index update and append receipts. Files/journals and event clones are prepared
before timing; each destination is new. Group chunk copies inside `append_events`
are part of both grouped complete-append cases.

The three boundaries are diagnostic controls, not additive exclusive phases:
complete append interleaves work, and codec preparation starts with admitted rows.
The write control uses the existing file flush policy; it does not add `fsync`.
Fresh complete appends have new commitment identities/timestamps, so they verify
encoded bytes against their own receipts instead of assuming another journal has
an identical byte length. Encoded size is recorded alongside time. These current-code measurements alone
cannot attribute the historical 8.4% regression. That requires the same contract on
both historical implementations, rather than subtracting unrelated benchmarks.

## Counters and comparability

`supervision-benchmarks` enables development-only production-operation counters
and a counting allocator. Timings include this instrumentation in every candidate.
One separate operation per case supplies the validated work/allocation census;
instrumentation remains enabled for repeated timing samples without allocating a
counter report after every tiny primitive. Full payload/provenance parity is checked
in that census; record counts, identities and completed-work checks also guard the
repeated operations. Setup, oracle work and census formatting are outside the
declared time boundary except the fan-in observations described above.
Counters record actual validation walks/visited entries, clock serialisations,
payload decodes, constructed records, accounting traversals, primary/auxiliary
bytes, verified frames, and blocking jobs. Requested Rust heap allocations and
incremental high-water heap bytes include worker threads; they do not measure RSS,
allocator overhead, OS page cache, or a hard memory bound. Successful structural
validation visits include the predecessor reference when present.
The decode/scan blocking-job counter covers explicit submissions at those
boundaries, not implicit Tokio filesystem offloads.

There are two async workers; blocking limits are two, except the explicitly labelled
live cases described above. Filesystem pages are warm; metadata definition stores
are warm or cold as explicitly labelled. Cold resolution does not imply cold OS pages.
Fixtures are lazily constructed for selected cases. No other build, test or benchmark
should run during measurements. Default collection is 20 samples, 300 ms warm-up,
and one second requested measurement per case; slow cases extend the window.
Async operations have a 30-second invalid-sample deadline.

## Run and preserve

From the repository root, first run fixture checks:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/journal-hot-path-check.json cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench journal_hot_path -- --test
```

Capture the unoptimised CI-profile reference (use a new name for a new reference):

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/journal-hot-path-v2-test-work.json cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench journal_hot_path -- --noplot --save-baseline journal-hot-path-v2-test
```

Export Criterion's raw samples, estimates, work census and source hashes with
`scripts/capture_component_baseline.py --suite hot-path --work-json <census>`.
The exporter requires all 138 cases and at least 20 samples per case. Supply the
exact successful Cargo command via `--command`, and `--profile test`.

After a production change, run the relevant named group with
`--baseline journal-hot-path-v2-test` and compare both timing and completed work.
For an optimised reference, omit `--profile test` and save a separate `*-bench`
baseline. Never compare across build profiles, instrumentation, changed dimensions
or measurement contracts. Larger sample counts can be requested for a close result.
