# Early binary supervision selection measurements

`supervision_selection` measures production report discovery before and after
protected binary selection. Its discovery cases call the real
`ReportReaders::stage` path, now using the core selective-reader port. The saved
`supervision-selection-v1-test` reference predates that implementation and must
not be overwritten by a candidate run. The original 59-case component suite and
its full-decoder benchmarks remain separate.

## Run and compare

Run from the repository root. The `supervision-benchmarks` feature enables counters at actual production
operations and the benchmark's process-wide allocation meter. Neither is present
in a normal build. Use the same instrumentation for baseline and candidate.

```bash
# Exercise all fixtures and completed-work assertions once.
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_SUPERVISION_WORK_OUTPUT=target/supervision-selection-check.json cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_selection -- --test

# Save the selective-reader candidate and require zero discarded business
# reconstruction/accounting. The old implementation fails this acceptance mode.
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_EXPECT_SELECTIVE_READS=1 OBZENFLOW_SUPERVISION_WORK_OUTPUT=target/supervision-selection-v2-test-work.json cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_selection -- --noplot --save-baseline supervision-selection-v2-test

# Compare the two saved sample sets without measuring again. Omit work output:
# this analysis-only invocation does not collect operation censuses.
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_selection -- --noplot --baseline supervision-selection-v1-test --load-baseline supervision-selection-v2-test

# Filter a single affected fixture instead of running the whole suite.
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_selection -- 'supervision_discovery/mixed_group_64/report_middle$' --baseline supervision-selection-v1-test
```

The default Cargo bench profile is an independent optimised series. Omit
`--profile test`, use a distinct `*-bench` baseline/work filename, and never
compare across profiles. All runtimes use two async workers and at most two
blocking workers. There is no flow, tracing subscriber or external service.

## Thirty named operations

| Group | Cases | Timed work |
| --- | ---: | --- |
| `supervision_discovery` | 23 | Actual reader registration/opening, physical scanning, decoding, accounting, selection, bounded handoff and consuming reports/coverage. |
| `journal_read_controls` | 2 | A complete ordinary reader scan and an explicit whole-record JSON serialisation; positive controls for the work counters. |
| `report_definition_resolution` | 2 | Decode one report frame with warm or fresh definition state, resolving real references into its preceding business carrier. No history scan warms that carrier first in the cold operation. |
| `journal_append_costs` | 3 | Commit 64 ordinary business records, one mixed 64-member group, or one report-only group. Tracks the write cost and encoded size of a later format change. |

The 23 discovery fixtures cover:

- One report after 0, 64, 1,024 and 10,000 business records; also 8,192-byte bodies.
- Empty journals, 1,024 business-only records, 256 dense reports and interleaved
  reports every eight records.
- Business-only/report-only atomic groups and mixed groups with reports at the
  beginning, middle, end, or all three positions.
- 32 external causal coordinates with prefixes of 32/128, and 1,024 external
  coordinates with prefixes of 4/16. Inputs are real admitted commitments, not
  fabricated clocks. Every record, including the last report, retains the full
  inherited clock. Actual witness minimum/maximum is recorded, rather than
  assuming witness count equals clock width throughout a journal.
- Fresh definition caches for narrow and wide prefixes; eight concurrent readers
  competing for two blocking workers.
- A mixed group with a foreign-owner lifecycle report, an execution fact that is
  not a supervision report, business data and an owned lifecycle report. Merely
  identifying all execution records as reports fails the oracle.

Fixtures, runtime construction, output-observation capacity, append input creation,
cache eviction and task teardown are outside timed elapsed work. Discovery starts
before task registration, so there is no free prefetch. Selected records are
compared against expected provenance and immediately dropped: the harness never
retains every report body as an artificial backlog. The small result-observation
and comparison cost is included consistently. Atomic checks, flags and budgets
use the current production implementation throughout.

Each iteration uses fresh readers/cursors or a new output journal. Scans finish
only after every initial prefix is covered. Exact report identities and append
order are checked per journal; no cross-journal order is invented. Coverage cannot
overtake an undelivered report. Report clocks, witnesses and lifecycle payloads
must match the append receipts. Every async operation has a 30-second failure
deadline. Partial/failed work never becomes a timing sample. Read tasks are
cancelled and joined before another iteration.

## Work counters and acceptance

`OBZENFLOW_SUPERVISION_WORK_OUTPUT` preserves one complete work census per named case,
collected by the same operation used in Criterion's timed iterations. It contains
input dimensions, actual encoded bytes, allocation measurements and counters at:

- The compact decoder's actual payload JSON call.
- `JournalRecord::from_parts`, including the constructed clock/witness sizes.
- `JournalRecord::serialize`, reached by both current accounting passes.
- Structural validation, frame reads, successful full-frame verification,
  definition-carrier reads and explicit frame-decode blocking dispatch.

The three selective-read acceptance counters are `business_payload_decodes`,
`business_records_constructed` and `business_record_accounting_serializations`.
They count ordinary `ChainPayload::Fact` business records. Other discarded kinds
are represented in total counts and the selection oracle. The opt-in acceptance
mode requires **all three to be zero in every discovery iteration**, including
mixed groups. The full-reader and accounting controls independently require
positive counts so removing instrumentation cannot make the suite pass.

`scanned_records` still counts skipped records. It is checked against fixture size,
separately from payload decodes/constructions. Exact primary frame bytes and frame
counts are checked. Successful checksum-verification counts/bytes must equal the
primary scan plus any auxiliary definition-carrier reads. A future scanner must
retain these observations at its actual operations, including a streaming CRC
path; replacing reconstruction with fake counters is not acceptable.

Byte counters measure encoded frame bytes consumed by the reader and auxiliary
loader, **not physical device traffic or OS read-ahead**. Files are OS-cache-warm.
"Cold" refers only to definition state. Cache eviction is confined to the fixture
archive, outside timing, with all its readers/writers quiescent. The isolated cold
dependency probe must observe nonzero carrier bytes; it is a codec measurement,
not an admitted selective reader or a claim about prefix continuity.
Ordinary discovery fixtures receive an untimed priming scan before their saved
census and Criterion warm-up; writer-populated cache entries alone do not establish
reader-validated warm state. Empty-journal throughput counts one completed journal
operation; all other discovery throughput counts scanned records.

The allocation meter tracks Rust allocation/reallocation requests and incremental
live heap above the operation's starting live allocation. It covers all worker
threads, including the blocking pool. It excludes allocator overhead, stack,
mapped files and page cache. Its census includes reader diagnostics and teardown,
while elapsed timing excludes those. This is an instrumented workload, not an
estimate of uninstrumented production throughput. The retained-report diagnostic
is the sum of per-reader high-water marks, **not** a simultaneous process peak.

No numerical speedup threshold is invented from the reference run. Use the
same profile/fixtures to establish elapsed improvement and inspect dense-report,
cold-dependency, allocation, dispatch and append regressions alongside zero-work
acceptance. The canonical-byte microbenchmark itself need not become faster; the
discovery path must stop invoking it on discarded business records.

## Evidence and limits

Use `scripts/capture_component_baseline.py --suite supervision --work-json <work.json>`
to archive all 30 cases, raw Criterion samples/estimates, work observations,
source hashes and host/toolchain provenance. Defaults are 20 samples, 300 ms
warm-up and one second requested measurement; Criterion extends slow cases.

Corruption, invalid tags, partial atomic groups, cancellation/retry, live appends
and slow-parent backpressure have separate focused correctness checks; valid
performance fixtures do not certify those behaviours. The selective reader uses
schema 10.0 routing metadata and complete-frame buffering, retaining the existing
runtime handoff. It checks commitment, integrity, continuity and selected records;
it does not semantically decode skipped payloads. No fixed-size streaming-buffer
or full-flow timeout claim follows from these component measurements.
