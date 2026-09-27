# Supervision delivery baselines

`supervision_delivery` measures whether parent responsiveness depends on business
traffic even when the control work is unchanged. It exercises production
`ReportReaders`, actual disk journals, concurrent appenders where specified, the
real parent FSM and its required journal publication. It does not construct a flow.
The production delivery mechanism is unchanged by these benchmarks.
The [local baseline and architectural investigation](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-control-delivery-simplification-2026-09-27.md)
records the results, interpretation and proposed change boundary.

## Measurement contract: supervision-delivery-v1

There are **30 Criterion cases**, comprising two timing metrics for 15 fixtures.
Each repeated iteration completes and validates the entire operation, including
the first-delivery cases. An early report followed by stalled completion is never
accepted as a successful sample.

| Fixture | Dimensions | Purpose |
| --- | --- | --- |
| `prepared_suffix` | One child, two reports; 0/63/1,024 business records after the first report; 256/8,192-byte payload strings for nonempty suffixes | A report already committed at the head must not need later business records to become useful to the parent. Both reports and the final coverage are checked. |
| `live_fixed_reports_600` | 8/32/50/75/100 children; exactly 600 reports total; zero or seven business records before each report; 256-byte payload strings | Separate child-count scaling from report-volume scaling, and measure sensitivity to live business writes at the same control volume. |

The two timing metrics are:

- `first_report_delivery`: reader registration until the first child report is
  returned to the parent, before its FSM application. This is neither
  commit-to-application latency nor durable parent acceptance latency.
- `completion`: registration until all child reports have been applied, coverage
  reaches the declared end, the parent's one required `ReadyForRun` publication
  has been committed and read back, and child writers/publications have settled.

The metrics run separate operations; their estimates are not paired observations.
Criterion repeats each metric, rather than estimating first-delivery latency from
the single work census. Confidence intervals describe mean timing estimates, not
per-report latency percentiles. Flat sampling limits iteration ramps when a short
reported first-delivery duration accompanies longer mandatory completion work.

Prepared histories have the first report at position one and `Running` after the
business suffix. This isolates the current 64-traversed-record handoff policy from
a business prefix before any report exists. Filesystem pages and metadata caches
are warm. The 63-record fixture crosses the current record-count handoff boundary;
the larger payload and suffix cases also exercise byte-budget boundaries.

Live histories are fresh for every iteration. Writers start inside the timed
operation, after reader registration, and use ordinary appends through the real
provider. Each child receives `600 / children` reports plus one for the first
`600 % children` children. Thus all cases have exactly 600 control records and
either zero or 4,200 business records, including the 32-child cases. Child
topology, fixtures and empty journals are constructed outside timing.

All cases use **two async workers and two blocking workers**. This is an explicit
contention condition, not a claim that every production deployment uses this pool
size. Completion includes business append work in live fixtures; extra completion
time alone does not attribute that work to supervision. Reader scan-byte counters
separately establish whether supervision traversed business storage. Shared CPU and
disk can still affect control latency after business reads are eliminated.

## Completed-work checks and observations

The shared fan-in consumer validates exact report identities and per-child order,
checks that coverage never passes an unapplied report, executes the real parent
publication action exactly once, reads that committed result, and checks inherited
causal coordinates from every child's last report. Reader shutdown and outstanding
reads are joined before the next operation, outside measured elapsed time. Each
async operation has a 30-second invalid-sample deadline.

Production-operation counters and the counting allocator are enabled identically
for all measurements. A separate completed operation per case records physical
scan bytes, auxiliary definition reads, frame checks, blocking submissions,
decodes, accounting and validation work, requested allocations and retained-byte
observations. Timings continue to include instrumentation but do not build a work
report on every iteration. The census includes `completed_elapsed_ns` even when
the selected Criterion metric is first delivery.

Business payload decoding must be zero. Prepared cases additionally require zero
business construction and accounting. Live process-wide construction/accounting
counts include legitimate writer work and cannot be called reader overhead.
Heap observations are allocator requests and incremental high-water bytes, not
RSS or a proof of a hard memory bound. First-delivery and service-gap observations
in the separate census are not the repeated Criterion samples.

## Architecture acceptance

Preserve this work contract when replacing delivery. A candidate must process the
same control payloads, retain their authoritative commitments, apply all reports
and complete the parent publication. Removing coverage checks or omitting the
live business writes would measure less work and invalidate the comparison.

If control facts move into a dedicated report journal, the fixture must write the
same business traffic to its business journal, write controls through the real new
publication path, and check processed coverage against the new report journal's
commitments. Preserve this baseline and record that storage-topology change in the
candidate's evidence. Report total bytes written and the new journal/file count.

The structural goal is **zero business-journal reads by live supervision**, with
business traffic still present. Whole-record decode counts already equal zero;
they cannot prove this stronger goal. The existing raw scan and auxiliary-read
counters expose today's work. A split-journal candidate will need attribution by
journal role so control bytes remain distinct from business bytes. No production
instrumentation has been added for a topology that does not exist yet.

This suite covers initial responsiveness and successful live readiness. It does
not establish crash recovery, reconnect deduplication, slow-parent fairness,
failure-report latency during a long-running flow, terminal drain, or resume
semantics for a new delivery architecture. Those need focused correctness checks
and corresponding Criterion fixtures once the proposed contract is selected.
An empty-channel microbenchmark would not establish durable-report delivery.

## Run and preserve

Run fixture checks before collecting measurements, with no concurrent builds or
test processes:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/supervision-delivery-check-work.json cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_delivery -- --test
```

Capture a new reference without overwriting previous baselines:

```sh
env CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 OBZENFLOW_WORK_CENSUS=target/supervision-delivery-v1-test-work.json cargo bench --offline --locked --profile test -p obzenflow_benchmarks --features supervision-benchmarks --bench supervision_delivery -- --noplot --save-baseline supervision-delivery-v1-test
```

Export raw samples, estimates, the census and source hashes using
`scripts/capture_component_baseline.py --suite delivery --work-json <census>`,
with `--profile test` and the exact command above in `--command`. The exporter
requires all 30 unique cases and at least 20 samples per case. Defaults are
20 samples, 300 ms warm-up and one second requested measurement; slow completed
operations extend that window.

The test profile is an unoptimised local CI-profile reference. Capture an optimised
reference separately with the default bench profile before making production
throughput claims. Compare only matching profiles, instrumentation, dimensions
and completed-work contracts.
