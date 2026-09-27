# Journal component measurements

FLOWIP-145h requires a Criterion baseline for each performance-sensitive component
before optimisation. This target constructs no flow and runs no business handlers,
metrics aggregator, web server or integration-test suite. Every performance result
comes from a named Criterion case. The `components` feature enables existing
development-only `test-support` boundaries so the private production decoder and
parent FSM can be measured without exposing an application API.

## Measurement contracts, version 1

All durations are elapsed time for **one complete declared operation**. Criterion
receives total time for all requested iterations, never an already averaged value.
`Elements` throughput uses the record/report count below, including discarded
business records when measuring scanning. It is not application throughput.

| Criterion group | One operation | Included / excluded | Dimensions |
| --- | --- | --- | --- |
| `causal_components/commitment` | Extract an admitted commitment | Production structural validation and clock copy; fixture append excluded | 1/32/1,024 witnesses; 256/8,192-byte payload body |
| `causal_components/byte_accounting` | Count one complete canonical record | Actual serialisation and its validation; fixture preparation excluded | Same |
| `causal_components/frontier_from_record` | Extract one admitted frontier | Validation, clock copies and witness map construction | Same |
| `causal_components/merge_overlapping` | Merge into a fresh populated frontier | Production merge; cloning the input target and dropping it excluded by `iter_batched_ref` | Same widths; input overlaps the retained predecessor |
| `causal_components/prepare_append` | Prepare one successor commitment | Production merge, witnesses and local increment; provider I/O excluded | Same |
| `disk_components/decode` | Classify and decode 64 records | Production framing/CRC, compact decode, payload/size validation, output destruction and work counting; encoded file reads and cursor creation excluded | 64 ordinary frames at 256/8,192-byte bodies; one 64-member atomic frame at 256 bytes |
| `disk_components/decode_and_continuity` | Decode and admit the same 64 records | Above plus the provider's real namespace/predecessor-continuity admission | Same |
| `disk_components/reader_next` | Read 64 records through the real journal reader | Physical buffered reads, blocking dispatch, decode and both admission layers; open and final EOF check excluded | Same |
| `decode_dispatch` | Decode 64 frames per concurrent reader | Same production classifier/admission through explicit blocking jobs; task dispatch, waiting, joining and output destruction included; file reads/cursor setup excluded | 1/8 readers; 1/64 physical frames per blocking job |
| `report_discovery` | Discover every report and cover every physical row in prepared journals | Actual `ReportReaders`, reader opening/task startup, decoding, byte accounting, selection, handoff and coverage; history creation and task teardown excluded | Memory/disk; one report after 0/64/1,024/10,000 business records; payload bodies 256/8,192 bytes; 1/8/32 readers |
| `ready_report_handoff` | Consume one ready report from each journal | Real fair `poll_next` and report destruction; preparation and waiting until all readers are ready excluded | 1/10/100 journals |
| `parent_admission` | Admit owned running reports into a fresh Running parent FSM | Real FSM dispatch, frontier incorporation and coverage/lifecycle folding; record cloning, topology/FSM construction and validation excluded | 1/10/100 reporting children plus one quiet sink |
| `parent_publication` | Commit 16 serial owner-scoped pipeline records | Actual publication scope/queue, frontier capture, provider append/encoding/write/flush and receipt observation; journal creation, input frontier and event creation excluded | Memory/disk; 1/32/1,024 inherited coordinates |

`w` labels count external witnesses/input coordinates; the destination contributes
an additional coordinate. `p` labels describe the business payload body, not full
record bytes. Dispatch uses the same 64 ordinary physical frames in both cases;
it does not turn them into an atomic group. That experiment does not change
production batching or constitute an end-to-end speedup claim.

Disk cases use prepared files and warm OS/definition caches. They make no cold-device
claim. Report discovery includes reader opening; `reader_next` deliberately measures
the subsequent iteration separately. The ready-handoff case deliberately excludes
discovery, which has its own group. The parent-admission case measures existing
Running-report handling, without terminal transitions or stage execution.

The runtime has **two async workers and a maximum of two blocking workers**. The
eight-reader dispatch case therefore creates controlled contention inside Criterion.
It includes no unrelated workload, wall-clock sleeps or external profiler. Normal
reader fallback timers remain production behaviour. No tracing subscriber is installed.

## Work validation and iteration isolation

Fixture records come from successful real journal appends. Frame fixtures are
checked against append receipt identities and order before timing. Every timed
scan checks completed record counts/order; every discovery iteration checks exact
report identities, per-journal covered positions, scanned counts and selected counts.
Decoder iterations check frame/record counts and sequence totals. Parent cases
check coverage, admitted clocks, expected state/actions and committed receipts.
Correctness checks remain in the harness, outside timing where possible.

Each iteration receives a fresh cursor, frontier, FSM or destination journal as
appropriate. Accepted publications settle and read tasks are cancelled and joined
before another iteration. Async work has a 30-second invalid-sample deadline;
failure aborts the benchmark instead of returning zero or partial work. The target
has 59 cases. Criterion's `--test` mode exercises each case once without collecting
a performance baseline.

Fixtures are lazy: filtering to one case does not populate unrelated journals.
Defaults are 20 samples, 300 ms warm-up and a one-second requested measurement
window per case. Criterion extends collection for slow operations. These are
exploratory component baselines; compare raw samples and uncertainty rather than
inventing a regression threshold from one run. Standard Criterion options can
increase warm-up, measurement time and sample size for the affected case.

## Baselines

Use the commands in [README.md](README.md). Keep the same build profile, environment
variables, features, runtime limits and measurement contract for comparisons.
`--profile test` measures unoptimised paths used by CI. Cargo's default bench profile
is a separate optimised series and must use a different baseline name. A report from
either profile does not qualify the other profile.

Criterion retains estimates, raw iteration counts/timings and benchmark metadata in
`target/criterion/**/<baseline>/`. Preserve those artifacts with the revision,
lockfile, source hashes and host/toolchain identity before changing the component.
Use `--baseline <name>` for comparisons; do not overwrite the reference with a
candidate. Each relevant optimisation must show its named Criterion result and keep
the completed-work checks passing. Full CI is outside that optimisation loop.

The initial unoptimised reference and its raw Criterion data are recorded in
[145h component baseline](../../../obzenflow-improvement-proposals/content/planning/obzenflow/P1/evidence/145h-component-baseline-2026-09-26.md).
`scripts/capture_component_baseline.py --help` describes the artifact capture command;
it requires all 59 cases and preserves Criterion's data without collecting new timings.
