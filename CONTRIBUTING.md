# Contributing to ObzenFlow

Thanks for your interest in contributing!

By participating, you agree to follow the Code of Conduct (`CODE_OF_CONDUCT.md`).

## Sign-off (DCO)

We use the **Developer Certificate of Origin (DCO)** instead of a Contributor License Agreement (CLA).

- All commits in a PR must be signed off.
- Sign off your commits with: `git commit -s`
- The sign-off line looks like: `Signed-off-by: Your Name <your.email@example.com>`

The full text is in `DCO.md`.

### Fixing missing sign-offs

- Amend the most recent commit: `git commit --amend -s`
- Sign off all commits on your branch (interactive): `git rebase -i --signoff main`

## Contribution provenance

If you are employed, you are responsible for ensuring your employer's intellectual property policies permit your contribution. Many employment contracts include IP assignment clauses that may cover work done outside of office hours or on personal equipment.

If your employer requires a corporate sign-off or approval for open source contributions, please obtain it before submitting a pull request.
By signing off your commits (DCO), you attest you have the right to contribute the work under the project's license terms.

## Development setup

### Prerequisites

- Rust toolchain (see `rust-toolchain.toml`)

### Common commands

```bash
# Build
cargo build --workspace

# Format + lint
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Tests
cargo xtask test                              # all six correctness lanes, native
cargo xtask test --lane production-features    # explicitly partial coverage
cargo xtask test --lane performance            # complete performance comparison

# Dependency policy checks (CI runs these)
cargo deny --all-features check
cargo machete --skip-target-dir
```

## Test authoring

`cargo xtask test` is the shared local and CI acceptance command. Cargo/Nextest
execute correctness tests, Criterion supplies performance measurements, and the
existing PostgreSQL coordinator owns its disposable services. Formatting, Clippy
and repository policy checks remain separate requirements.

| Lane | Required coverage |
| --- | --- |
| `default` | Whole workspace, no extra features |
| `production-features` | Whole workspace with every root production feature, discovered from Cargo metadata, including `cli` |
| `test-support` | Whole workspace with `test-support,obzenflow_infra/warp-server` |
| `journal-fixtures` | The intentionally ignored current-schema codec fixtures |
| `doctest` | Whole-workspace documentation tests |
| `postgres` | `cargo xtask postgres test`, including its isolated service lifecycle |
| `performance` | Selected Criterion workloads and required comparison/validity gates |

Omitting `--lane` requests all six correctness lanes. Repeat `--lane` to select
exactly those lanes; the result certifies only that declared scope. Report version
4 records unrequested lanes and dependency preparation separately. Default
success supplies no performance qualification. Independent lanes continue
after failures. Missing tools/services, unfinished reports, unexpected skips,
changed source and incomplete coverage cannot pass. Installing the pinned
`cargo-nextest` version from `.config/validation.toml` is required for native
Nextest lanes. CI installs that same pinned release.

Every PR update requires the six correctness lanes and the existing formatting,
Clippy and policy checks. Pushes to `main` and manual CI dispatches also require the
complete performance lane. Changes claiming performance improvements or changing
the benchmark driver, baseline or comparison policy require an explicit comparison
before merge, identifying the measured revision. Publication and release dry-runs
require successful main-push CI for the exact release SHA, including an executed,
passing performance job; a skipped job cannot qualify a release.

All acceptance uses `ci-fast`, four Nextest process slots, no fail-fast and zero
retries, on pull requests and on `main`. The expensive journal proofs share two
slots and can overlap. All 5,000-record workloads and three reporting interval
variants remain selected. The small archive-reference and result-integrity
regressions have the earliest priority. An exclusion must name its coverage
owner in `.config/validation.toml`; do not maintain binary allowlists.

The command forwards early failure output while independent tests continue.
Live diagnostics have a 16 KiB display budget; complete output remains in each
lane's logs and JUnit report.

Detected test-process leaks fail acceptance. Nextest retains its 200 ms wait for
captured stdout/stderr to close after a test exits; `LEAK-FAIL` includes the test
identity in live output, the final failure summary and JUnit diagnostics. The
native validator rejects missing or weakened leak policy, including overrides.
Join owned children and close inherited output handles before returning. A clean
rerun does not explain an earlier leak, and this detector does not detect every
kind of resource leak.

`ci-full`, stress and retry runs are supplementary diagnostics. A later passing
attempt never erases a failure and does not replace the shared acceptance run.
Runner timeouts protect against hangs; they are not performance thresholds.

Reports live at `target/test-runs/<run-id>/report.json`, alongside exact selections,
features, invocation/exit records, phase output and JUnit identities. Successful
output is retained too. CI uploads the complete report directory. A source digest
includes tracked and untracked non-ignored files, so a result identifies the dirty
checkout actually tested. Both initial and final checkout identities are retained.
Committing already-tested changes preserves acceptance when file paths, contents,
executable modes and symlink targets remain unchanged. Later edits require
validation of the affected scope.

Run the command directly on your development machine. CI runs it directly on its
Linux runner with the same feature discovery, profile, concurrency, retry policy
and report validation. Local Linux containers or VMs are not a prerequisite.
The report records the actual platform; sharing a test contract does not claim
identical hardware. Diagnose concrete failing orderings and durable outcomes
before attributing a failure to platform differences.

The existing `test-support` CI job starts its validation command with a fresh,
per-run Cargo dependency home on both PRs and `main`. Rust and the pinned Nextest
are installed first and remain on `PATH`; the workspace target directory is
retained. The native validator prepares the locked dependency set explicitly, so
an inherited registry cache is an optimisation rather than a prerequisite for
its offline leak controls. Cargo may download host dependencies while building
`xtask` before the native validator starts.

To exercise the same cold-dependency invocation locally without deleting your
normal Cargo cache or build output:

```sh
validation_cargo_home="$(mktemp -d "${TMPDIR:-/tmp}/obzenflow-cargo.XXXXXX")"
CARGO_HOME="$validation_cargo_home" cargo xtask test --lane test-support
```

Keep the installed Cargo/Nextest tool directory on `PATH`. This command creates a
separate dependency cache; it does not install a second toolchain or use a new
target directory. The result still certifies only the requested test-support
scope. Before directly invoking raw Cargo/Nextest test targets that include the
validator's leak controls, run `cargo fetch --locked` with the same `CARGO_HOME`.
The shared native command owns that preparation itself.

The PostgreSQL lane retains the existing database-service coordinator and its
prerequisites. It does not containerise the Rust test suite. Each invocation keeps
`coordinator.json` with source/run identity, required command outcomes, setup,
dependency preparation and cleanup. An unsuccessful xtask unit-conformance
command remains a required failure while independent PostgreSQL targets run;
service-dependent failures and interruption still stop subsequent dependent work.
The report preserves failed commands alongside unfinished obligations. Cargo
command status identifies the target, not the individual failed assertion; raw
test output retains that diagnostic detail.

Audit environmental dependencies before declaring them: remove incidental
operations, retain ownership through completion, isolate concurrent instances,
and identify the assertion that needs each operation. Use `--server-port 0` and
the actual bound address for application fixtures; TOML still requires a nonzero
port. A bind/drop/rebind reservation owns nothing.

Policy version 2 declares exceptional prerequisites by exact binary/test identity,
applicable lane inventories, typed capabilities and assertion rationale in
`.config/validation.toml`. Features and scheduling groups do not imply permissions.
Keep a compound case's complete prerequisites; split independent modes when their
requirements differ. Declarations are checked against the selected inventory.
Undeclared tests are not thereby certified independent of their environment.

The validation owner bounds operation-specific probes in the same execution
context, using system temporary storage, workspace target storage or the actual
case-artifact root as declared. A writable directory does not prove hard-link or
mode-change support. Probes release their resources; each fixture still owns its
own listener, directory and children. A successful probe followed by a test
failure remains a failed execution.

`prerequisites.json` retains operation, resource scope, OS error, affected case
identities and duration. Native report version 4 distinguishes completed probes
reporting unavailable capabilities from infrastructure failures that leave the
capability unknown. Launch, execution and decoding failures retain the phase,
executable and underlying error; process errors also have a `*.error.json`
artifact and are printed immediately. Validation owns a private executable copy
before Cargo builds and uses it for both probes and PostgreSQL delegation.
`coverage.json` retains the original required inventory,
runnable and blocked cases, and executed failures. Unaffected cases continue in
the normal concurrent scheduler; any blocked required case leaves the lane
incomplete and returns nonzero. Blocked cases are separate from intentional
exclusions and never receive fabricated passing JUnit results.

Classify time-sensitive tests before adding sleeps or timeouts:

- **Semantic timing assertion**: the test asserts time-driven behaviour. Prefer `tokio::test(start_paused = true)` and `obzenflow_runtime::testing::TestClock` when the production code uses Tokio time.
- **Synchronisation barrier**: the test waits for work to become observable. Prefer `JournalProbe`, `MetricsBarrier`, channel/notify readiness, or a state receiver instead of fixed sleeps.
- **Hang guard**: use nextest to bound the whole test, with an override when the workload needs more than the profile default. Keep `tokio::time::timeout` for specific operations that need their own bound.
- **Performance requirement**: benchmark code belongs in the benchmark crate; the shared `performance` lane owns the selected Criterion comparison gate. Keep arbitrary whole-flow speed assertions out of correctness tests.

The current incident/scale timeout inventory separates these responsibilities:

| Boundary | Limit and meaning |
| --- | --- |
| Nextest ordinary acceptance | 60-second hang watchdog; failure output is retained immediately and at the end |
| Full 5k Prometheus variants, including typed 5k | Warning at 300 seconds, termination at 600 seconds; rate-limited execution and complete archive assertions remain required |
| Archive observation proof and composed Studio proof | 120-second outer watchdog; existing phase output identifies build, run, export, admission/assertions and settlement |
| 10k replay proof | 120-second guards for live and replay with callback progress; 300-second outer watchdog also covers comparison, whose operation has separate Criterion coverage |
| Other scale binaries | 120-second outer watchdog |
| Cycle convergence | Five-second phase guards and ten-second completion guard; the separately configured five-second graceful deadline is product behaviour, verified by its durable terminal cause |
| CLI host/viewer | Two-second HTTP operations and fifteen-second discovery/fact/exit guards; absence assertions require owned completion or a controlled viewer decision |
| Composed Studio cases | 15-second application guard, 60 seconds for the 100-stage stress case, and five-second delivery guard; the remaining one/two-second reader controls guard individual acknowledgements |
| Metrics finalisation | The five-second attempt is the product's existing drain budget; causal completion/export/drain ordering remains a correctness obligation |
| Effect cancellation | Configured 100 ms graceful deadline with a 900 ms scheduling allowance for observing the aborted future; ancillary cleanup has its own five-second guard |
| Stateful emission, limiter and breaker clocks | Interval and cooldown assertions verify configured behaviour; they are not throughput thresholds |
| Criterion operations | Thirty-second invalid-sample guards on asynchronous completion; expired work cannot contribute a sample |
| Validation process owner | 1,800-second infrastructure watchdog, then up to sixty seconds for owned-child/service teardown; successful exit during teardown still leaves the check incomplete |

These watchdogs use workload-specific completion budgets. Expensive
correctness timings describe feedback cost, not an accepted performance envelope.
CLI failures retain bounded log tails and their phase in JUnit. Replay captures
source/sink callback counts; Studio captures pipeline state, source progress,
sink callbacks, frame count and a bounded cursor prefix without rescanning
journals or waiting on consumer locks. Controlled source/sink stalls verify
attribution before releasing and completing the same flow. These diagnostic
controls do not establish the cause of a historical timeout. Failed fixtures
remain under the lane's `cases` directory; successful phase output stays in JUnit.

Use shared-resource groups in `.config/nextest.toml` when tests contend for a hard-coded port, hard-coded disk journal path, process-global singleton, or other resource that cannot be safely parallelised. Add the group selector in the same PR as the test that needs it. Use per-test `slow-timeout` overrides only for tests with a documented reason to exceed the profile default.

Tier long-running e2e tests deliberately:

- Keep automated regressions in the shared native acceptance selection, using `ci-fast` locally and on PRs and `main`. Give slow tests an explicit time budget instead of excluding them from PRs.
- Use `#[ignore]` when the test should compile normally but run only on demand.
- Use `cfg(feature = "e2e")` only when the whole test binary needs external services, credentials, heavyweight optional dependencies, or compile-time-gated setup.

Production tests must not use `--all-features`. The shared command discovers production features from the root `Cargo.toml` through Cargo metadata. Test-only features such as `test-support` are exercised by their own workspace lane.

When adding a root Cargo feature, decide whether it is production or test-only:

- Production features join the `production-features` lane automatically.
- A new test-only feature requires an explicit classification and coverage owner in `xtask/src/validation/plan.rs`.

Full-application tests that launch `FlowApplication` under nextest must pass explicit argv through the builder:

```rust
FlowApplication::builder()
    .with_cli_args(["obzenflow"])
    .run_async(flow_definition)
    .await
```

The `obzenflow_runtime::testing` helpers operate on envelope clocks and journal state. Do not use payload `correlation_id` as a causal-ordering key; under fan-out, multiple derived events may intentionally share the same correlation id.

Prefer these primitives over fixed sleeps when writing tests:

- `JournalProbe` (stage data journals): assert `expect_event(n)`, `expect_event_at_cycle_depth(...)`, `expect_event_observing_clock_component(...)`, `expect_event_child_of(...)`, `expect_event_authored_by(...)`, using their positive observation semantics.
- `JournalSnapshot` (captured journals): assert append-order vs causal-order properties via `JournalOrder`, `SequenceMatchMode`, `JournalExpectation`, `assert_happens_before`, and `assert_concurrent`.
- `MetricsBarrier` (metrics/exporter): wait for exported watermarks without reading files or adding barrier sleeps.

The 0.2.6 test-support API retires `TestClock::settle_scheduler` and the three
`JournalProbe::expect_no_event_*` methods. Equal counts, yield loops and advancing
virtual time cannot establish that all work finished. For finite absence, join the
relevant producers **and accepted publications**, then read their journals. A
cancelled append waiter can leave a retained publication. For a completed prefix,
observe each covered journal through its exclusive `SettledRunPrefix` end position;
`TailRead::Pending` is neither EOF nor settlement. For a timer or decision assertion,
witness the actual operation becoming armed or reaching that decision before
checking non-progress, and keep a positive boundary assertion. One fan-out child
or fan-in input cannot close the others' scope. Clock advance/sleep and positive
probe waits remain available.

Migration patterns:

- Cycle ordering: hold the relevant acknowledgement or reader boundary, observe the committed control admission, then release outstanding work and inspect durable SCC/depth and terminal cause. Use paused time for deadline semantics separately; advancing a product deadline is not evidence that real I/O settled.
- Fan-in readback: use handle-level `wait_for_completion()` when the test needs a deterministic completion barrier, then read the causally ordered stage journal and assert vector-clock components directly. A clock component means "downstream of this writer", not "delivered from this upstream edge".
- Journal snapshots: use `JournalSnapshot` when the test needs a stable append-order or causal-order boundary. Complete the covered producers and accepted publications first; a snapshot does not itself close that scope. Later appends do not change the captured records.

## Pull request guidelines

- Keep changes focused (one feature/fix per PR when possible).
- Add tests for new behavior and bug fixes.
- Update docs/examples when behavior or APIs change.
- Prefer opening an issue (or a design proposal) before large changes.

## Source headers (SPDX)

All Rust source files (`*.rs`) must start with an SPDX header block.

Use:

```rust
// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev
```

Do not add individual names to per-file headers. Attribution lives in `NOTICE`, `LICENSE-MIT`, and `LICENSE-APACHE`.

## License

By contributing, you agree that your contributions will be licensed under the project’s dual license (MIT OR Apache-2.0).

## Security

Please do not open public issues for security vulnerabilities. See `SECURITY.md`.
