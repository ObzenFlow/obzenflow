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
cargo nextest run --workspace --profile ci-fast
cargo nextest run --workspace --profile ci-full
cargo nextest run --workspace --profile ci-fast --features console,http-pull,ai

# Dependency policy checks (CI runs these)
cargo deny --all-features check
cargo machete --skip-target-dir
```

## Test authoring

ObzenFlow uses `cargo-nextest` as the supported workspace test runner. The CI test matrix is intentionally small and maps directly to `.github/workflows/ci.yml`:

| CI job / matrix entry | Pull requests | Pushes to `main` and manual dispatch | What it proves |
| --- | --- | --- | --- |
| `test` / `default` | `ci-fast`, no extra features | `ci-full`, no extra features | The workspace passes without optional production features. |
| `test` / `production-features` | `ci-fast`, `--features console,http-pull,ai` | `ci-full`, `--features console,http-pull,ai` | The explicitly supported production feature set passes. |
| `test-test-support` | `ci-fast`, `--features test-support`, targeted integration-test binaries | `ci-full`, `--features test-support`, targeted integration-test binaries | The test-only support helpers compile and work in real tests. |

`ci-fast` is the required PR gate. `ci-full` is the merge/manual gate and includes the long-running binaries excluded from `ci-fast`. The `production-features` entry also runs a guard that compares the workflow feature list to the root `Cargo.toml` production features; if it fails, either update the workflow matrix or mark the feature as intentionally test-only in the guard allowlist.

Expanded, the normal PR test matrix is:

```bash
cargo nextest run --workspace --locked --profile ci-fast
cargo nextest run --workspace --locked --profile ci-fast --features console,http-pull,ai
```

The separate `test-test-support` job is narrower than the normal matrix. It exists only to prove that test-only helpers behind `--features test-support` still compile and work. It runs these integration-test binaries:

- `stateful_metrics_integration_test`: stateful flow metrics coverage.
- `metrics_exporter_integration_test`: metrics exporter integration coverage.
- `rate_limiter_integration_test`: rate-limiter integration coverage that uses test-support helpers.
- `divergence_mid_flight_abort_test`: paused-time contract-evaluation abort coverage (system journal assertions).
- `cycle_unified_guard_test`: paused-time cycle guard / SCC max-iteration coverage.
- `cycle_convergence_eof_gating_test`: paused-time cycle EOF gating coverage.
- `flowip_114n_ui_tests`: compile-fail coverage for test-support helper contracts (trybuild).

If you change `obzenflow_runtime::testing`, the `test-support` feature, or one of those three files, also run:

```bash
cargo nextest run --workspace --locked --profile ci-fast --features test-support \
  -E 'binary(/^(stateful_metrics_integration_test|metrics_exporter_integration_test|rate_limiter_integration_test|divergence_mid_flight_abort_test|cycle_unified_guard_test|cycle_convergence_eof_gating_test|flowip_114n_ui_tests)$/)'
```

Before opening a PR that touches runtime or tests, run the same profile that CI will run for your branch:

```bash
cargo nextest run --workspace --locked --profile ci-fast
cargo nextest run --workspace --locked --profile ci-fast --features console,http-pull,ai
```

Use `ci-full` locally when you change slow e2e coverage, nextest filters, test groups, or timeout policy.

Classify time-sensitive tests before adding sleeps or timeouts:

- **Semantic timing assertion**: the test asserts time-driven behaviour. Prefer `tokio::test(start_paused = true)` and `obzenflow_runtime::testing::TestClock` when the production code uses Tokio time.
- **Synchronisation barrier**: the test waits for work to become observable. Prefer `JournalProbe`, `MetricsBarrier`, channel/notify readiness, or a state receiver instead of fixed sleeps.
- **Hang guard**: the timeout only bounds a test that could otherwise hang. Keep it as wall-clock `tokio::time::timeout`, and add a nextest override if it legitimately exceeds the profile default.
- **Benchmark**: keep benchmark timing out of `ci-fast`; benchmark code belongs under the benchmark crate and `cargo bench` flow.

Use shared-resource groups in `.config/nextest.toml` when tests contend for a hard-coded port, hard-coded disk journal path, process-global singleton, or other resource that cannot be safely parallelised. Add the group selector in the same PR as the test that needs it. Use per-test `slow-timeout` overrides only for tests with a documented reason to exceed the profile default.

Tier long-running e2e tests deliberately:

- Use a nextest profile filter when the test should still run automatically in `ci-full`.
- Use `#[ignore]` when the test should compile normally but run only on demand.
- Use `cfg(feature = "e2e")` only when the whole test binary needs external services, credentials, heavyweight optional dependencies, or compile-time-gated setup.

Production CI must not use `--all-features` for tests. The workflow enumerates production features explicitly and verifies that list against the root `Cargo.toml` with `cargo metadata --no-deps`. Test-only features such as `test-support` are exercised by targeted commands.

When adding a root Cargo feature, decide whether it is production or test-only:

- Production features must be added to the `production-features` matrix entry in `.github/workflows/ci.yml`.
- Test-only or e2e-only features must be added to the workflow guard's non-production allowlist.

Full-application tests that launch `FlowApplication` under nextest must pass explicit argv through the builder:

```rust
FlowApplication::builder()
    .with_cli_args(["obzenflow"])
    .run_async(flow_definition)
    .await
```

The `obzenflow_runtime::testing` helpers operate on envelope clocks and journal state. Do not use payload `correlation_id` as a causal-ordering key; under fan-out, multiple derived events may intentionally share the same correlation id.

Prefer these primitives over fixed sleeps when writing tests:

- `JournalProbe` (stage data journals): assert `expect_event(n)`, `expect_event_at_cycle_depth(...)`, `expect_event_observing_clock_component(...)`, `expect_event_child_of(...)`, `expect_event_authored_by(...)`, and paused-time `expect_no_event_during(...)`.
- `JournalSnapshot` (captured journals): assert append-order vs causal-order properties via `JournalOrder`, `SequenceMatchMode`, `JournalExpectation`, `assert_happens_before`, and `assert_concurrent`.
- `MetricsBarrier` (metrics/exporter): wait for exported watermarks without reading files or adding barrier sleeps.

FLOWIP-114n migration patterns:

- Cycle timing: run the test under paused Tokio time, drive virtual time through `TestClock`, settle the scheduler, then assert the expected SCC/depth with `JournalProbe::expect_event_at_cycle_depth(...)`.
- Fan-in readback: use handle-level `wait_for_completion()` when the test needs a deterministic completion barrier, then read the causally ordered stage journal and assert vector-clock components directly. A clock component means "downstream of this writer", not "delivered from this upstream edge".
- Journal snapshots: use `JournalSnapshot` when the test needs a stable append-order or causal-order boundary. Capture after the scheduler is settled; later appends do not change the snapshot.

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
