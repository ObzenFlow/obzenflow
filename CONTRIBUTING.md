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

ObzenFlow uses `cargo-nextest` profiles for CI. Pull requests run `ci-fast`; pushes to `main` and manual workflow dispatch run `ci-full`.

Classify time-sensitive tests before adding sleeps or timeouts:

- **Semantic timing assertion**: the test asserts time-driven behaviour. Prefer `tokio::test(start_paused = true)` and `obzenflow_runtime::testing::TestClock` when the production code uses Tokio time.
- **Synchronisation barrier**: the test waits for work to become observable. Prefer `JournalProbe`, `MetricsBarrier`, channel/notify readiness, or a state receiver instead of fixed sleeps.
- **Hang guard**: the timeout only bounds a test that could otherwise hang. Keep it as wall-clock `tokio::time::timeout`, and add a nextest override if it legitimately exceeds the profile default.
- **Benchmark**: keep benchmark timing out of `ci-fast`; benchmark code belongs under the benchmark crate and `cargo bench` flow.

Use shared-resource groups in `.config/nextest.toml` when tests contend for a hard-coded port, hard-coded disk journal path, process-global singleton, or other resource that cannot be safely parallelised. Use per-test `slow-timeout` overrides only for tests with a documented reason to exceed the profile default.

Tier long-running e2e tests deliberately:

- Use a nextest profile filter when the test should still run automatically in `ci-full`.
- Use `#[ignore]` when the test should compile normally but run only on demand.
- Use `cfg(feature = "e2e")` only when the whole test binary needs external services, credentials, heavyweight optional dependencies, or compile-time-gated setup.

Production CI must not use `--all-features` for tests. The workflow enumerates production features explicitly and verifies that list against the root `Cargo.toml` with `cargo metadata --no-deps`. Test-only features such as `test-support` are exercised by targeted commands.

Full-application tests that launch `FlowApplication` under nextest must pass explicit argv through the builder:

```rust
FlowApplication::builder()
    .with_cli_args(["obzenflow"])
    .run_async(flow_definition)
    .await
```

The `obzenflow_runtime::testing` helpers operate on envelope clocks and journal state. Do not use payload `correlation_id` as a causal-ordering key; under fan-out, multiple derived events may intentionally share the same correlation id.

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
