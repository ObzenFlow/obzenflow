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
cargo test --workspace
cargo test --workspace --all-features

# Dependency policy checks (CI runs these)
cargo deny --all-features check
cargo machete --skip-target-dir
```

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
