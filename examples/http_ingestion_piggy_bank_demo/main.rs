// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod support;

#[cfg(test)]
fn main() -> anyhow::Result<()> {
    support::runner::run_example_in_tests()
}

#[cfg(not(test))]
fn main() -> anyhow::Result<()> {
    support::runner::run_example()
}
