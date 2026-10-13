// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod support;

use obzenflow::application::{Banner, Presentation};

fn main() -> anyhow::Result<()> {
    let presentation = Presentation::new(Banner::new("CSV Demo: Support SLA"));

    support::flow::run_example(presentation, std::env::args_os())
}
