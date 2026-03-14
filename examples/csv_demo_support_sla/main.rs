// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod support;

use obzenflow_infra::application::{Banner, Presentation};

fn main() -> anyhow::Result<()> {
    let paths = support::flow::DemoPaths::resolve()?;

    let presentation = Presentation::new(
        Banner::new("CSV Demo: Support SLA")
            .config("customers_csv", paths.customers_csv.display())
            .config("tickets_csv", paths.tickets_csv.display())
            .config("output_csv", paths.output_csv.display()),
    );

    support::flow::run_example(paths, presentation)
}
