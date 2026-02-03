// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{bail, Result};
use std::path::PathBuf;

pub struct FixturePaths {
    pub customers_csv: PathBuf,
    pub tickets_csv: PathBuf,
}

pub fn paths() -> Result<FixturePaths> {
    let fixtures_dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/csv_demo_support_sla/fixtures");

    let customers_csv = fixtures_dir.join("customers.csv");
    if !customers_csv.exists() {
        bail!("Missing fixture file: {}", customers_csv.display());
    }

    let tickets_csv = fixtures_dir.join("tickets.csv");
    if !tickets_csv.exists() {
        bail!("Missing fixture file: {}", tickets_csv.display());
    }

    Ok(FixturePaths {
        customers_csv,
        tickets_csv,
    })
}
