// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::path::{Path, PathBuf};

pub struct FixturePaths {
    pub customers_csv: PathBuf,
    pub tickets_csv: PathBuf,
}

/// Resolve configuration only. Supervised live opening validates input files;
/// strict replay must also get through this entry point when both are absent.
pub fn paths(fixtures_dir: &Path) -> FixturePaths {
    FixturePaths {
        customers_csv: fixtures_dir.join("customers.csv"),
        tickets_csv: fixtures_dir.join("tickets.csv"),
    }
}
