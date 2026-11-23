// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::stages::sources::{CsvRowDecoder, CsvSource};

#[test]
fn csv_handler_builds_before_file_exists() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("rows.csv");
    let source = CsvSource::builder(CsvRowDecoder)
        .path(&path)
        .chunk_size(1)
        .select_columns(["missing"])
        .build()?;
    assert!(format!("{:?}", source.clone()).contains("CsvSource"));
    assert!(!path.exists());
    Ok(())
}
