// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::stages::sources::{
    CsvRowDecoder, CsvSource, FiniteSourceConnector, PositionedFiniteReader,
    SourceReaderInitContext,
};
use obzenflow_core::StageId;

fn context() -> SourceReaderInitContext {
    SourceReaderInitContext {
        stage_id: StageId::new(),
        stage_name: "rows".into(),
        flow_name: "csv_lifecycle".into(),
    }
}

#[test]
fn csv_configuration_is_cold_and_opens_independent_readers() -> anyhow::Result<()> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("rows.csv");
    // Building, cloning and inspecting configuration work before the file exists.
    let source = CsvSource::builder(CsvRowDecoder)
        .path(&path)
        .chunk_size(1)
        .build()?;
    let copy = source.clone();
    assert!(format!("{copy:?}").contains("CsvSource"));
    assert!(copy.open(context()).is_err());
    std::fs::write(&path, "name\nalice\nbob\n")?;
    let mut first = source.open(context())?;
    let mut second = copy.open(context())?;
    let alice = first.next()?.unwrap();
    let bob = first.next()?.unwrap();
    assert_eq!(second.next()?.unwrap(), alice);
    assert!(first.next()?.is_none());
    assert_eq!(second.next()?.unwrap(), bob);
    assert!(second.next()?.is_none());

    // Header validation is acquisition work too, never application wiring.
    let invalid = CsvSource::builder(CsvRowDecoder)
        .path(&path)
        .select_columns(["missing"])
        .build()?;
    assert!(invalid.open(context()).is_err());
    Ok(())
}
