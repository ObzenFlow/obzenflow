// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Regenerate reference frames after an intentional journal-format change.

use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::ChainPayload;
use obzenflow_infra::testing::journal::encode_record_fixture;
use std::{fs, path::Path};

pub(super) fn run(
    root: &Path,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match args {
        [] => {}
        [arg] if super::is_help(arg) => {
            println!("usage: cargo xtask regenerate-journal-fixtures");
            println!("Replace the two reference .frame files using their JSON records.");
            return Ok(());
        }
        _ => return Err("regenerate-journal-fixtures accepts no options".into()),
    }

    let fixtures = root.join("crates/obzenflow_infra/src/journal/disk/codec/fixtures");
    let mut replacements = Vec::new();
    // Parse and encode both records before replacing either reference frame.
    for name in ["observations", "plain"] {
        let json = fs::read(fixtures.join(format!("{name}.json")))?;
        let record: JournalRecord<ChainPayload> = serde_json::from_slice(&json)?;
        let bytes = encode_record_fixture(&record)?;
        replacements.push((fixtures.join(format!("{name}.frame")), bytes));
    }
    for (path, bytes) in replacements {
        fs::write(&path, &bytes)?;
        println!("Regenerated {} ({} bytes)", path.display(), bytes.len());
    }
    Ok(())
}
