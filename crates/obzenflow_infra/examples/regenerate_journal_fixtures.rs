// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Regenerate reference frames after an intentional journal-format change.

use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::ChainPayload;
use obzenflow_infra::testing::journal::encode_record_fixture;
use std::{env, fs, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [] => {}
        [arg] if matches!(arg.as_str(), "-h" | "--help" | "help") => {
            println!("usage: cargo run -p obzenflow_infra --features test-support --example regenerate_journal_fixtures");
            println!("Replace the reference .frame files using their JSON records.");
            return Ok(());
        }
        _ => return Err("regenerate_journal_fixtures accepts no options".into()),
    }

    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/journal/disk/codec/fixtures");
    let mut replacements = Vec::new();
    // Parse and encode every record before replacing any reference frame.
    for name in ["observations", "plain", "recovery"] {
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
