// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::provenance::JournalProvenance;
use obzenflow_core::event::{CausalCoordinate, JournalCommitRef};
use obzenflow_core::EventId;
use serde::Deserialize;

/// Read a placement key from an already exported row without re-decoding its
/// payload. This projection does not admit evidence; typed checks remain with
/// the caller and the public export validates the stored records.
pub fn commitment(row: &serde_json::Value) -> JournalCommitRef {
    let provenance = &row["envelope"]["provenance"];
    let journal = JournalProvenance::deserialize(&provenance["journal"])
        .expect("complete exported journal provenance");
    JournalCommitRef {
        run_id: journal.run_id,
        journal_writer_id: journal.journal_writer_id,
        sequence: journal
            .vector_clock
            .get(&CausalCoordinate::new(journal.journal_writer_id)),
        event_id: EventId::deserialize(&provenance["event"]["id"])
            .expect("exported event identity"),
    }
}
