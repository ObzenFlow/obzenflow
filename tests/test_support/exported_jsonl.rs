// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Each integration executable uses the subset of these archive helpers it needs.
#![allow(dead_code)]

use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_infra::journal::disk::log_record::LogRecord;
use serde::Deserialize;

/// Read a placement key from an already exported row without re-decoding its
/// payload. This projection does not admit evidence; typed checks remain with
/// the caller and the public export validates the stored records.
pub fn commitment(row: &serde_json::Value) -> obzenflow_core::event::JournalCommitRef {
    use obzenflow_core::event::provenance::JournalProvenance;
    use obzenflow_core::event::{CausalCoordinate, JournalCommitRef};
    use obzenflow_core::EventId;

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

/// Decode the supported JSONL export without allowing a malformed typed row to
/// disappear from an acceptance oracle. System rows are validated and skipped;
/// every other row must be a complete chain-event log record.
pub fn chain_events(jsonl: &str) -> Vec<ChainEvent> {
    chain_records(jsonl)
        .into_iter()
        .map(|record| record.authored())
        .collect()
}

/// Retain commitment evidence when an oracle checks exact delivery subjects.
pub fn chain_records(jsonl: &str) -> Vec<LogRecord<ChainEvent>> {
    jsonl
        .lines()
        .enumerate()
        .filter_map(
            |(index, line)| match serde_json::from_str::<LogRecord<ChainEvent>>(line) {
                Ok(record) => Some(record),
                Err(chain_error) => {
                    serde_json::from_str::<LogRecord<SystemEvent>>(line).unwrap_or_else(
                        |system_error| {
                            panic!(
                                "export row {} is neither a ChainEvent nor a SystemEvent: \
                                 chain decode: {chain_error}; system decode: {system_error}",
                                index + 1
                            )
                        },
                    );
                    None
                }
            },
        )
        .collect()
}

/// Rewrite only optional attachments in a fixed, completed current-schema
/// archive. The physical frames, full provenance, payloads and member order
/// remain the oracle; only length/checksum framing is recomputed.
pub fn omit_observations(run: &std::path::Path, keep: impl Fn(usize) -> bool) -> usize {
    obzenflow_infra::testing::journal::omit_observations(run, keep)
        .expect("rewrite current-format fixture")
}

pub fn protected_records(jsonl: &str) -> Vec<serde_json::Value> {
    jsonl
        .lines()
        .map(|line| without_observations(serde_json::from_str(line).unwrap()))
        .collect()
}

pub fn without_observations(mut value: serde_json::Value) -> serde_json::Value {
    value["envelope"]
        .as_object_mut()
        .unwrap()
        .remove("observability");
    value
}
