// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// Each integration executable uses the subset of these archive helpers it needs.
#![allow(dead_code)]

use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_infra::journal::disk::log_record::LogRecord;

/// Decode the supported JSONL export without allowing a malformed typed row to
/// disappear from an acceptance oracle. System rows are validated and skipped;
/// every other row must be a complete chain-event log record.
pub fn chain_events(jsonl: &str) -> Vec<ChainEvent> {
    jsonl
        .lines()
        .enumerate()
        .filter_map(
            |(index, line)| match serde_json::from_str::<LogRecord<ChainEvent>>(line) {
                Ok(record) => Some(record.authored()),
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
        .map(|line| {
            let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
            value["envelope"]
                .as_object_mut()
                .unwrap()
                .remove("observability");
            value
        })
        .collect()
}
