// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

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
                Ok(record) => Some(record.event),
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
