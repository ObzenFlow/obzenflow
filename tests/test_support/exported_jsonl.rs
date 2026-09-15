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
    use serde_json::Value;
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json")).unwrap()).unwrap();
    let mut files = std::collections::BTreeSet::new();
    files.insert(manifest["system_journal_file"].as_str().unwrap());
    for stage in manifest["stages"].as_object().unwrap().values() {
        for field in ["data_journal_file", "error_journal_file"] {
            files.insert(stage[field].as_str().unwrap());
        }
    }
    let mut ordinal = 0;
    let mut removed = 0;
    for file in files {
        let path = run.join(file);
        let original = std::fs::read_to_string(&path).unwrap();
        let mut rewritten = String::new();
        for line in original.lines() {
            let mut fields = line.splitn(3, ':');
            let size: usize = fields.next().unwrap().parse().unwrap();
            let checksum: u32 = fields.next().unwrap().parse().unwrap();
            let body = fields.next().unwrap();
            assert_eq!(size, body.len());
            assert_eq!(checksum, crc32fast::hash(body.as_bytes()));
            let mut frame: Value = serde_json::from_str(body).unwrap();
            let records = match frame["frame_kind"].as_str().unwrap() {
                "record_v2" => std::slice::from_mut(frame.get_mut("record").unwrap()),
                "atomic_group_v2" => frame["records"].as_array_mut().unwrap().as_mut_slice(),
                other => panic!("unsupported fixture frame {other}"),
            };
            for record in records {
                let mut expected = record.clone();
                if !keep(ordinal) {
                    removed += usize::from(
                        record["envelope"]
                            .as_object_mut()
                            .unwrap()
                            .remove("observability")
                            .is_some(),
                    );
                }
                ordinal += 1;
                expected["envelope"]
                    .as_object_mut()
                    .unwrap()
                    .remove("observability");
                let mut protected = record.clone();
                protected["envelope"]
                    .as_object_mut()
                    .unwrap()
                    .remove("observability");
                assert_eq!(protected, expected);
                let canonical = if record["envelope"]["provenance"]["event"]["event_kind"]
                    == "system"
                {
                    serde_json::to_value(
                        serde_json::from_value::<LogRecord<SystemEvent>>(record.clone()).unwrap(),
                    )
                    .unwrap()
                } else {
                    serde_json::to_value(
                        serde_json::from_value::<LogRecord<ChainEvent>>(record.clone()).unwrap(),
                    )
                    .unwrap()
                };
                assert_eq!(
                    canonical, *record,
                    "full record must survive typed decoding"
                );
            }
            let body = serde_json::to_string(&frame).unwrap();
            rewritten.push_str(&format!(
                "{}:{}:{}\n",
                body.len(),
                crc32fast::hash(body.as_bytes()),
                body
            ));
        }
        std::fs::write(path, rewritten).unwrap();
    }
    removed
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
