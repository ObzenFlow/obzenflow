// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::manifest::archive_manifest;
use super::stage_journal::read_stage_envelopes_appended;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::ChainPayload;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;

pub async fn passing_verdicts(run_dir: &Path) -> BTreeMap<String, usize> {
    let manifest = archive_manifest(run_dir);
    let stages = manifest["stages"].as_object().expect("manifest stages");
    let names: HashMap<&str, &str> = stages
        .iter()
        .map(|(name, stage)| (stage["stage_id"].as_str().expect("stage id"), name.as_str()))
        .collect();
    let mut verdicts = BTreeMap::new();
    for stage in stages.keys() {
        for record in read_stage_envelopes_appended(run_dir, stage).await {
            if let ChainPayload::Execution(ExecutionPayload::ContractStatus {
                upstream,
                reader,
                pass: true,
                ..
            }) = &record.payload
            {
                let edge = format!(
                    "{} -> {}",
                    names[upstream.to_string().as_str()],
                    names[reader.to_string().as_str()]
                );
                *verdicts.entry(edge).or_insert(0) += 1;
            }
        }
    }
    verdicts
}

pub fn one_verdict_per_edge(edges: &[&str]) -> BTreeMap<String, usize> {
    edges.iter().map(|edge| (edge.to_string(), 1)).collect()
}
