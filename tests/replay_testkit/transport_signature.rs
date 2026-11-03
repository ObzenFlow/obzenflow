// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ordered_stage_journal::read_stage_envelopes;
use super::stage_journal::read_stage_envelopes_appended;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainPayload, JournalRecord};
use obzenflow_core::id::StageId;
use obzenflow_core::WriterId;
use std::collections::HashMap;
use std::path::Path;

/// The per-run `StageId` an upstream's authored EOF names, resolved from the
/// EOF payload's `writer_id` (falling back to the chain event's writer).
fn authored_eof_stage_id(envelopes: &[JournalRecord<ChainPayload>]) -> Option<StageId> {
    envelopes.iter().find_map(|envelope| {
        let ChainPayload::FlowControl(FlowControlPayload::Eof { writer_id, .. }) =
            &envelope.payload
        else {
            return None;
        };
        match writer_id {
            Some(WriterId::Stage(id)) => Some(*id),
            Some(_) => None,
            None => match &envelope.envelope.provenance.event.writer_id {
                WriterId::Stage(id) => Some(*id),
                _ => None,
            },
        }
    })
}

/// Project a stage's journal as a transport-row signature: `data:{event_type}`
/// for data rows and `eof:{upstream key}` for EOF rows attributed through each
/// upstream's authored EOF (FLOWIP-095d). EOFs whose author is not one of the
/// named upstreams (the stage's own authored EOF) are labelled `eof:local`.
///
/// Rows are read in physical append order, the order a downstream
/// subscription delivers. `StageId`s are per-run, so cross-run comparison
/// must resolve them through stable stage keys; this helper does that
/// resolution per run. The position of a forwarded reference EOF in this
/// signature is the join's phase-transition witness: equality of signatures
/// across runs proves the EOF-relative ordering reproduced.
///
/// Assumes each named upstream's journal contains only its own authored EOF
/// (true for source stages); reader telemetry and observability rows are
/// ignored.
pub async fn transport_row_signature(
    run_dir: &Path,
    stage_key: &str,
    upstream_stage_keys: &[&str],
) -> Vec<String> {
    let mut stage_id_to_key: HashMap<StageId, String> = HashMap::new();
    for upstream in upstream_stage_keys {
        let envelopes = read_stage_envelopes(run_dir, upstream).await;
        let stage_id = authored_eof_stage_id(&envelopes).unwrap_or_else(|| {
            panic!("upstream '{upstream}' should have an authored EOF naming its stage id")
        });
        stage_id_to_key.insert(stage_id, (*upstream).to_string());
    }

    read_stage_envelopes_appended(run_dir, stage_key)
        .await
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            payload if payload.consumes_data_credit() => {
                Some(format!("data:{}", envelope.event_type()))
            }
            ChainPayload::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                let author = match writer_id {
                    Some(WriterId::Stage(id)) => Some(*id),
                    Some(_) => None,
                    None => match &envelope.envelope.provenance.event.writer_id {
                        WriterId::Stage(id) => Some(*id),
                        _ => None,
                    },
                };
                let key = author
                    .and_then(|id| stage_id_to_key.get(&id).cloned())
                    .unwrap_or_else(|| "local".to_string());
                Some(format!("eof:{key}"))
            }
            _ => None,
        })
        .collect()
}
