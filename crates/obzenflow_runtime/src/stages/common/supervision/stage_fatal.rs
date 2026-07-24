// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::stages::common::handler_error::StageFatal;
use obzenflow_core::event::{
    ChainEventFactory, EventEnvelope, StageFatalRecorded, StageFatalSeverity,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, TypedPayload, WriterId};
use std::sync::Arc;

pub(crate) struct StageFatalCommit<'a> {
    pub error_journal: &'a Arc<dyn Journal<ChainEvent>>,
    pub writer_id: WriterId,
    pub stage_id: StageId,
    pub stage_key: &'a str,
    pub input_position: Option<StageInputPosition>,
    pub parent: Option<&'a EventEnvelope<ChainEvent>>,
    pub lineage: obzenflow_core::config::LineagePolicy,
}

pub(crate) async fn record_stage_fatal(
    fatal: &StageFatal,
    commit: StageFatalCommit<'_>,
) -> Result<EventEnvelope<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let payload = StageFatalRecorded {
        severity: if fatal.primary_cause_event_id.is_some() {
            StageFatalSeverity::Secondary
        } else {
            StageFatalSeverity::Primary
        },
        stage_id: commit.stage_id,
        stage_key: commit.stage_key.to_string(),
        causal_event_id: commit.parent.map(|parent| parent.event.id),
        input_position: commit.input_position.map(|position| position.0),
        primary_cause_event_id: fatal.primary_cause_event_id,
        code: fatal.code,
        reason: fatal.reason,
        detail: fatal.detail.clone(),
    };
    let payload = serde_json::to_value(payload)?;
    let event = match commit.parent {
        Some(parent) => ChainEventFactory::derived_data_event(
            commit.writer_id,
            &parent.event,
            StageFatalRecorded::versioned_event_type(),
            payload,
            commit.lineage,
        ),
        None => ChainEventFactory::data_event(
            commit.writer_id,
            StageFatalRecorded::versioned_event_type(),
            payload,
        ),
    };
    commit
        .error_journal
        .append(event, commit.parent)
        .await
        .map_err(|error| error.to_string().into())
}
