// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The verification projection (FLOWIP-095j): the single named definition of
//! which journal rows enter cross-run comparison and which fields of each row
//! count toward equality.
//!
//! There is deliberately no runtime predicate to borrow here. Source
//! re-admission (`ChainEvent::is_source_replayable`) and effect-history
//! re-admission (`EffectHistory`) are lane-specific replay mechanics; what to
//! compare is a third concept, owned by this module and shared with the
//! determinism tests.
//!
//! Rows admitted, per stage journal, in journal order:
//! - every `Data` row (user facts, `fx.emit` facts, effect outcome facts, and
//!   framework effect/capture records), compared positionally;
//! - `Watermark` flow-control rows, compared positionally on `timestamp` only
//!   (`stage_id` is run-scoped);
//! - `Eof` rows as per-type completion evidence (counts only; the run-scoped
//!   payload fields `writer_id`, `writer_seq`, `vector_clock`,
//!   `last_event_id`, `timestamp` are excluded), compared as an
//!   order-insensitive multiset per journal.
//!
//! Rows excluded: `Delivery` receipts, `Observability`/lifecycle rows, and
//! all other flow control (reader telemetry is wall-clock-gated by design;
//! checkpoint, drain, abort, and source contracts describe how a run executed
//! rather than what it concluded).
//!
//! Fields excluded everywhere: envelope wall-clock metadata, `processing_time`,
//! writer ids, the comparing runs' own flow ids, vector clocks,
//! `replay_context` (present on every `Data` row of a replay run by design),
//! `runtime_context`, `observability`, `ingress_context`, and cycle metadata.
//! Payloads are compared as parsed values, never bytes.
//!
//! Rows carrying `effect_provenance` additionally expose their deterministic
//! identity (event id, deterministic event time, and the full provenance,
//! whose cursor `recorded_flow_id` is the lineage namespace). Identity is
//! compared only within a replay lineage; see `lineage`.

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEvent, ChainEventContent};
use serde::Serialize;
use serde_json::{json, Value};

/// One projected journal row.
#[derive(Debug, Clone)]
pub enum ProjectedRow {
    /// Compared position by position on certified stages.
    Positional(PositionalRow),
    /// Aggregated into a per-journal multiset of completion evidence.
    EofEvidence(Value),
}

/// The comparable content of a positionally compared row.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PositionalRow {
    pub kind: RowKind,
    /// Parsed domain payload for `Data` rows; `{"timestamp": ..}` for
    /// watermarks.
    pub payload: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<SemanticStatus>,
    /// Present when the row carries `effect_provenance`. Compared only under
    /// `IdentityMode::Lineage`.
    pub identity: Option<EffectIdentity>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RowKind {
    Data { event_type: String },
    Watermark,
}

/// The semantic slice of `processing_info`: the outcome discriminant and the
/// byte-stable error reason (FLOWIP-120i), never timing or attribution.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct SemanticStatus {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
}

/// Deterministic identity of an effect-lane row (FLOWIP-120b). Every field
/// here is a pure function of the recorded run's namespace and the row's
/// cursor position, so within a replay lineage it must match byte for byte.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct EffectIdentity {
    pub event_id: String,
    pub event_time: u64,
    /// `cursor.recorded_flow_id`: the lineage namespace. Live runs mint in
    /// their own flow id's namespace; every replay generation inherits
    /// generation zero's.
    pub namespace: String,
    /// The full `EffectProvenance` (cursor, descriptor, descriptor hash,
    /// outcome ordinal, group id, owner, origin), all deterministic content.
    pub provenance: Value,
}

/// Project one journal row. `None` means the row is excluded from comparison.
pub fn project(event: &ChainEvent) -> Option<ProjectedRow> {
    match &event.content {
        ChainEventContent::Data {
            event_type,
            payload,
        } => Some(ProjectedRow::Positional(PositionalRow {
            kind: RowKind::Data {
                event_type: event_type.clone(),
            },
            payload: payload.clone(),
            status: Some(semantic_status(event)),
            identity: effect_identity(event),
        })),
        ChainEventContent::FlowControl(FlowControlPayload::Watermark { timestamp, .. }) => {
            Some(ProjectedRow::Positional(PositionalRow {
                kind: RowKind::Watermark,
                payload: json!({ "timestamp": timestamp }),
                status: None,
                identity: None,
            }))
        }
        ChainEventContent::FlowControl(FlowControlPayload::Eof {
            kind,
            writer_seq_by_event_type,
            ..
        }) => {
            let evidence = json!({
                "kind": serde_json::to_value(kind).unwrap_or(Value::Null),
                "counts": serde_json::to_value(writer_seq_by_event_type)
                    .unwrap_or(Value::Null),
            });
            Some(ProjectedRow::EofEvidence(evidence))
        }
        ChainEventContent::FlowControl(_)
        | ChainEventContent::Delivery(_)
        | ChainEventContent::Observability(_) => None,
    }
}

fn semantic_status(event: &ChainEvent) -> SemanticStatus {
    match &event.processing_info.status {
        ProcessingStatus::Success => SemanticStatus {
            status: "success".to_string(),
            error_message: None,
            error_kind: None,
        },
        ProcessingStatus::Error { message, kind } => SemanticStatus {
            status: "error".to_string(),
            error_message: Some(message.clone()),
            error_kind: kind
                .as_ref()
                .map(|k| serde_json::to_string(k).unwrap_or_default()),
        },
    }
}

fn effect_identity(event: &ChainEvent) -> Option<EffectIdentity> {
    let provenance = event.effect_provenance.as_ref()?;
    Some(EffectIdentity {
        event_id: event.id.to_string(),
        event_time: event.processing_info.event_time,
        namespace: provenance.cursor.recorded_flow_id.to_string(),
        provenance: serde_json::to_value(provenance).unwrap_or(Value::Null),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::context::replay_context::ReplayContext;
    use obzenflow_core::event::payloads::effect_payload::{
        EffectCursor, EffectDescriptor, EffectProvenance,
    };
    use obzenflow_core::{StageId, WriterId};

    fn writer() -> WriterId {
        WriterId::from(StageId::new())
    }

    #[test]
    fn data_rows_project_positionally_with_type_and_payload() {
        let event = ChainEventFactory::data_event(writer(), "order.placed", json!({"id": 7}));
        let Some(ProjectedRow::Positional(row)) = project(&event) else {
            panic!("data row must project positionally");
        };
        assert_eq!(
            row.kind,
            RowKind::Data {
                event_type: "order.placed".to_string()
            }
        );
        assert_eq!(row.payload, json!({"id": 7}));
        assert_eq!(row.status.as_ref().expect("data status").status, "success");
        assert!(row.identity.is_none());
    }

    #[test]
    fn error_status_projects_semantic_reason_only() {
        let event = ChainEventFactory::data_event(writer(), "order.placed", json!({}))
            .mark_as_validation_error("bad order");
        let Some(ProjectedRow::Positional(row)) = project(&event) else {
            panic!("error data row must project positionally");
        };
        let status = row.status.as_ref().expect("data status");
        assert_eq!(status.status, "error");
        assert_eq!(status.error_message.as_deref(), Some("bad order"));
    }

    #[test]
    fn effect_provenance_rows_expose_identity_and_namespace() {
        let provenance = EffectProvenance {
            cursor: EffectCursor::new("flow_gen0", "authorize", 3, 0),
            descriptor_hash: "hash".into(),
            descriptor: EffectDescriptor::new("payment.charge", "charge", 1, "1", "input"),
            outcome_fact_ordinal: None,
            group_id: None,
            fact_owner: Default::default(),
            origin: None,
        };
        let event = ChainEventFactory::data_event(writer(), "payment.authorized", json!({}))
            .with_effect_provenance(provenance);
        let Some(ProjectedRow::Positional(row)) = project(&event) else {
            panic!("effect row must project positionally");
        };
        let identity = row.identity.expect("effect row carries identity");
        assert_eq!(identity.namespace, "flow_gen0");
    }

    #[test]
    fn watermarks_project_timestamp_only() {
        let event =
            ChainEventFactory::watermark_event(writer(), 42, Some("run-scoped".to_string()));
        let Some(ProjectedRow::Positional(row)) = project(&event) else {
            panic!("watermark must project positionally");
        };
        assert_eq!(row.kind, RowKind::Watermark);
        assert_eq!(row.payload, json!({"timestamp": 42}));

        let other_stage_id = ChainEventFactory::watermark_event(writer(), 42, None);
        let Some(ProjectedRow::Positional(other)) = project(&other_stage_id) else {
            panic!()
        };
        assert_eq!(row, other, "watermark stage_id is run-scoped and excluded");
    }

    #[test]
    fn eof_projects_as_per_type_evidence() {
        let event = ChainEventFactory::eof_event(writer(), true);
        assert!(matches!(
            project(&event),
            Some(ProjectedRow::EofEvidence(_))
        ));
    }

    #[test]
    fn excluded_rows_project_to_none() {
        let drain = ChainEventFactory::drain_event(writer());
        assert!(project(&drain).is_none());
    }

    #[test]
    fn replay_context_does_not_change_the_projection() {
        let live = ChainEventFactory::data_event(writer(), "order.placed", json!({"id": 1}));
        let mut replayed = live.clone();
        replayed.replay_context = Some(ReplayContext {
            original_event_id: live.id,
            original_flow_id: "flow_gen0".to_string(),
            original_stage_id: StageId::new(),
            archive_path: std::path::PathBuf::from("/tmp/archive"),
            replayed_at: obzenflow_core::chrono::Utc::now(),
        });

        let Some(ProjectedRow::Positional(a)) = project(&live) else {
            panic!()
        };
        let Some(ProjectedRow::Positional(b)) = project(&replayed) else {
            panic!()
        };
        assert_eq!(a, b, "replay_context is excluded from equality");
    }
}
