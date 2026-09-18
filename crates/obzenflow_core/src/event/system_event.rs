// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! System orchestration events (written to control journal)

use crate::event::envelope::AuthoredEnvelope;
use crate::event::payloads::chain_payload::EventKind;
use crate::event::payloads::system_payload::*;
use crate::event::payloads::JournalPayload;
use crate::event::provenance::{ExecutionAccounting, SystemEventProvenance};
use crate::event::types::{DurationMs, EventId, WriterId};
use crate::id::{StageId, SystemId};
use crate::metrics::FlowLifecycleMetricsSnapshot;
use serde::{Deserialize, Serialize};
/// An authored system record, without journal commitment provenance.
#[derive(Debug, Clone)]
pub struct SystemEvent {
    pub envelope: AuthoredEnvelope<SystemEventProvenance>,
    pub payload: SystemPayload,
}

impl std::ops::Deref for SystemEvent {
    type Target = SystemEventProvenance;
    fn deref(&self) -> &Self::Target {
        &self.envelope.provenance.event
    }
}
impl std::ops::DerefMut for SystemEvent {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.envelope.provenance.event
    }
}

impl Serialize for SystemEvent {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{Error, SerializeStruct};
        JournalPayload::validate(&self.payload, &self.envelope.provenance.event)
            .map_err(S::Error::custom)?;
        let mut record = serializer.serialize_struct("SystemEvent", 2)?;
        record.serialize_field("envelope", &self.envelope)?;
        record.serialize_field("payload", &self.payload)?;
        record.end()
    }
}
impl<'de> Deserialize<'de> for SystemEvent {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let raw = crate::event::record_serde::deserialize::<
            _,
            AuthoredEnvelope<SystemEventProvenance>,
            SystemPayload,
        >(deserializer)?;
        JournalPayload::validate(&raw.payload, &raw.envelope.provenance.event)
            .map_err(D::Error::custom)?;
        Ok(Self {
            envelope: raw.envelope,
            payload: raw.payload,
        })
    }
}

impl SystemEvent {
    /// Create a new system event
    pub fn new(writer_id: WriterId, event: SystemPayload) -> Self {
        use crate::event::envelope::AuthoredEnvelope;
        use crate::event::provenance::{AuthoredProvenance, SystemEventProvenance};
        let provenance = SystemEventProvenance {
            id: EventId::new(),
            writer_id,
            event_kind: EventKind::System,
            event_type: event.event_type().to_string(),
            timestamp: current_timestamp(),
        };
        Self {
            envelope: AuthoredEnvelope {
                provenance: AuthoredProvenance { event: provenance },
                observability: None,
            },
            payload: event,
        }
    }

    /// Helper for stages to create lifecycle events
    pub fn stage_running(stage_id: StageId) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Running,
            },
        )
    }

    /// Helper for stages to create completed events
    pub fn stage_completed(stage_id: StageId) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { accounting: None },
            },
        )
    }

    /// Helper for stages to create cancelled events
    pub fn stage_cancelled(stage_id: StageId, reason: String) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    accounting: None,
                },
            },
        )
    }

    /// Helper for stages to create failed events
    pub fn stage_failed(stage_id: StageId, error: String, recoverable: bool) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: None,
                    causal_event_id: None,
                },
            },
        )
    }

    /// Helper for stages to create draining events with metrics
    pub fn stage_draining_with_accounting(
        stage_id: StageId,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Draining {
                    accounting: Some(accounting),
                },
            },
        )
    }

    /// Helper for stages to create completed events with metrics
    pub fn stage_completed_with_accounting(
        stage_id: StageId,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed {
                    accounting: Some(accounting),
                },
            },
        )
    }

    /// Helper for stages to create failed events with metrics
    pub fn stage_failed_with_accounting(
        stage_id: StageId,
        error: String,
        recoverable: bool,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: Some(accounting),
                    causal_event_id: None,
                },
            },
        )
    }

    /// Construct correctness-bearing failed lifecycle evidence causally linked
    /// to the final chain event in a sink failure sequence.
    pub fn stage_failed_with_accounting_causal(
        stage_id: StageId,
        error: String,
        recoverable: bool,
        accounting: ExecutionAccounting,
        causal_event_id: EventId,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: Some(accounting),
                    causal_event_id: Some(causal_event_id),
                },
            },
        )
    }

    /// Helper for stages to create cancelled events with metrics
    pub fn stage_cancelled_with_accounting(
        stage_id: StageId,
        reason: String,
        accounting: ExecutionAccounting,
    ) -> Self {
        Self::new(
            WriterId::from(stage_id),
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    accounting: Some(accounting),
                },
            },
        )
    }
}

/// Get current timestamp in milliseconds since epoch
fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// Factory for creating SystemEvents with proper conventions
pub struct SystemEventFactory {
    writer_id: WriterId,
}

impl SystemEventFactory {
    /// Create a new factory for system events
    pub fn new(system_id: SystemId) -> Self {
        Self {
            writer_id: WriterId::from(system_id),
        }
    }

    // === Stage Lifecycle Events ===

    pub fn stage_running(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Running,
            },
        )
    }

    pub fn stage_draining(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Draining { accounting: None },
            },
        )
    }

    pub fn stage_drained(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Drained,
            },
        )
    }

    pub fn stage_completed(&self, stage_id: StageId) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { accounting: None },
            },
        )
    }

    pub fn stage_failed(&self, stage_id: StageId, error: String, recoverable: bool) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Failed {
                    error,
                    recoverable: Some(recoverable),
                    accounting: None,
                    causal_event_id: None,
                },
            },
        )
    }

    pub fn stage_cancelled(&self, stage_id: StageId, reason: String) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Cancelled {
                    reason,
                    accounting: None,
                },
            },
        )
    }

    /// Contract status summary emitted by readers/subscribers (per upstream)
    pub fn contract_status(
        &self,
        upstream: StageId,
        reader: StageId,
        pass: bool,
        reader_seq: Option<crate::event::types::SeqNo>,
        advertised_writer_seq: Option<crate::event::types::SeqNo>,
        reason: Option<crate::event::types::ViolationCause>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::ContractStatus {
                upstream,
                reader,
                selected_event_type: None,
                feed_role: None,
                pass,
                reader_seq,
                advertised_writer_seq,
                reason,
            },
        )
    }

    // === Pipeline Lifecycle Events ===

    pub fn pipeline_starting(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Starting),
        )
    }

    pub fn pipeline_running(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running { stage_count: None }),
        )
    }

    pub fn pipeline_ready_for_run(&self, stage_count: Option<usize>) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::ReadyForRun { stage_count }),
        )
    }

    pub fn pipeline_stop_admitted(&self, admission: PipelineStopAdmission) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted { admission }),
        )
    }

    pub fn pipeline_not_started(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::NotStarted),
        )
    }

    pub fn pipeline_all_stages_completed(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::AllStagesCompleted {
                metrics: None,
            }),
        )
    }

    pub fn pipeline_draining(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Draining { metrics: None }),
        )
    }

    pub fn pipeline_drained(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
        )
    }

    pub fn pipeline_completed(
        &self,
        duration_ms: DurationMs,
        metrics: FlowLifecycleMetricsSnapshot,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed {
                duration_ms,
                metrics,
            }),
        )
    }

    pub fn pipeline_failed(
        &self,
        reason: String,
        duration_ms: DurationMs,
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        failure_cause: Option<crate::event::types::ViolationCause>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Failed {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            }),
        )
    }

    pub fn pipeline_cancelled(
        &self,
        reason: String,
        duration_ms: DurationMs,
        metrics: Option<FlowLifecycleMetricsSnapshot>,
        failure_cause: Option<crate::event::types::ViolationCause>,
    ) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Cancelled {
                reason,
                duration_ms,
                metrics,
                failure_cause,
            }),
        )
    }

    // === Metrics Coordination Events ===

    pub fn metrics_ready(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Ready),
        )
    }

    pub fn metrics_drain_requested(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::DrainRequested),
        )
    }

    pub fn metrics_drained(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Drained),
        )
    }

    pub fn metrics_shutdown(&self) -> SystemEvent {
        SystemEvent::new(
            self.writer_id,
            SystemPayload::MetricsCoordination(MetricsCoordinationEvent::Shutdown),
        )
    }
}

// Implement JournalEvent for SystemEvent
use crate::event::journal_event::{JournalEvent, Sealed};

// Implement the sealed trait first
impl Sealed for SystemEvent {}

impl JournalEvent for SystemEvent {
    type Payload = SystemPayload;
    fn into_parts(self) -> (AuthoredEnvelope<SystemEventProvenance>, Self::Payload) {
        (self.envelope, self.payload)
    }
    fn from_parts(
        envelope: AuthoredEnvelope<SystemEventProvenance>,
        payload: Self::Payload,
    ) -> Self {
        Self { envelope, payload }
    }

    fn id(&self) -> &EventId {
        &self.id
    }

    fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }

    fn event_type_name(&self) -> &str {
        self.payload.event_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::SeqNo;
    use crate::{EventType, StageId};
    use serde_json::json;

    #[test]
    fn lifecycle_admission_has_one_typed_schema_and_rejects_the_old_event() {
        for (admission, expected) in [
            (
                PipelineStopAdmission::Graceful {
                    timeout_ms: DurationMs(125),
                },
                json!({"mode": "graceful", "timeout_ms": 125}),
            ),
            (
                PipelineStopAdmission::Cancel {
                    cause: PipelineCancellationCause::Requested,
                },
                json!({"mode": "cancel", "cause": "requested"}),
            ),
            (
                PipelineStopAdmission::Cancel {
                    cause: PipelineCancellationCause::GracefulTimeout,
                },
                json!({"mode": "cancel", "cause": "graceful_timeout"}),
            ),
        ] {
            let payload = serde_json::to_value(PipelineLifecycleEvent::StopAdmitted {
                admission: admission.clone(),
            })
            .unwrap();
            assert_eq!(
                payload,
                json!({"pipeline_event": "stop_admitted", "admission": expected})
            );
            assert!(
                matches!(serde_json::from_value::<PipelineLifecycleEvent>(payload).unwrap(), PipelineLifecycleEvent::StopAdmitted { admission: decoded } if decoded == admission)
            );
        }
        assert_eq!(
            serde_json::to_value(PipelineLifecycleEvent::NotStarted).unwrap(),
            json!({"pipeline_event": "not_started"})
        );
        for obsolete in [
            json!({"pipeline_event": "stop_requested", "mode": "cancel"}),
            json!({"pipeline_event": "stop_admitted", "admission": {"mode": "cancel"}}),
            json!({"pipeline_event": "stop_admitted", "admission": {"mode": "graceful"}}),
        ] {
            assert!(serde_json::from_value::<PipelineLifecycleEvent>(obsolete).is_err());
        }
    }

    #[test]
    fn contract_result_feed_fields_are_typed_but_serialize_as_labels() {
        let payload = SystemPayload::ContractResult {
            upstream: StageId::new(),
            reader: StageId::new(),
            selected_event_type: Some(EventType::from("test.selected.v1")),
            feed_role: Some(SystemFeedRole::Reference),
            contract_name: ContractName::from("TransportContract"),
            status: ContractResultStatusLabel::Healthy,
            cause: None,
            reader_seq: Some(SeqNo(3)),
            advertised_writer_seq: Some(SeqNo(5)),
        };

        let serialized = serde_json::to_value(&payload).expect("system event should serialize");
        assert_eq!(serialized["selected_event_type"], "test.selected.v1");
        assert_eq!(serialized["feed_role"], "reference");
        assert_eq!(serialized["contract_name"], "TransportContract");
        assert_eq!(serialized["status"], "healthy");

        let decoded: SystemPayload = serde_json::from_value(json!({
            "system_event_type": "contract_result",
            "upstream": serialized["upstream"].clone(),
            "reader": serialized["reader"].clone(),
            "selected_event_type": "test.selected.v1",
            "feed_role": "reference",
            "contract_name": "TransportContract",
            "status": "healthy",
            "reader_seq": 3,
            "advertised_writer_seq": 5
        }))
        .expect("string-label system event should deserialize");

        match decoded {
            SystemPayload::ContractResult {
                selected_event_type,
                feed_role,
                contract_name,
                status,
                ..
            } => {
                assert_eq!(
                    selected_event_type,
                    Some(EventType::from("test.selected.v1"))
                );
                assert_eq!(feed_role, Some(SystemFeedRole::Reference));
                assert_eq!(contract_name.as_str(), "TransportContract");
                assert_eq!(status, ContractResultStatusLabel::Healthy);
            }
            other => panic!("expected ContractResult, got {other:?}"),
        }
    }
}
