// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! System orchestration events (written to control journal)

use crate::event::envelope::AuthoredEnvelope;
use crate::event::payloads::chain_payload::EventKind;
use crate::event::payloads::system_payload::*;
use crate::event::payloads::JournalPayload;
use crate::event::provenance::SystemEventProvenance;
use crate::event::types::{DurationMs, EventId, WriterId};
use crate::id::SystemId;
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
}
