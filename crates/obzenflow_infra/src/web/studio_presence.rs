// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime presence projection for Studio registration.
//!
//! This is an adapter-owned read model over journalled pipeline lifecycle
//! facts. It intentionally does not mirror the runtime FSM.

use obzenflow_core::event::{PipelineLifecycleEvent, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::journal::{Journal, JournalReader};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum RuntimePresencePhase {
    #[default]
    Unknown,
    Starting,
    ReadyForRun,
    Running,
    Draining,
    Completed,
    Cancelled,
    Failed,
}

impl RuntimePresencePhase {
    pub(crate) fn as_wire(self) -> &'static str {
        match self {
            Self::Unknown => "unknown",
            Self::Starting => "starting",
            Self::ReadyForRun => "ready_for_run",
            Self::Running => "running",
            Self::Draining => "draining",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Failed => "failed",
        }
    }

    fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }

    fn fold(self, event: &PipelineLifecycleEvent) -> Self {
        use PipelineLifecycleEvent as E;

        match event {
            E::Failed { .. } => Self::Failed,
            _ if self.is_terminal() => self,
            E::Completed { .. } => Self::Completed,
            E::Cancelled { .. } => Self::Cancelled,
            E::Starting => Self::Starting,
            E::ReadyForRun { .. } => Self::ReadyForRun,
            E::Running { .. } => Self::Running,
            E::Draining { .. } | E::AllStagesCompleted { .. } | E::Drained => Self::Draining,
            E::StopRequested { .. } => self,
        }
    }
}

pub(crate) struct RuntimePresenceProjection {
    phase: RuntimePresencePhase,
    pipeline_writer_id: WriterId,
    reader: Option<Box<dyn JournalReader<SystemEvent>>>,
}

impl RuntimePresenceProjection {
    pub(crate) async fn open(
        journal: Arc<dyn Journal<SystemEvent>>,
        pipeline_writer_id: WriterId,
    ) -> Self {
        let reader = match journal.reader().await {
            Ok(reader) => Some(reader),
            Err(error) => {
                tracing::warn!(%error, "studio presence projection could not open system journal reader; phase will stay unknown");
                None
            }
        };

        Self {
            phase: RuntimePresencePhase::Unknown,
            pipeline_writer_id,
            reader,
        }
    }

    #[cfg(test)]
    fn from_reader(
        reader: Box<dyn JournalReader<SystemEvent>>,
        pipeline_writer_id: WriterId,
    ) -> Self {
        Self {
            phase: RuntimePresencePhase::Unknown,
            pipeline_writer_id,
            reader: Some(reader),
        }
    }

    pub(crate) fn phase(&self) -> RuntimePresencePhase {
        self.phase
    }

    pub(crate) async fn catch_up(&mut self) {
        let Some(reader) = self.reader.as_mut() else {
            return;
        };

        loop {
            match reader.next().await {
                Ok(Some(envelope)) => {
                    if envelope.event.writer_id != self.pipeline_writer_id {
                        continue;
                    }

                    if let SystemEventType::PipelineLifecycle(event) = &envelope.event.event {
                        self.phase = self.phase.fold(event);
                    }
                }
                Ok(None) => return,
                Err(error) => {
                    tracing::warn!(%error, "studio presence projection stopped reading system journal; phase will remain at last known value");
                    self.reader = None;
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::MemoryJournal;
    use obzenflow_core::event::{SystemEvent, SystemEventType};
    use obzenflow_core::id::SystemId;
    use obzenflow_core::metrics::FlowLifecycleMetricsSnapshot;
    use obzenflow_core::JournalOwner;

    fn completed_metrics() -> FlowLifecycleMetricsSnapshot {
        FlowLifecycleMetricsSnapshot {
            events_in_total: 0,
            events_out_total: 0,
            errors_total: 0,
        }
    }

    #[tokio::test]
    async fn folds_only_current_pipeline_writer_and_keeps_terminal_phase_sticky() {
        let current_system = SystemId::new();
        let other_system = SystemId::new();
        let current_writer = WriterId::from(current_system);
        let other_writer = WriterId::from(other_system);
        let journal =
            MemoryJournal::<SystemEvent>::with_owner(JournalOwner::system(current_system));

        journal
            .append(
                SystemEvent::new(
                    other_writer,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Running {
                        stage_count: Some(99),
                    }),
                ),
                None,
            )
            .await
            .expect("append old run event");
        journal
            .append(
                SystemEvent::new(
                    current_writer,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Starting),
                ),
                None,
            )
            .await
            .expect("append starting");
        journal
            .append(
                SystemEvent::new(
                    current_writer,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::ReadyForRun {
                        stage_count: Some(3),
                    }),
                ),
                None,
            )
            .await
            .expect("append ready");
        journal
            .append(
                SystemEvent::new(
                    current_writer,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Running {
                        stage_count: Some(3),
                    }),
                ),
                None,
            )
            .await
            .expect("append running");
        journal
            .append(
                SystemEvent::new(
                    current_writer,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Completed {
                        duration_ms: obzenflow_core::event::types::DurationMs(5),
                        metrics: completed_metrics(),
                    }),
                ),
                None,
            )
            .await
            .expect("append completed");
        journal
            .append(
                SystemEvent::new(
                    current_writer,
                    SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Drained),
                ),
                None,
            )
            .await
            .expect("append drained");

        let reader = journal.reader().await.expect("reader opens");
        let mut projection = RuntimePresenceProjection::from_reader(reader, current_writer);

        projection.catch_up().await;

        assert_eq!(projection.phase(), RuntimePresencePhase::Completed);
    }

    #[test]
    fn wire_values_are_pinned() {
        let values = [
            (RuntimePresencePhase::Unknown, "unknown"),
            (RuntimePresencePhase::Starting, "starting"),
            (RuntimePresencePhase::ReadyForRun, "ready_for_run"),
            (RuntimePresencePhase::Running, "running"),
            (RuntimePresencePhase::Draining, "draining"),
            (RuntimePresencePhase::Completed, "completed"),
            (RuntimePresencePhase::Cancelled, "cancelled"),
            (RuntimePresencePhase::Failed, "failed"),
        ];

        for (phase, wire) in values {
            assert_eq!(phase.as_wire(), wire);
        }
    }
}
