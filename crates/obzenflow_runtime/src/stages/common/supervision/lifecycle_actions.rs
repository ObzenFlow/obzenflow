// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::metrics::instrumentation::{snapshot_stage_accounting, StageInstrumentation};
use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, Journal};
use std::future::Future;
use std::sync::Arc;

async fn publish(
    journal: &Arc<dyn Journal<ChainEvent>>,
    context: FlowContext,
    fact: StageLifecycleFact,
) -> Result<(), obzenflow_fsm::FsmError> {
    let event = ChainEventFactory::execution_event(
        context.stage_id.into(),
        ExecutionPayload::StageLifecycle(fact),
    )
    .with_flow_context(context);
    crate::supervised_base::publication::append(journal, event, Default::default())
        .await
        .map_err(|error| obzenflow_fsm::FsmError::HandlerError(error.to_string()))?;
    Ok(())
}

pub(crate) async fn publish_running(
    journal: &Arc<dyn Journal<ChainEvent>>,
    context: FlowContext,
) -> Result<(), obzenflow_fsm::FsmError> {
    let stage_id = context.stage_id;
    publish(journal, context, StageLifecycleFact::Running { stage_id }).await
}

pub(crate) async fn send_completion(
    journal: &Arc<dyn Journal<ChainEvent>>,
    context: FlowContext,
    instrumentation: &StageInstrumentation,
) -> Result<(), obzenflow_fsm::FsmError> {
    let stage_id = context.stage_id;
    publish(
        journal,
        context,
        StageLifecycleFact::Completed {
            stage_id,
            accounting: Some(snapshot_stage_accounting(instrumentation)),
        },
    )
    .await
}

pub(crate) async fn send_failure(
    journal: &Arc<dyn Journal<ChainEvent>>,
    context: FlowContext,
    message: &str,
    instrumentation: &StageInstrumentation,
    causal_event_id: Option<obzenflow_core::EventId>,
) -> Result<(), obzenflow_fsm::FsmError> {
    let stage_id = context.stage_id;
    let accounting = Some(snapshot_stage_accounting(instrumentation));
    let fact = match message {
        FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP | STOP_REASON_TIMEOUT => {
            let reason = if message == STOP_REASON_TIMEOUT {
                STOP_REASON_TIMEOUT
            } else {
                STOP_REASON_USER_STOP
            };
            StageLifecycleFact::Cancelled {
                stage_id,
                reason: reason.to_string(),
                accounting,
            }
        }
        _ => StageLifecycleFact::Failed {
            stage_id,
            error: message.to_string(),
            recoverable: Some(false),
            accounting,
            causal_event_id,
        },
    };
    publish(journal, context, fact).await
}

pub(crate) async fn cleanup_best_effort<E, Fut>(
    stage_label: &'static str,
    stage_name: &str,
    run: impl FnOnce() -> Fut,
) where
    Fut: Future<Output = Result<(), E>>,
    E: std::fmt::Debug,
{
    match run().await {
        Ok(()) => {
            tracing::info!(stage_name = %stage_name, "{} cleaned up resources", stage_label);
        }
        Err(e) => {
            tracing::warn!(
                stage_name = %stage_name,
                error = ?e,
                "{} cleanup errored; continuing",
                stage_label
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::types::EventId;
    use obzenflow_core::event::{ChainPayload, JournalWriterId};
    use obzenflow_core::id::{JournalId, StageId};
    use obzenflow_core::journal::{JournalError, JournalReader};
    use obzenflow_core::{ChainEvent, Journal, JournalRecord};
    use std::marker::PhantomData;
    use std::sync::{Arc, Mutex};

    struct EmptyReader<T> {
        position: u64,
        _phantom: PhantomData<T>,
    }

    #[async_trait]
    impl<T> obzenflow_core::journal::JournalStorageReader<T> for EmptyReader<T>
    where
        T: obzenflow_core::event::JournalEvent,
    {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        fn storage_position(&self) -> u64 {
            self.position
        }
    }

    struct RecordingJournal {
        owner: obzenflow_core::JournalOwner,
        id: JournalId,
        events: Mutex<Vec<ChainEvent>>,
    }

    impl RecordingJournal {
        fn new(stage: StageId) -> Self {
            Self {
                id: JournalId::new(),
                events: Mutex::new(Vec::new()),
                owner: obzenflow_core::JournalOwner::stage(stage),
            }
        }

        fn take(&self) -> Vec<ChainEvent> {
            std::mem::take(&mut self.events.lock().expect("lock poisoned"))
        }
    }

    #[async_trait]
    impl obzenflow_core::journal::JournalStorage<ChainEvent> for RecordingJournal {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            Some(&self.owner)
        }

        async fn storage_append(
            &self,
            event: ChainEvent,
            mut options: obzenflow_core::journal::AppendOptions<ChainEvent>,
        ) -> Result<JournalRecord<ChainPayload>, JournalError> {
            let event = options.capture.prepare(0, event);
            self.events
                .lock()
                .expect("lock poisoned")
                .push(event.clone());
            Ok(JournalRecord::new(JournalWriterId::from(self.id), event))
        }

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(Vec::new())
        }

        async fn storage_read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            Ok(None)
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn storage_read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    async fn exercise_failure(message: &str) -> ChainEvent {
        let stage_id = StageId::new();
        let system = Arc::new(RecordingJournal::new(stage_id));
        let journal: Arc<dyn Journal<ChainEvent>> = system.clone();
        let instrumentation = StageInstrumentation::new();
        send_failure(
            &journal,
            FlowContext::new("test_stage", stage_id),
            message,
            &instrumentation,
            None,
        )
        .await
        .unwrap();

        let events = system.take();
        assert_eq!(events.len(), 1, "expected exactly one stage fact");
        events.into_iter().next().expect("missing stage fact")
    }

    #[tokio::test]
    async fn send_failure_emits_cancelled_for_timeout() {
        let event = exercise_failure(STOP_REASON_TIMEOUT).await;
        match event.payload {
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) => match event {
                StageLifecycleFact::Cancelled { reason, .. } => {
                    assert_eq!(reason, STOP_REASON_TIMEOUT);
                }
                other => panic!("expected Cancelled, got {other:?}"),
            },
            other => panic!("expected StageLifecycle, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_failure_emits_cancelled_for_user_stop() {
        let event = exercise_failure(STOP_REASON_USER_STOP).await;
        match event.payload {
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) => match event {
                StageLifecycleFact::Cancelled { reason, .. } => {
                    assert_eq!(reason, STOP_REASON_USER_STOP);
                }
                other => panic!("expected Cancelled, got {other:?}"),
            },
            other => panic!("expected StageLifecycle, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn send_failure_emits_failed_for_other_errors() {
        let event = exercise_failure("boom").await;
        match event.payload {
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) => match event {
                StageLifecycleFact::Failed { error, .. } => {
                    assert_eq!(error, "boom");
                }
                other => panic!("expected Failed, got {other:?}"),
            },
            other => panic!("expected StageLifecycle, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn cleanup_best_effort_swallows_errors() {
        cleanup_best_effort("Test", "test_stage", || async { Err::<(), _>("boom") }).await;
    }
}
