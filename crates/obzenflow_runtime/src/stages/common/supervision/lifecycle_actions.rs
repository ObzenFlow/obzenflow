// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::metrics::instrumentation::{snapshot_stage_accounting, StageInstrumentation};
use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};
use std::future::Future;
use std::sync::Arc;

pub(crate) async fn publish_running_best_effort(
    stage_label: &'static str,
    stage_id: StageId,
    stage_name: &str,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
) {
    let running_event = SystemEvent::stage_running(stage_id);

    if let Err(e) = crate::supervised_base::publication::append(
        system_journal,
        running_event,
        Default::default(),
    )
    .await
    {
        tracing::error!(
            stage_name = %stage_name,
            journal_error = %e,
            "Failed to publish running event; continuing without system journal entry"
        );
    }

    tracing::info!(
        stage_name = %stage_name,
        "{} published running event",
        stage_label
    );
}

pub(crate) async fn send_completion_best_effort(
    stage_label: &'static str,
    stage_id: StageId,
    stage_name: &str,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    _data_journal: &Arc<dyn Journal<ChainEvent>>,
    _error_journal: Option<&Arc<dyn Journal<ChainEvent>>>,
    instrumentation: &StageInstrumentation,
) {
    // Terminal facts use owner accounting, including for empty or filtered streams.
    let metrics = snapshot_stage_accounting(instrumentation);
    let completion_event = SystemEvent::stage_completed_with_accounting(stage_id, metrics);

    if let Err(e) = crate::supervised_base::publication::append(
        system_journal,
        completion_event,
        Default::default(),
    )
    .await
    {
        tracing::error!(
            stage_name = %stage_name,
            journal_error = %e,
            "Failed to write completion event; continuing without system journal entry"
        );
    }

    tracing::info!(stage_name = %stage_name, "{} sent completion event", stage_label);
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn send_failure_best_effort(
    stage_label: &'static str,
    stage_id: StageId,
    stage_name: &str,
    message: &str,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    _data_journal: &Arc<dyn Journal<ChainEvent>>,
    _error_journal: Option<&Arc<dyn Journal<ChainEvent>>>,
    instrumentation: &StageInstrumentation,
) {
    // Failure accounting is independent of optional capture and journal lookup.
    let metrics = snapshot_stage_accounting(instrumentation);

    let cancel_reason = match message {
        FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP => Some(STOP_REASON_USER_STOP),
        STOP_REASON_TIMEOUT => Some(STOP_REASON_TIMEOUT),
        _ => None,
    };

    let system_event = if let Some(reason) = cancel_reason {
        SystemEvent::stage_cancelled_with_accounting(stage_id, reason.to_string(), metrics)
    } else {
        SystemEvent::stage_failed_with_accounting(
            stage_id,
            message.to_string(),
            false, // not recoverable
            metrics,
        )
    };

    match crate::supervised_base::publication::append(
        system_journal,
        system_event,
        Default::default(),
    )
    .await
    {
        Ok(_) => {
            if let Some(reason) = cancel_reason {
                tracing::info!(
                    stage_name = %stage_name,
                    reason = %reason,
                    "{} stage cancelled",
                    stage_label
                );
            } else {
                tracing::error!(
                    stage_name = %stage_name,
                    error = %message,
                    "{} stage encountered error",
                    stage_label
                );
            }
        }
        Err(e) => {
            tracing::error!(
                stage_name = %stage_name,
                error = %message,
                journal_error = %e,
                "{} stage encountered error but failed to write error event",
                stage_label
            );
        }
    }
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
    use obzenflow_core::event::{JournalWriterId, SystemPayload};
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
    impl<T> JournalReader<T> for EmptyReader<T>
    where
        T: obzenflow_core::event::JournalEvent,
    {
        async fn next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        fn position(&self) -> u64 {
            self.position
        }
    }

    struct EmptyJournal<T> {
        id: JournalId,
        _phantom: PhantomData<T>,
    }

    impl<T> EmptyJournal<T> {
        fn new() -> Self {
            Self {
                id: JournalId::new(),
                _phantom: PhantomData,
            }
        }
    }

    #[async_trait]
    impl<T> Journal<T> for EmptyJournal<T>
    where
        T: obzenflow_core::event::JournalEvent + 'static,
    {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            None
        }

        async fn append(
            &self,
            _event: T,
            _options: obzenflow_core::journal::AppendOptions<T>,
        ) -> Result<JournalRecord<T::Payload>, JournalError> {
            Err(JournalError::Implementation {
                message: "append not supported in EmptyJournal".to_string(),
                source: Box::new(std::io::Error::other("append not supported")),
            })
        }

        async fn read_all_unordered(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            Ok(None)
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    struct RecordingSystemJournal {
        id: JournalId,
        events: Mutex<Vec<SystemEvent>>,
    }

    impl RecordingSystemJournal {
        fn new() -> Self {
            Self {
                id: JournalId::new(),
                events: Mutex::new(Vec::new()),
            }
        }

        fn take(&self) -> Vec<SystemEvent> {
            std::mem::take(&mut self.events.lock().expect("lock poisoned"))
        }
    }

    #[async_trait]
    impl Journal<SystemEvent> for RecordingSystemJournal {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            None
        }

        async fn append(
            &self,
            event: SystemEvent,
            mut options: obzenflow_core::journal::AppendOptions<SystemEvent>,
        ) -> Result<JournalRecord<SystemPayload>, JournalError> {
            let event = options.capture.prepare(0, event);
            self.events
                .lock()
                .expect("lock poisoned")
                .push(event.clone());
            Ok(JournalRecord::new(JournalWriterId::new(), event))
        }

        async fn read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<SystemPayload>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<SystemPayload>>, JournalError> {
            Ok(None)
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<SystemEvent>>, JournalError> {
            Ok(Box::new(EmptyReader {
                position,
                _phantom: PhantomData,
            }))
        }

        async fn read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<SystemPayload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    async fn exercise_failure(message: &str) -> SystemEvent {
        let system = Arc::new(RecordingSystemJournal::new());
        let system_journal: Arc<dyn Journal<SystemEvent>> = system.clone();

        let data_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(EmptyJournal::<ChainEvent>::new());
        let error_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(EmptyJournal::<ChainEvent>::new());
        let instrumentation = StageInstrumentation::new();
        let stage_id = StageId::new();

        send_failure_best_effort(
            "Test",
            stage_id,
            "test_stage",
            message,
            &system_journal,
            &data_journal,
            Some(&error_journal),
            &instrumentation,
        )
        .await;

        let events = system.take();
        assert_eq!(events.len(), 1, "expected exactly one system event");
        events.into_iter().next().expect("missing system event")
    }

    #[tokio::test]
    async fn send_failure_emits_cancelled_for_timeout() {
        let event = exercise_failure(STOP_REASON_TIMEOUT).await;
        match event.payload {
            SystemPayload::StageLifecycle { event, .. } => match event {
                obzenflow_core::event::StageLifecycleEvent::Cancelled { reason, .. } => {
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
            SystemPayload::StageLifecycle { event, .. } => match event {
                obzenflow_core::event::StageLifecycleEvent::Cancelled { reason, .. } => {
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
            SystemPayload::StageLifecycle { event, .. } => match event {
                obzenflow_core::event::StageLifecycleEvent::Failed { error, .. } => {
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
