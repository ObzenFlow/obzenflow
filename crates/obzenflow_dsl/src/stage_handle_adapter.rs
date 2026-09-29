// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::StageId;
use obzenflow_runtime::stages::common::stage_handle::{
    StageAck, StageError, StageEvent, StageFailure, StageHandle, StageMilestone,
};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use std::sync::Arc;

/// Adapter that bridges generic StandardHandle to the StageHandle trait
pub struct StageHandleAdapter<H, E, S> {
    inner: H,
    stage_id: StageId,
    stage_name: String,
    stage_type: StageType,
    event_translator: Arc<dyn Fn(StageEvent) -> Result<E, String> + Send + Sync>,
    state_checker: Arc<dyn Fn(&S) -> StageStatus + Send + Sync>,
    _phantom: std::marker::PhantomData<(E, S)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StageStatus {
    Created,
    Initializing,
    Ready,
    Starting,
    AcquiringInput,
    Running,
    Draining,
    Flushing,
    DrainingWriter,
    CheckingContracts,
    ValidatingTerminal,
    ForwardingTerminal,
    ProducingFinalOutput,
    DrainingFinalOutput,
    Finalising,
    Drained,
    Failing,
    Failed,
    Cancelling,
    Cancelled,
}

impl<H, E, S> StageHandleAdapter<H, E, S>
where
    H: SupervisorHandle<Event = E, State = S> + Send + Sync + 'static,
    E: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    pub fn new(
        inner: H,
        stage_id: StageId,
        stage_name: String,
        stage_type: StageType,
        event_translator: impl Fn(StageEvent) -> Result<E, String> + Send + Sync + 'static,
        state_checker: impl Fn(&S) -> StageStatus + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner,
            stage_id,
            stage_name,
            stage_type,
            event_translator: Arc::new(event_translator),
            state_checker: Arc::new(state_checker),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<H, E, S> StageHandle for StageHandleAdapter<H, E, S>
where
    H: SupervisorHandle<Event = E, State = S> + Send + Sync + 'static,
    E: Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn stage_name(&self) -> &str {
        &self.stage_name
    }

    fn stage_type(&self) -> StageType {
        self.stage_type
    }

    async fn initialize(&self) -> Result<(), StageError> {
        let event = (self.event_translator)(StageEvent::Initialize)
            .map_err(StageError::InitializationFailed)?;
        self.inner.send_event(event).await.map_err(|e| {
            StageError::InitializationFailed(format!("Failed to send initialize event: {e:?}"))
        })
    }

    async fn ready(&self) -> Result<(), StageError> {
        let event =
            (self.event_translator)(StageEvent::Ready).map_err(StageError::EventSendFailed)?;
        self.inner
            .send_event(event)
            .await
            .map_err(|e| StageError::EventSendFailed(format!("Failed to send ready event: {e:?}")))
    }

    async fn start(&self) -> Result<(), StageError> {
        let event =
            (self.event_translator)(StageEvent::Start).map_err(StageError::EventSendFailed)?;
        self.inner
            .send_event(event)
            .await
            .map_err(|e| StageError::EventSendFailed(format!("Failed to send start event: {e:?}")))
    }

    async fn send_event(&self, event: StageEvent) -> Result<(), StageError> {
        let translated = (self.event_translator)(event).map_err(StageError::EventSendFailed)?;
        self.inner
            .send_event(translated)
            .await
            .map_err(|e| StageError::EventSendFailed(format!("Failed to send event: {e:?}")))
    }

    async fn begin_drain(&self) -> Result<(), StageError> {
        let event =
            (self.event_translator)(StageEvent::BeginDrain).map_err(StageError::EventSendFailed)?;
        self.inner
            .send_event(event)
            .await
            .map_err(|e| StageError::EventSendFailed(format!("Failed to send drain event: {e:?}")))
    }

    fn is_ready(&self) -> bool {
        matches!(
            (self.state_checker)(&self.inner.current_state()),
            StageStatus::Ready | StageStatus::Running
        )
    }

    fn is_drained(&self) -> bool {
        matches!(
            (self.state_checker)(&self.inner.current_state()),
            StageStatus::Drained
        )
    }

    async fn force_shutdown(&self) -> Result<(), StageError> {
        let event = (self.event_translator)(StageEvent::ForceShutdown)
            .map_err(StageError::EventSendFailed)?;
        self.inner
            .send_event(event)
            .await
            .map_err(|e| StageError::EventSendFailed(format!("Failed to force shutdown: {e:?}")))
    }

    async fn wait_for_milestone(&self, milestone: StageMilestone) -> Result<StageAck, StageError> {
        self.inner
            .wait_for_milestone(milestone)
            .await
            .map(|result| StageAck::from_result(self.stage_id, result))
    }

    async fn wait_for_failure(&self) -> Option<StageFailure> {
        self.inner
            .wait_for_failure()
            .await
            .map(|result| StageFailure::from_result(self.stage_id, result))
    }

    async fn wait_for_completion(
        &self,
    ) -> obzenflow_runtime::stages::common::stage_handle::StageExit {
        obzenflow_runtime::stages::common::stage_handle::StageExit {
            stage_id: self.stage_id,
            outcome: self.inner.wait_for_stage_exit().await,
        }
    }

    async fn abort_and_join(&self) -> Result<(), StageError> {
        self.inner
            .abort_and_wait()
            .await
            .map_err(stage_execution_error)
    }

    fn request_abort(&self) {
        self.inner.request_abort();
    }
}

fn stage_execution_error(error: impl std::error::Error + Send + Sync + 'static) -> StageError {
    let source: &(dyn std::error::Error + 'static) = &error;
    if matches!(
        source.downcast_ref::<obzenflow_runtime::supervised_base::HandleError>(),
        Some(obzenflow_runtime::supervised_base::HandleError::SupervisorAborted)
    ) {
        StageError::Aborted
    } else {
        StageError::Execution(Arc::new(error))
    }
}
