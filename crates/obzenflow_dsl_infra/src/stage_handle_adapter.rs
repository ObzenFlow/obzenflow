// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::StageId;
use obzenflow_runtime_services::stages::common::stage_handle::{
    StageError, StageEvent, StageHandle,
};
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
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
    Ready,
    Running,
    Draining,
    Drained,
    Failed,
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

    async fn wait_for_completion(&self) -> Result<(), StageError> {
        use std::time::{Duration, Instant};

        // Use the same env var as the pipeline cleanup path so operators
        // can tune shutdown behavior without code changes.
        let timeout = std::env::var("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs)
            .unwrap_or_else(|| Duration::from_secs(30));

        let start = Instant::now();

        loop {
            let status = (self.state_checker)(&self.inner.current_state());
            match status {
                StageStatus::Drained | StageStatus::Failed => {
                    return Ok(());
                }
                _ => {
                    if start.elapsed() >= timeout {
                        return Err(StageError::Other(format!(
                            "Timeout waiting for stage {} to complete during shutdown",
                            self.stage_name
                        )));
                    }
                    // Small backoff to avoid busy-waiting
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }
}
