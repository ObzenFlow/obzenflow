// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::ingestion::EventSubmission;
use obzenflow_core::event::ingestion::IngestionTelemetry;
use obzenflow_runtime::pipeline::fsm::PipelineState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use super::{AuthConfig, ValidationConfig};

/// Configuration for event ingestion (FLOWIP-084d).
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Base path for endpoints (default: `/api/ingest`).
    ///
    /// Results in:
    /// - `{base_path}/events`
    /// - `{base_path}/batch`
    /// - `{base_path}/health`
    pub base_path: String,

    /// Maximum events per batch (default: 1000).
    pub max_batch_size: usize,

    /// Maximum request body size in bytes (default: 1MB).
    pub max_body_size: usize,

    /// Channel buffer capacity (default: 10,000).
    pub buffer_capacity: usize,

    /// Optional authentication.
    pub auth: Option<AuthConfig>,

    /// Optional schema validation.
    pub validation: Option<ValidationConfig>,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            base_path: "/api/ingest".to_string(),
            max_batch_size: 1000,
            max_body_size: 1024 * 1024,
            buffer_capacity: 10_000,
            auth: None,
            validation: None,
        }
    }
}

/// Shared state between ingestion endpoints.
#[derive(Clone)]
pub struct IngestionState {
    pub tx: mpsc::Sender<EventSubmission>,
    pub ready: Arc<AtomicBool>,
    pub buffer_capacity: usize,
    pub config: IngestionConfig,
    pub telemetry: Arc<IngestionTelemetry>,
}

impl IngestionState {
    pub fn new(mut config: IngestionConfig) -> (Self, mpsc::Receiver<EventSubmission>) {
        config.base_path = normalize_base_path(&config.base_path);

        let buffer_capacity = config.buffer_capacity;
        let (tx, rx) = mpsc::channel(buffer_capacity);
        let tx_for_depth = tx.clone();
        let capacity_for_depth = buffer_capacity;
        let depth_fn: Arc<dyn Fn() -> usize + Send + Sync> =
            Arc::new(move || capacity_for_depth.saturating_sub(tx_for_depth.capacity()));
        let telemetry = Arc::new(IngestionTelemetry::new(
            config.base_path.clone(),
            buffer_capacity,
            depth_fn,
        ));
        let state = Self {
            tx,
            ready: Arc::new(AtomicBool::new(false)),
            buffer_capacity,
            config,
            telemetry,
        };
        (state, rx)
    }

    /// Current channel depth.
    ///
    /// Uses `mpsc::Sender::capacity()` which returns remaining capacity.
    pub fn channel_depth(&self) -> usize {
        self.buffer_capacity.saturating_sub(self.tx.capacity())
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub fn telemetry(&self) -> Arc<IngestionTelemetry> {
        self.telemetry.clone()
    }

    /// Wire ready signal from `FlowHandle::state_receiver()`.
    ///
    /// Returns a join handle that must be kept alive.
    pub fn watch_pipeline_state(
        &self,
        mut state_rx: watch::Receiver<PipelineState>,
    ) -> tokio::task::JoinHandle<()> {
        let ready = self.ready.clone();
        tokio::spawn(async move {
            let initial_running = matches!(state_rx.borrow().clone(), PipelineState::Running);
            ready.store(initial_running, Ordering::Release);
            loop {
                if state_rx.changed().await.is_err() {
                    break;
                }
                let state = state_rx.borrow().clone();
                let is_running = matches!(state, PipelineState::Running);
                ready.store(is_running, Ordering::Release);
            }
        })
    }
}

pub(crate) fn join_path(base_path: &str, suffix: &str) -> String {
    let base_path = normalize_base_path(base_path);
    format!("{base_path}/{suffix}")
}

fn normalize_base_path(base_path: &str) -> String {
    let trimmed = base_path.trim();
    if trimmed.is_empty() {
        return "/api/ingest".to_string();
    }

    let mut out = if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    };

    while out.len() > 1 && out.ends_with('/') {
        out.pop();
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn watch_pipeline_state_toggles_ready_on_running() {
        let (state, _rx) = IngestionState::new(IngestionConfig::default());
        assert!(!state.is_ready());

        let (tx, rx) = watch::channel(PipelineState::Created);
        let handle = state.watch_pipeline_state(rx);

        tx.send(PipelineState::Running).unwrap();
        tokio::task::yield_now().await;
        assert!(state.is_ready());

        tx.send(PipelineState::Draining).unwrap();
        tokio::task::yield_now().await;
        assert!(!state.is_ready());

        handle.abort();
    }
}
