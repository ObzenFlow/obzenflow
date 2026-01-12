use async_trait::async_trait;
use obzenflow_core::event::ingestion::{
    EventSubmission, IngestionTelemetry, IngestionTelemetrySnapshot,
};
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, WriterId};
use obzenflow_runtime_services::stages::common::handlers::AsyncInfiniteSourceHandler;
use obzenflow_runtime_services::stages::SourceError;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    /// Maximum number of events returned per `next()` call.
    pub max_batch_size: usize,

    /// How often to emit an ingestion telemetry wide event (FLOWIP-084d).
    ///
    /// Default: 5 seconds.
    pub telemetry_flush_interval: Duration,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            telemetry_flush_interval: Duration::from_secs(5),
        }
    }
}

#[derive(Clone)]
pub struct HttpSource {
    rx: Arc<Mutex<tokio::sync::mpsc::Receiver<EventSubmission>>>,
    telemetry: Option<Arc<IngestionTelemetry>>,
    last_flush: Instant,
    last_snapshot: Option<IngestionTelemetrySnapshot>,
    writer_id: Option<WriterId>,
    config: HttpSourceConfig,
}

impl std::fmt::Debug for HttpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSource")
            .field("writer_id_bound", &self.writer_id.is_some())
            .field("config", &self.config)
            .field("telemetry_enabled", &self.telemetry.is_some())
            .finish()
    }
}

impl HttpSource {
    pub fn new(rx: tokio::sync::mpsc::Receiver<EventSubmission>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
            telemetry: None,
            last_flush: Instant::now(),
            last_snapshot: None,
            writer_id: None,
            config: HttpSourceConfig::default(),
        }
    }

    pub fn with_telemetry(
        rx: tokio::sync::mpsc::Receiver<EventSubmission>,
        telemetry: Arc<IngestionTelemetry>,
    ) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
            telemetry: Some(telemetry),
            last_flush: Instant::now(),
            last_snapshot: None,
            writer_id: None,
            config: HttpSourceConfig::default(),
        }
    }

    pub fn with_config(
        rx: tokio::sync::mpsc::Receiver<EventSubmission>,
        config: HttpSourceConfig,
    ) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
            telemetry: None,
            last_flush: Instant::now(),
            last_snapshot: None,
            writer_id: None,
            config,
        }
    }

    pub fn with_config_and_telemetry(
        rx: tokio::sync::mpsc::Receiver<EventSubmission>,
        config: HttpSourceConfig,
        telemetry: Arc<IngestionTelemetry>,
    ) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
            telemetry: Some(telemetry),
            last_flush: Instant::now(),
            last_snapshot: None,
            writer_id: None,
            config,
        }
    }

    fn submission_to_event(&self, submission: EventSubmission) -> ChainEvent {
        let writer_id = self
            .writer_id
            .clone()
            .expect("WriterId must be bound before next() is called");
        let EventSubmission {
            event_type, data, ..
        } = submission;
        ChainEventFactory::data_event(writer_id, event_type, data)
    }

    fn telemetry_to_observability_event(
        &self,
        snapshot: &IngestionTelemetrySnapshot,
    ) -> Result<ChainEvent, SourceError> {
        let writer_id = self
            .writer_id
            .clone()
            .expect("WriterId must be bound before next() is called");
        let value = serde_json::to_value(snapshot).map_err(|e| {
            SourceError::Other(format!(
                "Failed to serialize ingestion telemetry snapshot: {e}"
            ))
        })?;

        Ok(ChainEventFactory::observability_event(
            writer_id,
            ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
                name: "http_ingestion.snapshot".to_string(),
                value,
                tags: None,
            }),
        ))
    }
}

#[async_trait]
impl AsyncInfiniteSourceHandler for HttpSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let mut rx = self.rx.lock().await;

        let has_telemetry =
            self.telemetry.is_some() && self.config.telemetry_flush_interval != Duration::ZERO;

        let mut out = Vec::new();

        if has_telemetry {
            let flush_deadline = self.last_flush + self.config.telemetry_flush_interval;

            tokio::select! {
                biased;
                maybe_submission = rx.recv() => {
                    let submission = maybe_submission.ok_or_else(|| {
                        SourceError::Transport("HTTP source channel closed (server shutdown)".into())
                    })?;
                    out.push(self.submission_to_event(submission));
                }
                _ = tokio::time::sleep_until(flush_deadline) => {
                    // Flush tick without an accepted event.
                }
            }

            while out.len() < self.config.max_batch_size {
                match rx.try_recv() {
                    Ok(submission) => out.push(self.submission_to_event(submission)),
                    Err(_) => break,
                }
            }

            if Instant::now() >= flush_deadline {
                if let Some(ref telemetry) = self.telemetry {
                    let snapshot = telemetry.snapshot();
                    let should_emit = self
                        .last_snapshot
                        .as_ref()
                        .map(|prev| prev != &snapshot)
                        .unwrap_or(true);
                    if should_emit {
                        out.push(self.telemetry_to_observability_event(&snapshot)?);
                        self.last_snapshot = Some(snapshot);
                    }
                }
                self.last_flush = Instant::now();
            }
        } else {
            let first = rx.recv().await.ok_or_else(|| {
                SourceError::Transport("HTTP source channel closed (server shutdown)".into())
            })?;

            out.push(self.submission_to_event(first));
            while out.len() < self.config.max_batch_size {
                match rx.try_recv() {
                    Ok(submission) => out.push(self.submission_to_event(submission)),
                    Err(_) => break,
                }
            }
        }

        Ok(out)
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ingestion::EventSubmission;
    use obzenflow_core::StageId;
    use serde_json::json;

    #[tokio::test]
    async fn http_source_emits_chain_events_from_submissions() {
        let (tx, rx) = tokio::sync::mpsc::channel(8);
        tx.send(EventSubmission {
            event_type: "order.created".to_string(),
            data: json!({"value": 1}),
            metadata: None,
        })
        .await
        .unwrap();
        tx.send(EventSubmission {
            event_type: "order.updated".to_string(),
            data: json!({"value": 2}),
            metadata: None,
        })
        .await
        .unwrap();

        let mut source = HttpSource::new(rx);
        source.bind_writer_id(WriterId::from(StageId::new()));

        let out = source.next().await.unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].event_type(), "order.created");
        assert_eq!(out[0].payload(), json!({"value": 1}));
        assert_eq!(out[1].event_type(), "order.updated");
        assert_eq!(out[1].payload(), json!({"value": 2}));
    }

    #[tokio::test]
    async fn http_source_errors_when_channel_closed() {
        let (tx, rx) = tokio::sync::mpsc::channel::<EventSubmission>(1);
        drop(tx);
        let mut source = HttpSource::new(rx);
        source.bind_writer_id(WriterId::from(StageId::new()));

        let err = source.next().await.unwrap_err();
        assert!(matches!(err, SourceError::Transport(_)));
    }
}
