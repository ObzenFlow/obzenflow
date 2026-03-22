// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::ingestion::{EventSubmission, IngressContext};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use obzenflow_runtime::stages::common::handlers::AsyncInfiniteSourceHandler;
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::typing::SourceTyping;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct HttpSourceConfig {
    /// Maximum number of events returned per `next()` call.
    pub max_batch_size: usize,
}

impl Default for HttpSourceConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
        }
    }
}

#[derive(Clone)]
pub struct HttpSource {
    rx: Arc<Mutex<tokio::sync::mpsc::Receiver<EventSubmission>>>,
    writer_id: Option<WriterId>,
    config: HttpSourceConfig,
}

impl std::fmt::Debug for HttpSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSource")
            .field("writer_id_bound", &self.writer_id.is_some())
            .field("config", &self.config)
            .finish()
    }
}

impl HttpSource {
    pub fn new(rx: tokio::sync::mpsc::Receiver<EventSubmission>) -> Self {
        Self {
            rx: Arc::new(Mutex::new(rx)),
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
            writer_id: None,
            config,
        }
    }

    /// Mark this ingestion source as producing a single typed payload.
    ///
    /// This is a pure authoring-time contract used by the typed DSL macros.
    /// It does not change runtime behaviour.
    pub fn typed<T>(self) -> HttpSourceTyped<T>
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        HttpSourceTyped {
            inner: self,
            _phantom: PhantomData,
        }
    }

    fn submission_to_event(&self, submission: EventSubmission) -> ChainEvent {
        let writer_id = self
            .writer_id
            .expect("WriterId must be bound before next() is called");
        let EventSubmission {
            event_type,
            data,
            ingress_handoff,
            ..
        } = submission;
        let mut event = ChainEventFactory::data_event(writer_id, event_type, data);
        if let Some(handoff) = ingress_handoff {
            event.ingress_context = Some(IngressContext {
                accepted_at_ns: handoff.accepted_at_ns,
                base_path: handoff.base_path,
                batch_index: handoff.batch_index,
            });
        }
        event
    }
}

#[derive(Debug)]
pub struct HttpSourceTyped<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    inner: HttpSource,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Clone for HttpSourceTyped<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T> SourceTyping for HttpSourceTyped<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Output = T;
}

#[async_trait]
impl<T> AsyncInfiniteSourceHandler for HttpSourceTyped<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.inner.bind_writer_id(id);
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        self.inner.next().await
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.inner.drain().await
    }
}

#[async_trait]
impl AsyncInfiniteSourceHandler for HttpSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        let mut rx = self.rx.lock().await;

        let first = rx.recv().await.ok_or_else(|| {
            SourceError::Transport("HTTP source channel closed (server shutdown)".into())
        })?;

        let mut out = vec![self.submission_to_event(first)];
        while out.len() < self.config.max_batch_size {
            match rx.try_recv() {
                Ok(submission) => out.push(self.submission_to_event(submission)),
                Err(_) => break,
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
            ingress_handoff: None,
        })
        .await
        .unwrap();
        tx.send(EventSubmission {
            event_type: "order.updated".to_string(),
            data: json!({"value": 2}),
            metadata: None,
            ingress_handoff: None,
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

    #[tokio::test]
    async fn http_source_attaches_ingress_context_from_submission_handoff() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tx.send(EventSubmission {
            event_type: "order.created".to_string(),
            data: json!({"value": 1}),
            metadata: None,
            ingress_handoff: Some(obzenflow_core::event::ingestion::SubmissionIngressContext {
                accepted_at_ns: 42,
                base_path: "/api/orders".to_string(),
                batch_index: Some(0),
            }),
        })
        .await
        .unwrap();

        let mut source = HttpSource::new(rx);
        source.bind_writer_id(WriterId::from(StageId::new()));

        let out = source.next().await.unwrap();
        assert_eq!(
            out[0].ingress_context,
            Some(IngressContext {
                accepted_at_ns: 42,
                base_path: "/api/orders".to_string(),
                batch_index: Some(0),
            })
        );
    }
}
