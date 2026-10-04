// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Terminal structured diagnostics through the application's tracing subscriber.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{InlineSink, SinkDescription, SinkWriteFailure};
use std::marker::PhantomData;

/// Emit one input's structured tracing diagnostic.
///
/// The synchronous callback emits the application's `tracing` event. Subscriber
/// filtering and routing remain unchanged; success means the callback completed,
/// not that a subscriber persisted the diagnostic. This convenience does not own
/// resources, retry, buffering or asynchronous delivery. Implement [`InlineSink`]
/// or the connector/writer traits for custom sink execution.
pub struct TracingSink<T, F> {
    emit: F,
    _input: PhantomData<fn() -> T>,
}

impl<T, F> TracingSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) + Clone + Send + Sync + 'static,
{
    pub fn new(emit: F) -> Self {
        Self {
            emit,
            _input: PhantomData,
        }
    }
}

impl<T, F: Clone> Clone for TracingSink<T, F> {
    fn clone(&self) -> Self {
        Self {
            emit: self.emit.clone(),
            _input: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for TracingSink<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TracingSink")
            .field("input", &std::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T, F> InlineSink for TracingSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) + Clone + Send + Sync + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("tracing".into()))
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, input: T) -> Result<(), SinkWriteFailure> {
        (self.emit)(&input);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::JournalRecord;
    use obzenflow_core::{JournalWriterId, StageId};
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, SinkWriterAdapter};
    use obzenflow_runtime::stages::sink::{SinkConnector, SinkWriterInitContext};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Deserialize, Serialize)]
    struct Item(u64);
    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "tracing.test.item";
    }

    #[tokio::test]
    async fn tracing_callback_runs_once_and_inherits_honest_completion_metadata() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let capture = observed.clone();
        let connector = TracingSink::new(move |item: &Item| {
            ::tracing::info!(value = item.0, "terminal item observed");
            capture.lock().unwrap().push(item.0);
        });
        let description = SinkConnector::describe(&connector);
        assert_eq!(
            description.default_method(),
            &DeliveryMethod::Custom("tracing".into())
        );
        assert_eq!(
            description.redelivery_safety(),
            Some(SinkRedeliverySafety::SafeToRepeat)
        );
        let stage = StageId::new();
        let writer = connector
            .open(SinkWriterInitContext::new(
                stage,
                "tracing".into(),
                "test".into(),
            ))
            .await
            .unwrap();
        let mut adapter =
            SinkWriterAdapter::new(writer, stage, description.default_method().clone());
        let input = JournalRecord::new(
            JournalWriterId::new(),
            Item(7).to_event(StageId::new().into()),
        );
        let report = adapter
            .consume_committed_report(input.into(), Default::default())
            .await
            .unwrap();
        assert_eq!(*observed.lock().unwrap(), vec![7]);
        assert!(matches!(
            report.primary.result,
            DeliveryResult::Success { .. }
        ));
        assert_eq!(
            report.primary.delivery_method,
            DeliveryMethod::Custom("tracing".into())
        );
        assert_eq!(report.primary.items_delivered, Some(1));
        assert_eq!(report.primary.bytes_processed, None);
        assert!(report.commit_receipts.is_empty());
        adapter.flush_report().await.unwrap();
        adapter.drain_report().await.unwrap();
        assert_eq!(*observed.lock().unwrap(), vec![7]);
    }
}
