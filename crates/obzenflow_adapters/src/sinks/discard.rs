// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Explicit terminal consumption without an external delivery.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkDescription, SinkOperationResult, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteReport, SinkWriteResult, SinkWriter, SinkWriterInitContext,
};
use std::marker::PhantomData;

/// Consume inputs deliberately without storing or publishing them.
///
/// Each input still receives a terminal receipt. Its method is `Noop`, with zero
/// physical bytes and items delivered. Use a real connector when delivery matters.
pub struct DiscardSink<T> {
    _input: PhantomData<fn() -> T>,
}

impl<T: TypedPayload + Send + Sync + 'static> DiscardSink<T> {
    #[allow(
        clippy::new_without_default,
        reason = "Built-in sinks expose one construction entry point"
    )]
    pub fn new() -> Self {
        Self {
            _input: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for DiscardSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscardSink")
            .field("input", &std::any::type_name::<T>())
            .finish()
    }
}

/// The stateless execution of a [`DiscardSink`].
pub struct DiscardWriter<T> {
    _input: PhantomData<fn() -> T>,
}

#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> SinkConnector for DiscardSink<T> {
    type Input = T;
    type Writer = DiscardWriter<T>;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        Ok(DiscardWriter {
            _input: PhantomData,
        })
    }
}

#[async_trait]
impl<T: TypedPayload + Send + Sync + 'static> SinkWriter for DiscardWriter<T> {
    type Input = T;

    async fn write(&mut self, _input: T, _context: SinkWriteContext) -> SinkWriteResult {
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success(Some(0)).with_items(0),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::JournalRecord;
    use obzenflow_core::{JournalWriterId, StageId};
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, SinkWriterAdapter};
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize, Serialize)]
    struct Item(u64);
    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "discard.test.item";
    }

    #[tokio::test]
    async fn discarded_input_is_terminal_without_claiming_physical_delivery() {
        let connector = DiscardSink::<Item>::new();
        let description = connector.describe();
        assert_eq!(description.default_method(), &DeliveryMethod::Noop);
        assert_eq!(
            description.redelivery_safety(),
            Some(SinkRedeliverySafety::SafeToRepeat)
        );
        let stage = StageId::new();
        let writer = connector
            .open(SinkWriterInitContext::new(
                stage,
                "discard".into(),
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
        assert!(matches!(
            report.primary.result,
            DeliveryResult::Success { .. }
        ));
        assert_eq!(report.primary.delivery_method, DeliveryMethod::Noop);
        assert_eq!(report.primary.items_delivered, Some(0));
        assert_eq!(report.primary.bytes_processed, Some(0));
        assert!(report.commit_receipts.is_empty());
        assert!(adapter
            .flush_report()
            .await
            .unwrap()
            .commit_receipts
            .is_empty());
        assert!(adapter
            .drain_report()
            .await
            .unwrap()
            .commit_receipts
            .is_empty());
    }
}
