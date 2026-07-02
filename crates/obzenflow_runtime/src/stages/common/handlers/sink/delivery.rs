// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class typed deliveries (FLOWIP-120s).
//!
//! A [`Delivery`] is a named, typed destination: the delivery lane's analogue
//! of `Effect`. It carries a stable destination-family identity
//! (`DELIVERY_TYPE`), a compile-time duplicate-safety declaration (`SAFETY`),
//! and the delivery behaviour itself. A `Delivery` enters a flow through the
//! ordinary `sink!(Input => handler)` arm via the blanket [`SinkHandler`]
//! bridge below; there is no separate macro or stage kind.
//!
//! The typed tier is always strict (a type mismatch errors; there is no
//! `allow_skip`) and per-event (no flush/drain buffering; buffered
//! destinations stay on raw [`SinkHandler`]).

use crate::effects::SinkDeliverySafety;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::SinkHandler;
use crate::stages::sink::DeliveryContext;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, TypedPayload};
use serde::de::DeserializeOwned;

/// A named, typed destination consumed by a sink stage.
///
/// `Clone + Debug + Send + Sync + 'static` matches what `SinkDescriptor`
/// demands of every handler it carries, and mirrors the `Effect` bounds.
#[async_trait]
pub trait Delivery: Clone + std::fmt::Debug + Send + Sync + 'static {
    type Input: TypedPayload + DeserializeOwned + Send + Sync + 'static;

    /// Stable destination-family identity. Never derived from Rust type
    /// names. A family, not an instance: two deliveries pointed at different
    /// queues share it, and instance coordinates live in
    /// [`Delivery::canonical_destination`].
    const DELIVERY_TYPE: &'static str;

    /// Duplicate-safety of the destination. No default: forgetting it fails
    /// to compile.
    const SAFETY: SinkDeliverySafety;

    /// How receipts describe this destination. Overridable per write through
    /// [`Delivered::with_method`].
    fn method(&self) -> DeliveryMethod {
        DeliveryMethod::Custom(Self::DELIVERY_TYPE.to_string())
    }

    /// Instance coordinates at which duplicate deliveries collide, for the
    /// FLOWIP-095g recovery compatibility gate. Include routing-relevant
    /// configuration (queue, table, URL); omit volatile detail (credentials,
    /// pool sizes). A per-event-routed destination declares its routing rule.
    /// `None` is undeclared: a non-idempotent delivery then never earns
    /// recovery suppression.
    fn canonical_destination(&self) -> Option<serde_json::Value> {
        None
    }

    async fn deliver(
        &mut self,
        input: Self::Input,
        ctx: &DeliveryContext,
    ) -> Result<Delivered, HandlerError>;
}

/// Outcome carrier returned by [`Delivery::deliver`] (the delivery-lane twin
/// of the FLOWIP-120m effect outcome carriers). The framework builds the
/// receipt from it.
#[derive(Debug, Clone, Default)]
pub struct Delivered {
    items: Option<u64>,
    method: Option<DeliveryMethod>,
}

impl Delivered {
    /// One item delivered.
    pub fn one() -> Self {
        Self {
            items: Some(1),
            method: None,
        }
    }

    /// `n` items delivered.
    pub fn count(n: u64) -> Self {
        Self {
            items: Some(n),
            method: None,
        }
    }

    /// Nothing delivered (a filtered no-op).
    pub fn none() -> Self {
        Self::default()
    }

    /// Override the receipt's method with write-produced detail (for example
    /// the partition a record landed on).
    pub fn with_method(mut self, method: DeliveryMethod) -> Self {
        self.method = Some(method);
        self
    }

    pub fn items(&self) -> Option<u64> {
        self.items
    }

    pub fn method_override(&self) -> Option<&DeliveryMethod> {
        self.method.as_ref()
    }
}

// Bridge onto the existing sink machinery. Coherence note: this is the only
// blanket impl of `SinkHandler`; a type implementing both traits directly
// fails at its own impl site, which is the intended steer. Fallback if a
// second blanket is ever needed: a `Deliver<D>` wrapper through the same arm.
#[async_trait]
impl<D: Delivery> SinkHandler for D {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let (event_type, payload) = match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => (event_type.as_str(), payload),
            _ => {
                return Ok(DeliveryPayload::success(
                    DeliveryMethod::Custom("Skipped".to_string()),
                    None,
                ))
            }
        };

        if !D::Input::event_type_matches(event_type) {
            return Err(HandlerError::Validation(format!(
                "Delivery '{}' expected event type '{}' (or '{}'), got '{}'",
                D::DELIVERY_TYPE,
                D::Input::EVENT_TYPE,
                D::Input::versioned_event_type(),
                event_type
            )));
        }

        let context = DeliveryContext::from_event(&event);

        let typed_input: D::Input = serde_json::from_value(payload.clone()).map_err(|e| {
            HandlerError::Deserialization(format!(
                "Delivery '{}' failed to deserialize {}: {e}",
                D::DELIVERY_TYPE,
                std::any::type_name::<D::Input>()
            ))
        })?;

        let delivered = self.deliver(typed_input, &context).await?;

        let method = delivered
            .method_override()
            .cloned()
            .unwrap_or_else(|| self.method());
        // The receipt destination is supervisor-stamped from `delivery_type()`.
        let mut receipt = DeliveryPayload::success(method, None);
        if let Some(items) = delivered.items() {
            receipt = receipt.with_items(items);
        }
        Ok(receipt)
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(D::SAFETY)
    }

    fn delivery_type(&self) -> Option<&'static str> {
        Some(D::DELIVERY_TYPE)
    }

    fn canonical_destination(&self) -> Option<serde_json::Value> {
        Delivery::canonical_destination(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::sink::DeliveryProvenance;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct TestPayload {
        n: usize,
    }

    impl TypedPayload for TestPayload {
        const EVENT_TYPE: &'static str = "test.payload";
    }

    type Seen = Arc<Mutex<Vec<(TestPayload, DeliveryProvenance)>>>;

    #[derive(Clone, Debug)]
    struct TestDelivery {
        seen: Seen,
        override_method: bool,
    }

    #[async_trait]
    impl Delivery for TestDelivery {
        type Input = TestPayload;
        const DELIVERY_TYPE: &'static str = "test.destination";
        const SAFETY: SinkDeliverySafety = SinkDeliverySafety::IdempotentProjection;

        fn method(&self) -> DeliveryMethod {
            DeliveryMethod::QueuePublish {
                queue_name: "test-queue".to_string(),
            }
        }

        fn canonical_destination(&self) -> Option<serde_json::Value> {
            Some(serde_json::json!({ "queue": "test-queue" }))
        }

        async fn deliver(
            &mut self,
            input: TestPayload,
            ctx: &DeliveryContext,
        ) -> Result<Delivered, HandlerError> {
            self.seen
                .lock()
                .expect("seen lock poisoned")
                .push((input, ctx.provenance()));
            if self.override_method {
                Ok(Delivered::one()
                    .with_method(DeliveryMethod::Custom("per-write-detail".to_string())))
            } else {
                Ok(Delivered::one())
            }
        }
    }

    fn delivery() -> (TestDelivery, Seen) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (
            TestDelivery {
                seen: seen.clone(),
                override_method: false,
            },
            seen,
        )
    }

    #[tokio::test]
    async fn delivery_bridge_skips_non_data_events() {
        let (mut sink, seen) = delivery();
        let event = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        let receipt = SinkHandler::consume(&mut sink, event)
            .await
            .expect("non-data events skip");
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "Skipped"
        ));
        assert!(seen.lock().expect("seen lock poisoned").is_empty());
    }

    #[tokio::test]
    async fn delivery_bridge_is_always_strict_on_type_mismatch() {
        let (mut sink, _) = delivery();
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "other.type",
            serde_json::json!({"n": 1}),
        );
        let err = SinkHandler::consume(&mut sink, event)
            .await
            .expect_err("mismatch errors; typed tier has no allow_skip");
        assert!(matches!(err, HandlerError::Validation(_)));
    }

    #[tokio::test]
    async fn delivery_bridge_errors_on_parse_failure() {
        let (mut sink, _) = delivery();
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"wrong": 1}),
        );
        let err = SinkHandler::consume(&mut sink, event)
            .await
            .expect_err("bad payload");
        assert!(matches!(err, HandlerError::Deserialization(_)));
    }

    #[tokio::test]
    async fn delivery_bridge_builds_receipt_from_the_type() {
        let (mut sink, seen) = delivery();
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"n": 42}),
        );
        let receipt = SinkHandler::consume(&mut sink, event)
            .await
            .expect("delivery succeeds");

        // Destination is supervisor-stamped at journalling from delivery_type();
        // handler-level receipts leave it empty.
        assert_eq!(receipt.destination, "");
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::QueuePublish { ref queue_name } if queue_name == "test-queue"
        ));
        assert_eq!(receipt.items_delivered, Some(1));
        assert_eq!(receipt.bytes_processed, None);
        assert_eq!(
            seen.lock().expect("seen lock poisoned")[0],
            (TestPayload { n: 42 }, DeliveryProvenance::Live)
        );
    }

    #[tokio::test]
    async fn delivery_bridge_reports_replayed_provenance() {
        let (mut sink, seen) = delivery();
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"n": 9}),
        );
        event.replay_context = Some(obzenflow_core::event::context::ReplayContext {
            original_event_id: obzenflow_core::event::types::EventId::new(),
            original_flow_id: "flow_01SOURCE".to_string(),
            original_stage_id: StageId::new(),
            archive_path: std::path::PathBuf::from("tmp/archive"),
            replayed_at: chrono::Utc::now(),
        });
        SinkHandler::consume(&mut sink, event)
            .await
            .expect("delivery succeeds");
        assert_eq!(
            seen.lock().expect("seen lock poisoned")[0].1,
            DeliveryProvenance::Replayed
        );
    }

    #[tokio::test]
    async fn delivered_with_method_overrides_the_receipt_method() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mut sink = TestDelivery {
            seen,
            override_method: true,
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestPayload::EVENT_TYPE,
            serde_json::json!({"n": 1}),
        );
        let receipt = SinkHandler::consume(&mut sink, event)
            .await
            .expect("delivery succeeds");
        assert!(matches!(
            receipt.delivery_method,
            DeliveryMethod::Custom(ref name) if name == "per-write-detail"
        ));
    }

    #[test]
    fn declarations_forward_through_sink_handler() {
        let (sink, _) = delivery();
        assert_eq!(
            SinkHandler::delivery_safety(&sink),
            Some(SinkDeliverySafety::IdempotentProjection)
        );
        assert_eq!(SinkHandler::delivery_type(&sink), Some("test.destination"));
        assert_eq!(
            SinkHandler::canonical_destination(&sink),
            Some(serde_json::json!({ "queue": "test-queue" }))
        );
    }
}
