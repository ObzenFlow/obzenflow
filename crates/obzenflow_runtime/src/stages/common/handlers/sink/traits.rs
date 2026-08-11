// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Erased runtime protocol for sink stages.
//!
//! ## The sink contract (FLOWIP-120f/120s)
//!
//! A sink is delivery-only: it consumes facts and emits receipts, and it is
//! the surface for **idempotent, recompute-safe, receipt-governed** writes,
//! `view = f(facts)`: a materialized-view upsert, a keyed queue publish, a
//! console projection. Replay and resume re-consume a sink's tape, so a
//! non-idempotent external write whose outcome matters belongs behind the
//! effect boundary instead: an effectful transform performs it through
//! `fx.perform`, authors named outcome facts, and a plain sink projects
//! those facts. A destination that absorbs duplicates itself may stay a
//! sink, declared `NonIdempotentExternal`, governed by the archive-verb
//! gates.
//!
//! The runtime journals each `DeliveryPayload`, stamping its `destination`
//! from the typed handler's snapshotted [`SinkDeliveryDeclaration`]
//! (else the stage name), so delivery success, partials, and failures are
//! durable and queryable.
//!
//! ## Quick start: typed sink authoring
//!
//! This trait is the erased runtime substrate. Authored sinks implement
//! [`TypedSinkHandler`](super::typed::TypedSinkHandler); a quick projection can
//! bind a `SinkTyped` adapter before its `sink!` declaration:
//!
//! ```ignore
//! // Bind typed adapters, optionally with declared safety and provenance.
//! let quick_handler = SinkTyped::new(|authorized: PaymentAuthorized| async move {
//!     println!("{authorized:?}");
//! });
//! let quick = sink!(PaymentAuthorized => quick_handler);
//!
//! let declared_handler = SinkTyped::with_delivery(
//!     |authorized: PaymentAuthorized, delivery| async move {
//!         audit(authorized, delivery.provenance());
//!     },
//! );
//! let declared = sink!(PaymentAuthorized => declared_handler, delivery: idempotent);
//!
//! // A named destination implements TypedSinkHandler directly.
//! let shipping = ShippingHandoff::new(queue);
//! let production = sink!(PaymentAuthorized => shipping);
//! ```
//!
//! Buffered destinations use `SinkInputContext::defer` and return typed commit
//! receipts from `consume`, `flush`, or `drain`; the sole runtime adapter lowers
//! those capabilities onto this protocol.

use crate::effects::EffectInvocationContext;
use crate::stages::common::handler_error::HandlerError;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
use obzenflow_core::{ChainEvent, EventId};

#[derive(Debug, Clone)]
pub struct CommitReceipt {
    pub parent_event_id: EventId,
    pub payload: DeliveryPayload,
}

#[derive(Debug, Clone)]
pub struct SinkConsumeReport {
    pub primary: DeliveryPayload,
    pub commit_receipts: Vec<CommitReceipt>,
}

impl SinkConsumeReport {
    pub fn new(primary: DeliveryPayload) -> Self {
        Self {
            primary,
            commit_receipts: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SinkLifecycleReport {
    pub audit_payload: Option<DeliveryPayload>,
    pub commit_receipts: Vec<CommitReceipt>,
}

/// Trait every **sink stage** must implement.
#[async_trait]
pub trait SinkHandler: Send + Sync {
    /// Consume a single event and return a `DeliveryPayload` describing
    /// the outcome (success, partial, or failure).
    ///
    /// Returning `Err(HandlerError)` means the handler experienced a failure
    /// while processing this event (e.g., remote timeout, decode failure).
    /// The supervisor will turn this into an error-marked event using
    /// ErrorKind, route it appropriately, and keep the sink running.
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError>;

    /// Extended consume hook for buffered sinks that may need to emit
    /// additional commit receipts after accepting the current event.
    ///
    /// Default behaviour preserves the legacy `consume()` contract so existing
    /// sinks do not need to change.
    async fn consume_report(
        &mut self,
        event: ChainEvent,
    ) -> Result<SinkConsumeReport, HandlerError> {
        Ok(SinkConsumeReport::new(self.consume(event).await?))
    }

    /// Flush in‑memory buffers **and optionally** emit a `DeliveryPayload`
    /// capturing the flush action (e.g., `DeliveryResult::Success` for a batch
    /// commit).  Default impl returns `Ok(None)` so simple sinks can ignore it.
    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        Ok(None)
    }

    /// Extended flush hook for buffered sinks that need to emit per-event
    /// commit receipts after a successful flush.
    ///
    /// Default behaviour preserves the legacy `flush()` contract so existing
    /// sinks do not need to change.
    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        Ok(SinkLifecycleReport {
            audit_payload: self.flush().await?,
            commit_receipts: Vec::new(),
        })
    }

    /// Draining hook called during graceful shutdown.
    /// Default behaviour delegates to `flush()` so most sinks only override
    /// one method.
    async fn drain(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        self.flush().await
    }

    /// Extended drain hook for buffered sinks that need to emit per-event
    /// commit receipts after a successful drain.
    ///
    /// Default behaviour preserves the legacy `drain()` contract so existing
    /// sinks do not need to change.
    async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        Ok(SinkLifecycleReport {
            audit_payload: self.drain().await?,
            commit_receipts: Vec::new(),
        })
    }
}

#[doc(hidden)]
#[async_trait]
pub trait UnifiedSinkHandler: Send + Sync {
    /// Consume one event. `scope` is the per-event middleware execution
    /// scope computed by the supervisor at dispatch (FLOWIP-120c H3);
    /// handlers without middleware ignore it.
    async fn consume_report(
        &mut self,
        event: ChainEvent,
        effect_context: Option<EffectInvocationContext>,
        scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<SinkConsumeReport, HandlerError>;

    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError>;

    async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError>;

    fn stage_logic_version(&self) -> &str {
        "1"
    }

    // Delivery declarations deliberately do not exist on either erased
    // runtime trait. The DSL snapshots the aggregate declaration from
    // `TypedSinkHandler` before this boundary (FLOWIP-134h).
}

#[async_trait]
impl<T: SinkHandler + Send + Sync> UnifiedSinkHandler for T {
    async fn consume_report(
        &mut self,
        event: ChainEvent,
        _effect_context: Option<EffectInvocationContext>,
        _scope: obzenflow_core::MiddlewareExecutionScope,
    ) -> Result<SinkConsumeReport, HandlerError> {
        SinkHandler::consume_report(self, event).await
    }

    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        SinkHandler::flush_report(self).await
    }

    async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        SinkHandler::drain_report(self).await
    }
}
