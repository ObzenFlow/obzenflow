// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler trait for **sink stages** that *consume* events and emit a
//! delivery receipt.
//!
//! Typical sinks: database writers, file outputs, HTTP/REST clients, Kafka
//! producers.  The runtime journals each `DeliveryPayload` so delivery
//! success, partials, and failures are durable and queryable.
//!
//! ## Quick start
//! ```ignore
//! use obzenflow_runtime::stages::common::handlers::SinkHandler;
//! use obzenflow_core::{ChainEvent, Result};
//! use async_trait::async_trait;
//! use std::collections::HashMap;
//!
//! use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
//!
//! /// Minimal HTTP POST sink.
//! struct HttpSink {
//!     client: YourHttpClient, // e.g., reqwest::Client
//!     url:    String,
//! }
//!
//! #[async_trait]
//! impl SinkHandler for HttpSink {
//!     async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload> {
//!         let start = std::time::Instant::now();
//!         let body  = event.payload().to_string();
//!         let resp  = self.client.post(&self.url).body(body).send().await?;
//!         let elapsed = start.elapsed().as_millis() as u64;
//!
//!         let headers: HashMap<_, _> = resp.headers().iter()
//!             .map(|(k,v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
//!             .collect();
//!
//!         Ok(DeliveryPayload::http_post_success(
//!             &self.url,
//!             elapsed,
//!             Some(resp.content_length().unwrap_or(0)),
//!             Some(headers),
//!             Some(resp.status().to_string()),
//!         ))
//!     }
//! }
//! ```

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
