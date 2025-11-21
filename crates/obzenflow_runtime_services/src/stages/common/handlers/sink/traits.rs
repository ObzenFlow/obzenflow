//! Handler trait for **sink stages** that *consume* events and emit a
//! delivery receipt.
//!
//! Typical sinks: database writers, file outputs, HTTP/REST clients, Kafka
//! producers.  The runtime journals each `DeliveryPayload` so delivery
//! success, partials, and failures are durable and queryable.
//!
//! ## Quick start
//! ```ignore
//! use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
//! use obzenflow_core::{ChainEvent, Result};
//! use async_trait::async_trait;
//! use std::collections::HashMap;
//! use reqwest::Client;
//!
//! use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
//!
//! /// Minimal HTTP POST sink.
//! struct HttpSink {
//!     client: reqwest::Client,
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

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
use obzenflow_core::{ChainEvent, Result};

/// Trait every **sink stage** must implement.
#[async_trait]
pub trait SinkHandler: Send + Sync {
    /// Consume a single event and return a `DeliveryPayload` describing
    /// the outcome (success, partial, or failure).
    ///
    /// Returning `Err(_)` means the handler itself experienced an unrecoverable
    /// failure *before* it could build a coherent receipt (e.g., panicked,
    /// network down, etc.).
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload>;

    /// Flush in‑memory buffers **and optionally** emit a `DeliveryPayload`
    /// capturing the flush action (e.g., `DeliveryResult::Success` for a batch
    /// commit).  Default impl returns `Ok(None)` so simple sinks can ignore it.
    async fn flush(&mut self) -> Result<Option<DeliveryPayload>> {
        Ok(None)
    }

    /// Draining hook called during graceful shutdown.
    /// Default behaviour delegates to `flush()` so most sinks only override
    /// one method.
    async fn drain(&mut self) -> Result<Option<DeliveryPayload>> {
        self.flush().await
    }
}
