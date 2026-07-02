// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Delivery payloads & results for sink stages
//!
//! A `SinkHandler::consume()` must return one of these to let the runtime
//! journal whether delivery fully succeeded, partially succeeded, or failed.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

// ────────────────────────────────────────────────────────────────────────────
// Core payload
// ────────────────────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryPayload {
    /// Delivery outcome
    pub result: DeliveryResult,

    /// Where and how
    pub destination: String,
    pub delivery_method: DeliveryMethod,

    /// Performance
    pub bytes_processed: Option<u64>,

    /// Items delivered (typed deliveries, FLOWIP-120s). Distinct from
    /// `bytes_processed`, which some closure-tier sinks historically misuse
    /// as an item count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_delivered: Option<u64>,

    /// When + any middleware extensions
    pub processed_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub middleware_context: Option<Value>,
}

// ────────────────────────────────────────────────────────────────────────────
// Delivery method taxonomy
// ────────────────────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryMethod {
    HttpPost { url: String },
    HttpPut { url: String },
    S3Upload { bucket: String, key: String },
    DatabaseInsert { table: String },
    QueuePublish { queue_name: String },
    FileWrite { path: PathBuf },
    Noop,           // /dev/null sink
    Custom(String), // user‑defined
}

// ────────────────────────────────────────────────────────────────────────────
// Outcome variants
// ────────────────────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum DeliveryResult {
    Buffered {},
    Success {
        #[serde(skip_serializing_if = "Option::is_none")]
        confirmation: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        response_headers: Option<HashMap<String, String>>,
    },
    Failed {
        error_type: String,
        error_message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_code: Option<String>,
        #[serde(default)]
        final_attempt: bool,
    },
    Partial {
        successful_count: u64,
        failed_count: u64,
        error_summary: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        failed_items: Option<Vec<String>>,
    },
}

// ────────────────────────────────────────────────────────────────────────────
// Convenience builders
// ────────────────────────────────────────────────────────────────────────────
impl DeliveryPayload {
    // The `destination` field is framework-stamped at journalling time from
    // the sink's declared delivery type, else the stage name (FLOWIP-120s);
    // constructors leave it empty.

    /// Generic full‑success builder (works for *any* delivery method).
    pub fn success(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            result: DeliveryResult::Success {
                confirmation: None,
                response_headers: None,
            },
            destination: String::new(),
            delivery_method: method,
            bytes_processed,
            items_delivered: None,
            processed_at: Utc::now(),
            middleware_context: None,
        }
    }

    /// Generic buffered-accept builder for sinks that have accepted work into
    /// an in-memory or connector-local buffer but have not durably committed it yet.
    pub fn buffered(method: DeliveryMethod, bytes_processed: Option<u64>) -> Self {
        Self {
            result: DeliveryResult::Buffered {},
            destination: String::new(),
            delivery_method: method,
            bytes_processed,
            items_delivered: None,
            processed_at: Utc::now(),
            middleware_context: None,
        }
    }

    /// Failure helper (any method).
    pub fn failed(
        method: DeliveryMethod,
        error_type: impl Into<String>,
        error_msg: impl Into<String>,
        final_attempt: bool,
    ) -> Self {
        Self {
            result: DeliveryResult::Failed {
                error_type: error_type.into(),
                error_message: error_msg.into(),
                error_code: None,
                final_attempt,
            },
            destination: String::new(),
            delivery_method: method,
            bytes_processed: None,
            items_delivered: None,
            processed_at: Utc::now(),
            middleware_context: None,
        }
    }

    /// Partial‑success helper.
    pub fn partial(
        method: DeliveryMethod,
        ok: u64,
        bad: u64,
        summary: impl Into<String>,
        failed_items: Option<Vec<String>>,
    ) -> Self {
        Self {
            result: DeliveryResult::Partial {
                successful_count: ok,
                failed_count: bad,
                error_summary: summary.into(),
                failed_items,
            },
            destination: String::new(),
            delivery_method: method,
            bytes_processed: None,
            items_delivered: None,
            processed_at: Utc::now(),
            middleware_context: None,
        }
    }

    /// HTTP‑specific convenience (kept from your earlier helper).
    pub fn http_post_success(
        url: impl Into<String>,
        bytes: Option<u64>,
        headers: Option<HashMap<String, String>>,
        confirmation: Option<String>,
    ) -> Self {
        let url: String = url.into();
        Self {
            destination: String::new(),
            delivery_method: DeliveryMethod::HttpPost { url },
            bytes_processed: bytes,
            result: DeliveryResult::Success {
                confirmation,
                response_headers: headers,
            },
            items_delivered: None,
            processed_at: Utc::now(),
            middleware_context: None,
        }
    }
}

// Builder-style methods for enhancing payloads
impl DeliveryPayload {
    /// Update the middleware context (builder style)
    pub fn with_middleware_context(mut self, context: Value) -> Self {
        self.middleware_context = Some(context);
        self
    }

    /// Update bytes processed (builder style)
    pub fn with_bytes_processed(mut self, bytes: u64) -> Self {
        self.bytes_processed = Some(bytes);
        self
    }

    /// Set items delivered (builder style; typed deliveries, FLOWIP-120s)
    pub fn with_items(mut self, items: u64) -> Self {
        self.items_delivered = Some(items);
        self
    }
}
