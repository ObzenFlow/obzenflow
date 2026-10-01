// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Delivery payloads & results for sink stages
//!
//! Typed sinks describe delivery through their outcome carriers; the runtime
//! lowers those outcomes into this durable shape and journals whether delivery
//! fully succeeded, partially succeeded, was buffered, or failed.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;

/// Exact committed input that this receipt settles. Additional causal parents
/// are dependencies only and never grant settlement authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliverySubject {
    pub input: crate::event::JournalCommitRef,
    pub event_kind: super::chain_payload::EventKind,
    pub event_type: crate::EventType,
    pub payload_schema_version: std::num::NonZeroU32,
}

impl DeliverySubject {
    pub fn from_record(record: &crate::event::JournalRecord<super::ChainPayload>) -> Self {
        let provenance = &record.envelope.provenance.event;
        Self {
            input: record.commitment(),
            event_kind: provenance.event_kind,
            event_type: provenance.event_type.clone().into(),
            payload_schema_version: provenance.payload_schema_version,
        }
    }
    pub fn matches_record(
        &self,
        record: &crate::event::JournalRecord<super::ChainPayload>,
    ) -> bool {
        self == &Self::from_record(record)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryPayload {
    pub subject: DeliverySubject,
    #[serde(flatten)]
    pub outcome: DeliveryOutcome,
}

impl std::ops::Deref for DeliveryPayload {
    type Target = DeliveryOutcome;
    fn deref(&self) -> &Self::Target {
        &self.outcome
    }
}

impl DeliveryPayload {
    pub fn event_type(&self) -> &'static str {
        match self.result {
            DeliveryResult::Buffered { .. } => "delivery.buffered",
            DeliveryResult::Success { .. } => "delivery.succeeded",
            DeliveryResult::Partial { .. } => "delivery.partially_succeeded",
            DeliveryResult::Failed { .. } => "delivery.failed",
            DeliveryResult::Rejected { .. } => "delivery.rejected",
        }
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.subject.input.sequence == 0 {
            return Err("delivery subject must be committed");
        }
        self.outcome.validate()
    }
}

/// A lifecycle audit describes an operation, without any input settlement authority.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SinkAuditPayload {
    pub operation: SinkLifecycleOperation,
    #[serde(flatten)]
    pub outcome: DeliveryOutcome,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkLifecycleOperation {
    Flush,
    Drain,
}

impl SinkAuditPayload {
    pub fn event_type(&self) -> &'static str {
        match (self.operation, &self.outcome.result) {
            (SinkLifecycleOperation::Flush, DeliveryResult::Partial { .. }) => {
                "sink.flush_partially_succeeded"
            }
            (SinkLifecycleOperation::Drain, DeliveryResult::Partial { .. }) => {
                "sink.drain_partially_succeeded"
            }
            (SinkLifecycleOperation::Flush, _) => "sink.flush_succeeded",
            (SinkLifecycleOperation::Drain, _) => "sink.drain_succeeded",
        }
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        if !matches!(
            self.outcome.result,
            DeliveryResult::Success { .. } | DeliveryResult::Partial { .. }
        ) {
            return Err("sink audit requires success or partial success");
        }
        self.outcome.validate()
    }
}

// ────────────────────────────────────────────────────────────────────────────
// Core payload
// ────────────────────────────────────────────────────────────────────────────
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryOutcome {
    /// Delivery outcome
    pub result: DeliveryResult,

    /// Where and how
    pub destination: String,
    pub delivery_method: DeliveryMethod,

    /// Bytes processed, when measured by the connector.
    pub bytes_processed: Option<u64>,

    /// Items delivered (typed deliveries, FLOWIP-120s).
    /// Report item counts here even when the byte count is unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_delivered: Option<u64>,

    /// When + any middleware extensions
    pub processed_at: DateTime<Utc>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::serde_support::present_json"
    )]
    pub middleware_context: Option<Value>,
}

// ────────────────────────────────────────────────────────────────────────────
// Delivery method taxonomy
// ────────────────────────────────────────────────────────────────────────────
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    Rejected {
        policy: String,
        reason: String,
    },
    Failed {
        error_type: String,
        error_message: String,
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
impl DeliveryOutcome {
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
    ) -> Self {
        Self {
            result: DeliveryResult::Failed {
                error_type: error_type.into(),
                error_message: error_msg.into(),
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
impl DeliveryOutcome {
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

impl DeliveryOutcome {
    pub fn validate(&self) -> Result<(), &'static str> {
        if let DeliveryResult::Partial {
            successful_count,
            failed_count,
            ..
        } = &self.result
        {
            if *successful_count == 0 || *failed_count == 0 {
                return Err("partial delivery requires positive successful and failed counts");
            }
            if self
                .items_delivered
                .is_some_and(|items| items != *successful_count)
            {
                return Err("partial delivery items_delivered disagrees with successful_count");
            }
        }
        Ok(())
    }
    pub fn rejected(
        method: DeliveryMethod,
        policy: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        let mut outcome = Self::success(method, None);
        outcome.result = DeliveryResult::Rejected {
            policy: policy.into(),
            reason: reason.into(),
        };
        outcome
    }
}

#[cfg(test)]
pub(crate) fn test_receipt(input: crate::EventId, outcome: DeliveryOutcome) -> DeliveryPayload {
    DeliveryPayload {
        subject: DeliverySubject {
            input: crate::event::JournalCommitRef {
                run_id: crate::FlowId::new(),
                journal_writer_id: crate::JournalWriterId::new(),
                sequence: 1,
                event_id: input,
            },
            event_kind: super::chain_payload::EventKind::Fact,
            event_type: "test.event".into(),
            payload_schema_version: std::num::NonZeroU32::MIN,
        },
        outcome,
    }
}
