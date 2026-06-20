// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core type aliases and newtypes used throughout the event system

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::fmt;

// Re-export identity types from their newtype modules
pub use crate::event::identity::{CorrelationId, EventId, JournalWriterId, WriterId};

// === Pipeline types ===
// Note: StageId and SystemId are proper newtypes in the id module
pub type FlowId = String;

/// Domain newtype for sequence numbers to avoid raw u64 usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeqNo(pub u64);

/// Canonical event-type identity used when event types are map keys.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct EventType(pub String);

impl EventType {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for EventType {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for EventType {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<EventType> for String {
    fn from(value: EventType) -> Self {
        value.0
    }
}

impl AsRef<str> for EventType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for EventType {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for EventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl PartialEq<&str> for EventType {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<EventType> for &str {
    fn eq(&self, other: &EventType) -> bool {
        *self == other.as_str()
    }
}

/// Domain newtype for counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Count(pub u64);

/// Duration in milliseconds, as a domain newtype.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DurationMs(pub u64);

/// Route key for routed streams.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RouteKey(pub String);

/// Journal path identifier.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JournalPath(pub String);

/// Journal index identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct JournalIndex(pub u64);

/// Reason for at-least-once violations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "violation_type", content = "details", rename_all = "snake_case")]
pub enum ViolationCause {
    ExpectedCountMismatch {
        expected: Count,
        actual: Count,
    },
    SeqDivergence {
        advertised: Option<SeqNo>,
        reader: SeqNo,
    },
    GapDetected {
        from: SeqNo,
        to: SeqNo,
    },
    StallExceeded {
        threshold_ms: DurationMs,
    },
    Divergence {
        predicate: String,
        observed: f64,
        threshold: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        window_seconds: Option<u64>,
    },
    Other(String),
}
