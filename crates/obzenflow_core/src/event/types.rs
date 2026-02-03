// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core type aliases and newtypes used throughout the event system

use serde::{Deserialize, Serialize};

// Re-export identity types from their newtype modules
pub use crate::event::identity::{CorrelationId, EventId, JournalWriterId, WriterId};

// === Pipeline types ===
// Note: StageId and SystemId are proper newtypes in the id module
pub type FlowId = String;

/// Domain newtype for sequence numbers to avoid raw u64 usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeqNo(pub u64);

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    Other(String),
}
