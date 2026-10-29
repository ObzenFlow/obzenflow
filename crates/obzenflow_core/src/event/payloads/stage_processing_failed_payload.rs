// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A stage's record of an input its handler failed on.

use crate::event::status::processing_status::ErrorKind;
use crate::event::ChainEvent;
use crate::EventType;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroU32;

/// An input a stage's handler returned a non-fatal error for, with that error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StageProcessingFailed {
    pub kind: ErrorKind,
    pub detail: String,
    pub input: FailedInput,
}

/// The failed input as its upstream stage recorded it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailedInput {
    pub event_type: EventType,
    pub payload_schema_version: NonZeroU32,
    pub payload: Value,
}

impl FailedInput {
    /// The input `event` a handler received.
    pub fn of(event: &ChainEvent) -> Self {
        Self {
            event_type: EventType(event.event_type()),
            payload_schema_version: event.payload_schema_version,
            payload: event.typed_payload().unwrap_or_else(|| event.payload()),
        }
    }
}
