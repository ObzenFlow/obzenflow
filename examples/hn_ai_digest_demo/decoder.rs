// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::HnStory;
use obzenflow::stages::sources::{SourceDiagnosticReason, SourceError};
use serde_json::Value;

pub fn decode_item(record: Value) -> Result<HnStory, SourceError> {
    serde_json::from_value(record)
        .map_err(|_| SourceError::Deserialization(SourceDiagnosticReason::MalformedInput.into()))
}
