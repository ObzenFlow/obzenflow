// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::payloads::execution_payload::{SourcePollContinuation, SourcePollErrorKind};
use crate::event::{SourceDiagnostic, SourceDiagnosticReason, SourceReadFailure};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

/// A supplier's serializable position within its input.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct QueryBookmark(Value);

impl QueryBookmark {
    pub fn encode<T: Serialize>(position: &T) -> Result<Self, QueryError> {
        serde_json::to_value(position).map(Self).map_err(|_| {
            QueryError::terminal(
                SourcePollErrorKind::Validation,
                SourceDiagnosticReason::UnrepresentableValue,
            )
        })
    }

    pub fn decode<T: DeserializeOwned>(&self) -> Result<T, QueryError> {
        T::deserialize(&self.0).map_err(|_| {
            QueryError::terminal(
                SourcePollErrorKind::Deserialization,
                SourceDiagnosticReason::MalformedInput,
            )
        })
    }
}

/// The diagnostic and retry delay recorded for a failed query.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, thiserror::Error)]
#[error("query failed: {}", failure.diagnostic)]
pub struct QueryError {
    #[serde(flatten)]
    pub failure: Box<SourceReadFailure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<Duration>,
}

impl QueryError {
    pub fn terminal(kind: SourcePollErrorKind, diagnostic: impl Into<SourceDiagnostic>) -> Self {
        Self {
            failure: Box::new(SourceReadFailure {
                category: kind,
                continuation: SourcePollContinuation::Terminal,
                diagnostic: diagnostic.into(),
            }),
            retry_after: None,
        }
    }

    pub fn recoverable(kind: SourcePollErrorKind, diagnostic: impl Into<SourceDiagnostic>) -> Self {
        Self {
            failure: Box::new(SourceReadFailure {
                category: kind,
                continuation: SourcePollContinuation::Recoverable,
                diagnostic: diagnostic.into(),
            }),
            retry_after: None,
        }
    }

    pub fn with_retry_after(mut self, delay: Duration) -> Self {
        self.retry_after = Some(delay);
        self
    }
}
