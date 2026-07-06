// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime incarnation identity for Studio phonebook and SSE fencing.

use serde::Serialize;
use std::fmt;

/// Per-process runtime incarnation identity.
///
/// This is an opaque equality/fencing token, not an auth secret and not a
/// persisted flow/job identity. Its wire representation is a string so it can
/// cross JSON, SSE, and query parameters unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct RuntimeInstanceId(String);

impl RuntimeInstanceId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[cfg(all(test, feature = "studio-registration"))]
    pub(crate) fn for_test(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl Default for RuntimeInstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for RuntimeInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
