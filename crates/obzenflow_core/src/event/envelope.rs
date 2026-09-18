// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The authored and committed envelope compositions.

use super::observability::ObservabilityContext;
use super::provenance::{AuthoredProvenance, Provenance};
use serde::{Deserialize, Serialize};

/// An author cannot supply physical journal commitment through this type.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoredEnvelope<E> {
    pub provenance: AuthoredProvenance<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventEnvelope<E> {
    pub provenance: Provenance<E>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observability: Option<ObservabilityContext>,
}
