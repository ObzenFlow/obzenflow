// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Durable, credential-free effect-binding evidence types (FLOWIP-132a).

use serde::{Deserialize, Serialize};

/// Maximum canonical evidence carried by one typed binding before hashing.
///
/// The bytes are never serialised into a run archive. The bound exists so an
/// outward evidence implementation cannot hand the runtime an unbounded value
/// to hash while materialising a flow.
pub const MAX_BINDING_EVIDENCE_BYTES: usize = 4096;

/// Bounded canonical bytes supplied by an effect-specific evidence schema.
#[derive(Clone, PartialEq, Eq)]
pub struct BoundedBindingEvidence(Vec<u8>);

impl BoundedBindingEvidence {
    pub fn try_new(bytes: impl Into<Vec<u8>>) -> Result<Self, BindingEvidenceError> {
        let bytes = bytes.into();
        if bytes.len() > MAX_BINDING_EVIDENCE_BYTES {
            return Err(BindingEvidenceError::TooLarge);
        }
        Ok(Self(bytes))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Debug for BoundedBindingEvidence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BoundedBindingEvidence")
            .field("bytes", &"<not disclosed>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum BindingEvidenceError {
    #[error("binding evidence exceeds the framework byte bound")]
    TooLarge,
}

/// Credential-free evidence naming the system that answers a request.
///
/// Its canonical bytes are the one-key JSON object `{"system":"<name>"}`.
#[derive(Clone, PartialEq, Eq)]
pub struct SystemBindingEvidence {
    canonical: BoundedBindingEvidence,
}

impl SystemBindingEvidence {
    pub fn named(system: &str) -> Result<Self, BindingEvidenceError> {
        let canonical = serde_json::json!({ "system": system }).to_string();
        Ok(Self {
            canonical: BoundedBindingEvidence::try_new(canonical.into_bytes())?,
        })
    }

    pub fn canonical_bytes(&self) -> &BoundedBindingEvidence {
        &self.canonical
    }
}

impl std::fmt::Debug for SystemBindingEvidence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SystemBindingEvidence")
            .field("canonical", &self.canonical)
            .finish()
    }
}

/// Versioned digest included in durable effect descriptor identity.
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BindingEvidenceDigest {
    pub schema_version: u32,
    pub digest: String,
}

impl BindingEvidenceDigest {
    pub fn new(schema_version: u32, digest: impl Into<String>) -> Self {
        Self {
            schema_version,
            digest: digest.into(),
        }
    }
}

impl std::fmt::Debug for BindingEvidenceDigest {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("BindingEvidenceDigest")
            .field("schema_version", &self.schema_version)
            .field("digest", &"<descriptor identity>")
            .finish()
    }
}

/// Durable binding identity carried by every effect descriptor.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum EffectBindingIdentity {
    Portless,
    Named { evidence: BindingEvidenceDigest },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_evidence_debug_never_projects_bytes() {
        let evidence = BoundedBindingEvidence::try_new(b"credential-canary".to_vec()).unwrap();
        let rendered = format!("{evidence:?}");
        assert!(!rendered.contains("credential-canary"));
        assert!(rendered.contains("not disclosed"));
    }

    #[test]
    fn system_evidence_is_the_one_key_system_object() {
        let evidence = SystemBindingEvidence::named("simulated-risk-engine").unwrap();
        assert_eq!(
            evidence.canonical_bytes().as_bytes(),
            br#"{"system":"simulated-risk-engine"}"#
        );
    }

    #[test]
    fn system_evidence_refuses_bytes_past_the_framework_bound() {
        let oversize = "s".repeat(MAX_BINDING_EVIDENCE_BYTES);
        assert_eq!(
            SystemBindingEvidence::named(&oversize),
            Err(BindingEvidenceError::TooLarge)
        );
    }

    #[test]
    fn binding_identity_has_an_explicit_required_wire_mode() {
        let identity = EffectBindingIdentity::Named {
            evidence: BindingEvidenceDigest::new(3, "abc"),
        };
        let wire = serde_json::to_value(identity).unwrap();
        assert_eq!(wire["mode"], "named");
        assert_eq!(wire["evidence"]["schema_version"], 3);
    }
}
