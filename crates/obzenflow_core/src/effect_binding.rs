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
    #[error("the configured system location cannot be represented in binding evidence")]
    InvalidLocation,
}

/// `SystemBinding` identifies a configured system and its optional location. A source
/// and the descriptor of its read record it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemBinding {
    system: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    location: Option<SystemLocation>,
}

/// `SystemLocation` is the place that a framework system reads and the format
/// configuration that it reads with. It holds no credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "format", rename_all = "snake_case")]
pub enum SystemLocation {
    /// `Delimited` is a file of delimited text records, such as CSV or TSV.
    Delimited {
        path: std::path::PathBuf,
        delimiter: char,
        headers: DelimitedHeaders,
    },
    Yaml {
        path: std::path::PathBuf,
    },
    Json {
        endpoint: HttpEndpoint,
    },
    Application {
        configuration: serde_json::Value,
    },
    /// `Ingress` is the ingress key of a hosted endpoint.
    Ingress {
        key: String,
    },
}

/// `DelimitedHeaders` tells where the column names of a delimited file come from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DelimitedHeaders {
    /// The first row of the file names the columns.
    FirstRow,
    /// The configuration names the columns, and each row of the file is a record.
    Given(Vec<String>),
}

/// `HttpEndpoint` is the base URL of an HTTP API with no credentials, query or fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct HttpEndpoint(String);

impl HttpEndpoint {
    pub fn from_url(url: &url::Url) -> Result<Self, BindingEvidenceError> {
        if !matches!(url.scheme(), "http" | "https")
            || url.host().is_none()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(BindingEvidenceError::InvalidLocation);
        }
        let mut endpoint = url.clone();
        endpoint
            .set_password(None)
            .map_err(|_| BindingEvidenceError::InvalidLocation)?;
        endpoint
            .set_username("")
            .map_err(|_| BindingEvidenceError::InvalidLocation)?;
        Ok(Self(endpoint.into()))
    }
}

impl TryFrom<String> for HttpEndpoint {
    type Error = BindingEvidenceError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let url = url::Url::parse(&value).map_err(|_| BindingEvidenceError::InvalidLocation)?;
        let endpoint = Self::from_url(&url)?;
        if endpoint.0 == value {
            Ok(endpoint)
        } else {
            Err(BindingEvidenceError::InvalidLocation)
        }
    }
}

impl From<HttpEndpoint> for String {
    fn from(endpoint: HttpEndpoint) -> Self {
        endpoint.0
    }
}

impl SystemBinding {
    pub fn name(&self) -> &str {
        &self.system
    }

    pub fn location(&self) -> Option<&SystemLocation> {
        self.location.as_ref()
    }

    /// `quoted` returns the binding as a refusal quotes it.
    pub fn quoted(&self) -> String {
        match &self.location {
            None => format!("'{}'", self.system),
            Some(location) => format!("'{}' at '{}'", self.system, location.place()),
        }
    }

    /// `format` returns the format that the system reads at its location.
    pub fn format(&self) -> Option<String> {
        self.location.as_ref().map(SystemLocation::format)
    }
}

impl SystemLocation {
    fn place(&self) -> String {
        match self {
            Self::Delimited { path, .. } | Self::Yaml { path } => path.display().to_string(),
            Self::Json { endpoint } => endpoint.0.clone(),
            Self::Ingress { key } => key.clone(),
            Self::Application { configuration } => configuration.to_string(),
        }
    }

    fn format(&self) -> String {
        match self {
            Self::Delimited {
                delimiter, headers, ..
            } => {
                let text = match delimiter {
                    ',' => "CSV".to_owned(),
                    '\t' => "TSV".to_owned(),
                    other => format!("text delimited by '{other}'"),
                };
                match headers {
                    DelimitedHeaders::FirstRow => text,
                    DelimitedHeaders::Given(columns) => {
                        let columns: Vec<String> =
                            columns.iter().map(|column| format!("'{column}'")).collect();
                        format!("{text} with the columns {}", columns.join(", "))
                    }
                }
            }
            Self::Yaml { .. } => "YAML".to_owned(),
            Self::Json { .. } => "JSON".to_owned(),
            Self::Ingress { .. } => "submissions".to_owned(),
            Self::Application { .. } => "application configuration".to_owned(),
        }
    }
}

impl std::fmt::Display for SystemBinding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.system)?;
        match &self.location {
            None => Ok(()),
            Some(location) => write!(f, " at {}", location.place()),
        }
    }
}

/// `SystemBindingEvidence` is bounded canonical evidence for an effect binding.
#[derive(Clone, PartialEq, Eq)]
pub struct SystemBindingEvidence {
    binding: SystemBinding,
    canonical: BoundedBindingEvidence,
}

impl SystemBindingEvidence {
    pub fn named(system: &str) -> Result<Self, BindingEvidenceError> {
        Self::new(system, None)
    }

    pub fn located(system: &str, location: SystemLocation) -> Result<Self, BindingEvidenceError> {
        Self::new(system, Some(location))
    }

    fn new(system: &str, location: Option<SystemLocation>) -> Result<Self, BindingEvidenceError> {
        let binding = SystemBinding {
            system: system.to_owned(),
            location: location.map(|location| match location {
                SystemLocation::Application { configuration } => SystemLocation::Application {
                    configuration: crate::source::canonicalize(configuration),
                },
                other => other,
            }),
        };
        let canonical =
            serde_json::to_vec(&binding).map_err(|_| BindingEvidenceError::InvalidLocation)?;
        Ok(Self {
            binding,
            canonical: BoundedBindingEvidence::try_new(canonical)?,
        })
    }

    pub fn binding(&self) -> &SystemBinding {
        &self.binding
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
    fn recorded_endpoint_with_credentials_refused() {
        let refused = serde_json::from_str::<HttpEndpoint>(r#""https://user:secret@example.com/""#);
        assert!(refused.is_err(), "{refused:?}");
        let endpoint: HttpEndpoint = serde_json::from_str(r#""https://example.com/""#)
            .expect("a canonical endpoint reads back");
        assert_eq!(
            serde_json::to_string(&endpoint).expect("the endpoint writes"),
            r#""https://example.com/""#
        );
    }

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
