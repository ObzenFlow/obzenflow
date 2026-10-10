// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source-failure diagnostics (FLOWIP-084n B2, B6).
//!
//! A diagnostic carries a closed reason, safe coordinates and an optional
//! connector code. Field names are compile-time strings, so input-controlled
//! keys, rejected values and parser text cannot be authored into one.

use super::sink_operation_payload::{is_error_code_namespace, is_error_code_value};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::num::NonZeroU32;

/// Maximum number of field-path segments retained in one diagnostic.
pub const MAX_FIELD_PATH_DEPTH: usize = 16;

/// Closed diagnostic authority for source failures. Every variant except
/// `Unclassified` has a first-party detector; the category stays with the source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceDiagnosticReason {
    InputUnavailable,
    InputClosed,
    TimedOut,
    RateLimited,
    RemoteRejected,
    RemoteFailed,
    MalformedInput,
    UnsupportedConstruct,
    DuplicateKey,
    SizeLimitExceeded,
    SelectionNotFound,
    UnexpectedShape,
    MissingField,
    UnknownField,
    InvalidValue,
    InvalidRecord,
    #[serde(other)]
    Unclassified,
}

impl SourceDiagnosticReason {
    /// Fixed human phrase; rendering never includes connector text.
    pub const fn phrase(self) -> &'static str {
        match self {
            Self::InputUnavailable => "the input is unavailable",
            Self::InputClosed => "the input closed",
            Self::TimedOut => "the input timed out",
            Self::RateLimited => "the input rate-limited the request",
            Self::RemoteRejected => "the remote service rejected the request",
            Self::RemoteFailed => "the remote service failed",
            Self::MalformedInput => "the input is malformed",
            Self::UnsupportedConstruct => "the input uses an unsupported construct",
            Self::DuplicateKey => "the input repeats a key",
            Self::SizeLimitExceeded => "the input exceeds its configured size limit",
            Self::SelectionNotFound => "the configured selection was not found",
            Self::UnexpectedShape => "the input has an unexpected shape",
            Self::MissingField => "missing field",
            Self::UnknownField => "unknown field",
            Self::InvalidValue => "invalid value",
            Self::InvalidRecord => "invalid record",
            Self::Unclassified => "unclassified source failure",
        }
    }
}

/// A schema-declared field name. Authoring accepts only `&'static str`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FieldName(Cow<'static, str>);

impl FieldName {
    pub const fn new(name: &'static str) -> Self {
        Self(Cow::Borrowed(name))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One segment of a diagnostic field path, from the record root to the leaf.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldSegment {
    Field(FieldName),
    Index(u32),
    /// An input-controlled map key occupied this position.
    Redacted,
    /// Segments above this point exceeded the depth bound.
    Elided,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<FieldSegment>", into = "Vec<FieldSegment>")]
struct FieldPath(Vec<FieldSegment>);

impl FieldPath {
    fn prepend(&mut self, segment: FieldSegment) {
        if self.0.len() < MAX_FIELD_PATH_DEPTH {
            self.0.insert(0, segment);
        } else if let Some(root) = self.0.first_mut() {
            *root = FieldSegment::Elided;
        }
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn names_something(&self) -> bool {
        self.0
            .iter()
            .any(|segment| matches!(segment, FieldSegment::Field(_) | FieldSegment::Index(_)))
    }
}

#[derive(Debug)]
struct FieldPathTooDeep;

impl fmt::Display for FieldPathTooDeep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "field path exceeds {MAX_FIELD_PATH_DEPTH} segments")
    }
}

impl TryFrom<Vec<FieldSegment>> for FieldPath {
    type Error = FieldPathTooDeep;

    fn try_from(segments: Vec<FieldSegment>) -> Result<Self, Self::Error> {
        if segments.len() > MAX_FIELD_PATH_DEPTH {
            return Err(FieldPathTooDeep);
        }
        Ok(Self(segments))
    }
}

impl From<FieldPath> for Vec<FieldSegment> {
    fn from(path: FieldPath) -> Self {
        path.0
    }
}

impl fmt::Display for FieldPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (position, segment) in self.0.iter().enumerate() {
            match segment {
                FieldSegment::Index(index) => write!(f, "[{index}]")?,
                FieldSegment::Field(name) => {
                    if position > 0 {
                        f.write_str(".")?;
                    }
                    f.write_str(name.as_str())?;
                }
                FieldSegment::Redacted => {
                    if position > 0 {
                        f.write_str(".")?;
                    }
                    f.write_str("*")?;
                }
                FieldSegment::Elided => f.write_str("...")?,
            }
        }
        Ok(())
    }
}

/// One-based text coordinates; a column without a line is unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextPosition {
    pub line: NonZeroU32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<NonZeroU32>,
}

/// Safe source coordinates. Never a selection, configured pointer or path.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceLocation {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    record_index: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    position: Option<TextPosition>,
    #[serde(default, skip_serializing_if = "FieldPath::is_empty")]
    field_path: FieldPath,
}

impl SourceLocation {
    /// Zero-based index of the record within the source's record sequence.
    pub fn record_index(&self) -> Option<u64> {
        self.record_index
    }

    pub fn position(&self) -> Option<TextPosition> {
        self.position
    }

    pub fn field_path(&self) -> &[FieldSegment] {
        &self.field_path.0
    }

    pub fn is_unknown(&self) -> bool {
        self.record_index.is_none() && self.position.is_none() && self.field_path.is_empty()
    }
}

/// A bounded, connector-namespaced code such as `http.status` / `401`. Evidence
/// only, never a metric label. Shares the sink destination code grammar.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "RawSourceErrorCode")]
pub struct SourceErrorCode {
    namespace: String,
    value: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawSourceErrorCode {
    namespace: String,
    value: String,
}

impl TryFrom<RawSourceErrorCode> for SourceErrorCode {
    type Error = SourceErrorCodeError;

    fn try_from(raw: RawSourceErrorCode) -> Result<Self, Self::Error> {
        Self::try_new(raw.namespace, raw.value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceErrorCodeError {
    InvalidNamespace,
    InvalidValue,
}

impl fmt::Display for SourceErrorCodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidNamespace => {
                f.write_str("source error code namespace must be 1..=48 bytes of [a-z0-9._-]")
            }
            Self::InvalidValue => {
                f.write_str("source error code value must be 1..=64 bytes of [A-Za-z0-9._:-]")
            }
        }
    }
}

impl std::error::Error for SourceErrorCodeError {}

impl SourceErrorCode {
    pub fn try_new(
        namespace: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, SourceErrorCodeError> {
        let namespace = namespace.into();
        let value = value.into();
        if !is_error_code_namespace(&namespace) {
            return Err(SourceErrorCodeError::InvalidNamespace);
        }
        if !is_error_code_value(&value) {
            return Err(SourceErrorCodeError::InvalidValue);
        }
        Ok(Self { namespace, value })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

/// The one diagnostic payload for source opening and polling failures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceDiagnostic {
    reason: SourceDiagnosticReason,
    #[serde(default, skip_serializing_if = "SourceLocation::is_unknown")]
    location: SourceLocation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    code: Option<SourceErrorCode>,
}

impl SourceDiagnostic {
    pub const fn new(reason: SourceDiagnosticReason) -> Self {
        Self {
            reason,
            location: SourceLocation {
                record_index: None,
                position: None,
                field_path: FieldPath(Vec::new()),
            },
            code: None,
        }
    }

    pub fn record(mut self, index: u64) -> Self {
        self.location.record_index = Some(index);
        self
    }

    pub fn position(mut self, line: NonZeroU32, column: Option<NonZeroU32>) -> Self {
        self.location.position = Some(TextPosition { line, column });
        self
    }

    /// Prepends a schema-declared field, building paths from the leaf outward.
    pub fn within_field(mut self, name: &'static str) -> Self {
        self.location
            .field_path
            .prepend(FieldSegment::Field(FieldName::new(name)));
        self
    }

    pub fn within_index(mut self, index: u32) -> Self {
        self.location.field_path.prepend(FieldSegment::Index(index));
        self
    }

    pub fn within_redacted(mut self) -> Self {
        self.location.field_path.prepend(FieldSegment::Redacted);
        self
    }

    pub fn code(mut self, code: SourceErrorCode) -> Self {
        self.code = Some(code);
        self
    }

    pub fn reason(&self) -> SourceDiagnosticReason {
        self.reason
    }

    pub fn location(&self) -> &SourceLocation {
        &self.location
    }

    pub fn error_code(&self) -> Option<&SourceErrorCode> {
        self.code.as_ref()
    }
}

impl From<SourceDiagnosticReason> for SourceDiagnostic {
    fn from(reason: SourceDiagnosticReason) -> Self {
        Self::new(reason)
    }
}

/// Renders only typed fields: the reason phrase, a schema path and coordinates.
/// The record index is left to callers, which name the source stage.
impl fmt::Display for SourceDiagnostic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason.phrase())?;
        if self.location.field_path.names_something() {
            write!(f, " in field {}", self.location.field_path)?;
        }
        if let Some(code) = &self.code {
            write!(f, " [{}={}]", code.namespace, code.value)?;
        }
        if let Some(position) = self.location.position {
            match position.column {
                Some(column) => write!(f, " (line {}, column {column})", position.line)?,
                None => write!(f, " (line {})", position.line)?,
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn line(value: u32) -> NonZeroU32 {
        NonZeroU32::new(value).expect("nonzero line")
    }

    #[test]
    fn diagnostic_serialises_to_the_documented_shape() {
        let diagnostic = SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue)
            .within_field("amount_cents")
            .record(2)
            .position(line(9), Some(line(19)));
        assert_eq!(
            serde_json::to_value(&diagnostic).unwrap(),
            json!({
                "reason": "invalid_value",
                "location": {
                    "record_index": 2,
                    "position": { "line": 9, "column": 19 },
                    "field_path": [{ "field": "amount_cents" }]
                }
            })
        );
        let decoded: SourceDiagnostic =
            serde_json::from_value(serde_json::to_value(&diagnostic).unwrap()).unwrap();
        assert_eq!(decoded, diagnostic);
    }

    #[test]
    fn unknown_location_and_code_are_absent() {
        let diagnostic = SourceDiagnostic::new(SourceDiagnosticReason::TimedOut);
        assert_eq!(
            serde_json::to_value(&diagnostic).unwrap(),
            json!({ "reason": "timed_out" })
        );
    }

    #[test]
    fn unknown_reason_decodes_as_unclassified() {
        let decoded: SourceDiagnostic =
            serde_json::from_value(json!({ "reason": "from_a_newer_writer" })).unwrap();
        assert_eq!(decoded.reason(), SourceDiagnosticReason::Unclassified);
    }

    #[test]
    fn paths_build_from_the_leaf_and_elide_above_the_depth_bound() {
        let mut diagnostic =
            SourceDiagnostic::new(SourceDiagnosticReason::MissingField).within_field("street");
        for _ in 0..20 {
            diagnostic = diagnostic.within_index(0);
        }
        let path = diagnostic.location().field_path();
        assert_eq!(path.len(), MAX_FIELD_PATH_DEPTH);
        assert_eq!(path.first(), Some(&FieldSegment::Elided));
        assert_eq!(
            path.last(),
            Some(&FieldSegment::Field(FieldName::new("street")))
        );
    }

    #[test]
    fn decoding_rejects_an_unbounded_path() {
        let segments = vec![json!("redacted"); MAX_FIELD_PATH_DEPTH + 1];
        let error = serde_json::from_value::<SourceDiagnostic>(json!({
            "reason": "unknown_field",
            "location": { "field_path": segments }
        }))
        .unwrap_err();
        assert!(error.to_string().contains("field path exceeds"));
    }

    #[test]
    fn codes_share_the_sink_grammar_on_construction_and_decoding() {
        let code = SourceErrorCode::try_new("http.status", "401").unwrap();
        assert_eq!((code.namespace(), code.value()), ("http.status", "401"));
        assert_eq!(
            SourceErrorCode::try_new("HTTP", "401"),
            Err(SourceErrorCodeError::InvalidNamespace)
        );
        assert_eq!(
            SourceErrorCode::try_new("http.status", "not allowed"),
            Err(SourceErrorCodeError::InvalidValue)
        );
        assert!(serde_json::from_value::<SourceErrorCode>(
            json!({ "namespace": "http.status", "value": "a b" })
        )
        .is_err());
    }

    #[test]
    fn display_renders_only_typed_fields() {
        let rejected = SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue)
            .within_field("amount_cents")
            .record(2)
            .position(line(9), Some(line(19)));
        assert_eq!(
            rejected.to_string(),
            "invalid value in field amount_cents (line 9, column 19)"
        );

        let unknown = SourceDiagnostic::new(SourceDiagnosticReason::UnknownField)
            .within_redacted()
            .position(line(23), Some(line(5)));
        assert_eq!(unknown.to_string(), "unknown field (line 23, column 5)");

        let nested = SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue)
            .within_field("street")
            .within_field("address")
            .within_index(3)
            .within_field("customers");
        assert_eq!(
            nested.to_string(),
            "invalid value in field customers[3].address.street"
        );

        let remote = SourceDiagnostic::new(SourceDiagnosticReason::RemoteRejected)
            .code(SourceErrorCode::try_new("http.status", "401").unwrap());
        assert_eq!(
            remote.to_string(),
            "the remote service rejected the request [http.status=401]"
        );
    }
}
