// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::stages::common::handlers::source::record_de::deserialize_json;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::ingress::{IngressContext, IngressReceipt};
use obzenflow_core::TypedPayload;
use serde::de::DeserializeOwned;
use std::fmt;
use tokio::sync::oneshot;

/// Error returned by an [`IngressDecoder`]. It carries a typed diagnostic only,
/// so a refusal journals a reason and field, never submitted values.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct IngressDecodeError(SourceDiagnostic);

impl IngressDecodeError {
    pub fn missing_field(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::MissingField).within_field(field))
    }

    pub fn invalid_value(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue).within_field(field))
    }

    pub fn invalid_record() -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord))
    }

    /// Nests the path under a schema-declared parent field.
    pub fn within_field(self, name: &'static str) -> Self {
        Self(self.0.within_field(name))
    }

    pub fn code(self, code: SourceErrorCode) -> Self {
        Self(self.0.code(code))
    }

    pub fn into_diagnostic(self) -> SourceDiagnostic {
        self.0
    }
}

/// One submitted ingress body. Exposes neither raw text nor request metadata.
pub struct IngressRecord<'a> {
    data: &'a serde_json::Value,
}

impl<'a> IngressRecord<'a> {
    /// Wraps a body, for example to test a decoder directly.
    pub fn new(data: &'a serde_json::Value) -> Self {
        Self { data }
    }

    /// Deserialize this body with serde. Errors name schema fields only.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, IngressDecodeError> {
        deserialize_json(self.data).map_err(IngressDecodeError)
    }
}

impl fmt::Debug for IngressRecord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngressRecord").finish_non_exhaustive()
    }
}

/// User-owned mapping from one submitted ingress body to one domain output.
///
/// The decoder value owns the source's `Output` contract. The default method
/// deserializes the record with serde; applications override it for an
/// external representation or a record-local rule, rejecting with a typed
/// [`IngressDecodeError`]. Hosted admission runs it exactly once per
/// submission, before anything is queued. Typed `IngressHandle` submissions
/// already have the `Output` shape and bypass it.
pub trait IngressDecoder: Clone + Send + Sync + 'static {
    type Output: TypedPayload + Send + Sync + 'static;

    fn decode(&self, record: IngressRecord<'_>) -> Result<Self::Output, IngressDecodeError> {
        record.deserialize()
    }
}

/// Prove that `value`'s journal form decodes as its own type, returning the
/// decoded copy so the queued value is exactly what replay reconstructs. A
/// failure is `UnrepresentableValue` at the failing field.
pub fn prove_journal_form<T: TypedPayload>(value: T) -> Result<T, IngressDecodeError> {
    let unrepresentable = || {
        IngressDecodeError(SourceDiagnostic::new(
            SourceDiagnosticReason::UnrepresentableValue,
        ))
    };
    let payload = value.into_chain_payload().map_err(|_| unrepresentable())?;
    let body = payload.contract_body().map_err(|_| unrepresentable())?;
    IngressRecord::new(&body)
        .deserialize::<T>()
        .map_err(|error| {
            IngressDecodeError(
                error
                    .into_diagnostic()
                    .with_reason(SourceDiagnosticReason::UnrepresentableValue),
            )
        })
}

/// Resolves one submitted member's commit evidence.
pub type IngressReceiptSender = oneshot::Sender<IngressReceipt>;

/// `HostedSubmission` is one admitted member that waits for the stage to read it.
pub struct HostedSubmission<T> {
    pub(crate) value: T,
    pub(crate) context: IngressContext,
    pub(crate) receipt: IngressReceiptSender,
}

impl<T> HostedSubmission<T> {
    pub fn new(value: T, context: IngressContext, receipt: IngressReceiptSender) -> Self {
        Self {
            value,
            context,
            receipt,
        }
    }
}

impl<T> fmt::Debug for HostedSubmission<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedSubmission")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Row(u32);

    impl TypedPayload for Row {
        const EVENT_TYPE: &'static str = "typed_source.row";
    }

    #[derive(Clone)]
    struct OffsetIngress {
        offset: u32,
    }

    impl IngressDecoder for OffsetIngress {
        type Output = Row;

        fn decode(&self, record: IngressRecord<'_>) -> Result<Self::Output, IngressDecodeError> {
            let row = record.deserialize::<u32>()?;
            if row == 0 {
                return Err(IngressDecodeError::invalid_value("row"));
            }
            Ok(Row(row + self.offset))
        }
    }

    #[test]
    fn decoder_rejections_keep_a_typed_diagnostic_without_input_text() {
        let decoder = OffsetIngress { offset: 40 };
        assert_eq!(
            decoder.decode(IngressRecord::new(&serde_json::json!(2))),
            Ok(Row(42))
        );

        let rule = decoder
            .decode(IngressRecord::new(&serde_json::json!(0)))
            .unwrap_err()
            .into_diagnostic();
        assert_eq!(rule.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(
            rule.location().field_path(),
            [obzenflow_core::event::FieldSegment::Field(
                obzenflow_core::event::FieldName::new("row")
            )]
        );

        let malformed = decoder
            .decode(IngressRecord::new(&serde_json::json!("SECRET_VALUE")))
            .unwrap_err()
            .into_diagnostic();
        assert_eq!(malformed.reason(), SourceDiagnosticReason::InvalidValue);
        assert!(!format!("{malformed} {malformed:?}").contains("SECRET"));
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Tagged {
        name: String,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
    }

    impl TypedPayload for Tagged {
        const EVENT_TYPE: &'static str = "typed_source.tagged";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Measured {
        value: f64,
    }

    impl TypedPayload for Measured {
        const EVENT_TYPE: &'static str = "typed_source.measured";
    }

    #[test]
    fn journal_form_proof_returns_the_decoded_copy() {
        let tagged = Tagged {
            name: "a".into(),
            tags: vec!["x".into()],
        };
        assert_eq!(prove_journal_form(tagged.clone()), Ok(tagged));
    }

    #[test]
    fn unrepresentable_values_name_the_failing_field() {
        let skipped = prove_journal_form(Tagged {
            name: "a".into(),
            tags: Vec::new(),
        })
        .unwrap_err()
        .into_diagnostic();
        assert_eq!(
            skipped.reason(),
            SourceDiagnosticReason::UnrepresentableValue
        );
        assert_eq!(
            skipped.location().field_path(),
            [obzenflow_core::event::FieldSegment::Field(
                obzenflow_core::event::FieldName::new("tags")
            )]
        );

        let non_finite = prove_journal_form(Measured { value: f64::NAN })
            .unwrap_err()
            .into_diagnostic();
        assert_eq!(
            non_finite.reason(),
            SourceDiagnosticReason::UnrepresentableValue
        );
    }
}
