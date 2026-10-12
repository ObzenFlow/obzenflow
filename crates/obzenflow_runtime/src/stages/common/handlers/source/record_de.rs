// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Serde over one source record, with typed and safe errors (FLOWIP-084n B6).
//!
//! [`RecordDeError`] is the record policy of the shared classified
//! deserializer: it classifies by which serde hook fired and drops serde's
//! text and rejected values. Field names come only from serde's static struct
//! and variant lists, so input-controlled map keys are redacted. Record
//! deserializers share it; ingress bodies decode through [`deserialize_json`].

use crate::classified_de::ClassifiedError;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};
use serde::de::{self, DeserializeOwned, Expected, Unexpected};
use serde_json::Value;
use std::fmt;
use std::num::NonZeroU32;

/// Serde error of a record deserializer; it carries only a typed diagnostic.
#[derive(Debug)]
pub struct RecordDeError(SourceDiagnostic);

impl RecordDeError {
    pub fn new(reason: SourceDiagnosticReason) -> Self {
        Self(SourceDiagnostic::new(reason))
    }

    pub fn into_diagnostic(self) -> SourceDiagnostic {
        self.0
    }

    /// The innermost value that failed supplies the position.
    pub fn at_position(self, line: NonZeroU32, column: Option<NonZeroU32>) -> Self {
        if self.0.location().position().is_some() {
            return self;
        }
        Self(self.0.position(line, column))
    }

    /// Names the key only when it is one of the statically known fields.
    pub fn within_key(self, key: &str, known: Option<&'static [&'static str]>) -> Self {
        match known.and_then(|names| names.iter().find(|name| **name == key)) {
            Some(name) => Self(self.0.within_field(name)),
            None => Self(self.0.within_redacted()),
        }
    }

    pub fn within_index(self, index: usize) -> Self {
        Self(
            self.0
                .within_index(u32::try_from(index).unwrap_or(u32::MAX)),
        )
    }
}

impl fmt::Display for RecordDeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for RecordDeError {}

impl de::Error for RecordDeError {
    fn custom<T: fmt::Display>(_message: T) -> Self {
        Self::new(SourceDiagnosticReason::InvalidRecord)
    }

    fn missing_field(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::MissingField).within_field(field))
    }

    fn unknown_field(_field: &str, _expected: &'static [&'static str]) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::UnknownField).within_redacted())
    }

    fn duplicate_field(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::DuplicateKey).within_field(field))
    }

    fn invalid_type(_unexpected: Unexpected<'_>, _expected: &dyn Expected) -> Self {
        Self::new(SourceDiagnosticReason::InvalidValue)
    }

    fn invalid_value(_unexpected: Unexpected<'_>, _expected: &dyn Expected) -> Self {
        Self::new(SourceDiagnosticReason::InvalidValue)
    }

    fn invalid_length(_len: usize, _expected: &dyn Expected) -> Self {
        Self::new(SourceDiagnosticReason::InvalidValue)
    }

    fn unknown_variant(_variant: &str, _expected: &'static [&'static str]) -> Self {
        Self::new(SourceDiagnosticReason::InvalidValue)
    }
}

impl ClassifiedError for RecordDeError {
    fn within_key(self, key: &str, known: Option<&'static [&'static str]>) -> Self {
        RecordDeError::within_key(self, key, known)
    }

    fn within_index(self, index: usize) -> Self {
        RecordDeError::within_index(self, index)
    }
}

/// Deserializes one JSON value; a failure names a reason and a static path.
pub(crate) fn deserialize_json<T: DeserializeOwned>(value: &Value) -> Result<T, SourceDiagnostic> {
    crate::classified_de::from_value::<T, RecordDeError>(value)
        .map_err(RecordDeError::into_diagnostic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{FieldName, FieldSegment};
    use serde::Deserialize;
    use serde_json::json;
    use std::collections::HashMap;

    fn decode<T: DeserializeOwned>(value: Value) -> Result<T, SourceDiagnostic> {
        deserialize_json(&value)
    }

    fn field(name: &'static str) -> FieldSegment {
        FieldSegment::Field(FieldName::new(name))
    }

    #[derive(Debug, Deserialize, PartialEq)]
    enum Kind {
        Credit,
        Debit,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Address {
        street: String,
        number: u32,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Entry {
        account_id: String,
        kind: Kind,
        amount_cents: u64,
        note: Option<String>,
        address: Option<Address>,
        tags: Vec<String>,
    }

    #[test]
    fn ordinary_serde_structs_decode() {
        let entry: Entry = decode(json!({
            "account_id": "acct-1",
            "kind": "Credit",
            "amount_cents": 250,
            "address": {"street": "Main", "number": 7},
            "tags": ["new"],
        }))
        .unwrap();
        assert_eq!(entry.account_id, "acct-1");
        assert_eq!(entry.kind, Kind::Credit);
        assert_eq!(entry.note, None);
        assert_eq!(entry.address.unwrap().number, 7);

        let signed: i64 = decode(json!(-5)).unwrap();
        let float: f64 = decode(json!(1.5)).unwrap();
        let untyped: Value = decode(json!({"any": [1, "two"]})).unwrap();
        assert_eq!((signed, float), (-5, 1.5));
        assert_eq!(untyped, json!({"any": [1, "two"]}));
    }

    #[test]
    fn errors_are_classified_by_serde_hook_with_static_paths() {
        let missing =
            decode::<Entry>(json!({"kind": "Credit", "amount_cents": 1, "tags": []})).unwrap_err();
        assert_eq!(missing.reason(), SourceDiagnosticReason::MissingField);
        assert_eq!(missing.location().field_path(), [field("account_id")]);

        let negative = decode::<Entry>(json!({
            "account_id": "x", "kind": "Credit", "amount_cents": -1, "tags": [],
        }))
        .unwrap_err();
        assert_eq!(negative.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(negative.location().field_path(), [field("amount_cents")]);

        let wrong_type = decode::<Entry>(json!({
            "account_id": "x", "kind": "Credit", "amount_cents": "SECRET_VALUE", "tags": [],
        }))
        .unwrap_err();
        assert_eq!(wrong_type.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(wrong_type.location().field_path(), [field("amount_cents")]);

        let variant = decode::<Entry>(json!({
            "account_id": "x", "kind": "SECRET_KIND", "amount_cents": 1, "tags": [],
        }))
        .unwrap_err();
        assert_eq!(variant.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(variant.location().field_path(), [field("kind")]);

        let unknown = decode::<Entry>(json!({
            "account_id": "x", "kind": "Credit", "amount_cents": 1, "tags": [], "SECRET_KEY": 1,
        }))
        .unwrap_err();
        assert_eq!(unknown.reason(), SourceDiagnosticReason::UnknownField);
        assert_eq!(unknown.location().field_path(), [FieldSegment::Redacted]);

        let nested = decode::<Entry>(json!({
            "account_id": "x", "kind": "Credit", "amount_cents": 1,
            "address": {"street": "Main", "number": -1}, "tags": ["ok", 3],
        }))
        .unwrap_err();
        assert_eq!(nested.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(
            nested.location().field_path(),
            [field("address"), field("number")]
        );

        let indexed = decode::<Entry>(json!({
            "account_id": "x", "kind": "Credit", "amount_cents": 1, "tags": ["ok", ["nested"]],
        }))
        .unwrap_err();
        assert_eq!(
            indexed.location().field_path(),
            [field("tags"), FieldSegment::Index(1)]
        );

        for diagnostic in [
            missing, negative, wrong_type, variant, unknown, nested, indexed,
        ] {
            let rendered = format!("{diagnostic} {diagnostic:?}");
            assert!(!rendered.contains("SECRET"), "{rendered}");
            assert!(diagnostic.location().position().is_none());
        }
    }

    #[test]
    fn input_controlled_map_keys_are_redacted() {
        #[derive(Debug, Deserialize)]
        struct Account {
            #[serde(rename = "balance")]
            _balance: u64,
        }

        let error =
            decode::<HashMap<String, Account>>(json!({"SECRET_ACCOUNT_ID": {"balance": "lots"}}))
                .unwrap_err();
        assert_eq!(
            error.location().field_path(),
            [FieldSegment::Redacted, field("balance")]
        );
        assert!(!format!("{error} {error:?}").contains("SECRET"));
    }

    #[test]
    fn externally_tagged_variants_decode() {
        #[derive(Debug, Deserialize, PartialEq)]
        enum Payment {
            Card { last4: String },
            Cash(u32),
        }

        assert_eq!(
            decode::<Payment>(json!({"Card": {"last4": "4242"}})).unwrap(),
            Payment::Card {
                last4: "4242".into()
            }
        );
        assert_eq!(
            decode::<Payment>(json!({"Cash": 5})).unwrap(),
            Payment::Cash(5)
        );
        let error = decode::<Payment>(json!({"Cash": "lots"})).unwrap_err();
        assert_eq!(error.location().field_path(), [field("Cash")]);
    }
}
