// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Serde over one source record, with typed and safe errors (FLOWIP-084n B6).
//!
//! [`RecordDeError`] classifies by which serde hook fired and drops serde's
//! text and rejected values. Field names come only from serde's static struct
//! and variant lists, so input-controlled map keys are redacted. Record
//! deserializers share it; the JSON one for ingress bodies lives here.

use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};
use serde::de::value::StrDeserializer;
use serde::de::{
    self, DeserializeOwned, DeserializeSeed, Expected, Unexpected, VariantAccess, Visitor,
};
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

/// Deserializes one JSON value; a failure names a reason and a static path.
pub(crate) fn deserialize_json<T: DeserializeOwned>(value: &Value) -> Result<T, SourceDiagnostic> {
    T::deserialize(JsonDeserializer(value)).map_err(RecordDeError::into_diagnostic)
}

struct JsonDeserializer<'a>(&'a Value);

impl<'a> JsonDeserializer<'a> {
    fn map(&self, known: Option<&'static [&'static str]>) -> Option<JsonMapAccess<'a>> {
        match self.0 {
            Value::Object(entries) => Some(JsonMapAccess {
                entries: entries.iter(),
                known,
                pending: None,
            }),
            _ => None,
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for JsonDeserializer<'a> {
    type Error = RecordDeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RecordDeError> {
        match self.0 {
            Value::Null => visitor.visit_unit(),
            Value::Bool(value) => visitor.visit_bool(*value),
            Value::Number(number) => match (number.as_u64(), number.as_i64(), number.as_f64()) {
                (Some(unsigned), _, _) => visitor.visit_u64(unsigned),
                (None, Some(signed), _) => visitor.visit_i64(signed),
                (None, None, Some(float)) => visitor.visit_f64(float),
                (None, None, None) => Err(RecordDeError::new(SourceDiagnosticReason::InvalidValue)),
            },
            Value::String(value) => visitor.visit_str(value),
            Value::Array(items) => visitor.visit_seq(JsonSeqAccess {
                items: items.iter().enumerate(),
            }),
            Value::Object(_) => match self.map(None) {
                Some(access) => visitor.visit_map(access),
                None => Err(RecordDeError::new(SourceDiagnosticReason::InvalidRecord)),
            },
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RecordDeError> {
        match self.0 {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        match self.map(Some(fields)) {
            Some(access) => visitor.visit_map(access),
            None => self.deserialize_any(visitor),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        match self.0 {
            Value::String(variant) => {
                visitor.visit_enum(StrDeserializer::<RecordDeError>::new(variant))
            }
            Value::Object(entries) if entries.len() == 1 => match entries.iter().next() {
                Some((variant, value)) => visitor
                    .visit_enum(JsonEnumAccess { variant, value })
                    .map_err(|error| error.within_key(variant, Some(variants))),
                None => Err(RecordDeError::new(SourceDiagnosticReason::InvalidValue)),
            },
            _ => Err(RecordDeError::new(SourceDiagnosticReason::InvalidValue)),
        }
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
        byte_buf unit unit_struct seq tuple tuple_struct map identifier
    }
}

struct JsonSeqAccess<'a> {
    items: std::iter::Enumerate<std::slice::Iter<'a, Value>>,
}

impl<'de, 'a> de::SeqAccess<'de> for JsonSeqAccess<'a> {
    type Error = RecordDeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, RecordDeError> {
        match self.items.next() {
            Some((index, value)) => seed
                .deserialize(JsonDeserializer(value))
                .map(Some)
                .map_err(|error| error.within_index(index)),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len())
    }
}

struct JsonMapAccess<'a> {
    entries: serde_json::map::Iter<'a>,
    known: Option<&'static [&'static str]>,
    pending: Option<(&'a str, &'a Value)>,
}

impl<'de, 'a> de::MapAccess<'de> for JsonMapAccess<'a> {
    type Error = RecordDeError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, RecordDeError> {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.pending = Some((key, value));
        // A key error, such as an unknown field, already names its subject.
        seed.deserialize(StrDeserializer::<RecordDeError>::new(key))
            .map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, RecordDeError> {
        let (key, value) = self
            .pending
            .take()
            .ok_or_else(|| RecordDeError::new(SourceDiagnosticReason::InvalidRecord))?;
        seed.deserialize(JsonDeserializer(value))
            .map_err(|error| error.within_key(key, self.known))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

struct JsonEnumAccess<'a> {
    variant: &'a str,
    value: &'a Value,
}

impl<'de, 'a> de::EnumAccess<'de> for JsonEnumAccess<'a> {
    type Error = RecordDeError;
    type Variant = JsonVariantAccess<'a>;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), RecordDeError> {
        let variant = seed.deserialize(StrDeserializer::<RecordDeError>::new(self.variant))?;
        Ok((variant, JsonVariantAccess { value: self.value }))
    }
}

struct JsonVariantAccess<'a> {
    value: &'a Value,
}

impl<'de, 'a> VariantAccess<'de> for JsonVariantAccess<'a> {
    type Error = RecordDeError;

    fn unit_variant(self) -> Result<(), RecordDeError> {
        match self.value {
            Value::Null => Ok(()),
            _ => Err(RecordDeError::new(SourceDiagnosticReason::InvalidValue)),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, RecordDeError> {
        seed.deserialize(JsonDeserializer(self.value))
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        de::Deserializer::deserialize_seq(JsonDeserializer(self.value), visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        de::Deserializer::deserialize_struct(JsonDeserializer(self.value), "", fields, visitor)
    }
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
