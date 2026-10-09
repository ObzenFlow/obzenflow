// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Serde over one snapshot record, with typed and safe errors (FLOWIP-084n B6).
//!
//! The error type classifies by which serde hook fired and drops serde's text
//! and rejected values. Field names come only from serde's static struct and
//! variant lists, so input-controlled map keys are redacted.

use super::document::{Node, NodeKind};
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};
use serde::de::value::StrDeserializer;
use serde::de::{self, DeserializeSeed, Expected, Unexpected, VariantAccess, Visitor};
use std::fmt;

#[derive(Debug)]
pub(super) struct RecordDeError(SourceDiagnostic);

impl RecordDeError {
    fn new(reason: SourceDiagnosticReason) -> Self {
        Self(SourceDiagnostic::new(reason))
    }

    pub(super) fn into_diagnostic(self) -> SourceDiagnostic {
        self.0
    }

    /// The innermost node that failed supplies the position.
    fn at(self, node: &Node) -> Self {
        if self.0.location().position().is_some() {
            return self;
        }
        Self(self.0.position(node.start.line, node.start.column))
    }

    fn within_key(self, key: &str, known: Option<&'static [&'static str]>) -> Self {
        match known.and_then(|names| names.iter().find(|name| **name == key)) {
            Some(name) => Self(self.0.within_field(name)),
            None => Self(self.0.within_redacted()),
        }
    }

    fn within_index(self, index: usize) -> Self {
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

pub(super) struct NodeDeserializer<'a> {
    node: &'a Node,
}

impl<'a> NodeDeserializer<'a> {
    pub(super) fn new(node: &'a Node) -> Self {
        Self { node }
    }

    fn map(&self, known: Option<&'static [&'static str]>) -> Option<NodeMapAccess<'a>> {
        match &self.node.kind {
            NodeKind::Map(entries) => Some(NodeMapAccess {
                entries: entries.iter(),
                known,
                pending: None,
            }),
            _ => None,
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for NodeDeserializer<'a> {
    type Error = RecordDeError;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RecordDeError> {
        let node = self.node;
        match &node.kind {
            NodeKind::Null => visitor.visit_unit(),
            NodeKind::Bool(value) => visitor.visit_bool(*value),
            NodeKind::Int(value) => match (i64::try_from(*value), u64::try_from(*value)) {
                (Ok(signed), _) => visitor.visit_i64(signed),
                (_, Ok(unsigned)) => visitor.visit_u64(unsigned),
                _ => visitor.visit_i128(*value),
            },
            NodeKind::Float(value) => visitor.visit_f64(*value),
            NodeKind::Str(value) => visitor.visit_str(value),
            NodeKind::Seq(items) => visitor.visit_seq(NodeSeqAccess {
                items: items.iter().enumerate(),
            }),
            NodeKind::Map(_) => match self.map(None) {
                Some(access) => visitor.visit_map(access),
                None => Err(RecordDeError::new(SourceDiagnosticReason::InvalidRecord)),
            },
        }
        .map_err(|error| error.at(node))
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, RecordDeError> {
        match self.node.kind {
            NodeKind::Null => visitor.visit_none(),
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
        let node = self.node;
        match self.map(Some(fields)) {
            Some(access) => visitor.visit_map(access).map_err(|error| error.at(node)),
            None => self.deserialize_any(visitor),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        let node = self.node;
        match &node.kind {
            NodeKind::Str(variant) => {
                visitor.visit_enum(StrDeserializer::<RecordDeError>::new(variant))
            }
            NodeKind::Map(entries) if entries.len() == 1 => {
                let (variant, value) = &entries[0];
                visitor
                    .visit_enum(NodeEnumAccess { variant, value })
                    .map_err(|error| error.within_key(variant, Some(variants)))
            }
            _ => Err(RecordDeError::new(SourceDiagnosticReason::InvalidValue)),
        }
        .map_err(|error| error.at(node))
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

struct NodeSeqAccess<'a> {
    items: std::iter::Enumerate<std::slice::Iter<'a, Node>>,
}

impl<'de, 'a> de::SeqAccess<'de> for NodeSeqAccess<'a> {
    type Error = RecordDeError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, RecordDeError> {
        match self.items.next() {
            Some((index, node)) => seed
                .deserialize(NodeDeserializer::new(node))
                .map(Some)
                .map_err(|error| error.within_index(index)),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len())
    }
}

struct NodeMapAccess<'a> {
    entries: std::slice::Iter<'a, (String, Node)>,
    known: Option<&'static [&'static str]>,
    pending: Option<(&'a str, &'a Node)>,
}

impl<'de, 'a> de::MapAccess<'de> for NodeMapAccess<'a> {
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
            .map_err(|error| error.at(value))
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, RecordDeError> {
        let (key, value) = self
            .pending
            .take()
            .ok_or_else(|| RecordDeError::new(SourceDiagnosticReason::InvalidRecord))?;
        seed.deserialize(NodeDeserializer::new(value))
            .map_err(|error| error.within_key(key, self.known))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

struct NodeEnumAccess<'a> {
    variant: &'a str,
    value: &'a Node,
}

impl<'de, 'a> de::EnumAccess<'de> for NodeEnumAccess<'a> {
    type Error = RecordDeError;
    type Variant = NodeVariantAccess<'a>;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), RecordDeError> {
        let variant = seed.deserialize(StrDeserializer::<RecordDeError>::new(self.variant))?;
        Ok((variant, NodeVariantAccess { value: self.value }))
    }
}

struct NodeVariantAccess<'a> {
    value: &'a Node,
}

impl<'de, 'a> VariantAccess<'de> for NodeVariantAccess<'a> {
    type Error = RecordDeError;

    fn unit_variant(self) -> Result<(), RecordDeError> {
        match self.value.kind {
            NodeKind::Null => Ok(()),
            _ => Err(RecordDeError::new(SourceDiagnosticReason::InvalidValue).at(self.value)),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, RecordDeError> {
        seed.deserialize(NodeDeserializer::new(self.value))
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        de::Deserializer::deserialize_seq(NodeDeserializer::new(self.value), visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, RecordDeError> {
        de::Deserializer::deserialize_struct(NodeDeserializer::new(self.value), "", fields, visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::super::document::parse;
    use super::*;
    use obzenflow_core::event::{FieldName, FieldSegment};
    use serde::de::DeserializeOwned;
    use serde::Deserialize;
    use std::collections::HashMap;

    fn decode<T: DeserializeOwned>(text: &str) -> Result<T, SourceDiagnostic> {
        let node = parse(text.as_bytes()).expect("document parses");
        T::deserialize(NodeDeserializer::new(&node)).map_err(RecordDeError::into_diagnostic)
    }

    fn field(name: &'static str) -> FieldSegment {
        FieldSegment::Field(FieldName::new(name))
    }

    #[derive(Debug, Deserialize, PartialEq)]
    enum Channel {
        Web,
        Store,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Address {
        street: String,
        number: u32,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Order {
        order_id: String,
        channel: Channel,
        amount_cents: i64,
        note: Option<String>,
        address: Option<Address>,
        tags: Vec<String>,
    }

    #[test]
    fn ordinary_serde_structs_decode() {
        let order: Order = decode(
            "order_id: \"web-001\"\nchannel: Web\namount_cents: 1500\naddress: {street: Main, number: 7}\ntags: [new]\n",
        )
        .unwrap();
        assert_eq!(order.order_id, "web-001");
        assert_eq!(order.channel, Channel::Web);
        assert_eq!(order.note, None);
        assert_eq!(order.address.unwrap().number, 7);
    }

    #[test]
    fn errors_are_classified_by_serde_hook_with_static_paths() {
        let missing = decode::<Order>("channel: Web\namount_cents: 1\ntags: []\n").unwrap_err();
        assert_eq!(missing.reason(), SourceDiagnosticReason::MissingField);
        assert_eq!(missing.location().field_path(), [field("order_id")]);

        let wrong_type =
            decode::<Order>("order_id: x\nchannel: Web\namount_cents: SECRET_VALUE\ntags: []\n")
                .unwrap_err();
        assert_eq!(wrong_type.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(wrong_type.location().field_path(), [field("amount_cents")]);
        let at = wrong_type.location().position().unwrap();
        assert_eq!((at.line.get(), at.column.map(|c| c.get())), (3, Some(15)));

        let variant =
            decode::<Order>("order_id: x\nchannel: SECRET_CHANNEL\namount_cents: 1\ntags: []\n")
                .unwrap_err();
        assert_eq!(variant.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(variant.location().field_path(), [field("channel")]);

        let unknown = decode::<Order>(
            "order_id: x\nchannel: Web\namount_cents: 1\ntags: []\nSECRET_KEY: 1\n",
        )
        .unwrap_err();
        assert_eq!(unknown.reason(), SourceDiagnosticReason::UnknownField);
        assert_eq!(unknown.location().field_path(), [FieldSegment::Redacted]);

        let nested = decode::<Order>(
            "order_id: x\nchannel: Web\namount_cents: 1\naddress: {street: Main, number: -1}\ntags: [ok, 3]\n",
        )
        .unwrap_err();
        assert_eq!(nested.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(
            nested.location().field_path(),
            [field("address"), field("number")]
        );

        let indexed =
            decode::<Order>("order_id: x\nchannel: Web\namount_cents: 1\ntags: [ok, [nested]]\n")
                .unwrap_err();
        assert_eq!(
            indexed.location().field_path(),
            [field("tags"), FieldSegment::Index(1)]
        );

        for diagnostic in [missing, wrong_type, variant, unknown, nested, indexed] {
            let rendered = format!("{diagnostic} {diagnostic:?}");
            assert!(!rendered.contains("SECRET"), "{rendered}");
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
            decode::<HashMap<String, Account>>("SECRET_ACCOUNT_ID: {balance: lots}\n").unwrap_err();
        assert_eq!(
            error.location().field_path(),
            [FieldSegment::Redacted, field("balance")]
        );
        assert!(!format!("{error} {error:?}").contains("SECRET"));
    }

    #[test]
    fn flattened_fields_lose_their_path_rather_than_echo_keys() {
        #[derive(Debug, Deserialize)]
        struct Inner {
            #[serde(rename = "count")]
            _count: u8,
        }

        #[derive(Debug, Deserialize)]
        struct Outer {
            #[serde(flatten)]
            _inner: Inner,
        }

        // Flattening buffers entries outside this deserializer, so no path
        // survives; the reason and record position still do.
        let error = decode::<Outer>("count: 300\n").unwrap_err();
        assert_eq!(error.reason(), SourceDiagnosticReason::InvalidValue);
        assert!(error.location().field_path().is_empty());
        assert!(error.location().position().is_some());
    }

    #[test]
    fn externally_tagged_variants_decode() {
        #[derive(Debug, Deserialize, PartialEq)]
        enum Payment {
            Card { last4: String },
            Cash(u32),
        }

        assert_eq!(
            decode::<Payment>("Card: {last4: \"4242\"}\n").unwrap(),
            Payment::Card {
                last4: "4242".into()
            }
        );
        assert_eq!(decode::<Payment>("Cash: 5\n").unwrap(), Payment::Cash(5));
        let error = decode::<Payment>("Cash: lots\n").unwrap_err();
        assert_eq!(error.location().field_path(), [field("Cash")]);
    }
}
