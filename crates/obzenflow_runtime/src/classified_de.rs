// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Serde over one JSON value, with errors classified by the serde hook that
//! fired (FLOWIP-084n B6, FLOWIP-010t B7).
//!
//! The error type is the policy: it decides which keys a failure names and
//! drops serde's text and rejected values. Source records use the record
//! policy; the config file's `[app]` table uses the config policy.

use serde::de::value::StrDeserializer;
use serde::de::{self, DeserializeOwned, DeserializeSeed, Unexpected, VariantAccess, Visitor};
use serde_json::Value;
use std::marker::PhantomData;

/// A serde error that records where in the value it arose.
pub(crate) trait ClassifiedError: de::Error {
    /// The failure arose under `key`; `known` holds serde's static field or
    /// variant names when the value is a struct or an enum.
    fn within_key(self, key: &str, known: Option<&'static [&'static str]>) -> Self;

    fn within_index(self, index: usize) -> Self;
}

/// Deserializes one JSON value; a failure carries only what `E` keeps.
pub(crate) fn from_value<T: DeserializeOwned, E: ClassifiedError>(value: &Value) -> Result<T, E> {
    T::deserialize(ValueDeserializer::<E>::new(value))
}

/// The value's kind for a type error; the policy decides how much to keep.
fn unexpected(value: &Value) -> Unexpected<'_> {
    match value {
        Value::Null => Unexpected::Unit,
        Value::Bool(value) => Unexpected::Bool(*value),
        Value::Number(number) => match (number.as_u64(), number.as_i64(), number.as_f64()) {
            (Some(unsigned), _, _) => Unexpected::Unsigned(unsigned),
            (None, Some(signed), _) => Unexpected::Signed(signed),
            (None, None, Some(float)) => Unexpected::Float(float),
            (None, None, None) => Unexpected::Other("number"),
        },
        Value::String(value) => Unexpected::Str(value),
        Value::Array(_) => Unexpected::Seq,
        Value::Object(_) => Unexpected::Map,
    }
}

struct ValueDeserializer<'a, E> {
    value: &'a Value,
    error: PhantomData<fn() -> E>,
}

impl<'a, E> ValueDeserializer<'a, E> {
    fn new(value: &'a Value) -> Self {
        Self {
            value,
            error: PhantomData,
        }
    }
}

impl<'de, 'a, E: ClassifiedError> de::Deserializer<'de> for ValueDeserializer<'a, E> {
    type Error = E;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.value {
            Value::Null => visitor.visit_unit(),
            Value::Bool(value) => visitor.visit_bool(*value),
            Value::Number(number) => match (number.as_u64(), number.as_i64(), number.as_f64()) {
                (Some(unsigned), _, _) => visitor.visit_u64(unsigned),
                (None, Some(signed), _) => visitor.visit_i64(signed),
                (None, None, Some(float)) => visitor.visit_f64(float),
                (None, None, None) => Err(E::invalid_type(unexpected(self.value), &visitor)),
            },
            Value::String(value) => visitor.visit_str(value),
            Value::Array(items) => visitor.visit_seq(SeqAccess::<E> {
                items: items.iter().enumerate(),
                error: PhantomData,
            }),
            Value::Object(entries) => visitor.visit_map(MapAccess::<E> {
                entries: entries.iter(),
                known: None,
                pending: None,
                error: PhantomData,
            }),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        match self.value {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, E> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        match self.value {
            Value::Object(entries) => visitor.visit_map(MapAccess::<E> {
                entries: entries.iter(),
                known: Some(fields),
                pending: None,
                error: PhantomData,
            }),
            _ => self.deserialize_any(visitor),
        }
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        match self.value {
            Value::String(variant) => visitor.visit_enum(StrDeserializer::<E>::new(variant)),
            Value::Object(entries) if entries.len() == 1 => match entries.iter().next() {
                Some((variant, value)) => visitor
                    .visit_enum(EnumAccess::<E> {
                        variant,
                        value,
                        error: PhantomData,
                    })
                    .map_err(|error| error.within_key(variant, Some(variants))),
                None => Err(E::invalid_type(unexpected(self.value), &"an enum variant")),
            },
            _ => Err(E::invalid_type(unexpected(self.value), &"an enum variant")),
        }
    }

    fn deserialize_ignored_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_unit()
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes
        byte_buf unit unit_struct seq tuple tuple_struct map identifier
    }
}

struct SeqAccess<'a, E> {
    items: std::iter::Enumerate<std::slice::Iter<'a, Value>>,
    error: PhantomData<fn() -> E>,
}

impl<'de, 'a, E: ClassifiedError> de::SeqAccess<'de> for SeqAccess<'a, E> {
    type Error = E;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, E> {
        match self.items.next() {
            Some((index, value)) => seed
                .deserialize(ValueDeserializer::<E>::new(value))
                .map(Some)
                .map_err(|error| error.within_index(index)),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.items.len())
    }
}

struct MapAccess<'a, E> {
    entries: serde_json::map::Iter<'a>,
    known: Option<&'static [&'static str]>,
    pending: Option<(&'a str, &'a Value)>,
    error: PhantomData<fn() -> E>,
}

impl<'de, 'a, E: ClassifiedError> de::MapAccess<'de> for MapAccess<'a, E> {
    type Error = E;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>, E> {
        let Some((key, value)) = self.entries.next() else {
            return Ok(None);
        };
        self.pending = Some((key, value));
        // A key error, such as an unknown field, already names its subject.
        seed.deserialize(StrDeserializer::<E>::new(key)).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value, E> {
        let (key, value) = self
            .pending
            .take()
            .ok_or_else(|| E::custom("a map value was requested before its key"))?;
        seed.deserialize(ValueDeserializer::<E>::new(value))
            .map_err(|error| error.within_key(key, self.known))
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.entries.len())
    }
}

struct EnumAccess<'a, E> {
    variant: &'a str,
    value: &'a Value,
    error: PhantomData<fn() -> E>,
}

impl<'de, 'a, E: ClassifiedError> de::EnumAccess<'de> for EnumAccess<'a, E> {
    type Error = E;
    type Variant = VariantAccessor<'a, E>;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self::Variant), E> {
        let variant = seed.deserialize(StrDeserializer::<E>::new(self.variant))?;
        Ok((
            variant,
            VariantAccessor {
                value: self.value,
                error: PhantomData,
            },
        ))
    }
}

struct VariantAccessor<'a, E> {
    value: &'a Value,
    error: PhantomData<fn() -> E>,
}

impl<'de, 'a, E: ClassifiedError> VariantAccess<'de> for VariantAccessor<'a, E> {
    type Error = E;

    fn unit_variant(self) -> Result<(), E> {
        match self.value {
            Value::Null => Ok(()),
            _ => Err(E::invalid_type(unexpected(self.value), &"a unit variant")),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, E> {
        seed.deserialize(ValueDeserializer::<E>::new(self.value))
    }

    fn tuple_variant<V: Visitor<'de>>(self, _len: usize, visitor: V) -> Result<V::Value, E> {
        de::Deserializer::deserialize_seq(ValueDeserializer::<E>::new(self.value), visitor)
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, E> {
        de::Deserializer::deserialize_struct(
            ValueDeserializer::<E>::new(self.value),
            "",
            fields,
            visitor,
        )
    }
}
