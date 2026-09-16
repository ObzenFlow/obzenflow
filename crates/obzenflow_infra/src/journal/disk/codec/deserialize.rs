// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Feed positional framework structures straight into their Core deserializers.
//! Immutable definitions and uncommon dynamic values retain the shared decoder.

use super::primitives::Cursor;
use super::schema::{Field, Kind};
use super::values::{self, ReadDefinitions};
use super::{invalid, Error, Result};
use serde::de::{
    DeserializeOwned, DeserializeSeed, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};
use serde::Deserializer;

impl serde::de::Error for Error {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        invalid(message.to_string())
    }
}

pub(super) fn read<T: DeserializeOwned>(
    kind: Kind,
    input: &mut Cursor<'_>,
    definitions: &mut impl ReadDefinitions,
) -> Result<T> {
    T::deserialize(Decode {
        kind,
        input,
        definitions,
    })
}

struct Decode<'a, 'b, D> {
    kind: Kind,
    input: &'a mut Cursor<'b>,
    definitions: &'a mut D,
}

impl<'de, D: ReadDefinitions> Deserializer<'de> for Decode<'_, '_, D> {
    type Error = Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        match self.kind {
            Kind::Unsigned => visitor.visit_u64(self.input.unsigned()?),
            Kind::Float => {
                let value = f64::from_le_bytes(self.input.take(8)?.try_into().unwrap());
                if !value.is_finite() {
                    return Err(invalid("non-finite float"));
                }
                visitor.visit_f64(value)
            }
            Kind::Boolean => match self.input.byte()? {
                0 => visitor.visit_bool(false),
                1 => visitor.visit_bool(true),
                _ => Err(invalid("invalid boolean")),
            },
            Kind::Text => visitor.visit_string(self.input.text()?),
            Kind::Enum(variants) => visitor.visit_str(
                variants.get(self.input.length()?)
                    .ok_or_else(|| invalid("unknown closed enum ordinal"))?,
            ),
            Kind::Struct(shape) => {
                let fields = shape.fields();
                let mask = self.input.unsigned()?;
                if mask >> (fields.len() * 2) != 0 {
                    return Err(invalid("unknown schema presence bits"));
                }
                visitor.visit_map(Fields {
                    fields,
                    mask,
                    next: 0,
                    selected: None,
                    input: self.input,
                    definitions: self.definitions,
                })
            }
            Kind::List(kind) => {
                let remaining = values::bounded_count(self.input)?;
                visitor.visit_seq(Elements {
                    kind: *kind,
                    remaining,
                    input: self.input,
                    definitions: self.definitions,
                })
            }
            _ => values::read(self.kind, self.input, self.definitions)?
                .into_deserializer()
                .deserialize_any(visitor)
                .map_err(Error::from),
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value> {
        visitor.visit_some(self)
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        visitor: V,
    ) -> Result<V::Value> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value> {
        if let Kind::Enum(variants) = self.kind {
            let variant = variants.get(self.input.length()?)
                .ok_or_else(|| invalid("unknown closed enum ordinal"))?;
            return visitor.visit_enum(serde::de::value::BorrowedStrDeserializer::<Error>::new(variant));
        }
        values::read(self.kind, self.input, self.definitions)?
            .into_deserializer()
            .deserialize_enum(name, variants, visitor)
            .map_err(Error::from)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit unit_struct seq tuple tuple_struct map struct identifier ignored_any
    }
}

struct Fields<'a, 'b, D> {
    fields: &'static [Field],
    mask: u64,
    next: usize,
    selected: Option<(Field, u64)>,
    input: &'a mut Cursor<'b>,
    definitions: &'a mut D,
}

impl<'de, D: ReadDefinitions> MapAccess<'de> for Fields<'_, '_, D> {
    type Error = Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(&mut self, seed: K) -> Result<Option<K::Value>> {
        while let Some(&field) = self.fields.get(self.next) {
            let state = (self.mask >> (self.next * 2)) & 3;
            self.next += 1;
            if state == 0 {
                continue;
            }
            self.selected = Some((field, state));
            return seed
                .deserialize(serde::de::value::BorrowedStrDeserializer::<Error>::new(
                    field.name,
                ))
                .map(Some);
        }
        Ok(None)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(&mut self, seed: V) -> Result<V::Value> {
        let (field, state) = self
            .selected
            .take()
            .ok_or_else(|| invalid("schema value without field"))?;
        match state {
            1 => seed
                .deserialize(serde_json::Value::Null.into_deserializer())
                .map_err(Error::from),
            2 => {
                let default = field
                    .default
                    .ok_or_else(|| invalid("field has no fixed default"))?;
                seed.deserialize(values::default_value(default).into_deserializer())
                    .map_err(Error::from)
            }
            _ => seed.deserialize(Decode {
                kind: field.kind,
                input: self.input,
                definitions: self.definitions,
            }),
        }
    }
}

struct Elements<'a, 'b, D> {
    kind: Kind,
    remaining: usize,
    input: &'a mut Cursor<'b>,
    definitions: &'a mut D,
}

impl<'de, D: ReadDefinitions> SeqAccess<'de> for Elements<'_, '_, D> {
    type Error = Error;
    fn next_element_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<Option<T::Value>> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.remaining -= 1;
        seed.deserialize(Decode {
            kind: self.kind,
            input: self.input,
            definitions: self.definitions,
        })
        .map(Some)
    }
    fn size_hint(&self) -> Option<usize> {
        Some(self.remaining)
    }
}
