// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Split Core's clock entries directly into immutable keys and current numbers.
//! The key list uses the existing Coordinate layout and scalar encoders.

use super::layout::{DefinitionKind, Kind};
use super::primitives::unsigned;
use super::values::{Standalone, WriteDefinitions};
use super::{invalid, serialize, Error, Result};
use serde::ser::{Impossible, SerializeSeq, SerializeStruct};
use serde::{Serialize, Serializer};

#[derive(Default)]
struct Parts {
    count: u64,
    keys: Vec<u8>,
    numbers: Vec<u8>,
}

pub(super) fn write<T: Serialize + ?Sized>(
    value: &T,
    out: &mut Vec<u8>,
    definitions: &mut impl WriteDefinitions,
) -> Result<u64> {
    let mut parts = Parts::default();
    let state = value.serialize(Encode {
        context: Context::Clock,
        parts: &mut parts,
    })?;
    if state == 3 {
        let mut keys = Vec::new();
        unsigned(parts.count, &mut keys);
        keys.extend_from_slice(&parts.keys);
        definitions.reference_encoded(DefinitionKind::ClockKeys, keys, out)?;
        out.extend_from_slice(&parts.numbers);
    }
    Ok(state)
}

#[derive(Clone, Copy)]
enum Context {
    Clock,
    Entries,
    Entry,
}

struct Encode<'a> {
    context: Context,
    parts: &'a mut Parts,
}

macro_rules! unsupported {
    ($($name:ident($($argument:ident: $ty:ty),*)),* $(,)?) => {$(
        fn $name(self, $(_: $ty),*) -> Result<u64> {
            Err(invalid("expected typed clock structure"))
        }
    )*};
}

impl<'a> Serializer for Encode<'a> {
    type Ok = u64;
    type Error = Error;
    type SerializeSeq = Entries<'a>;
    type SerializeStruct = Fields<'a>;
    type SerializeTuple = Impossible<u64, Error>;
    type SerializeTupleStruct = Impossible<u64, Error>;
    type SerializeTupleVariant = Impossible<u64, Error>;
    type SerializeMap = Impossible<u64, Error>;
    type SerializeStructVariant = Impossible<u64, Error>;

    unsupported! {
        serialize_bool(v: bool), serialize_i8(v: i8), serialize_i16(v: i16),
        serialize_i32(v: i32), serialize_i64(v: i64), serialize_i128(v: i128),
        serialize_u8(v: u8), serialize_u16(v: u16), serialize_u32(v: u32),
        serialize_u64(v: u64), serialize_u128(v: u128), serialize_f32(v: f32),
        serialize_f64(v: f64), serialize_char(v: char), serialize_str(v: &str),
        serialize_bytes(v: &[u8]),
        serialize_unit_variant(name: &'static str, index: u32, variant: &'static str)
    }

    fn serialize_none(self) -> Result<u64> {
        if matches!(self.context, Context::Clock) {
            Ok(1)
        } else {
            Err(invalid("null clock entries or entry"))
        }
    }
    fn serialize_unit(self) -> Result<u64> {
        self.serialize_none()
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<u64> {
        self.serialize_none()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<u64> {
        value.serialize(self)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<u64> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<u64> {
        Err(invalid("enum in typed clock"))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple> {
        Err(invalid("tuple in typed clock"))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        Err(invalid("tuple in typed clock"))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(invalid("enum in typed clock"))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Err(invalid("map in typed clock"))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(invalid("enum in typed clock"))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        if !matches!(self.context, Context::Entries) {
            return Err(invalid("unexpected list in typed clock"));
        }
        Ok(Entries(self.parts))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        if matches!(self.context, Context::Entries) {
            return Err(invalid("expected clock entries list"));
        }
        let mask_position = self.parts.keys.len();
        if matches!(self.context, Context::Entry) {
            // Coordinate has one field, so its presence mask fits in one byte.
            self.parts.keys.push(0);
        }
        Ok(Fields {
            context: self.context,
            parts: self.parts,
            seen: 0,
            mask_position,
        })
    }
}

struct Entries<'a>(&'a mut Parts);

impl SerializeSeq for Entries<'_> {
    type Ok = u64;
    type Error = Error;

    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        value.serialize(Encode {
            context: Context::Entry,
            parts: self.0,
        })?;
        self.0.count += 1;
        Ok(())
    }
    fn end(self) -> Result<u64> {
        Ok(3)
    }
}

struct Fields<'a> {
    context: Context,
    parts: &'a mut Parts,
    seen: u8,
    mask_position: usize,
}

impl SerializeStruct for Fields<'_> {
    type Ok = u64;
    type Error = Error;

    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let bit = match (self.context, key) {
            (Context::Clock, "entries") | (Context::Entry, "journal_writer_id") => 1,
            (Context::Entry, "sequence") => 2,
            _ => return Err(invalid("unknown typed clock field")),
        };
        if self.seen & bit != 0 {
            return Err(invalid("duplicate typed clock field"));
        }
        self.seen |= bit;
        match (self.context, key) {
            (Context::Clock, _) => {
                value.serialize(Encode {
                    context: Context::Entries,
                    parts: self.parts,
                })?;
            }
            (Context::Entry, "journal_writer_id") => {
                let state =
                    serialize::write(Kind::Id, value, None, &mut self.parts.keys, &mut Standalone)?;
                self.parts.keys[self.mask_position] = state as u8;
            }
            _ => {
                if serialize::write(
                    Kind::Unsigned,
                    value,
                    None,
                    &mut self.parts.numbers,
                    &mut Standalone,
                )? != 3
                {
                    return Err(invalid("null absolute clock value"));
                }
            }
        }
        Ok(())
    }
    fn end(self) -> Result<u64> {
        let expected = if matches!(self.context, Context::Clock) {
            1
        } else {
            3
        };
        if self.seen != expected {
            return Err(invalid("missing typed clock field"));
        }
        Ok(3)
    }
}
