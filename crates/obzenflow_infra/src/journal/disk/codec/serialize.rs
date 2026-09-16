// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stream hot framework structures without building a JSON object tree first.
//! Both paths use the same field table, defaults and scalar wire encodings.

use super::primitives::unsigned;
use super::schema::{DefaultValue, Field, Kind};
use super::values::{self, WriteDefinitions};
use super::{invalid, Error, Result};
use serde::ser::{Impossible, SerializeSeq, SerializeStruct, SerializeTuple, SerializeTupleStruct};
use serde::{Serialize, Serializer};

impl serde::ser::Error for Error {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        invalid(message.to_string())
    }
}

pub(super) fn write<T: Serialize + ?Sized>(
    kind: Kind,
    value: &T,
    default: Option<DefaultValue>,
    out: &mut Vec<u8>,
    definitions: &mut impl WriteDefinitions,
) -> Result<u64> {
    if matches!(kind, Kind::Struct(_) | Kind::List(_)) {
        return value.serialize(Encode {
            kind,
            default,
            out,
            definitions,
        });
    }
    let value = serde_json::to_value(value)?;
    if value.is_null() {
        return Ok(1);
    }
    if default.is_some_and(|default| values::is_default(&value, default)) {
        return Ok(2);
    }
    values::write(kind, &value, out, definitions)?;
    Ok(3)
}

struct Encode<'a, D> {
    kind: Kind,
    default: Option<DefaultValue>,
    out: &'a mut Vec<u8>,
    definitions: &'a mut D,
}

macro_rules! unsupported_scalar {
    ($($name:ident($value:ident: $ty:ty)),* $(,)?) => {$(
        fn $name(self, _: $ty) -> Result<u64> { Err(invalid("scalar in a schema container")) }
    )*};
}

impl<'a, D: WriteDefinitions> Serializer for Encode<'a, D> {
    type Ok = u64;
    type Error = Error;
    type SerializeSeq = Elements<'a, D>;
    type SerializeTuple = Elements<'a, D>;
    type SerializeTupleStruct = Elements<'a, D>;
    type SerializeTupleVariant = Impossible<u64, Error>;
    type SerializeMap = Impossible<u64, Error>;
    type SerializeStruct = Fields<'a, D>;
    type SerializeStructVariant = Impossible<u64, Error>;

    unsupported_scalar! {
        serialize_bool(v: bool), serialize_i8(v: i8), serialize_i16(v: i16), serialize_i32(v: i32),
        serialize_i64(v: i64), serialize_i128(v: i128), serialize_u8(v: u8), serialize_u16(v: u16),
        serialize_u32(v: u32), serialize_u64(v: u64), serialize_u128(v: u128), serialize_f32(v: f32),
        serialize_f64(v: f64), serialize_char(v: char), serialize_str(v: &str), serialize_bytes(v: &[u8]),
    }
    fn serialize_none(self) -> Result<u64> {
        Ok(1)
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<u64> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<u64> {
        Ok(1)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<u64> {
        Ok(1)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        value: &T,
    ) -> Result<u64> {
        value.serialize(self)
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<u64> {
        Err(invalid("enum in schema container"))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<u64> {
        Err(invalid("enum in schema container"))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant> {
        Err(invalid("enum in schema container"))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant> {
        Err(invalid("enum in schema container"))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap> {
        Err(invalid("map in schema container"))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct> {
        let Kind::Struct(shape) = self.kind else {
            return Err(invalid("expected list"));
        };
        Ok(Fields {
            fields: shape.fields(),
            mask: 0,
            ranges: Vec::new(),
            body: Vec::new(),
            out: self.out,
            definitions: self.definitions,
        })
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq> {
        let Kind::List(kind) = self.kind else {
            return Err(invalid("expected structure"));
        };
        Ok(Elements {
            kind: *kind,
            default: self.default,
            count: 0,
            body: Vec::new(),
            out: self.out,
            definitions: self.definitions,
        })
    }
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(
        self,
        _: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct> {
        self.serialize_seq(Some(len))
    }
}

struct Fields<'a, D> {
    fields: &'static [Field],
    mask: u64,
    ranges: Vec<(usize, std::ops::Range<usize>)>,
    body: Vec<u8>,
    out: &'a mut Vec<u8>,
    definitions: &'a mut D,
}

impl<D: WriteDefinitions> SerializeStruct for Fields<'_, D> {
    type Ok = u64;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<()> {
        let index = self
            .fields
            .iter()
            .position(|field| field.name == key)
            .ok_or_else(|| invalid(format!("unknown schema field: {key}")))?;
        if (self.mask >> (index * 2)) & 3 != 0 {
            return Err(invalid("duplicate schema field"));
        }
        let field = self.fields[index];
        let start = self.body.len();
        let state = write(
            field.kind,
            value,
            field.default,
            &mut self.body,
            self.definitions,
        )?;
        self.mask |= state << (index * 2);
        self.ranges.push((index, start..self.body.len()));
        Ok(())
    }
    fn end(mut self) -> Result<u64> {
        unsigned(self.mask, self.out);
        if self.ranges.windows(2).all(|pair| pair[0].0 < pair[1].0) {
            self.out.extend_from_slice(&self.body);
        } else {
            self.ranges.sort_unstable_by_key(|(index, _)| *index);
            for (_, range) in self.ranges {
                self.out.extend_from_slice(&self.body[range]);
            }
        }
        Ok(3)
    }
}

struct Elements<'a, D> {
    kind: Kind,
    default: Option<DefaultValue>,
    count: u64,
    body: Vec<u8>,
    out: &'a mut Vec<u8>,
    definitions: &'a mut D,
}
impl<D: WriteDefinitions> SerializeSeq for Elements<'_, D> {
    type Ok = u64;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        let state = write(self.kind, value, None, &mut self.body, self.definitions)?;
        if state != 3 {
            return Err(invalid("null element in non-null schema list"));
        }
        self.count += 1;
        Ok(())
    }
    fn end(self) -> Result<u64> {
        if self.count == 0 && matches!(self.default, Some(DefaultValue::EmptyList)) {
            return Ok(2);
        }
        unsigned(self.count, self.out);
        self.out.extend_from_slice(&self.body);
        Ok(3)
    }
}
impl<D: WriteDefinitions> SerializeTuple for Elements<'_, D> {
    type Ok = u64;
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<u64> {
        SerializeSeq::end(self)
    }
}
impl<D: WriteDefinitions> SerializeTupleStruct for Elements<'_, D> {
    type Ok = u64;
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<()> {
        SerializeSeq::serialize_element(self, value)
    }
    fn end(self) -> Result<u64> {
        SerializeSeq::end(self)
    }
}
