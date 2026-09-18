// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stream hot framework structures without building a JSON object tree first.
//! Both paths use the same field table, defaults and scalar wire encodings.

use super::layout::{DefaultValue, Field, Kind};
use super::primitives::{text, unsigned};
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
    if matches!(
        kind,
        Kind::Struct(_)
            | Kind::List(_)
            | Kind::Unsigned
            | Kind::Float
            | Kind::Boolean
            | Kind::Text
            | Kind::Id
            | Kind::FlowId
            | Kind::Enum(_)
    ) {
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

macro_rules! unsigned_scalar {
    ($($name:ident($ty:ty)),* $(,)?) => {$(
        fn $name(self, value: $ty) -> Result<u64> { self.serialize_u64(u64::from(value)) }
    )*};
}

macro_rules! signed_scalar {
    ($($name:ident($ty:ty)),* $(,)?) => {$(
        fn $name(self, value: $ty) -> Result<u64> { self.serialize_i64(i64::from(value)) }
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

    unsupported_scalar! { serialize_bytes(v: &[u8]) }
    unsigned_scalar! { serialize_u8(u8), serialize_u16(u16), serialize_u32(u32) }
    signed_scalar! { serialize_i8(i8), serialize_i16(i16), serialize_i32(i32) }

    fn serialize_u64(self, value: u64) -> Result<u64> {
        if matches!(self.kind, Kind::Float) {
            return self.serialize_f64(value as f64);
        }
        if !matches!(self.kind, Kind::Unsigned) {
            return Err(invalid("unsigned integer in a non-numeric schema field"));
        }
        if value == 0 && matches!(self.default, Some(DefaultValue::Zero)) {
            return Ok(2);
        }
        unsigned(value, self.out);
        Ok(3)
    }

    fn serialize_i64(self, value: i64) -> Result<u64> {
        if matches!(self.kind, Kind::Float) {
            return self.serialize_f64(value as f64);
        }
        self.serialize_u64(u64::try_from(value).map_err(|_| invalid("negative unsigned integer"))?)
    }

    fn serialize_u128(self, value: u128) -> Result<u64> {
        self.serialize_u64(u64::try_from(value).map_err(|_| invalid("unsigned integer overflow"))?)
    }

    fn serialize_i128(self, value: i128) -> Result<u64> {
        match u64::try_from(value) {
            Ok(value) => self.serialize_u64(value),
            Err(_) => self.serialize_i64(
                i64::try_from(value).map_err(|_| invalid("signed integer overflow"))?,
            ),
        }
    }

    fn serialize_f32(self, value: f32) -> Result<u64> {
        self.serialize_f64(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> Result<u64> {
        // serde_json::to_value, used by the previous adapter, maps non-finite
        // values to null. Preserve its presence state as well as finite bits.
        if !value.is_finite() {
            return Ok(1);
        }
        if !matches!(self.kind, Kind::Float) {
            return Err(invalid("float in a non-float schema field"));
        }
        if value.to_bits() == 0 && matches!(self.default, Some(DefaultValue::FloatZero)) {
            return Ok(2);
        }
        self.out.extend_from_slice(&value.to_le_bytes());
        Ok(3)
    }

    fn serialize_bool(self, value: bool) -> Result<u64> {
        if !matches!(self.kind, Kind::Boolean) {
            return Err(invalid("boolean in a non-boolean schema field"));
        }
        if !value && matches!(self.default, Some(DefaultValue::False)) {
            return Ok(2);
        }
        self.out.push(u8::from(value));
        Ok(3)
    }

    fn serialize_str(self, value: &str) -> Result<u64> {
        if matches!(self.default, Some(DefaultValue::Text(default)) if default == value) {
            return Ok(2);
        }
        match self.kind {
            Kind::Text => text(value, self.out),
            Kind::Id | Kind::FlowId => values::id(value, self.out)?,
            Kind::Enum(variants) => {
                let index = variants
                    .iter()
                    .position(|variant| *variant == value)
                    .ok_or_else(|| invalid(format!("unknown closed enum value: {value}")))?;
                unsigned(index as u64, self.out);
            }
            _ => return Err(invalid("string in a non-string schema field")),
        }
        Ok(3)
    }

    fn serialize_char(self, value: char) -> Result<u64> {
        self.serialize_str(value.encode_utf8(&mut [0; 4]))
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
    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<u64> {
        self.serialize_str(variant)
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
            next: 0,
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
    next: usize,
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
        let index = if self
            .fields
            .get(self.next)
            .is_some_and(|field| field.name == key)
        {
            self.next
        } else {
            self.fields
                .iter()
                .position(|field| field.name == key)
                .ok_or_else(|| invalid(format!("unknown schema field: {key}")))?
        };
        self.next = index + 1;
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
