// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::layout::{DefaultValue, DefinitionKind, Kind, Layout, NAMES};
use super::primitives::{bytes, text, unsigned, Cursor};
use super::{invalid, Result};
use serde_json::{Map, Value};

pub(super) trait WriteDefinitions {
    fn reference(&mut self, kind: DefinitionKind, value: &Value, out: &mut Vec<u8>) -> Result<()>;
    fn remember_capture(&mut self, _capture: &Value) {}
    fn matches_capture(&self, _capture: &Value) -> bool {
        false
    }
}

pub(super) trait ReadDefinitions {
    fn resolve(&mut self, kind: DefinitionKind, input: &mut Cursor<'_>) -> Result<Value>;
    fn remember_capture(&mut self, _capture: &Value) {}
    fn capture(&self) -> Option<Value> {
        None
    }
}

/// Definition bodies are complete values, never another layer of references.
pub(super) struct Standalone;
impl WriteDefinitions for Standalone {
    fn reference(&mut self, _: DefinitionKind, _: &Value, _: &mut Vec<u8>) -> Result<()> {
        Err(invalid("reference inside an immutable definition"))
    }
}
impl ReadDefinitions for Standalone {
    fn resolve(&mut self, _: DefinitionKind, _: &mut Cursor<'_>) -> Result<Value> {
        Err(invalid("reference inside an immutable definition"))
    }
}

pub(super) fn default_value(default: DefaultValue) -> Value {
    match default {
        DefaultValue::Zero => Value::from(0u64),
        DefaultValue::FloatZero => Value::from(0.0),
        DefaultValue::False => Value::Bool(false),
        DefaultValue::EmptyList => Value::Array(Vec::new()),
        DefaultValue::Text(text) => Value::String(text.into()),
    }
}

pub(super) fn is_default(value: &Value, default: DefaultValue) -> bool {
    match default {
        // Negative zero has a different representation and must remain explicit.
        DefaultValue::FloatZero => value.as_f64().is_some_and(|n| n.to_bits() == 0),
        _ => *value == default_value(default),
    }
}

fn string(value: &Value) -> Result<&str> {
    value.as_str().ok_or_else(|| invalid("expected a string"))
}

pub(super) fn id(value: &str, out: &mut Vec<u8>) -> Result<()> {
    let id = value
        .parse::<ulid::Ulid>()
        .map_err(|_| invalid("invalid typed ULID"))?;
    if id.to_string() != value {
        return Err(invalid("non-canonical typed ULID"));
    }
    out.extend_from_slice(&id.to_bytes());
    Ok(())
}

fn read_id(input: &mut Cursor<'_>) -> Result<String> {
    Ok(ulid::Ulid::from_bytes(input.take(16)?.try_into().unwrap()).to_string())
}

pub(super) fn write(
    kind: Kind,
    value: &Value,
    out: &mut Vec<u8>,
    definitions: &mut impl WriteDefinitions,
) -> Result<()> {
    match kind {
        Kind::Unsigned => unsigned(
            value
                .as_u64()
                .ok_or_else(|| invalid("expected absolute unsigned integer"))?,
            out,
        ),
        Kind::Float => out.extend_from_slice(
            &value
                .as_f64()
                .ok_or_else(|| invalid("expected float"))?
                .to_le_bytes(),
        ),
        Kind::Boolean => out.push(u8::from(
            value.as_bool().ok_or_else(|| invalid("expected boolean"))?,
        )),
        Kind::Text => text(string(value)?, out),
        Kind::Id | Kind::FlowId => id(string(value)?, out)?,
        Kind::StageIdentity => definitions.reference(
            DefinitionKind::Writer,
            &serde_json::json!({"type": "Stage", "id": value}),
            out,
        )?,
        Kind::Timestamp => {
            let time = chrono::DateTime::parse_from_rfc3339(string(value)?)
                .map_err(|_| invalid("invalid timestamp"))?;
            out.extend_from_slice(&time.timestamp().to_le_bytes());
            unsigned(u64::from(time.timestamp_subsec_nanos()), out);
        }
        Kind::Json => bytes(&serde_json::to_vec(value)?, out),
        Kind::PacketCapture => {
            write(Kind::Struct(Layout::Capture), value, out, definitions)?;
            definitions.remember_capture(value);
        }
        Kind::SnapshotCapture => {
            if definitions.matches_capture(value) {
                out.push(1);
            } else {
                out.push(0);
                write(Kind::Struct(Layout::Capture), value, out, definitions)?;
            }
        }
        Kind::Value => write_dynamic(value, out, 0)?,
        Kind::Enum(variants) => {
            let index = variants
                .iter()
                .position(|variant| Some(*variant) == value.as_str())
                .ok_or_else(|| invalid(format!("unknown closed enum value: {value}")))?;
            unsigned(index as u64, out);
        }
        Kind::Struct(shape) => {
            let object = value
                .as_object()
                .ok_or_else(|| invalid("expected schema object"))?;
            let fields = shape.fields();
            if let Some(key) = object
                .keys()
                .find(|key| !fields.iter().any(|field| field.name == key.as_str()))
            {
                return Err(invalid(format!("unknown field in {shape:?}: {key}")));
            }
            let mut mask = 0u64;
            for (index, field) in fields.iter().enumerate() {
                let state = match object.get(field.name) {
                    None => 0,
                    Some(Value::Null) => 1,
                    Some(value)
                        if field
                            .default
                            .is_some_and(|default| is_default(value, default)) =>
                    {
                        2
                    }
                    Some(_) => 3,
                };
                mask |= state << (index * 2);
            }
            unsigned(mask, out);
            for (index, field) in fields.iter().enumerate() {
                if (mask >> (index * 2)) & 3 == 3 {
                    write(field.kind, &object[field.name], out, definitions)?;
                }
            }
        }
        Kind::List(element) => {
            let values = value.as_array().ok_or_else(|| invalid("expected list"))?;
            unsigned(values.len() as u64, out);
            for value in values {
                write(*element, value, out, definitions)?;
            }
        }
        Kind::Clock => {
            let object = value
                .as_object()
                .ok_or_else(|| invalid("expected vector clock"))?;
            if object.len() != 1 {
                return Err(invalid("unknown vector clock fields"));
            }
            let entries = object
                .get("entries")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("missing typed clock entries"))?;
            let keys = Value::Array(entries.iter().map(|entry| serde_json::json!({
                "journal_writer_id": entry["journal_writer_id"], "writer_id": entry["writer_id"]
            })).collect());
            definitions.reference(DefinitionKind::ClockKeys, &keys, out)?;
            for entry in entries {
                write(Kind::Unsigned, &entry["sequence"], out, definitions)?;
            }
        }
        Kind::Definition(kind) => definitions.reference(kind, value, out)?,
    }
    Ok(())
}

pub(super) fn read(
    kind: Kind,
    input: &mut Cursor<'_>,
    definitions: &mut impl ReadDefinitions,
) -> Result<Value> {
    Ok(match kind {
        Kind::Unsigned => Value::from(input.unsigned()?),
        Kind::Float => {
            let number = f64::from_le_bytes(input.take(8)?.try_into().unwrap());
            Value::Number(
                serde_json::Number::from_f64(number).ok_or_else(|| invalid("non-finite float"))?,
            )
        }
        Kind::Boolean => match input.byte()? {
            0 => Value::Bool(false),
            1 => Value::Bool(true),
            _ => return Err(invalid("invalid boolean")),
        },
        Kind::Text => Value::String(input.text()?),
        Kind::Id | Kind::FlowId => Value::String(read_id(input)?),
        Kind::StageIdentity => {
            let writer = definitions.resolve(DefinitionKind::Writer, input)?;
            if writer["type"].as_str() != Some("Stage") {
                return Err(invalid("stage identity references a system writer"));
            }
            writer["id"].clone()
        }
        Kind::Timestamp => {
            let seconds = i64::from_le_bytes(input.take(8)?.try_into().unwrap());
            let nanos = u32::try_from(input.unsigned()?)
                .map_err(|_| invalid("timestamp nanoseconds overflow"))?;
            let time = chrono::DateTime::from_timestamp(seconds, nanos)
                .ok_or_else(|| invalid("invalid absolute timestamp"))?;
            Value::String(time.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
        }
        Kind::Json => serde_json::from_slice(input.bytes()?)?,
        Kind::PacketCapture => {
            let value = read(Kind::Struct(Layout::Capture), input, definitions)?;
            definitions.remember_capture(&value);
            value
        }
        Kind::SnapshotCapture => match input.byte()? {
            0 => read(Kind::Struct(Layout::Capture), input, definitions)?,
            1 => definitions
                .capture()
                .ok_or_else(|| invalid("capture alias has no packet capture"))?,
            _ => return Err(invalid("unknown capture alias tag")),
        },
        Kind::Value => read_dynamic(input, 0)?,
        Kind::Enum(variants) => Value::String(
            variants
                .get(input.length()?)
                .ok_or_else(|| invalid("unknown enum tag"))?
                .to_string(),
        ),
        Kind::Struct(shape) => {
            let fields = shape.fields();
            let mask = input.unsigned()?;
            if mask >> (fields.len() * 2) != 0 {
                return Err(invalid("unknown schema presence bits"));
            }
            let mut object = Map::new();
            for (index, field) in fields.iter().enumerate() {
                let value = match (mask >> (index * 2)) & 3 {
                    0 => continue,
                    1 => Value::Null,
                    2 => default_value(
                        field
                            .default
                            .ok_or_else(|| invalid("field has no fixed default"))?,
                    ),
                    _ => read(field.kind, input, definitions)?,
                };
                object.insert(field.name.into(), value);
            }
            Value::Object(object)
        }
        Kind::List(element) => {
            let length = bounded_count(input)?;
            let mut values = Vec::new();
            for _ in 0..length {
                values.push(read(*element, input, definitions)?);
            }
            Value::Array(values)
        }
        Kind::Clock => {
            let keys = definitions.resolve(DefinitionKind::ClockKeys, input)?;
            let keys = keys
                .as_array()
                .ok_or_else(|| invalid("invalid clock key definition"))?;
            if keys.len() > input.remaining() {
                return Err(invalid("missing absolute clock values"));
            }
            let mut entries = Vec::with_capacity(keys.len());
            for coordinate in keys {
                let mut entry = coordinate
                    .as_object()
                    .ok_or_else(|| invalid("invalid typed clock coordinate"))?
                    .clone();
                entry.insert("sequence".into(), Value::from(input.unsigned()?));
                entries.push(Value::Object(entry));
            }
            serde_json::json!({"entries": entries})
        }
        Kind::Definition(kind) => definitions.resolve(kind, input)?,
    })
}

pub(super) fn bounded_count(input: &mut Cursor<'_>) -> Result<usize> {
    let length = input.length()?;
    if length > input.remaining() {
        return Err(invalid("count exceeds remaining frame bytes"));
    }
    Ok(length)
}

// Primitive tags, map-key tokens and complete numbers for the remaining typed
// framework structures. These are not JSON blobs or mutable-value dictionaries.
fn write_dynamic(value: &Value, out: &mut Vec<u8>, depth: usize) -> Result<()> {
    if depth > 128 {
        return Err(invalid("typed value nesting limit exceeded"));
    }
    match value {
        Value::Null => out.push(0),
        Value::Bool(false) => out.push(1),
        Value::Bool(true) => out.push(2),
        Value::Number(number) if number.is_f64() => {
            out.push(5);
            out.extend_from_slice(&number.as_f64().unwrap().to_le_bytes());
        }
        Value::Number(number) if number.is_u64() => {
            out.push(3);
            unsigned(number.as_u64().unwrap(), out);
        }
        Value::Number(number) => {
            out.push(4);
            out.extend_from_slice(&number.as_i64().unwrap().to_le_bytes());
        }
        Value::String(value) => {
            out.push(6);
            text(value, out);
        }
        Value::Array(values) => {
            out.push(7);
            unsigned(values.len() as u64, out);
            for value in values {
                write_dynamic(value, out, depth + 1)?;
            }
        }
        Value::Object(values) => {
            out.push(8);
            unsigned(values.len() as u64, out);
            for (key, value) in values {
                match NAMES.iter().position(|name| *name == key) {
                    Some(index) => unsigned(index as u64 + 1, out),
                    None => {
                        unsigned(0, out);
                        text(key, out);
                    }
                }
                write_dynamic(value, out, depth + 1)?;
            }
        }
    }
    Ok(())
}

fn read_dynamic(input: &mut Cursor<'_>, depth: usize) -> Result<Value> {
    if depth > 128 {
        return Err(invalid("typed value nesting limit exceeded"));
    }
    Ok(match input.byte()? {
        0 => Value::Null,
        1 => Value::Bool(false),
        2 => Value::Bool(true),
        3 => Value::from(input.unsigned()?),
        4 => Value::from(i64::from_le_bytes(input.take(8)?.try_into().unwrap())),
        5 => read(Kind::Float, input, &mut Standalone)?,
        6 => Value::String(input.text()?),
        7 => {
            let length = bounded_count(input)?;
            let mut values = Vec::new();
            for _ in 0..length {
                values.push(read_dynamic(input, depth + 1)?);
            }
            Value::Array(values)
        }
        8 => {
            let length = bounded_count(input)?;
            let mut values = Map::new();
            for _ in 0..length {
                let token = input.length()?;
                let key = if token == 0 {
                    input.text()?
                } else {
                    NAMES
                        .get(token - 1)
                        .ok_or_else(|| invalid("unknown field token"))?
                        .to_string()
                };
                let value = read_dynamic(input, depth + 1)?;
                if values.insert(key, value).is_some() {
                    return Err(invalid("duplicate typed field"));
                }
            }
            Value::Object(values)
        }
        _ => return Err(invalid("unknown typed value tag")),
    })
}
