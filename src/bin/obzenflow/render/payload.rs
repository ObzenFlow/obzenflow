// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Domain-neutral payload formatting. Field names and units stay as recorded.

use serde_json::Value;

pub(super) fn fields(value: &Value) -> Vec<String> {
    match value {
        Value::Object(object) if !object.is_empty() => object
            .iter()
            .map(|(key, value)| format!("{}: {}", safe_text(key), compact(value, 0)))
            .collect(),
        _ => vec![compact(value, 0)],
    }
}

fn compact(value: &Value, depth: usize) -> String {
    let text = match value {
        Value::String(value) => {
            let value = abbreviated(&safe_text(value), 200);
            // Keep strings distinguishable from numbers, booleans and null.
            if value.is_empty()
                || value.trim() != value
                || serde_json::from_str::<Value>(&value).is_ok()
            {
                serde_json::to_string(&value).unwrap()
            } else {
                value
            }
        }
        Value::Array(values) if depth < 3 => {
            let mut items: Vec<_> = values
                .iter()
                .take(4)
                .map(|v| compact(v, depth + 1))
                .collect();
            if values.len() > 4 {
                items.push(format!("… {} more", values.len() - 4));
            }
            format!("[{}]", items.join(", "))
        }
        Value::Object(values) if depth < 3 => {
            let mut items: Vec<_> = values
                .iter()
                .take(8)
                .map(|(k, v)| format!("{}: {}", safe_text(k), compact(v, depth + 1)))
                .collect();
            if values.len() > 8 {
                items.push(format!("… {} more fields", values.len() - 8));
            }
            format!("{{{}}}", items.join(", "))
        }
        _ => abbreviated(&safe_text(&value.to_string()), 200),
    };
    abbreviated(&text, 240)
}

pub(super) fn wrap_fields(fields: &[String], width: usize) -> Vec<String> {
    let width = width.max(8);
    let mut lines = Vec::new();
    let mut line = String::new();
    for field in fields {
        if !line.is_empty() && line.chars().count() + 3 + field.chars().count() > width {
            lines.push(std::mem::take(&mut line));
        }
        if !line.is_empty() {
            line.push_str(" · ");
        }
        // A single nested value or long string also respects the available
        // width. Continuations are indented, without discarding later fields.
        let mut rest = field.as_str();
        while rest.chars().count() + line.chars().count() > width {
            let remaining = width.saturating_sub(line.chars().count());
            let boundary = rest
                .char_indices()
                .nth(remaining)
                .map_or(rest.len(), |(i, _)| i);
            let split = rest[..boundary]
                .rfind(' ')
                .filter(|index| *index > remaining / 2)
                .unwrap_or(boundary);
            line.push_str(&rest[..split]);
            lines.push(std::mem::take(&mut line));
            rest = rest[split..].trim_start();
            line.push_str("  ");
        }
        line.push_str(rest);
    }
    if !line.is_empty() {
        lines.push(line);
    }
    lines
}

pub(super) fn safe_text(text: &str) -> String {
    let mut safe = String::with_capacity(text.len());
    for ch in text.chars() {
        if ch.is_control() || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}') {
            safe.extend(ch.escape_default());
        } else {
            safe.push(ch);
        }
    }
    safe
}

pub(super) fn abbreviated(text: &str, limit: usize) -> String {
    let mut chars = text.chars();
    let mut short: String = chars.by_ref().take(limit).collect();
    if chars.next().is_some() {
        short.push_str("… [--detail]");
    }
    short
}
