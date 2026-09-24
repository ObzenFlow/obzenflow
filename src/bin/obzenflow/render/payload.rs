// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Domain-neutral payload formatting. Field names and units stay as recorded.

use serde_json::Value;
use std::fmt::Write;

/// Preserve JSON structure and scalar types. Callers label any string value
/// shortened for the terminal and offer the complete record via --full.
pub(super) fn pretty(value: &Value, width: usize) -> (String, bool) {
    let mut preview = value.clone();
    let mut shortened = false;
    fit_strings(&mut preview, 0, 0, width, &mut shortened);
    (
        json_text(&serde_json::to_string_pretty(&preview).unwrap()),
        shortened,
    )
}

pub(super) fn compact(value: &Value) -> String {
    json_text(&value.to_string())
}

fn fit_strings(
    value: &mut Value,
    indent: usize,
    column: usize,
    width: usize,
    shortened: &mut bool,
) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                let key_width = compact(&Value::String(key.clone())).chars().count();
                fit_strings(
                    value,
                    indent + 2,
                    indent + 2 + key_width + 2,
                    width,
                    shortened,
                );
            }
        }
        Value::Array(values) => {
            for value in values {
                fit_strings(value, indent + 2, indent + 2, width, shortened);
            }
        }
        Value::String(text) => {
            let available = width.saturating_sub(column + 1); // Room for a comma.
            if compact(&Value::String(text.clone())).chars().count() > available {
                let mut prefix = String::new();
                let mut remaining = available.saturating_sub(3); // Quotes and ellipsis.
                for ch in text.chars() {
                    let encoded_width = compact(&Value::String(ch.to_string())).chars().count() - 2;
                    if encoded_width > remaining {
                        break;
                    }
                    prefix.push(ch);
                    remaining -= encoded_width;
                }
                prefix.push('…');
                *text = prefix;
                *shortened = true;
            }
        }
        _ => {}
    }
}

/// Escape terminal controls and bidi overrides with valid JSON escapes.
fn json_text(text: &str) -> String {
    let mut safe = String::with_capacity(text.len());
    for ch in text.chars() {
        if (ch.is_control() && ch != '\n')
            || matches!(ch, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        {
            write!(&mut safe, "\\u{:04x}", ch as u32).unwrap();
        } else {
            safe.push(ch);
        }
    }
    safe
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
                .filter(|index| *index > 0)
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
    const SUFFIX: &str = "… [--full]";
    if text.chars().count() <= limit {
        return text.into();
    }
    let mut short: String = text
        .chars()
        .take(limit.saturating_sub(SUFFIX.chars().count()))
        .collect();
    short.push_str(SUFFIX);
    short
}
