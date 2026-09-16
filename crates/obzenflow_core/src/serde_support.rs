// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Logical JSON presence rules shared by durable domain values.

use serde::{Deserialize, Deserializer};

/// With `#[serde(default)]`, a missing field is `None`; an explicitly supplied
/// JSON null remains `Some(Value::Null)`, just like any other JSON value.
pub(crate) fn present_json<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<serde_json::Value>, D::Error> {
    serde_json::Value::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    #[test]
    fn durable_tool_reply_and_checkpoint_json_retain_explicit_nulls() {
        for optional in [None, Some(Value::Null), Some(json!({})), Some(json!([]))] {
            let mut tool = json!({"name":"example"});
            let mut reply = json!({"text":"example"});
            if let Some(value) = &optional {
                tool["parameters_schema"] = value.clone();
                reply["raw"] = value.clone();
            }
            let tool_value: crate::ai::ToolDefinition =
                serde_json::from_value(tool.clone()).unwrap();
            let reply_value: crate::ai::ChatResponse =
                serde_json::from_value(reply.clone()).unwrap();
            assert_eq!(serde_json::to_value(tool_value).unwrap(), tool);
            assert_eq!(serde_json::to_value(reply_value).unwrap(), reply);

            let event = crate::event::ChainEventFactory::checkpoint_event(
                crate::WriterId::from(crate::StageId::new()),
                "checkpoint".into(),
                optional,
            );
            let record = crate::event::JournalRecord::new(crate::JournalWriterId::new(), event);
            let original = serde_json::to_value(&record).unwrap();
            let restored: crate::event::JournalRecord<crate::event::ChainPayload> =
                serde_json::from_value(original.clone()).unwrap();
            assert_eq!(serde_json::to_value(restored).unwrap(), original);
        }
    }
}
