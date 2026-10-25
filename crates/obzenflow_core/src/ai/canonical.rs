// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde_json::{Map, Value};

pub const AI_MAP_REDUCE_COLLECTOR_FACT_FORMAT_V2: &str =
    "obzenflow.ai_map_reduce.collector_fact.v2";

/// Canonical byte representation used for AI map-reduce collector equality.
///
/// Object keys are ordered by their UTF-8 byte representation, arrays retain
/// their original order, strings are not normalised, and the result is compact
/// JSON. Composite activations, runtime envelope, writer, replay and causality
/// metadata are absent.
pub fn canonical_json_bytes_v2(
    event_type: &str,
    payload: Value,
) -> Result<Vec<u8>, serde_json::Error> {
    let value = Value::Object(Map::from_iter([
        (
            "format".to_string(),
            Value::String(AI_MAP_REDUCE_COLLECTOR_FACT_FORMAT_V2.to_string()),
        ),
        (
            "event_type".to_string(),
            Value::String(event_type.to_string()),
        ),
        ("payload".to_string(), payload),
    ]));

    serde_json::to_vec(&sort_objects(value))
}

fn sort_objects(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries = map
                .into_iter()
                .map(|(key, value)| (key, sort_objects(value)))
                .collect::<Vec<_>>();
            entries.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
            Value::Object(entries.into_iter().collect())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(sort_objects).collect()),
        scalar => scalar,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn sorts_nested_object_keys_and_preserves_array_order() {
        let bytes =
            canonical_json_bytes_v2("example", json!({"z": {"b": 2, "a": 1}, "a": [3, 2, 1]}))
                .expect("canonical JSON");
        let encoded = String::from_utf8(bytes).expect("UTF-8 JSON");

        assert_eq!(
            encoded,
            r#"{"event_type":"example","format":"obzenflow.ai_map_reduce.collector_fact.v2","payload":{"a":[3,2,1],"z":{"a":1,"b":2}}}"#
        );
    }
}
