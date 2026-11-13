// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub fn without_observations(mut value: serde_json::Value) -> serde_json::Value {
    value["envelope"]
        .as_object_mut()
        .unwrap()
        .remove("observability");
    value
}
