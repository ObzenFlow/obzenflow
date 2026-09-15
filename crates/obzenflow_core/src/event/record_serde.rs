// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared record-boundary diagnostics without a second JSON decoding pass.

use serde::{de::Error, Deserialize, Deserializer};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WireRecord<E, P> {
    pub envelope: E,
    pub payload: P,
}

pub(super) fn deserialize<'de, D, E, P>(deserializer: D) -> Result<WireRecord<E, P>, D::Error>
where
    D: Deserializer<'de>,
    E: Deserialize<'de>,
    P: Deserialize<'de>,
{
    serde_path_to_error::deserialize(deserializer).map_err(|error| {
        let path = error.path().to_string();
        let reason = error.inner().to_string();
        if path.starts_with("envelope.provenance") {
            D::Error::custom(format!("invalid provenance at {path}: {reason}"))
        } else if path.starts_with("envelope.observability") {
            if let Some(field) = reason
                .strip_prefix("unknown field `")
                .and_then(|rest| rest.split('`').next())
            {
                let path = if path.ends_with(&format!(".{field}")) {
                    path
                } else {
                    format!("{path}.{field}")
                };
                D::Error::custom(format!("unknown observability field at {path}"))
            } else {
                D::Error::custom(error)
            }
        } else if path == "payload" || path.starts_with("payload.") {
            D::Error::custom(error)
        } else {
            D::Error::custom(format!(
                "invalid record shape: expected envelope and payload: {reason}"
            ))
        }
    })
}
