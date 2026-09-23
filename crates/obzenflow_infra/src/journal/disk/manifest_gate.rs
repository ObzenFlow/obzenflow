// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared raw-JSON archive epoch gate.

use obzenflow_core::journal::archive::manifest::JOURNAL_SCHEMA_VERSION;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unsupported journal schema version: {found}")]
pub(crate) struct UnsupportedJournalSchemaVersion {
    found: String,
}

impl UnsupportedJournalSchemaVersion {
    pub(crate) fn found(&self) -> &str {
        &self.found
    }
}

/// Require the one archive epoch this build understands before any typed
/// manifest deserialisation or journal access.
pub(crate) fn require_current_journal_schema_version(
    manifest: &serde_json::Value,
) -> Result<(), UnsupportedJournalSchemaVersion> {
    match manifest.get("journal_schema_version") {
        Some(serde_json::Value::String(version)) if version == JOURNAL_SCHEMA_VERSION => Ok(()),
        Some(serde_json::Value::String(version)) => Err(UnsupportedJournalSchemaVersion {
            found: version.clone(),
        }),
        Some(value) => Err(UnsupportedJournalSchemaVersion {
            found: value.to_string(),
        }),
        None => Err(UnsupportedJournalSchemaVersion {
            found: "<missing>".to_string(),
        }),
    }
}

pub(crate) fn require_observability_capture(manifest: &serde_json::Value) -> Result<(), String> {
    use obzenflow_core::journal::archive::manifest::OBSERVABILITY_CAPTURE_CAPABILITY;
    let found = manifest
        .get("capabilities")
        .and_then(|capabilities| capabilities.get(OBSERVABILITY_CAPTURE_CAPABILITY))
        .and_then(serde_json::Value::as_u64);
    if found == Some(1) {
        Ok(())
    } else {
        Err(format!("unsupported archive capability {OBSERVABILITY_CAPTURE_CAPABILITY}={found:?} (supported: 1); re-record the archive"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_string_epoch_is_the_only_accepted_shape() {
        assert!(require_current_journal_schema_version(&serde_json::json!({
            "journal_schema_version": JOURNAL_SCHEMA_VERSION
        }))
        .is_ok());

        for (value, found) in [
            (serde_json::json!({}), "<missing>"),
            (serde_json::json!({"journal_schema_version": 3.0}), "3.0"),
            (serde_json::json!({"journal_schema_version": "3.0"}), "3.0"),
            (serde_json::json!({"journal_schema_version": "2.0"}), "2.0"),
            (serde_json::json!({"journal_schema_version": "7.0"}), "7.0"),
        ] {
            let error = require_current_journal_schema_version(&value)
                .expect_err("non-exact version must fail");
            assert_eq!(error.found(), found);
            assert_eq!(
                error.to_string(),
                format!("unsupported journal schema version: {found}")
            );
        }
    }
}
