// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared raw-JSON archive epoch gate.

use obzenflow_core::journal::run_manifest::RUN_MANIFEST_VERSION;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnsupportedManifestVersion {
    found: String,
}

impl UnsupportedManifestVersion {
    pub(crate) fn found(&self) -> &str {
        &self.found
    }
}

/// Require the one archive epoch this build understands before any typed
/// manifest deserialisation or journal access.
pub(crate) fn require_current_manifest_version(
    manifest: &serde_json::Value,
) -> Result<(), UnsupportedManifestVersion> {
    match manifest.get("manifest_version") {
        Some(serde_json::Value::String(version)) if version == RUN_MANIFEST_VERSION => Ok(()),
        Some(serde_json::Value::String(version)) => Err(UnsupportedManifestVersion {
            found: version.clone(),
        }),
        Some(value) => Err(UnsupportedManifestVersion {
            found: value.to_string(),
        }),
        None => Err(UnsupportedManifestVersion {
            found: "<missing>".to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_string_epoch_is_the_only_accepted_shape() {
        assert!(require_current_manifest_version(&serde_json::json!({
            "manifest_version": RUN_MANIFEST_VERSION
        }))
        .is_ok());

        for (value, found) in [
            (serde_json::json!({}), "<missing>"),
            (serde_json::json!({"manifest_version": 3.0}), "3.0"),
            (serde_json::json!({"manifest_version": "2.0"}), "2.0"),
            (serde_json::json!({"manifest_version": "4.0"}), "4.0"),
        ] {
            assert_eq!(
                require_current_manifest_version(&value)
                    .expect_err("non-exact version must fail")
                    .found(),
                found
            );
        }
    }
}
