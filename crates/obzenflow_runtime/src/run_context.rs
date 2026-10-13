// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The explicit per-run build context (FLOWIP-010 §7 carrier).
//!
//! `FlowApplication` resolves the runtime config snapshot once, owns it, and
//! hands it to the flow build through this context; the build never reads
//! 010-owned values ambiently. This type is the named future home for the
//! 010h bootstrap-field migration (recorded follow-up). Not `RuntimeProvenance`:
//! that name belongs to the core metrics snapshot.
//!
//! It also holds the build's one extraction of the `[app]` table until the
//! flow build records it; the snapshot stays immutable.

use crate::runtime_config::{ApplicationSettingsError, ResolvedRuntimeConfig};
use obzenflow_core::metrics::MetricsSnapshotExporter;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::{Arc, OnceLock};

#[derive(Clone)]
pub struct FlowBuildContext {
    runtime_config: Arc<ResolvedRuntimeConfig>,
    metrics_exporter: Option<Arc<dyn MetricsSnapshotExporter>>,
    app_settings: Arc<OnceLock<serde_json::Value>>,
}

impl FlowBuildContext {
    pub fn new(runtime_config: Arc<ResolvedRuntimeConfig>) -> Self {
        Self {
            runtime_config,
            metrics_exporter: None,
            app_settings: Arc::new(OnceLock::new()),
        }
    }

    pub fn with_metrics_exporter(mut self, exporter: Arc<dyn MetricsSnapshotExporter>) -> Self {
        self.metrics_exporter = Some(exporter);
        self
    }

    pub fn metrics_exporter(&self) -> Option<&Arc<dyn MetricsSnapshotExporter>> {
        self.metrics_exporter.as_ref()
    }

    pub fn runtime_config(&self) -> &Arc<ResolvedRuntimeConfig> {
        &self.runtime_config
    }

    #[doc(hidden)]
    pub fn extract_app_settings<T: DeserializeOwned + Serialize>(
        &self,
    ) -> Result<T, ApplicationSettingsError> {
        if self.app_settings.get().is_some() {
            return Err(ApplicationSettingsError::AlreadyExtracted);
        }
        let (settings, evidence) = self.runtime_config.app().extract::<T>()?;
        self.app_settings
            .set(evidence)
            .map_err(|_| ApplicationSettingsError::AlreadyExtracted)?;
        Ok(settings)
    }

    /// The serialized settings this build extracted, if it extracted any.
    pub fn app_settings_evidence(&self) -> Option<&serde_json::Value> {
        self.app_settings.get()
    }

    /// Refuse a non-empty `[app]` table that no extraction consumed.
    pub fn ensure_app_settings_extracted(&self) -> Result<(), ApplicationSettingsError> {
        let app = self.runtime_config.app();
        if app.is_empty() || self.app_settings.get().is_some() {
            Ok(())
        } else {
            Err(app.unextracted())
        }
    }

    /// Built-in defaults only, for tests and harnesses that build flows
    /// without a host. Explicit at the call site, never an ambient fallback.
    pub fn for_tests() -> Self {
        Self::new(Arc::new(ResolvedRuntimeConfig::builtin_defaults()))
    }
}

impl std::fmt::Debug for FlowBuildContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowBuildContext")
            .field("runtime_config", &self.runtime_config)
            .field("metrics_enabled", &self.metrics_exporter.is_some())
            .field("app_settings_extracted", &self.app_settings.get().is_some())
            .finish()
    }
}
