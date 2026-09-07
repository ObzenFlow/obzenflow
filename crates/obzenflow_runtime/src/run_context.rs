// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The explicit per-run build context (FLOWIP-010 §7 carrier).
//!
//! `FlowApplication` resolves the runtime config snapshot once, owns it, and
//! hands it to the flow build through this context; the build never reads
//! 010-owned values ambiently. This type is the named future home for the
//! 010h bootstrap-field migration (recorded follow-up). Not `RuntimeContext`:
//! that name belongs to the core metrics snapshot.

use crate::runtime_config::ResolvedRuntimeConfig;
use obzenflow_core::metrics::MetricsSnapshotExporter;
use std::sync::Arc;

#[derive(Clone)]
pub struct FlowBuildContext {
    runtime_config: Arc<ResolvedRuntimeConfig>,
    metrics_exporter: Option<Arc<dyn MetricsSnapshotExporter>>,
}

impl FlowBuildContext {
    pub fn new(runtime_config: Arc<ResolvedRuntimeConfig>) -> Self {
        Self {
            runtime_config,
            metrics_exporter: None,
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
            .finish()
    }
}
