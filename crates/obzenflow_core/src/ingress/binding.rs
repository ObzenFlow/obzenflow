// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: the hosted-ingress binding slot.
//!
//! The write-once coordination primitive shared between a hosted source's typed
//! half (which enters flow topology) and its ingress handle or optional hosted
//! web-surface half. The ingress source/handle constructor creates it, the DSL
//! fills it during source-stage materialization, and the host reads it during
//! handle or web-surface wiring.

use super::boundary::IngressBoundaryMiddleware;
use crate::StageId;
use std::sync::{Arc, OnceLock};

/// The boundary plus replay-stable identity the DSL fills into a hosted-ingress
/// slot during source-stage materialization.
///
/// It carries only core-native types so the runtime source-handler trait can
/// return the slot without depending on adapter carrier concepts. The adapter
/// carrier ingress surface/unit are built transiently by the DSL binder from
/// this data when it materializes the boundary.
pub struct FilledHostedIngress {
    /// Runtime stage id of the linked source stage.
    pub stage_id: StageId,
    /// Replay-stable source stage key (`StageConfig.name`, the `run_manifest.json`
    /// key).
    pub stage_key: String,
    /// The composed ingress admission boundary, or `None` when a hosted source
    /// has no ingress middleware attached (the slot is still filled so startup
    /// can verify every hosted surface was placed in topology).
    pub boundary: Option<Arc<dyn IngressBoundaryMiddleware>>,
}

/// A second source stage attempted to bind a hosted surface that was already
/// bound (cloned source halves must not silently attach one listener surface to
/// multiple source stages).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HostedIngressAlreadyBound;

impl std::fmt::Display for HostedIngressAlreadyBound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "hosted ingress binding slot is already bound to a source stage"
        )
    }
}

impl std::error::Error for HostedIngressAlreadyBound {}

/// Write-once binding slot shared between a hosted source's typed half (which
/// enters flow topology) and its hosted web-surface half (which enters
/// `FlowApplication`).
///
/// `http_ingress` creates it, the DSL fills it during source-stage
/// materialization, and `FlowApplication` reads it during web-surface wiring.
/// Cloning shares the same write-once cell, so a fill through the source half is
/// visible to the surface half.
#[derive(Clone)]
pub struct HostedIngressBindingSlot {
    ingress_key: String,
    cell: Arc<OnceLock<FilledHostedIngress>>,
}

impl HostedIngressBindingSlot {
    pub fn new(ingress_key: impl Into<String>) -> Self {
        Self {
            ingress_key: ingress_key.into(),
            cell: Arc::new(OnceLock::new()),
        }
    }

    /// The protocol-neutral key identifying this hosted ingress surface.
    pub fn ingress_key(&self) -> &str {
        &self.ingress_key
    }

    /// Fill the slot once. Returns `Err` if it was already filled, which the
    /// DSL surfaces as a build error for a double-bound listener surface.
    pub fn fill(&self, filled: FilledHostedIngress) -> Result<(), HostedIngressAlreadyBound> {
        self.cell.set(filled).map_err(|_| HostedIngressAlreadyBound)
    }

    /// The filled binding, or `None` if the source half was never placed in
    /// flow topology (a startup error for a registered hosted surface).
    pub fn filled(&self) -> Option<&FilledHostedIngress> {
        self.cell.get()
    }

    /// Whether the slot has been filled by source-stage materialization.
    pub fn is_filled(&self) -> bool {
        self.cell.get().is_some()
    }
}
