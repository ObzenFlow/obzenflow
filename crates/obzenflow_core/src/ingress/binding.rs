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
use super::timing::IngressTiming;
use super::IngressKey;
use crate::{StageId, StageKey};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    pub stage_key: StageKey,
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
    ingress_key: IngressKey,
    cell: Arc<OnceLock<FilledHostedIngress>>,
    /// FLOWIP-120n F12: whether the linked source admits live input. True
    /// outside replay; held false from bind until the resume handoff, and for
    /// the whole of a strict replay.
    resume_live: Arc<AtomicBool>,
    /// FLOWIP-084l: the configured timing, installed once at hosting.
    timing: Arc<OnceLock<IngressTiming>>,
    /// FLOWIP-084l: identity-index entries, published by the source's writer.
    idempotency_entries: Arc<AtomicU64>,
}

impl HostedIngressBindingSlot {
    pub fn new(ingress_key: impl Into<IngressKey>) -> Self {
        Self {
            ingress_key: ingress_key.into(),
            cell: Arc::new(OnceLock::new()),
            resume_live: Arc::new(AtomicBool::new(true)),
            timing: Arc::new(OnceLock::new()),
            idempotency_entries: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The protocol-neutral key identifying this hosted ingress surface.
    pub fn ingress_key(&self) -> &IngressKey {
        &self.ingress_key
    }

    /// Whether two handles share the same write-once binding cell.
    ///
    /// This is an in-memory ownership comparison for flow construction, not a
    /// durable identifier. Independently created slots remain distinct even
    /// when their ingress keys match.
    pub fn same_binding(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cell, &other.cell)
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

    /// FLOWIP-120n F12: hold ingress admission until the linked source goes
    /// live. Applied at DSL bind time under `--resume-from` and
    /// `--replay-from`; a strict replay never goes live (FLOWIP-084l).
    pub fn hold_until_live(&self) {
        self.resume_live.store(false, Ordering::Release);
    }

    /// FLOWIP-120n F12: mark the linked source live. The source supervisor
    /// calls this at the catch-up handoff.
    pub fn mark_resume_live(&self) {
        self.resume_live.store(true, Ordering::Release);
    }

    /// Whether the linked source admits live input (always true outside
    /// replay).
    pub fn is_resume_live(&self) -> bool {
        self.resume_live.load(Ordering::Acquire)
    }

    /// Install the configured timing once; a second install returns it back.
    pub fn install_timing(&self, timing: IngressTiming) -> Result<(), IngressTiming> {
        self.timing.set(timing)
    }

    /// The installed timing, or the defaults when hosting installed none.
    pub fn timing(&self) -> IngressTiming {
        self.timing.get().copied().unwrap_or_default()
    }

    /// Publish the source's current identity-index size.
    pub fn publish_idempotency_entries(&self, entries: u64) {
        self.idempotency_entries.store(entries, Ordering::Relaxed);
    }

    pub fn idempotency_entries(&self) -> u64 {
        self.idempotency_entries.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn binding_identity_follows_shared_cell_instead_of_ingress_key() {
        let slot = HostedIngressBindingSlot::new("orders");
        assert!(slot.same_binding(&slot.clone()));
        assert!(!slot.same_binding(&HostedIngressBindingSlot::new("orders")));
    }

    #[test]
    fn resume_live_defaults_true_and_flag_is_shared_across_clones() {
        let slot = HostedIngressBindingSlot::new("orders");
        let surface_half = slot.clone();
        assert!(surface_half.is_resume_live());

        slot.hold_until_live();
        assert!(!surface_half.is_resume_live());

        slot.mark_resume_live();
        assert!(surface_half.is_resume_live());
    }
}
