// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::boundary::IngressBoundaryMiddleware;
use super::timing::IngressTiming;
use super::IngressKey;
use crate::event::schema::TypedFact;
use crate::event::SourceDiagnostic;
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

/// `IngressFactDecoder` decodes one submitted body into the fact that its source
/// stage publishes. The hosted ingress runs the decoder once on each body, at admission.
pub trait IngressFactDecoder: Send + Sync {
    /// `event_type` is the event type of the decoded fact.
    fn event_type(&self) -> &'static str;

    /// `accepts` returns `true` if this decoder takes a submission of `event_type`.
    fn accepts(&self, event_type: &str) -> bool;

    /// `decode` returns the fact of one body, or a diagnostic that holds no submitted value.
    fn decode(&self, data: &serde_json::Value) -> Result<TypedFact, SourceDiagnostic>;
}

/// `HostedIngressAlreadyBound` is the error that [`HostedIngressBindingSlot::fill`]
/// returns for a filled slot. One hosted ingress serves one source stage.
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

/// `HostedIngressBindingSlot` is a write-once slot that the event system of a hosted
/// ingress shares with its web surface or submission handle. All clones share one cell.
#[derive(Clone)]
pub struct HostedIngressBindingSlot {
    ingress_key: IngressKey,
    cell: Arc<OnceLock<FilledHostedIngress>>,
    /// FLOWIP-084l: the configured timing, installed once at hosting.
    timing: Arc<OnceLock<IngressTiming>>,
    /// FLOWIP-084l: identity-index entries, published by the source's writer.
    idempotency_entries: Arc<AtomicU64>,
    restoring: Arc<AtomicBool>,
    decoder: Arc<OnceLock<Arc<dyn IngressFactDecoder>>>,
}

impl HostedIngressBindingSlot {
    pub fn new(ingress_key: impl Into<IngressKey>) -> Self {
        Self {
            ingress_key: ingress_key.into(),
            cell: Arc::new(OnceLock::new()),
            timing: Arc::new(OnceLock::new()),
            idempotency_entries: Arc::new(AtomicU64::new(0)),
            restoring: Arc::new(AtomicBool::new(false)),
            decoder: Arc::new(OnceLock::new()),
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

    /// `filled` returns the [`FilledHostedIngress`] of the slot, or `None` when no source
    /// stage reads the hosted ingress.
    pub fn filled(&self) -> Option<&FilledHostedIngress> {
        self.cell.get()
    }

    /// Whether the slot has been filled by source-stage materialization.
    pub fn is_filled(&self) -> bool {
        self.cell.get().is_some()
    }

    /// Install the configured timing once; a second install returns it back.
    pub fn install_timing(&self, timing: IngressTiming) -> Result<(), IngressTiming> {
        self.timing.set(timing)
    }

    /// The installed timing, or the defaults when hosting installed none.
    pub fn timing(&self) -> IngressTiming {
        self.timing.get().copied().unwrap_or_default()
    }

    /// Hold admission until the source restores its committed identities.
    pub fn set_restoring(&self, restoring: bool) {
        self.restoring.store(restoring, Ordering::Release);
    }

    pub fn is_restoring(&self) -> bool {
        self.restoring.load(Ordering::Acquire)
    }

    /// Publish the source's current identity-index size.
    pub fn publish_idempotency_entries(&self, entries: u64) {
        self.idempotency_entries.store(entries, Ordering::Relaxed);
    }

    pub fn idempotency_entries(&self) -> u64 {
        self.idempotency_entries.load(Ordering::Relaxed)
    }

    /// `install_decoder` installs the decoder of the `subscribe` line of this hosted
    /// ingress. Only the first call installs a decoder.
    pub fn install_decoder(&self, decoder: Arc<dyn IngressFactDecoder>) {
        self.decoder.get_or_init(|| decoder);
    }

    /// `decoder` returns the decoder of the `subscribe` line of this hosted ingress, or
    /// `None` before a line installs one.
    pub fn decoder(&self) -> Option<&Arc<dyn IngressFactDecoder>> {
        self.decoder.get()
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
}
