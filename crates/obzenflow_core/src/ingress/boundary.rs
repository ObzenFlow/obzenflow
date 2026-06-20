// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: the core-owned neutral ingress admission port.
//!
//! Listener ingress is the moment outside work arrives at a hosted edge, before
//! it enters the internal source buffer. This port is the fail-fast admission
//! contract the hosted HTTP endpoints (`obzenflow_infra`) call and the middleware
//! adapters (`obzenflow_adapters`) implement. It lives in core so infra can call
//! it without depending on adapter middleware concepts, and adapters can
//! implement it without infra depending on them (the FLOWIP-115b onion-ownership
//! rule: putting it in infra creates a dependency cycle, putting it in adapters
//! makes hosted web endpoints depend on adapter middleware).

use super::admission::{IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext};

/// The neutral, core-owned ingress admission port.
///
/// Infra (hosted HTTP endpoints) calls it; adapters implement it (composing
/// their policy onion behind it). `on_ingress` is fail-fast and never blocks the
/// listener; `observe` records the admission outcome for the forward-fail-fast
/// chain. Durable reject/shed evidence is owned by the hosting layer's evidence
/// recorder, not this port.
pub trait IngressBoundaryMiddleware: Send + Sync {
    fn label(&self) -> &'static str;

    /// Run admission before an accepted submission enters the internal source
    /// buffer. Fail-fast: it must not wait for a token while holding the request.
    fn on_ingress(&self, attempt: &IngressAttemptContext) -> IngressAdmissionDecision;

    /// Observe the terminal admission outcome for one attempt.
    fn observe(&self, attempt: &IngressAttemptContext, outcome: IngressAdmissionOutcome);
}
