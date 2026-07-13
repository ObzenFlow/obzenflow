// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::SinkPolicyCtx;
use crate::middleware::BoundaryRetryOwner;
use async_trait::async_trait;
use obzenflow_runtime::stages::common::handlers::SinkConsumeReport;

/// RAII guard returned by sink-policy admission for a reserved resource (such as
/// a half-open probe slot), held by the boundary across the delivery attempt.
pub trait SinkAdmissionGuard: Send + Sync {}

impl<T: Send + Sync> SinkAdmissionGuard for T {}

/// Admission decision from one sink-delivery policy.
pub enum SinkAdmission {
    /// Admit the delivery, optionally holding a guard across the attempt.
    Admit(Option<Box<dyn SinkAdmissionGuard>>),
    /// Reject before delivery. The supervisor maps this to a failed delivery
    /// receipt, never a successful `Noop`.
    Reject { reason: String },
}

/// Raw sink-delivery outcome shown independently to each admitted policy.
pub enum SinkDeliveryPolicyOutcome<'a> {
    /// The handler ran and returned a consume report.
    Delivered { report: &'a SinkConsumeReport },
    /// The handler errored or panicked. Typed health-classification facts are
    /// retained separately in the policy context, leaving retry eligibility
    /// to the boundary coordinator.
    Failed,
    /// A later policy rejected before delivery; the protected call never went out.
    RejectedBy {
        policy: &'static str,
        reason: &'a str,
    },
}

/// A sink-delivery resilience policy behind the adapter-owned boundary.
///
/// The boundary owns typed delivery identity and attempt facts; policies see
/// only the control outcome facts they actually consume. Future sink policies
/// may extend this contract with a same-slice reader and proof.
#[async_trait]
pub trait SinkPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission;

    /// Commit any execution-based reservations immediately before the sink
    /// delivery executor starts. Default policies reserve no such resource.
    fn commit_execution(&self, _ctx: &mut SinkPolicyCtx) {}

    fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx);

    #[doc(hidden)]
    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        None
    }

    #[doc(hidden)]
    fn recovery_allowed_after_settlement(&self, _ctx: &SinkPolicyCtx) -> bool {
        true
    }
}
