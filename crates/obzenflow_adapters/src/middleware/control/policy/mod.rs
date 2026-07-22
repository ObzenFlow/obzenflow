// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control-policy authoring contracts for live I/O boundaries.
//!
//! These traits are for middleware that can admit, wait, reject, or shed work
//! around a protected runtime unit. They are intentionally separate
//! from the generic [`Middleware`](crate::middleware::Middleware) trait, which
//! remains the handler-chain observation/structural authoring surface. This
//! module lives under [`control`](crate::middleware::control) because policy is
//! a control-only authoring contract; built-in control middleware and
//! third-party control middleware both implement these traits.
//!
//! Ingress is asymmetric because infra hosts the call site. Its neutral
//! boundary port lives in [`obzenflow_core::ingress`], while adapter factories
//! still materialize ingress control middleware through the same carrier model.

pub mod effect;
pub mod sink;
pub mod source;

pub use effect::{
    EffectAttemptOutcome, EffectPolicy, EffectPolicyAttachment, EventAwareEffectPolicy,
    PerEffectPolicyBoundary, PolicyAdmission,
};
pub use sink::{
    PerSinkDeliveryPolicyBoundary, SinkAdmission, SinkAdmissionGuard, SinkDeliveryPolicyOutcome,
    SinkPolicy, SinkPolicyCtx,
};
pub use source::{
    PerSourcePolicyBoundary, SourceAdmission, SourceAdmissionGuard, SourceAfterPoll,
    SourceBatchFacts, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
