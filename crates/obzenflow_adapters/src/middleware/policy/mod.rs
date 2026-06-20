// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control-policy authoring contracts for live I/O boundaries.
//!
//! These traits are for middleware that can admit, wait, reject, shed, or
//! synthesize around a protected runtime unit. They are intentionally separate
//! from the generic [`Middleware`](crate::middleware::Middleware) trait, which
//! remains the handler-chain observation/structural authoring surface. Built-in
//! control middleware lives under [`control`](crate::middleware::control); this
//! module is the public contract that built-ins and third-party control
//! middleware implement.
//!
//! Ingress is asymmetric because infra hosts the call site. Its neutral
//! boundary port lives in [`obzenflow_core::ingress`], while adapter factories
//! still materialize ingress control middleware through the same carrier model.

pub mod effect;
pub mod sink;
pub mod source;

pub use effect::{
    effect_policy_from_middleware, EffectAttemptOutcome, EffectPolicy, EffectPolicyAttachment,
    EventAwareEffectPolicy, PerEffectPolicyBoundary, PolicyAdmission,
};
pub use sink::{
    PerSinkDeliveryPolicyBoundary, SinkAdmission, SinkAdmissionGuard, SinkDeliveryPolicyOutcome,
    SinkPolicy, SinkPolicyCtx,
};
pub use source::{
    PerSourcePolicyBoundary, SourceAdmission, SourceAdmissionGuard, SourceAfterPoll,
    SourceBatchFacts, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
