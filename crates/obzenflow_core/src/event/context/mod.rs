// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event context and metadata

pub mod causality_context;
pub mod composite_activation_context;
pub mod flow_context;
pub mod measurement_snapshots;
pub mod middleware_execution_scope;
pub mod processing_context;
pub mod replay_context;
pub mod runtime_observability;
pub mod runtime_provenance;
pub mod runtime_snapshot;
pub mod stage_type;

pub use composite_activation_context::CompositeActivationContext;
pub use flow_context::FlowContext;
pub use middleware_execution_scope::MiddlewareExecutionScope;
pub use processing_context::ProcessingContext;
pub use replay_context::ReplayContext;
pub use runtime_observability::{
    CircuitBreakerMeasurements, EffectCircuitBreakerContext, EffectRateLimiterContext,
    MeasurementWindow, RateLimiterMeasurements, RuntimeObservability, TimingMeasurements,
};
pub use runtime_provenance::{
    EventTypeCountContext, ExecutionAccounting, RuntimeProvenance, UpstreamEventTypeCountContext,
};
pub use runtime_snapshot::{ExecutionProgress, RuntimeSnapshot};
pub use stage_type::{SimpleStageType, StageType};
