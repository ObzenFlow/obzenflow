// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Narrow support for exported macro expansion. Not an authoring API.

pub use obzenflow_adapters::ai::ChatBindingMetadata;
pub use obzenflow_adapters::ai::ChatCompletion;
pub use obzenflow_adapters::middleware::{capture_middleware, MiddlewareFactory};
pub use obzenflow_core::ai::OversizeExhaustion;
pub use obzenflow_core::ai::OversizePolicy;
pub use obzenflow_core::ai::TokenCount;
pub use obzenflow_core::assert_distinct_stage_fact_set;
pub use obzenflow_core::stage_fact_set;
pub use obzenflow_core::StageFactSet;
pub use obzenflow_runtime::effect_set;
pub use obzenflow_runtime::effects::declare_named_at_least_once_effect;
pub use obzenflow_runtime::effects::effectful_stateful_effects;
pub use obzenflow_runtime::effects::effectful_transform_effects;
pub use obzenflow_runtime::effects::Effect;
pub use obzenflow_runtime::effects::EffectAcknowledgement;
pub use obzenflow_runtime::effects::EffectBinding;
pub use obzenflow_runtime::effects::EffectDeclaration;
pub use obzenflow_runtime::run_context::FlowBuildContext;
pub use obzenflow_runtime::stages::sink::SetSinkRedeliverySafety;
pub use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;
pub use obzenflow_runtime::stages::EffectfulStatefulHandler;
pub use obzenflow_runtime::stages::EffectfulTransformHandler;
pub use obzenflow_topology::EdgeKind;

// Semantic authored site retained by cross-crate middleware lowering.
pub use obzenflow_adapters::middleware::MiddlewareAttachmentSite;
