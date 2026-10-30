// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Narrow support for exported macro expansion. Not an authoring API.

pub use crate::dsl::ai_stage::{
    assert_ai_arrow, assert_ai_group_size, assert_ai_input, assert_ai_reduce_trigger,
};
pub use obzenflow_adapters::middleware::{capture_middleware, MiddlewareFactory};
pub use obzenflow_core::assert_distinct_stage_fact_set;
pub use obzenflow_core::stage_fact_set;
pub use obzenflow_core::StageFactSet;
pub use obzenflow_runtime::effects::assert_handler_performs_no_ai_effect;
pub use obzenflow_runtime::effects::assert_stateful_handler_performs_no_ai_effect;
pub use obzenflow_runtime::effects::effectful_stateful_effects;
pub use obzenflow_runtime::effects::effectful_transform_effects;
pub use obzenflow_runtime::effects::EffectAcknowledgement;
pub use obzenflow_runtime::run_context::FlowBuildContext;
pub use obzenflow_runtime::stages::sink::SetSinkRedeliverySafety;
pub use obzenflow_runtime::stages::EffectfulStatefulHandler;
pub use obzenflow_runtime::stages::EffectfulTransformHandler;
pub use obzenflow_topology::EdgeKind;

// Semantic authored site retained by cross-crate middleware lowering.
pub use obzenflow_adapters::middleware::MiddlewareAttachmentSite;
