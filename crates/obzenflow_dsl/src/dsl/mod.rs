// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! DSL module for ObzenFlow
//!
//! This module contains the flow! macro and related DSL components
//! that provide the high-level API for building ObzenFlow pipelines.
//!
//! ## Handler references and construction
//!
//! Direct stage and AI-role slots take a reference name, unit value path, or
//! qualified value path. They do not take calls, builder chains, closures, or
//! struct literals. Construct builder-owned handlers as ordinary Rust inside
//! the flow's deferred materialiser, immediately above the inner `flow!`, then
//! pass only the binding name.
//!
//! ```ignore
//! FlowDefinition::materialize(move |_runtime_config| {
//!     let input = sources::ValuesSource::new(events);
//!     let transform = MyTransform::new(options);
//!     let output = sinks::ConsoleSink::new(sinks::DebugFormatter);
//!
//!     Ok(flow! {
//!         stages: {
//!             input = source!(Input => input);
//!             transformed = transform!(Input -> Output => transform);
//!             output = sink!(Output => output);
//!         },
//!         topology: {
//!             input |> transformed;
//!             transformed |> output;
//!         }
//!     })
//! })
//! ```
//!
//! Both `placeholder!()` and `placeholder!("reason")` remain valid sketch
//! markers. Async-source poll timeout is handler configuration exposed through
//! `poll_timeout()`; timeout tuples are no longer stage syntax. Exported
//! `__obzenflow_*` macros are doc-hidden cross-crate lowering machinery and are
//! unsupported for direct author calls.
//!
//! ## Effects come from the command's ports
//!
//! A command's ports allow the effects its stage performs. A port allows every
//! effect it declares, or only the requests a `#[port(...)]` field lists, and
//! the row never restates them. A command with no port performs no effects and
//! is written `In -> Out => handler`. Each port method's `#[effect]` attribute
//! declares its guarantee.
//!
//! ```
//! use async_trait::async_trait;
//! use obzenflow_core::TypedPayload;
//! use obzenflow_dsl::effectful_transform;
//! use obzenflow_runtime::effects::{Effects, StageCompletion};
//! use obzenflow_runtime::stages::common::{
//!     handlers::EffectfulTransformHandler,
//!     HandlerError,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! struct In;
//! impl TypedPayload for In {
//!     const EVENT_TYPE: &'static str = "docs.effectful-transform.input";
//! }
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! struct Out;
//! impl TypedPayload for Out {
//!     const EVENT_TYPE: &'static str = "docs.effectful-transform.output";
//! }
//!
//! #[obzenflow_runtime::effects::command]
//! #[derive(Clone, Debug)]
//! struct Handler;
//!
//! #[async_trait]
//! impl EffectfulTransformHandler for Handler {
//!     type Input = In;
//!     type Output = Out;
//!
//!     async fn process(
//!         &self,
//!         _input: Self::Input,
//!         _fx: &mut Effects<Self::Output, Self::AllowedEffects>,
//!     ) -> Result<StageCompletion<Self::Output>, HandlerError> {
//!         unimplemented!()
//!     }
//! }
//!
//! let _ = effectful_transform!(In -> Out => Handler);
//! ```
//!
//! ## Middleware attachments
//!
//! Controls and passive observers attach with `with middleware` or a flat,
//! non-empty `with { first, second }` group, written after the stage's types
//! and before `=>`. Typed context determines which operation a control
//! protects and which declared points an observer sees. On a stage that
//! performs several effects, middleware built in a `let` with
//! `.for_effect::<E>()` guards or watches only `E`, and middleware that names
//! no effect covers the stage's implementation.
//!
//! ## Sinks are effects
//!
//! A sink row takes a command that builds one request per input: `perform(...)`
//! with its port, a ready-made helper such as `ConsoleSink`, or a struct
//! implementing `SinkRole`. The stage sends each request through the command's
//! port as an effect call, and the port method's `#[effect]` states its
//! guarantee, so the row takes no decoration. A flow chooses a sink's system
//! as it chooses any effect's, by building its port with `Port::to` or
//! `Port::deferred`.

pub mod ai_stage;
mod attachment_plan;
mod backpressure_edges;
mod binder;
pub mod composites;
/// FLOWIP-128a composite substrate. Public for macro reachability only;
/// not a stability surface (D9). A plugin FLOWIP de-hides it deliberately.
#[doc(hidden)]
pub mod composition;
#[path = "dsl.rs"]
mod dsl_impl;
pub mod error;
/// Ordinary flow materialisation. Public only because exported authoring
/// macros must reach it when expanded in downstream crates.
#[doc(hidden)]
pub mod flow_builder;
mod flow_definition;
pub mod stage_descriptor;
mod stage_macros;
mod then_live;
#[doc(hidden)]
pub mod topology;
pub mod typing;

#[cfg(test)]
mod tests;

// Re-export all public items
pub use error::{
    DuplicateEffectOutcomeFactEventType, EdgeTypingMismatch, EffectFactNotInContract,
    FlowBuildError, MiddlewarePlanError, StageCreationError, StageCreationResult,
};
pub use flow_definition::{FlowBuildFailure, FlowDefinition};
