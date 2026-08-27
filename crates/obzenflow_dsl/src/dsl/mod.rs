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
//! pass only the binding name. The one exception is a config-selected sink's
//! syntax-only `handler_set!` operand, documented below; it is consumed by
//! `sink!` and cannot exist as a handler value on its own.
//!
//! ```ignore
//! FlowDefinition::materialize(move |_runtime_config| {
//!     let input = sources::finite(events);
//!     let transform = MyTransform::new(options);
//!     let output = SinkTyped::new(|event: Output| async move {
//!         println!("{event:?}");
//!     });
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
//! ## FLOWIP-132a: effect capabilities follow the type transformation
//!
//! A pure effectful handler omits the capability clause: `In -> Out`. A
//! singleton declaration uses `In -> Out uses Effect`; braces are reserved for
//! unordered sets of at least two effects. Detached `effects: [...]`, empty
//! sets, braced singletons, and arrow-embedded effect rows are rejected.
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
//! #[derive(Clone, Debug)]
//! struct Handler;
//!
//! #[async_trait]
//! impl EffectfulTransformHandler for Handler {
//!     type Input = In;
//!     type Output = Out;
//!     type AllowedEffects = obzenflow_runtime::effect_set![];
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
//! // Pure signature: no `uses` clause.
//! let _ = effectful_transform!(In -> Out => Handler, observers: []);
//! ```
//!
//! ## FLOWIP-120c H7: per-effect policies attach inside `uses`
//!
//! A policy attaches to the exact effect it guards (`Effect with policy`).
//!
//! ## FLOWIP-115s: the canonical `sink!` grammar
//!
//! Control policies use handler-adjacent `with [...]`; passive middleware uses
//! the named `observers: [...]` clause.
//!
//! The clause order is `with [...]`, then `delivery:`, then `observers:`.
//!
//! The `delivery:` clause accepts only `idempotent` or `non_idempotent`.
//!
//! Connector descriptions and a site-level `delivery:` classification compose,
//! but the connector's typed input remains authoritative for the arrow.
//!
//! A sink may instead consume one compile-time-closed set of heterogeneous,
//! cold sink bindings. Each binding identifier is also its exact configuration
//! key. The framework resolves `sinks.handler` for the logical sink stage,
//! lowers only that binding, and independently checks every branch against the
//! declared input before descriptor erasure:
//!
//! ```ignore
//! let console_sink = sinks::console(render);
//! let postgres_sink = sinks::postgres(postgres_config);
//! flow! {
//!     stages: {
//!         output = sink!(
//!             Out => handler_set!(console_sink, postgres_sink),
//!             delivery: idempotent,
//!         )?;
//!     },
//!     // topology
//! }
//! ```
//!
//! For a stage bound as `output`, the file address is
//! `[sinks.stages.output] handler = "postgres_sink"`; the canonical
//! Twelve-Factor environment spelling is
//! `OBZENFLOW_SINKS_STAGES_OUTPUT_HANDLER=postgres_sink`. Selection is consumed
//! before topology admission and introduces no registry, common handler trait,
//! stored selector, or lifecycle authority. The ordinary
//! `sink!(Out => output)` form remains unchanged.
//!
//! This is the sink-role instance of a reusable Twelve-Factor law: deployment
//! configuration names one member of a code-closed typed integration set, and
//! that choice is consumed before the integration enters its role lifecycle.
//! Source and effect selection are not provided by this sink surface; later
//! role-local designs can preserve the law without adding a cross-role registry,
//! common selection trait, or runtime selector.
//!
//! A small named integration can implement `InlineSink` directly. It needs no
//! separate connector or description method; a site-level clause can classify
//! redelivery when archive replay matters.
//!
//! ```
//! use async_trait::async_trait;
//! use obzenflow_core::event::schema::TypedPayload;
//! use obzenflow_dsl::sink;
//! use obzenflow_runtime::stages::common::handler_error::HandlerError;
//! use obzenflow_runtime::stages::common::handlers::{
//!     InlineSink, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
//! };
//! use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! struct Out;
//! impl TypedPayload for Out {
//!     const EVENT_TYPE: &'static str = "doc.out";
//! }
//!
//! #[derive(Clone, Debug)]
//! struct Typed;
//!
//! #[async_trait]
//! impl InlineSink for Typed {
//!     type Input = Out;
//!     async fn write(
//!         &mut self,
//!         _input: Out,
//!         _ctx: SinkWriteContext,
//!     ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
//!         Ok(SinkWriteReport::terminal(
//!             SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
//!         ))
//!     }
//! }
//!
//! // The inline sink stays small; the flow row owns this deployment choice.
//! let _ = sink!(Out => Typed, delivery: idempotent);
//! ```

#[doc(hidden)]
pub mod ai_effect;
pub mod backpressure_clause;
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
#[doc(hidden)]
pub mod inference;
pub mod stage_descriptor;
mod stage_macros;
pub mod typing;

#[cfg(test)]
mod tests;

// Re-export all public items
pub use error::{FlowBuildError, StageCreationError, StageCreationResult};
pub use flow_definition::{FlowBuildFailure, FlowDefinition};
