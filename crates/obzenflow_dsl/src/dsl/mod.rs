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
//! ## Effects come from the handler's effect set
//!
//! An effectful handler's `type AllowedEffects` declares the effects its stage
//! performs, as one request type or `effect_set![...]`, and the row never
//! restates them. A handler whose effect set is empty is written
//! `In -> Out => handler`. Each effect is answered by a system the handler
//! holds in a `#[system]` field, and an `at_least_once` effect is acknowledged
//! after the handler with `.at_least_once::<R>()`.
//!
//! ```
//! use async_trait::async_trait;
//! use obzenflow_core::TypedPayload;
//! use obzenflow_dsl::effectful_transform;
//! use obzenflow_runtime::effects::{EffectHandler, Effects, StageCompletion};
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
//! #[derive(Clone, Debug, EffectHandler)]
//! # #[effect_handler(effects = obzenflow_runtime::effects)]
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
//! A sink classifies its redelivery after its handler with `.idempotent()` or
//! `.non_idempotent()`. Connector descriptions and that classification
//! compose, but the connector's typed input remains authoritative for the
//! arrow.
//!
//! A sink may instead consume one compile-time-closed set of heterogeneous,
//! cold sink bindings. Each binding identifier is also its exact configuration
//! key. The framework resolves `sinks.handler` for the logical sink stage,
//! lowers only that binding, and independently checks every branch against the
//! declared input before descriptor erasure:
//!
//! ```ignore
//! let console_sink = sinks::ConsoleSink::new(render);
//! let postgres_sink = PostgresSink::builder(binder)
//!     .connection(connection)
//!     .insert_into(schema, table, insert_body)?
//!     .build()?;
//! flow! {
//!     stages: {
//!         output = sink!(
//!             Out => handler_set!(console_sink, postgres_sink).idempotent()
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
//! separate connector. Its required description declares the delivery method,
//! and `.idempotent()` or `.non_idempotent()` after the handler classifies
//! redelivery when archive replay matters.
//!
//! ```
//! use async_trait::async_trait;
//! use obzenflow_core::event::schema::TypedPayload;
//! use obzenflow_dsl::sink;
//! use obzenflow_runtime::stages::common::handlers::{
//!     InlineSink, SinkDescription, SinkWriteFailure,
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
//!     fn describe(&self) -> SinkDescription {
//!         SinkDescription::method(DeliveryMethod::Noop)
//!     }
//!     async fn write(&mut self, _input: Out) -> Result<(), SinkWriteFailure> {
//!         Ok(())
//!     }
//! }
//!
//! // The inline sink stays small; the flow row owns this deployment choice.
//! let _ = sink!(Out => Typed.idempotent());
//! ```

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
mod resume_edges;
pub mod stage_descriptor;
mod stage_macros;
mod then_live;
#[doc(hidden)]
pub mod topology;
pub mod typing;

#[cfg(test)]
mod tests;

// Re-export all public items
pub use error::{FlowBuildError, MiddlewarePlanError, StageCreationError, StageCreationResult};
pub use flow_definition::{FlowBuildFailure, FlowDefinition};
