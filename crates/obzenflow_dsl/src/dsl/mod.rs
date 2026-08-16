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
//! Supported stage and AI-role slots take a reference name, unit value path,
//! or qualified value path. They do not take calls, builder chains, closures,
//! or struct literals. Construct builder-owned handlers as ordinary Rust inside
//! the flow's deferred materialiser, immediately above the inner `flow!`, then
//! pass only the binding name:
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
//! ```ignore
//!
//! Both `placeholder!()` and `placeholder!("reason")` remain valid sketch
//! markers. Async-source poll timeout is handler configuration exposed through
//! `poll_timeout()`; timeout tuples are no longer stage syntax. Exported
//! `__obzenflow_*` macros are doc-hidden cross-crate lowering machinery and are
//! unsupported for direct author calls.
//!
//! ## Legacy syntax (compile-fail)
//!
//! FLOWIP-105g-part-2 intentionally breaks the legacy stage-macro grammars.
//! The following forms must not compile.
//!
//! ```compile_fail
//! use obzenflow_dsl::dsl::typing::PlaceholderFiniteSource;
//! use obzenflow_dsl::source;
//!
//! struct MyEvent;
//!
//! // Legacy labelled typed syntax: `out:` + `; "name" =>`.
//! let _ = source!(out: MyEvent; "sentences" => PlaceholderFiniteSource::<MyEvent>::new(None));
//! ```
//!
//! ```compile_fail
//! use obzenflow_dsl::dsl::typing::PlaceholderFiniteSource;
//! use obzenflow_dsl::source;
//!
//! struct MyEvent;
//!
//! // Legacy quoted-name untyped syntax.
//! let _ = source!("sentences" => PlaceholderFiniteSource::<MyEvent>::new(None));
//! ```
//!
//! ```compile_fail
//! use obzenflow_dsl::dsl::typing::PlaceholderJoin;
//! use obzenflow_dsl::join;
//!
//! struct Carrier;
//! struct Order;
//! struct Enriched;
//!
//! // Legacy untyped join syntax (missing the `catalog` role).
//! let _ = join!(carriers => PlaceholderJoin::<Carrier, Order, Enriched>::new(None));
//! ```
//!
//! ```compile_fail
//! use obzenflow_dsl::dsl::typing::PlaceholderJoin;
//! use obzenflow_dsl::join;
//!
//! struct Carrier;
//! struct Order;
//! struct Enriched;
//!
//! // Provide a local `with_ref!` shim so the failure is due to the legacy join grammar,
//! // not the absence of the macro.
//! macro_rules! with_ref {
//!     ($ref_stage:ident, $handler:expr) => { $handler };
//! }
//!
//! // Legacy typed join syntax (labelled clauses + with_ref!).
//! let handler = PlaceholderJoin::<Carrier, Order, Enriched>::new(None);
//! let _ = join!(reference: Carrier, stream: Order, out: Enriched; "enricher" => with_ref!(carriers, handler));
//! ```
//!
//! ## FLOWIP-132a: effect rows live on the arrow
//!
//! A pure effectful handler omits the row: `In -> Out`. A non-empty declaration
//! uses `In ->{ Effect } Out`; `effects: [...]` and empty rows are rejected.
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
//! // Pure signature: no effect row.
//! let _ = effectful_transform!(In -> Out => Handler, observers: []);
//! ```
//!
//! ## FLOWIP-120c H7: per-effect policies attach inline in the effect row
//!
//! A policy attaches to the exact effect it guards (`Effect with policy`).
//!
//! A malformed attachment (a `with` clause without its policy list) must not
//! compile either.
//!
//! ```compile_fail
//! use obzenflow_dsl::effectful_transform;
//!
//! struct In;
//! struct Out;
//! struct MyEffect;
//! let handler = ();
//!
//! // `with` must be followed by one bare policy expression.
//! let _ = effectful_transform!(In ->{ MyEffect with } Out => handler, observers: []);
//! ```
//!
//! ## FLOWIP-115s: the canonical `sink!` grammar
//!
//! Control policies use handler-adjacent `with [...]`; passive middleware uses
//! the named `observers: [...]` clause.
//!
//! ```compile_fail
//! use obzenflow_dsl::sink;
//!
//! struct Out;
//! let handler = ();
//!
//! // Deleted positional middleware list.
//! let _ = sink!(Out => handler, []);
//! ```
//!
//! The clause order is `with [...]`, then `delivery:`, then `observers:`; the reverse must not
//! compile.
//!
//! ```compile_fail
//! use obzenflow_dsl::sink;
//!
//! struct Out;
//! let handler = ();
//!
//! // Misordered clauses: `observers:` before `delivery:`.
//! let _ = sink!(Out => handler, observers: [], delivery: idempotent);
//! ```
//!
//! The `delivery:` clause accepts only `idempotent` or `non_idempotent`.
//!
//! ```compile_fail
//! use obzenflow_dsl::sink;
//!
//! struct Out;
//! let handler = ();
//!
//! // Unknown safety token.
//! let _ = sink!(Out => handler, delivery: sometimes);
//! ```
//!
//! Connector descriptions and a site-level `delivery:` classification compose,
//! but the connector's typed input remains authoritative for the arrow.
//!
//! ```compile_fail
//! use obzenflow_adapters::sinks::{ConsoleSink, JsonFormatter};
//! use obzenflow_core::TypedPayload;
//! use obzenflow_dsl::sink;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! struct Out;
//! impl TypedPayload for Out {
//!     const EVENT_TYPE: &'static str = "doc.out";
//! }
//!
//! #[derive(Clone, Debug, Deserialize, Serialize)]
//! struct Wrong;
//! impl TypedPayload for Wrong {
//!     const EVENT_TYPE: &'static str = "doc.wrong";
//! }
//!
//! let output = ConsoleSink::<Out, JsonFormatter>::json();
//! let _ = sink!(Wrong => output, delivery: idempotent);
//! ```
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
//!     ) -> Result<SinkWriteReport, HandlerError> {
//!         Ok(SinkWriteReport::terminal(
//!             SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
//!         ))
//!     }
//! }
//!
//! // The inline sink stays small; the flow row owns this deployment choice.
//! let _ = sink!(Out => Typed, delivery: idempotent);
//! ```
//!
//! ## Retired effectful sink surface (FLOWIP-120v)
//!
//! Sinks are delivery-only; a non-idempotent external write belongs behind
//! the effect boundary as an effectful transform authoring named outcome
//! facts, consumed by a plain sink. The `effectful_sink!` macro is removed
//! and must not return.
//!
//! ```compile_fail
//! use obzenflow_dsl::effectful_sink;
//!
//! struct Out;
//! struct Handler;
//!
//! let _ = effectful_sink!(Out => Handler, effects: [], observers: []);
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
