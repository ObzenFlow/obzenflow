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
//! ```
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
//! ## FLOWIP-120g: the `effects:` clause is mandatory on effectful macros
//!
//! An effectful stage must declare its effects, even when the list is empty
//! (`effects: []`). Omitting the clause must not compile, so "no effects" stays
//! distinct from "forgot the declaration".
//!
//! ```compile_fail
//! use obzenflow_dsl::effectful_transform;
//!
//! struct In;
//! struct Out;
//! let handler = ();
//!
//! // Missing the mandatory `effects:` clause (jumping straight to `middleware:`).
//! let _ = effectful_transform!(In -> Out => handler, middleware: []);
//! ```
//!
//! ## FLOWIP-120c H7: per-effect policies attach inline in `effects:`
//!
//! A policy attaches to the exact effect it guards (`Effect with [...]`).
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
//! // `with` must be followed by a bracketed policy list.
//! let _ = effectful_transform!(In -> Out => handler, effects: [MyEffect with], middleware: []);
//! ```
//!
//! ## FLOWIP-120s: the canonical `sink!` grammar
//!
//! Positional trailing middleware is deleted; the named `middleware:` clause
//! is the only spelling.
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
//! The clause order is `delivery:` then `middleware:`; the reverse must not
//! compile.
//!
//! ```compile_fail
//! use obzenflow_dsl::sink;
//!
//! struct Out;
//! let handler = ();
//!
//! // Misordered clauses: `middleware:` before `delivery:`.
//! let _ = sink!(Out => handler, middleware: [], delivery: idempotent);
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
//! Facade helpers are constructed and bound before `sink!`. They self-declare
//! their safety, so the `delivery:` clause is rejected on the bound facade.
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
//! let output = ConsoleSink::<Out, JsonFormatter>::json();
//! // Bound facade handlers take no `delivery:` clause.
//! let _ = sink!(Out => output, delivery: idempotent);
//! ```
//!
//! A typed `Delivery` carries `SAFETY` on the type; the site adverb fails by
//! the sealed `DeclareDeliverySafety` bound.
//!
//! ```compile_fail
//! use async_trait::async_trait;
//! use obzenflow_core::event::schema::TypedPayload;
//! use obzenflow_dsl::sink;
//! use obzenflow_runtime::effects::SinkDeliverySafety;
//! use obzenflow_runtime::stages::common::handler_error::HandlerError;
//! use obzenflow_runtime::stages::common::handlers::{Delivered, Delivery};
//! use obzenflow_runtime::stages::sink::DeliveryContext;
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
//! impl Delivery for Typed {
//!     type Input = Out;
//!     const DELIVERY_TYPE: &'static str = "doc.typed";
//!     const SAFETY: SinkDeliverySafety = SinkDeliverySafety::IdempotentProjection;
//!     async fn deliver(
//!         &mut self,
//!         _input: Out,
//!         _ctx: &DeliveryContext,
//!     ) -> Result<Delivered, HandlerError> {
//!         Ok(Delivered::one())
//!     }
//! }
//!
//! // The adverb has no home on the typed tier.
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
//! let _ = effectful_sink!(Out => Handler, effects: [], middleware: []);
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
