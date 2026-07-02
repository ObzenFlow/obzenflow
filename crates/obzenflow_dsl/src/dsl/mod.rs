// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! DSL module for ObzenFlow
//!
//! This module contains the flow! macro and related DSL components
//! that provide the high-level API for building ObzenFlow pipelines.
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
//! The `output_middleware:` lane is retired from the macro surface; a policy
//! attaches to the effect it guards (`Effect with [...]`). The lane form must
//! not compile.
//!
//! ```compile_fail
//! use obzenflow_dsl::effectful_transform;
//!
//! struct In;
//! struct Out;
//! struct MyEffect;
//! let handler = ();
//!
//! // Retired lane: policies attach per effect, `MyEffect with [...]`.
//! let _ = effectful_transform!(In -> Out => handler, effects: [MyEffect], output_middleware: [], middleware: []);
//! ```
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

mod binder;
pub mod composites;
#[path = "dsl.rs"]
mod dsl_impl;
pub mod error;
mod flow_definition;
pub mod stage_descriptor;
mod stage_macros;
pub mod typing;

#[cfg(test)]
mod tests;

// Re-export all public items
pub use error::{FlowBuildError, StageCreationError, StageCreationResult};
pub use flow_definition::{FlowBuildFailure, FlowDefinition};
