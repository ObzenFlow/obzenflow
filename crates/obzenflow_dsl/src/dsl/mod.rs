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
pub use error::FlowBuildError;
pub use flow_definition::FlowDefinition;
