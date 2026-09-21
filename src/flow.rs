// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow definitions and stage macros.
//!
//! Construct handlers inside a deferred [`FlowDefinition`] materialiser, then
//! pass those bindings to the stage macros. `handler_set!` is syntax consumed
//! by `sink!`, not a standalone handler value.
//!
//! ## Combining different input types
//!
//! A non-join stage accepts one input type. When several sources describe the
//! same downstream concept with different Rust types, give each branch a
//! transform that produces that common type before merging the branches.
//! Use a join when one input instead supplies reference data for another.
//!
//! ```no_run
//! use obzenflow::flow::{flow, sink, source, transform, FlowDefinition};
//! use obzenflow::stages::transforms;
//! # use obzenflow::{journal::disk_journals, schema::TypedPayload};
//! # use obzenflow::stages::{sinks, sources};
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct QueueOrder { id: u64 }
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct WebOrder { order_id: u64 }
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct CsvOrder { number: u64 }
//! # #[derive(Clone, Debug, Serialize, Deserialize)] struct OrderPlaced { id: u64 }
//! # impl TypedPayload for QueueOrder { const EVENT_TYPE: &'static str = "queue.order"; }
//! # impl TypedPayload for WebOrder { const EVENT_TYPE: &'static str = "web.order"; }
//! # impl TypedPayload for CsvOrder { const EVENT_TYPE: &'static str = "csv.order"; }
//! # impl TypedPayload for OrderPlaced { const EVENT_TYPE: &'static str = "order.placed"; }
//!
//! let definition = FlowDefinition::materialize(|_| {
//! # let queue = sources::finite(Vec::<QueueOrder>::new());
//! # let web = sources::finite(Vec::<WebOrder>::new());
//! # let csv = sources::finite(Vec::<CsvOrder>::new());
//! # let output = sinks::console(|order: &OrderPlaced| order.id.to_string());
//!     let from_queue = transforms::map(|order: QueueOrder| OrderPlaced { id: order.id });
//!     let from_web = transforms::map(|order: WebOrder| OrderPlaced { id: order.order_id });
//!     let from_csv = transforms::map(|order: CsvOrder| OrderPlaced { id: order.number });
//!
//!     Ok(flow! {
//!         name: "aligned_orders",
//! #       journals: disk_journals(std::path::PathBuf::from("target/aligned-orders")),
//!         stages: {
//!             queue = source!(QueueOrder => queue);
//!             web = source!(WebOrder => web);
//!             csv = source!(CsvOrder => csv);
//!             align_queue = transform!(QueueOrder -> OrderPlaced => from_queue);
//!             align_web = transform!(WebOrder -> OrderPlaced => from_web);
//!             align_csv = transform!(CsvOrder -> OrderPlaced => from_csv);
//!             output = sink!(OrderPlaced => output);
//!         },
//!         topology: {
//!             queue |> align_queue;
//!             web |> align_web;
//!             csv |> align_csv;
//!             align_queue |> output;
//!             align_web |> output;
//!             align_csv |> output;
//!         }
//!     })
//! });
//! ```
//!
//! All three incoming edges at `output` now carry `OrderPlaced`. Each source
//! keeps its own schema, and the conversions remain visible in the topology.

pub use obzenflow_dsl::backpressure;
pub use obzenflow_dsl::{
    ai_map_reduce, async_infinite_source, async_source, effectful_stateful, effectful_transform,
    flow, handler_set, inference, infinite_source, join, placeholder, sink, source, stateful,
    transform, FlowBuildError, FlowBuildFailure, FlowDefinition, StageCreationError,
    StageCreationResult,
};
