// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![doc = include_str!("../README.md")]
//!
//! # Anatomy of a FlowApplication
//!
//! Every ObzenFlow application follows the same shape: define domain types,
//! implement handlers, wire them together with the `flow!` macro, and launch
//! with `FlowApplication::run()`.
//!
//! ## 1. Domain types
//!
//! Define your events as Rust structs and implement [`obzenflow_core::TypedPayload`]
//! so the framework knows the event type string and schema version at compile time.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use obzenflow_core::TypedPayload;
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct TemperatureReading {
//!     sensor_id: String,
//!     celsius: f64,
//! }
//!
//! impl TypedPayload for TemperatureReading {
//!     const EVENT_TYPE: &'static str = "sensor.temperature";
//!     const SCHEMA_VERSION: u32 = 1;
//! }
//! ```
//!
//! ## 2. Handlers
//!
//! Handlers contain the processing logic for each stage. The framework provides
//! several handler traits, each matching a different stage role.
//!
//! **Sources** produce events. [`obzenflow_runtime::stages::source::FiniteSourceTyped`]
//! is the easiest way to emit a `Vec<T>` of typed payloads:
//!
//! ```rust,ignore
//! use obzenflow_runtime::stages::source::FiniteSourceTyped;
//!
//! let readings = vec![
//!     TemperatureReading { sensor_id: "A1".into(), celsius: 22.5 },
//!     TemperatureReading { sensor_id: "B2".into(), celsius: 35.1 },
//! ];
//! let source = FiniteSourceTyped::new(readings);
//! ```
//!
//! **Transforms** process events one at a time. Implement
//! [`obzenflow_runtime::stages::TransformHandler`] for full control, or use
//! the typed helpers like `MapTyped` for simple one-to-one mappings.
//!
//! **Sinks** consume events at the end of a pipeline. Implement
//! [`obzenflow_runtime::stages::SinkHandler`], or use a closure with the
//! `sink!` macro for quick prototyping.
//!
//! ## 3. The `flow!` block
//!
//! The [`obzenflow_dsl::flow!`] macro takes five sections:
//!
//! ### `name:`
//! A string identifier for the flow. Used for journal directory naming and
//! metrics labelling.
//!
//! ### `journals:`
//! An expression that returns a per-flow journal factory.
//! - `disk_journals(path)` produces durable, file-backed journals (production).
//! - `memory_journals()` produces in-memory journals (tests and benchmarks).
//!
//! ### `middleware:`
//! Flow-level middleware factories applied to every stage by default.
//! Individual stages can override by supplying the same middleware type at the
//! stage level.
//! - `rate_limit(events_per_sec)` applies token-bucket rate limiting.
//!
//! ### `stages:`
//! Let-bindings that produce stage descriptors via macros:
//! - `source!("name" => handler)` for a finite source.
//! - `async_source!("name" => handler)` for an async finite source.
//! - `infinite_source!("name" => handler)` for an infinite source.
//! - `async_infinite_source!("name" => handler)` for an async infinite source.
//! - `transform!("name" => handler)` for a synchronous transform.
//! - `async_transform!("name" => handler)` for an async transform.
//! - `sink!("name" => handler)` for a sink (struct or closure).
//! - `stateful!("name" => handler)` for stateful aggregation.
//! - `join!("name" => with_ref!(ref_stage, handler))` for joining with
//!   reference data.
//!
//! All stage macros accept an optional middleware array:
//! `source!("name" => handler, [rate_limit(10.0)])`.
//!
//! ### `topology:`
//! Edges connecting stages:
//! - `a |> b;` declares a forward edge (a feeds into b).
//! - `a <| b;` declares a backward/feedback edge.
//! - `(reference, stream) |> joiner;` wires both inputs into a join stage.
//!
//! ## 4. `FlowApplication::run()`
//!
//! [`obzenflow_infra::application::FlowApplication`] handles runtime setup,
//! optional HTTP server, CLI argument parsing, Prometheus metrics, and graceful
//! shutdown. Pass the `flow! { ... }` block directly to `run()`:
//!
//! ```rust,ignore
//! FlowApplication::run(flow! { ... }).await?;
//! ```
//!
//! Or use the builder for finer control:
//!
//! ```rust,ignore
//! FlowApplication::builder()
//!     .with_log_level(LogLevel::Info)
//!     .run_async(flow! { ... })
//!     .await?;
//! ```
//!
//! ## End-to-end example
//!
//! ```rust,ignore
//! use anyhow::Result;
//! use obzenflow_core::TypedPayload;
//! use obzenflow_dsl::{flow, sink, source, transform};
//! use obzenflow_infra::application::FlowApplication;
//! use obzenflow_infra::journal::disk_journals;
//! use obzenflow_runtime::stages::transform::MapTyped;
//! use obzenflow_runtime::stages::source::FiniteSourceTyped;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Measurement {
//!     sensor: String,
//!     celsius: f64,
//! }
//! impl TypedPayload for Measurement {
//!     const EVENT_TYPE: &'static str = "sensor.measurement";
//!     const SCHEMA_VERSION: u32 = 1;
//! }
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Alert {
//!     sensor: String,
//!     message: String,
//! }
//! impl TypedPayload for Alert {
//!     const EVENT_TYPE: &'static str = "sensor.alert";
//!     const SCHEMA_VERSION: u32 = 1;
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let readings = vec![
//!         Measurement { sensor: "A1".into(), celsius: 22.5 },
//!         Measurement { sensor: "B2".into(), celsius: 85.0 },
//!         Measurement { sensor: "C3".into(), celsius: 42.1 },
//!     ];
//!
//!     FlowApplication::run(flow! {
//!         name: "temp_alerts",
//!         journals: disk_journals("target/temp-alerts-logs".into()),
//!         middleware: [],
//!
//!         stages: {
//!             src = source!("readings" => FiniteSourceTyped::new(readings));
//!             check = transform!("threshold_check" => MapTyped::new(|m: Measurement| {
//!                 Alert {
//!                     sensor: m.sensor.clone(),
//!                     message: if m.celsius > 50.0 {
//!                         format!("{}: HIGH {:.1}C", m.sensor, m.celsius)
//!                     } else {
//!                         format!("{}: normal {:.1}C", m.sensor, m.celsius)
//!                     },
//!                 }
//!             }));
//!             out = sink!("alerts" => |alert: Alert| {
//!                 println!("[ALERT] {}", alert.message);
//!             });
//!         },
//!
//!         topology: {
//!             src |> check;
//!             check |> out;
//!         }
//!     })
//!     .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Crate organisation
//!
//! This facade crate re-exports common types from the internal crates so that
//! simple applications only need `obzenflow` in their `[dependencies]`. The
//! internal crates provide the full implementation:
//!
//! - [`obzenflow_core`] defines the business domain (events, journals,
//!   contracts, typed IDs).
//! - [`obzenflow_runtime`] contains the execution engine (stage supervisors,
//!   pipeline orchestration, metrics).
//! - [`obzenflow_adapters`] provides middleware, concrete sources/sinks, and
//!   monitoring exporters.
//! - [`obzenflow_dsl`] implements the `flow!` macro and stage descriptor
//!   macros.
//! - [`obzenflow_infra`] houses `FlowApplication`, journal backends, and the
//!   optional web server.

pub mod ai;
pub mod sinks;
pub mod sources;
