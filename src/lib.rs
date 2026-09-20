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
//! Define your events as Rust structs and implement [`crate::schema::TypedPayload`]
//! so the framework knows the event type string and schema version at compile time.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use obzenflow::schema::TypedPayload;
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
//! **Sources** produce events. [`crate::stages::sources::finite`] is the easiest way
//! to emit a `Vec<T>` (or any iterator) of typed payloads:
//!
//! ```rust,ignore
//! use obzenflow::stages::sources;
//!
//! let readings = vec![
//!     TemperatureReading { sensor_id: "A1".into(), celsius: 22.5 },
//!     TemperatureReading { sensor_id: "B2".into(), celsius: 35.1 },
//! ];
//! let source = sources::finite(readings);
//! ```
//!
//! **Transforms** process typed payloads one at a time. Implement
//! [`crate::stages::transforms::TypedTransformHandler`], or use helper facades
//! like [`crate::stages::transforms::map`] for simple one-to-one mappings.
//!
//! **Sinks** consume events at the end of a pipeline. Implement
//! [`crate::stages::sinks::SinkWriter`], or construct a
//! [`crate::stages::sinks::SinkTyped`] adapter from a closure inside
//! the deferred materialiser and pass its binding to `sink!`.
//!
//! ## 3. The `flow!` block
//!
//! The [`crate::dsl::flow!`] macro takes four sections:
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
//! ### `stages:`
//! Let-bindings that produce stage descriptors via macros:
//! - `source!(Out => handler)` for a finite source.
//! - `async_source!(Out => handler)` for an async finite source.
//! - `infinite_source!(Out => handler)` for an infinite source.
//! - `async_infinite_source!(Out => handler)` for an async infinite source.
//! - `transform!(In -> Out => handler)` for a synchronous transform.
//! - `effectful_transform!(In -> Out uses ... => handler, observers: [])`
//!   for a transform that performs declared external work.
//! - `sink!(In => handler)` for a sink.
//! - `stateful!(In -> Out => handler)` for stateful aggregation.
//! - `join!(catalog ref_stage: Ref, Stream -> Out => handler)` for joining with
//!   reference data.
//!
//! Control middleware attaches with the live I/O unit it protects, for example
//! `source!(Out => handler with [rate_limit(10.0)])`. Passive middleware uses
//! the named `observers: [...]` lane on every stage macro.
//!
//! ### `topology:`
//! Edges connecting stages:
//! - `a |> b;` declares a forward edge (a feeds into b).
//! - `a <| b;` declares a backward/feedback edge.
//! - `(reference, stream) |> joiner;` wires both inputs into a join stage.
//!
//! ## 4. `FlowApplication::run()`
//!
//! [`crate::application::FlowApplication`] handles runtime setup,
//! optional HTTP server, CLI argument parsing, Prometheus metrics, and graceful
//! shutdown. Pass a deferred [`crate::dsl::FlowDefinition`] to `run()`:
//!
//! ```rust,ignore
//! FlowApplication::run(build_flow()).await?;
//! ```
//!
//! Or use the builder for finer control:
//!
//! ```rust,ignore
//! FlowApplication::builder()
//!     .with_log_level(LogLevel::Info)
//!     .run_async(build_flow())
//!     .await?;
//! ```
//!
//! ## End-to-end example
//!
//! ```no_run
//! use anyhow::Result;
//! use obzenflow::schema::TypedPayload;
//! use obzenflow::stages::{sources, transforms};
//! use obzenflow::dsl::{flow, sink, source, transform, FlowDefinition};
//! use obzenflow::application::FlowApplication;
//! use obzenflow::journal::disk_journals;
//! use obzenflow::stages::sinks::SinkTyped;
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
//!     let flow = FlowDefinition::materialize(move |_runtime_config| {
//!         let readings_source = sources::finite(readings);
//!         let check_temperature = transforms::map(|m: Measurement| {
//!                 Alert {
//!                     sensor: m.sensor.clone(),
//!                     message: if m.celsius > 50.0 {
//!                         format!("{}: HIGH {:.1}C", m.sensor, m.celsius)
//!                     } else {
//!                         format!("{}: normal {:.1}C", m.sensor, m.celsius)
//!                     },
//!                 }
//!             });
//!         let print_alert = SinkTyped::new(|alert: Alert| async move {
//!             println!("[ALERT] {}", alert.message);
//!         });
//!
//!         Ok(flow! {
//!             name: "temp_alerts",
//!             journals: disk_journals("target/temp-alerts-logs".into()),
//!
//!             stages: {
//!                 src = source!(Measurement => readings_source);
//!                 check = transform!(Measurement -> Alert => check_temperature);
//!                 out = sink!(Alert => print_alert);
//!             },
//!
//!             topology: {
//!                 src |> check;
//!                 check |> out;
//!             }
//!         })
//!     });
//!
//!     FlowApplication::run(flow).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Capability modules
//!
//! Applications use one ObzenFlow dependency. The facade preserves the original
//! types and their owner implementations:
//!
//! - [`prelude`] supplies the common flow, macro, error, and schema vocabulary.
//! - [`schema`] defines typed payloads and fact carriers.
//! - [`stages`] groups sources, transforms, stateful handlers, joins, and sinks.
//! - [`effects`] describes replay-safe external operations.
//! - [`middleware`] provides live-I/O policies and passive observers.
//! - [`application`] configures and runs the application, including ingress.
//! - [`journal`] constructs, inspects, and exports recorded runs.
//! - [`ai`] provides inference, model bindings, and budget planning.
//! - [`env`] reads typed application environment values.

pub mod ai;
pub mod application;
pub mod dsl;
pub mod effects;
pub mod env;
pub mod error;
pub mod journal;
pub mod middleware;
pub mod prelude;
pub mod schema;
pub mod stages;

#[cfg(feature = "test-support")]
pub mod testing;
