// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ready-made sinks, the stages where a flow's results leave it.
//!
//! A sink is an effect. Its row takes a command, as an effectful transform's
//! row does, and the command sends one request per input through a port. The
//! port method's `#[effect]` states the guarantee, so the row states none, and
//! the method is marked `records = reply`. Replay makes no call to a sink's
//! system, and a resume makes only the calls the recording didn't settle.
//!
//! ```
//! use obzenflow::effects::{perform, EffectError, Port};
//! # use obzenflow::schema::TypedPayload;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Debug, Serialize, Deserialize)]
//! # struct Order { id: String }
//! # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "orders.order"; }
//!
//! #[derive(Clone, Debug, Serialize)]
//! struct Dispatch {
//!     order_id: String,
//! }
//!
//! #[obzenflow::port]
//! trait DispatchPort {
//!     #[effect(type = "orders.dispatch", at_least_once, records = reply)]
//!     fn dispatch(&self, request: &Dispatch) -> Result<(), EffectError>;
//! }
//!
//! struct Warehouse;
//!
//! #[obzenflow::system("warehouse")]
//! impl DispatchPort for Warehouse {
//!     fn dispatch(&self, request: &Dispatch) -> Result<(), EffectError> {
//!         println!("dispatching {}", request.order_id);
//!         Ok(())
//!     }
//! }
//!
//! let dispatch = perform(|order: &Order| Dispatch { order_id: order.id.clone() })
//!     .through(Port::to(Warehouse));
//! let _stage = obzenflow::flow::sink!(Order => dispatch);
//! ```
//!
//! ## Ready-made destinations
//!
//! | Destination | Command |
//! |---|---|
//! | Console | [`ConsoleSink::new`] with a formatter |
//! | Structured diagnostics | [`TracingSink::new`] with a callback that returns a [`TracingOutput`] |
//! | Intentional discard | [`DiscardSink::new`] |
//! | CSV or TSV register | [`CsvSink::builder`] |
//!
//! Each sends its request through a port and system the framework declares,
//! and [`ConsoleSink::through`], [`TracingSink::through`] and
//! [`CsvSink::through`] send it through another system instead. TSV is the CSV
//! builder's `.tab_delimited()`.
//!
//! The console and tracing writes are `at_least_once`, so a crash between the
//! write and its record can repeat a line. The CSV register is `exactly_once`.
//! It checks its file against the recorded length and digest before its first
//! append, and moves any unrecorded tail aside to `<path>.<length>.damage`.
//!
//! ## PostgreSQL
//!
//! PostgreSQL output is an application's port, with a system that writes through
//! the framework's client in [`crate::effects::postgres`]. An insert with
//! `ON CONFLICT ... DO NOTHING` is `exactly_once`, and a plain insert is
//! `at_least_once`.

pub use obzenflow_adapters::sinks::{
    Appended, ConsoleFormatError, ConsoleOutput, ConsolePort, ConsoleSink, ConsoleTablePort,
    CsvProjection, CsvRegisterPort, CsvRow, CsvSink, CsvSinkBuilder, DebugFormatter, DiscardPort,
    DiscardSink, Discarded, Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination,
    SnapshotTableFormatter, TableConsoleSink, TableFormatter, TableRow, TracingLevel,
    TracingOutput, TracingPort, TracingSink,
};
