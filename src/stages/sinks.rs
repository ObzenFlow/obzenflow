// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Traits for application sinks, with built-in destination adapters.
//!
//! Sinks are the terminal stages of a pipeline. This module re-exports the
//! contracts for implementing application sinks and the built-in destinations.
//!
//! ## Implement an application sink
//!
//! Use [`InlineSink`] when a configured sink completes each input before
//! returning. Declare its input and destination once; `write` performs the
//! application operation and returns `Ok(())` or [`SinkWriteFailure`]. The
//! framework creates the delivery receipt, including one delivered input and
//! an unknown byte count. It owns input identity, provenance and journalling.
//!
//! ```
//! use obzenflow::stages::sinks::{
//!     DeliveryMethod, InlineSink, SinkDescription, SinkOperationError,
//!     SinkWriteFailure, SinkWritePhase,
//! };
//! # use obzenflow::schema::TypedPayload;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Debug, Serialize, Deserialize)]
//! # struct Order { id: String }
//! # impl TypedPayload for Order { const EVENT_TYPE: &'static str = "orders.order"; }
//!
//! #[derive(Clone, Debug)]
//! struct DispatchOrders {
//!     sender: tokio::sync::mpsc::Sender<Order>,
//! }
//!
//! #[async_trait::async_trait]
//! impl InlineSink for DispatchOrders {
//!     type Input = Order;
//!
//!     fn describe(&self) -> SinkDescription {
//!         SinkDescription::destination(
//!             "order_dispatch", DeliveryMethod::Custom("channel".into()),
//!         )
//!     }
//!
//!     async fn write(&mut self, order: Order) -> Result<(), SinkWriteFailure> {
//!         self.sender.send(order).await.map_err(|_| {
//!             SinkWriteFailure::current_only(
//!                 SinkWritePhase::Execute,
//!                 SinkOperationError::permanent("order dispatcher closed"),
//!             )
//!         })
//!     }
//! }
//! # let (sender, _receiver) = tokio::sync::mpsc::channel(16);
//! # let dispatch_orders = DispatchOrders { sender };
//! # let _stage = obzenflow::flow::sink!(Order => dispatch_orders);
//! ```
//!
//! Bind this sink with `sink!(Order => dispatch_orders)`. The operation must be
//! complete when it returns success. Cloning opens the inline writer, so clones
//! must isolate transient execution state; a shared destination handle is fine.
//!
//! Use [`SinkConnector`] and [`SinkWriter`] when opening destination resources,
//! buffering, inspecting delivery provenance, measuring bytes, or reporting
//! partial outcomes. Configuration belongs to the connector; `open` creates an
//! isolated writer. Only that richer writer receives [`SinkWriteContext`] and
//! authors [`SinkWriteReport`] values. Both tiers use the same runtime protocol.
//! A [`SinkDescription`] always declares a [`DeliveryMethod`]; redelivery safety
//! is a separate property that the integration or flow must classify.
//!
//! ## Built-in destinations
//!
//! These adapters cover common destinations. The traits above are the extension
//! points for application integrations.
//!
//! | Integration | Entry point |
//! |---|---|
//! | Console | [`ConsoleSink::new`] with a pure formatter |
//! | Batched console table | `ConsoleSink::new(TableFormatter::new(columns, extractor)).batch_size(256)?` |
//! | Structured diagnostics | [`TracingSink::new`] with a synchronous tracing emitter |
//! | Intentional discard | [`DiscardSink::new`] |
//! | CSV or TSV | [`CsvSink::builder`] |
//! | PostgreSQL | `postgres::PostgresSink::builder` (`postgres` feature) |
//!
//! TSV is a CSV builder setting, `.tab_delimited()`. [`ConsoleSink::batch_size`]
//! requires a positive row limit and moves the formatter, destination and replay
//! labels into isolated table writers. Batches also have a default 64 KiB cap on
//! prepared UTF-8 cell text, configurable through [`TableConsoleSink::batch_byte_limit`].
//! This excludes rendered table overhead and allocation overhead; an oversized
//! row is rejected. Thresholds or runtime flush/drain trigger output, with replay
//! label changes forming another boundary. There is no timed flush guarantee.
//!
//! ## Console sinks
//!
//! [`ConsoleSink`] prints events to stdout using a pluggable [`Formatter`].
//! Built-in formatters include [`DebugFormatter`], [`JsonFormatter`],
//! [`JsonPrettyFormatter`], and [`TableFormatter`].
//! Use `.label_replays()` to let the adapter label replay output while keeping
//! the formatter independent of runtime provenance. The adapter measures bytes
//! and reports I/O failures; [`ConsoleOutput::Empty`] is an explicit no-op.
//!
//! ## CSV sinks
//!
//! [`CsvSink`] writes typed events to CSV files on disk. A user-owned
//! [`CsvProjection`] value declares the accepted input and CSV-facing row with
//! associated types, matching the framework's handler style.
//!
//! ## PostgreSQL sinks
//!
//! Enable the `postgres` Cargo feature. A binder maps a domain event to SQL
//! parameters; the sink owns a fixed statement and batching configuration.
//! This UPSERT makes repeated delivery of a payment update the same row:
//!
//! ```no_run
//! # #[cfg(feature = "postgres")]
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use obzenflow::stages::sinks::postgres::{
//!     PostgresBind, PostgresBindings, PostgresConnection, PostgresSink, PostgresTransport,
//! };
//! use obzenflow::stages::sinks::SinkRedeliverySafety;
//! # use obzenflow::schema::TypedPayload;
//! # use serde::{Deserialize, Serialize};
//! # #[derive(Clone, Debug, Serialize, Deserialize)]
//! # struct PaymentAuthorized {
//! #     payment_id: i64, order_id: String, customer_id: String, amount_cents: i64,
//! # }
//! # impl TypedPayload for PaymentAuthorized {
//! #     const EVENT_TYPE: &'static str = "payments.payment_authorized";
//! # }
//!
//! #[derive(Clone)]
//! struct PaymentBinder;
//!
//! impl PostgresBind for PaymentBinder {
//!     type Input = PaymentAuthorized;
//!
//!     fn bind(&self, bindings: &mut PostgresBindings, payment: &PaymentAuthorized) {
//!         bindings.bind(payment.payment_id)
//!             .bind(&payment.order_id)
//!             .bind(&payment.customer_id)
//!             .bind(payment.amount_cents);
//!     }
//! }
//!
//! let payments = PostgresSink::builder(PaymentBinder)
//!     .connection(PostgresConnection::deferred_from_env(
//!         "OBZENFLOW_POSTGRES_URL", PostgresTransport::VerifiedTls,
//!     ))
//!     .insert_into(
//!         "obzenflow_example", "payments",
//!         "(payment_id, order_id, customer_id, amount_cents) VALUES ($1, $2, $3, $4) \
//!          ON CONFLICT (payment_id) DO UPDATE SET order_id = EXCLUDED.order_id, \
//!          customer_id = EXCLUDED.customer_id, amount_cents = EXCLUDED.amount_cents",
//!     )?
//!     .batch_size(2)?
//!     .redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
//!     .build()?;
//! # Ok(())
//! # }
//! # #[cfg(not(feature = "postgres"))]
//! # fn main() {}
//! ```
//!
//! Bind `payments` to `sink!(PaymentAuthorized => payments)`. Provision the
//! schema and table separately. The connection URL must select
//! `sslmode=verify-full`; use `sslrootcert` for a private certificate authority.
//! Construction performs no database I/O; the connection is resolved when a
//! live writer opens.

pub use obzenflow_adapters::sinks::csv::CsvWriter;
/// Built-in destinations, formatters, and output configuration.
pub use obzenflow_adapters::sinks::{
    ConsoleConfigError, ConsoleFormatError, ConsoleOutput, ConsoleSink, ConsoleWriter,
    CsvProjection, CsvSink, CsvSinkBuilder, DebugFormatter, DiscardSink, Formatter, JsonFormatter,
    JsonPrettyFormatter, OutputDestination, SnapshotTableFormatter, TableConsoleSink,
    TableFormatter, TracingSink,
};

pub use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
pub use obzenflow_runtime::effects::SinkRedeliverySafety;
pub use obzenflow_runtime::stages::common::handlers::WithRedeliverySafety;
pub use obzenflow_runtime::stages::sink::{
    DeliveryContext, DeliveryProvenance, InlineSink, PendingSinkInput, SinkAuditOutcome,
    SinkBufferedOutcome, SinkCommitReceipt, SinkConnector, SinkDescription,
    SinkDestinationErrorCode, SinkInputOrder, SinkOperationError,
    SinkOperationErrorConversionError, SinkOperationResult, SinkPrimaryOutcome,
    SinkTerminalOutcome, SinkWriteContext, SinkWriteFailure, SinkWriteFailureDisposition,
    SinkWritePhase, SinkWriteReport, SinkWriteResult, SinkWriter, SinkWriterInitContext,
    SinkWriterLifecycleReport,
};

/// Feature-gated PostgreSQL sink and its typed parameter-binding surface.
///
/// The connector witnesses its exact input type, so the generic `sink!` arm
/// proves arrow equality before erasing the writer.
#[cfg(feature = "postgres")]
pub use obzenflow_adapters::sinks::postgres;
