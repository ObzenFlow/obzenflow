// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sink constructors, formatting, and custom sink authoring contracts.
//!
//! Sinks are the terminal stages of a pipeline. This module re-exports the
//! built-in destinations and the contracts for implementing application sinks.
//!
//! ## Console sinks
//!
//! [`ConsoleSink`] prints events to stdout using a pluggable [`Formatter`].
//! Built-in formatters include [`DebugFormatter`], [`JsonFormatter`],
//! [`JsonPrettyFormatter`], and [`TableFormatter`].
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
/// Console and CSV sinks, formatters, and output configuration.
pub use obzenflow_adapters::sinks::{
    console, debug, json, json_pretty, table, ConsoleSink, CsvProjection, CsvSink, CsvSinkBuilder,
    DebugFormatter, Formatter, JsonFormatter, JsonPrettyFormatter, OutputDestination,
    SnapshotTableFormatter, TableFormatter,
};

pub use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
pub use obzenflow_runtime::effects::SinkRedeliverySafety;
pub use obzenflow_runtime::stages::common::handlers::WithRedeliverySafety;
pub use obzenflow_runtime::stages::sink::{
    DeliveryContext, DeliveryProvenance, InlineSink, PendingSinkInput, SinkAuditOutcome,
    SinkBufferedOutcome, SinkCommitReceipt, SinkConnector, SinkDescription,
    SinkDestinationErrorCode, SinkInputOrder, SinkOperationError,
    SinkOperationErrorConversionError, SinkOperationResult, SinkPrimaryOutcome,
    SinkTerminalOutcome, SinkTyped, SinkWriteContext, SinkWriteFailure,
    SinkWriteFailureDisposition, SinkWritePhase, SinkWriteReport, SinkWriteResult, SinkWriter,
    SinkWriterInitContext, SinkWriterLifecycleReport,
};

/// Feature-gated PostgreSQL sink and its typed parameter-binding surface.
///
/// The connector witnesses its exact input type, so the generic `sink!` arm
/// proves arrow equality before erasing the writer.
#[cfg(feature = "postgres")]
pub use obzenflow_adapters::sinks::postgres;
