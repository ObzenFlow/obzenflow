// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The `sources` module re-exports the built-in source handlers and the
//! contracts for application source handlers.
//!
//! Sources are the entry points of every pipeline.
//!
//! ## Source handlers
//!
//! A `source!` row takes a source handler. A handler that pulls its input
//! declares one query through a bound port. The source stage asks each query,
//! and the system behind the port does the physical I/O. A handler that receives
//! pushed input records each admitted event. The journal records the binding of
//! the system and each answer or event. A replay uses only those records.
//!
//! | Input | Handler | System |
//! |---|---|---|
//! | Values in this process | [`ValuesSource::new`] | `in-memory` |
//! | Source channel | [`ChannelSource::new`] | `channel` |
//! | CSV or TSV file | [`CsvSource::builder`] | `file-system` |
//! | YAML document | [`YamlSource::builder`] | `file-system` |
//! | Hosted HTTP ingress | `FlowApplication::builder().http_ingress(decoder, config)` | `http-ingress` |
//! | HTTP API | `query(ListDetailRead::first(n)).through(port)` | `http-api` |
//!
//! An application writes a pull source as one line, `query(first).through(port)`,
//! from the request of its first read and the port of the system that answers it.
//! The `#[query]` method of the port answers with a
//! [`FiniteRead`](crate::queries::FiniteRead). Each answer
//! gives the records and the request of the next read, or ends the input.
//! `.decode(f)` maps each recorded record to the output, and `.poll_timeout(d)`
//! sets the deadline of each call to the system, 30 seconds when the line sets
//! none. An answer that arrives is recorded, also when the deadline passes.
//!
//! A source handler with state of its own implements [`TypedFiniteSourceHandler`]
//! or [`TypedAsyncFiniteSourceHandler`], and its `read` returns a [`SourceRead`]
//! that [`SourceRead::finite`] builds. A source of pushed events is a
//! [`ChannelSource`] or the hosted HTTP ingress.
//!
//! ## In-process sources
//!
//! A [`ValuesSource`] reads its values through the `in-memory` system, up to 1,000
//! values in each read. [`ValuesSource::chunk_size`] sets a smaller number. Only
//! this process holds the values, so ObzenFlow refuses to resume a run that did
//! not read all of them. A [`ChannelSource`] receives the values that a sender
//! puts on a channel from [`source_channel`](crate::events::source_channel). Its
//! system is `channel`, and the
//! stage records each value before it publishes it. A closed channel is a source
//! error. Use topology fan-out for more than one consumer.
//! Values that are still in the channel when a run stops are lost, so a resumed
//! run continues its live reads with [`ReattachGuarantee::AtMostOnce`].
//!
//! ## Source errors
//!
//! A [`SourceError`] names a category and carries a typed [`SourceDiagnostic`]:
//! a closed [`SourceDiagnosticReason`], safe coordinates and an optional
//! namespaced [`SourceErrorCode`]. A record-local rejection is
//! [`SourceError::Validation`] and reading continues. [`SourceError::Terminal`]
//! reports that the source cannot safely continue. The supervisor then fails the
//! stage without EOF.
//!
//! ```rust
//! use obzenflow::stages::sources::{SourceDiagnosticReason, SourceError};
//!
//! let timeout = SourceError::Timeout(SourceDiagnosticReason::TimedOut.into());
//! assert_eq!(timeout.to_string(), "source timeout error: the input timed out");
//! ```
//!
//! ## CSV sources
//!
//! A CSV source reads the file and asks the application for one thing, a
//! [`CsvDecoder`] that names the event each row becomes. When the columns match
//! the event's fields, naming [`CsvDecoder::Output`] is enough, because the
//! default `decode` uses serde. Override `decode` when the shapes differ.
//!
//! ```rust
//! use obzenflow::schema::TypedPayload;
//! use obzenflow::stages::sources::{CsvDecoder, CsvSource};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct Customer {
//!     customer_id: String,
//!     tier: String,
//! }
//!
//! impl TypedPayload for Customer {
//!     const EVENT_TYPE: &'static str = "support.customer";
//! }
//!
//! #[derive(Clone)]
//! struct CustomerCsv;
//!
//! impl CsvDecoder for CustomerCsv {
//!     type Output = Customer;
//! }
//!
//! let customers = CsvSource::builder(CustomerCsv)
//!     .path("customers.csv")
//!     .build()?;
//! // Inside flow!'s stages block: customers = source!(Customer => customers);
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! [`CsvSourceBuilder`] does no file I/O when it builds a [`CsvSource`]. The
//! `file-system` system reads the file only for a live read, so a strict replay
//! works when the original CSV is not available. [`CsvRowDecoder`] gives
//! [`CsvRow`] output that keeps each field as a string.
//!
//! ## YAML sources
//!
//! A YAML source reads one bounded document and asks the application for one
//! method, [`YamlDecoder::decode`], which turns each selected [`YamlRecord`]
//! into an event. A [`YamlSelection`] names the records: the whole document, a
//! root sequence, or a sequence at an RFC 6901 pointer.
//!
//! ```rust
//! use obzenflow::schema::TypedPayload;
//! use obzenflow::stages::sources::{
//!     YamlDecodeError, YamlDecoder, YamlRecord, YamlSelection, YamlSource,
//! };
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct AccountOpened {
//!     account_id: String,
//!     initial_balance_cents: i64,
//! }
//!
//! impl TypedPayload for AccountOpened {
//!     const EVENT_TYPE: &'static str = "bank.account_opened";
//! }
//!
//! #[derive(Clone)]
//! struct AccountYaml;
//!
//! impl YamlDecoder for AccountYaml {
//!     type Output = AccountOpened;
//!
//!     fn decode(&self, record: YamlRecord<'_>) -> Result<AccountOpened, YamlDecodeError> {
//!         let account: AccountOpened = record.deserialize()?;
//!         if account.initial_balance_cents < 0 {
//!             return Err(YamlDecodeError::invalid_value("initial_balance_cents"));
//!         }
//!         Ok(account)
//!     }
//! }
//!
//! let accounts = YamlSource::builder(AccountYaml)
//!     .path("accounts.yaml")
//!     .selection(YamlSelection::SequenceAt("/accounts".into()))
//!     .build()?;
//! // Inside flow!'s stages block: accounts = source!(AccountOpened => accounts);
//! # Ok::<(), anyhow::Error>(())
//! ```
//!
//! A [`YamlDecodeError`] rejects only that record, and reading continues.
//! `decode` has no default, because a YAML output may be a sum of several fact
//! types. Building does no file I/O. Aliases, anchors, tags, merge keys and
//! duplicate keys cause a terminal source error.
//!
//! ## Hosted ingress sources
//!
//! A hosted ingress source receives admitted push submissions and asks the
//! application for an [`IngressDecoder`](crate::events::IngressDecoder) that
//! names the event each body becomes. The default `decode` deserializes the
//! record with serde. Override it for a record-local rule. A decode error
//! refuses that submission, and the journalled refusal names the field.
//!
//! Register the decoder through `FlowApplication::builder().http_ingress(decoder,
//! config)`, or through `.ingress` for another transport. Each call returns the
//! [`HostedSource`] handler for a `source!` stage. The journal records each
//! admitted submission before its row commits. For custom hosting, compose the
//! bundles with
//! [`crate::application::ingress::ingress_source`] or
//! [`crate::application::ingress::http_ingress`]. Hosted ingress acknowledges a
//! submission only after its row commits, so a resumed run reattaches it live
//! and records that the sender redelivers what was never acknowledged.
//!
//! ## HTTP sources
//!
//! An HTTP API that lists IDs and then reads each item answers the framework's
//! [`ListDetail`] port. `HttpApi::json(base)` binds a base URL to the
//! [`HttpApi`](crate::systems::http::HttpApi) system, `http-api`, and
//! `.list_detail(list, detail)` names the list path and the path of each item.
//! The line `query(ListDetailRead::first(n)).decode(f).through(port)` reads at
//! most `n` items. The port resolves the system only for a live read. The
//! default HTTP client is available only with the `http` feature.

/// CSV file source, decoder contract, and string-preserving row support.
pub use obzenflow_infra::systems::file::csv::{
    CsvDecodeError, CsvDecoder, CsvPosition, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};

/// These items are the CSV and YAML ports of the file system and their read types.
pub use obzenflow_infra::systems::file::{CsvFile, CsvLine, CsvPage, CsvRead, YamlFile};

/// These items are the in-process sources, which own values or a source channel.
pub use obzenflow_adapters::sources::{ChannelSource, ValuesSource};

/// These items are the list-and-detail port of a pull source and its request.
pub use obzenflow_adapters::sources::{ListDetail, ListDetailRead};

/// Finite YAML source and its application decoder contract.
pub use obzenflow_infra::systems::file::yaml::{
    YamlDecodeError, YamlDecoder, YamlLine, YamlNode, YamlPage, YamlRead, YamlRecord,
    YamlSelection, YamlSource, YamlSourceBuilder,
};

/// The hosted source handler for hosted ingress.
pub use obzenflow_adapters::sources::HostedSource;

/// HTTP primitives re-exported from `obzenflow_core` for building requests.
pub use obzenflow_core::http_client::{
    Bytes, HeaderMap, HttpClient, HttpClientError, HttpMethod, HttpRequest, Url,
};

pub use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
pub use obzenflow_runtime::stages::common::handlers::source::{
    Continuous, Finite, ReadLifetime, SourceRead,
};
pub use obzenflow_runtime::stages::common::handlers::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};

/// Typed source-failure diagnostics. Every [`SourceError`] carries one; field
/// names are compile-time strings, so diagnostics never echo input.
pub use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
pub use obzenflow_core::event::{
    FieldName, FieldSegment, SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode,
    SourceErrorCodeError, SourceLocation, TextPosition, MAX_FIELD_PATH_DEPTH,
};
