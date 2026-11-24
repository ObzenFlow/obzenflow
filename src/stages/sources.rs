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
//! A `source!` row takes a source handler. The handler declares one read
//! through a bound port. The framework performs each read through the effect
//! runtime. The system behind the port does the physical I/O. The journal
//! records the binding of the system and each reply. A replay uses only the
//! recorded replies.
//!
//! | Input | Handler | System |
//! |---|---|---|
//! | Values in this process | [`ValuesSource::new`] | `in-memory` |
//! | Source channel | [`ChannelSource::new`] | `channel` |
//! | CSV or TSV file | [`CsvSource::builder`] | `file-system` |
//! | YAML document | [`YamlSource::builder`] | `file-system` |
//! | Hosted HTTP ingress | `FlowApplication::builder().http_ingress(decoder, config)` | `http-ingress` |
//! | HTTP API | An application handler with a port that [`HttpApi`] implements | `http-api` |
//!
//! An application source handler implements [`TypedFiniteSourceHandler`],
//! [`TypedAsyncFiniteSourceHandler`], [`TypedInfiniteSourceHandler`] or
//! [`TypedAsyncInfiniteSourceHandler`]. Its `read` returns a [`SourceRead`].
//! A finite handler builds it with [`SourceRead::finite`] for input that ends,
//! and an infinite handler builds it with [`SourceRead::continuous`] for input
//! that stays open. The port method replies with a [`FiniteRead`] or a
//! [`ContinuousRead`]. Each reply gives the records and the argument of the
//! next read, or ends a finite input. The handler maps each recorded record to
//! its output. A continuous port has [`InputReadiness`] as a supertrait, so each
//! system behind it tells the framework when it holds input. A continuous read
//! continues after a resume under the guarantee of its port method.
//!
//! ## In-process sources
//!
//! A [`ValuesSource`] reads its values through the `in-memory` system, up to 1,000
//! values in each read. Only this process holds the values, so ObzenFlow refuses
//! to resume a run that did not read all of them. A [`ChannelSource`] reads a
//! channel that [`source_channel`] makes, through the `channel` system. A closed
//! channel is a source error. Use topology fan-out for more than one consumer.
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
//! application for an [`IngressDecoder`] that names the event each body
//! becomes. The default `decode` deserializes the [`IngressRecord`] with serde.
//! Override it for a record-local rule; an [`IngressDecodeError`] refuses that
//! submission, and the journalled refusal names the field.
//!
//! ```rust
//! use obzenflow::schema::TypedPayload;
//! use obzenflow::stages::sources::{IngressDecodeError, IngressDecoder, IngressRecord};
//! use serde::{Deserialize, Serialize};
//! use serde_json::json;
//!
//! #[derive(Debug, Clone, Serialize, Deserialize)]
//! struct LedgerEntry {
//!     account_id: String,
//!     amount_cents: u64,
//! }
//!
//! impl TypedPayload for LedgerEntry {
//!     const EVENT_TYPE: &'static str = "bank.ledger_entry";
//! }
//!
//! #[derive(Clone)]
//! struct LedgerIngress;
//!
//! impl IngressDecoder for LedgerIngress {
//!     type Output = LedgerEntry;
//!
//!     fn decode(&self, record: IngressRecord<'_>) -> Result<LedgerEntry, IngressDecodeError> {
//!         let entry: LedgerEntry = record.deserialize()?;
//!         if entry.amount_cents == 0 {
//!             return Err(IngressDecodeError::invalid_value("amount_cents"));
//!         }
//!         Ok(entry)
//!     }
//! }
//!
//! let zero = json!({"account_id": "acct-1", "amount_cents": 0});
//! let refused = LedgerIngress.decode(IngressRecord::new(&zero)).unwrap_err();
//! assert_eq!(refused.to_string(), "invalid value in field amount_cents");
//! ```
//!
//! Register the decoder through `FlowApplication::builder().http_ingress(decoder,
//! config)`, or through `.ingress` for another transport. Each call returns the
//! [`HostedSource`] handler for a `source!` stage. The handler reads the queue of
//! the endpoint. The journal records each read. For custom hosting, compose the
//! bundles with
//! [`crate::application::ingress::ingress_source`] or
//! [`crate::application::ingress::http_ingress`]. Hosted ingress acknowledges a
//! submission only after its row commits, so a resumed run reattaches it live
//! and records that the sender redelivers what was never acknowledged.
//!
//! ## HTTP sources
//!
//! An HTTP source declares an application port and implements that port for
//! [`HttpApi`]. The system of [`HttpApi`] is `http-api`. `HttpApi::json` binds a
//! base URL. The port resolves the system only for a live read. Each port method
//! calls `HttpApi::get_json` and replies with the records and the argument of
//! the next read. The default HTTP client is available only with the `http`
//! feature.

/// CSV file source, decoder contract, and string-preserving row support.
pub use obzenflow_adapters::sources::{
    CsvDecodeError, CsvDecoder, CsvPosition, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};

/// These items are the framework file system, its CSV configuration and read types, and
/// its YAML port.
pub use obzenflow_adapters::sources::{
    CsvFile, CsvFileSystem, CsvLine, CsvPage, CsvRead, FileSystem, YamlFile, YamlFileSystem,
};

/// These items are the in-process sources, which own values or a source channel.
pub use obzenflow_adapters::sources::{ChannelSource, ValuesSource};

/// Finite YAML source and its application decoder contract.
pub use obzenflow_adapters::sources::{
    YamlDecodeError, YamlDecoder, YamlLine, YamlNode, YamlPage, YamlRead, YamlRecord,
    YamlSelection, YamlSource, YamlSourceBuilder,
};

/// These items are the hosted source handler and the application-owned decoder contract
/// for hosted ingress.
pub use obzenflow_adapters::sources::{
    HostedSource, IngressDecodeError, IngressDecoder, IngressRecord,
};

/// HTTP primitives re-exported from `obzenflow_core` for building requests.
pub use obzenflow_core::http_client::{
    Bytes, HeaderMap, HttpClient, HttpClientError, HttpMethod, HttpRequest, Url,
};

/// These items are the HTTP API system and the error of its client factory.
pub use obzenflow_infra::clients::http::{HttpApi, HttpClientFactoryError};

pub use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
pub use obzenflow_runtime::stages::common::handlers::source::{
    source_channel, Continuous, ContinuousRead, Finite, FiniteRead, InputReadiness, ReadLifetime,
    SourceClosed, SourcePermit, SourceRead, SourceReceiver, SourceSender,
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
