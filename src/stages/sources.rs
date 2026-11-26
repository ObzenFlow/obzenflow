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
//! pushed input takes each event from the system behind its event port. The journal
//! records each answer or event. A replay uses only those records.
//!
//! | Input | Handler |
//! |---|---|
//! | Values in this process | [`ValuesSource::new`] |
//! | Source channel | `subscribe(decoder).through(Port::to(receiver))` |
//! | CSV or TSV file | [`CsvSource::builder`] |
//! | YAML document | [`YamlSource::builder`] |
//! | Hosted HTTP ingress | `subscribe(decoder).through(Port::to(hosted.http(config)?))` |
//! | HTTP API | `query(ListDetailRead::first(n)).through(port)` |
//!
//! An application writes a pull source as one line, `query(first).through(port)`.
//! `first` is the request of the first read, and `port` is the port of the system
//! that answers it. The `#[query]` method of the port answers with a
//! [`FiniteRead`](crate::queries::FiniteRead). Each answer gives the records and the
//! request of the next read, or ends the input.
//!
//! `.decode(f)` maps each recorded record to the output. `.poll_timeout(d)` sets the
//! deadline of each call to the system. The default deadline is 30 seconds. The
//! journal records each answer that arrives, also when the deadline passes.
//!
//! A source handler with state of its own implements [`TypedFiniteSourceHandler`]
//! or [`TypedAsyncFiniteSourceHandler`]. Its `read` returns a [`SourceRead`] that
//! [`SourceRead::finite`] builds.
//!
//! An application writes a push source as one line, `subscribe(decoder).through(port)`.
//! `port` is the event port of the system that holds the events. `decoder` maps each
//! event to the output.
//!
//! A push source handler with state of its own implements
//! [`TypedInfiniteSourceHandler`] or [`TypedAsyncInfiniteSourceHandler`]. Its `read`
//! returns a [`SourceRead`] that [`SourceRead::events`] builds.
//!
//! ## In-process sources
//!
//! A [`ValuesSource`] reads values that this process holds, up to 1,000 values in
//! each read. [`ValuesSource::chunk_size`] sets a smaller number. Only this
//! process holds the values, so ObzenFlow refuses to resume a run that did not
//! read all of them.
//!
//! The line `subscribe(decoder).through(Port::to(receiver))` reads the values that a
//! sender puts on a [`source_channel`](crate::events::source_channel). The stage records
//! each value before it decodes it. A closed channel is a source error. Use topology
//! fan-out for more than one consumer.
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
//! [`CsvSourceBuilder`] does no file I/O when it builds a [`CsvSource`].
//! [`FileSystem`](crate::systems::file::FileSystem) reads the file only for a live
//! read, so a strict replay works when the original CSV is not available.
//! [`CsvRowDecoder`] gives [`CsvRow`] output that keeps each field as a string.
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
//! A hosted ingress endpoint runs the [`IngressDecoder`](crate::events::IngressDecoder)
//! of its `subscribe` line on each push submission at admission. The decoder names the
//! event that each body becomes. The default `decode` deserializes the record with
//! serde. Override it for a record-local rule. The endpoint refuses a submission when
//! `decode` returns an error. The journalled refusal names the field.
//!
//! `FlowApplication::builder().hosted_ingress()` returns the
//! [`HostedIngress`](crate::application::HostedIngress) of the application. Give it to
//! the factory of the flow. In the factory, each call to `hosted.http(config)` registers
//! an endpoint and returns its event system. The line
//! `subscribe(decoder).through(Port::to(events))` reads that event system. For an
//! application-owned protocol, `.ingress` returns the event system of an
//! [`ingress_source`](crate::application::ingress::ingress_source).
//!
//! The journal records each admitted submission before its row commits. Hosted
//! ingress acknowledges a submission only after its row commits. A resumed run
//! reattaches the ingress live with [`ReattachGuarantee::SenderRedelivers`].
//!
//! ## HTTP sources
//!
//! An HTTP API that lists IDs and gives each item by its ID answers the framework's
//! [`ListDetail`] port. `HttpApi::json(base)` configures the
//! [`HttpApi`](crate::systems::http::HttpApi) system with a base URL.
//! `.list_detail(list, detail)` names the list path and the path of each item. The
//! line `query(ListDetailRead::first(n)).decode(f).through(port)` reads at most `n`
//! items. The port resolves the system only for a live read. The default HTTP client
//! is available only with the `http` feature.
//!
//! Another system answers [`ListDetail`] with its one `read` method. The method
//! asks the request for its [`step`](ListDetailRead::step) and answers with
//! [`list_reply`](ListDetailRead::list_reply) or
//! [`detail_reply`](ListDetailRead::detail_reply).

/// CSV file source, decoder contract, and string-preserving row support.
pub use obzenflow_infra::systems::file::csv::{
    CsvDecodeError, CsvDecoder, CsvPosition, CsvRecord, CsvRow, CsvRowDecoder, CsvSource,
    CsvSourceBuilder,
};

/// These items are the CSV and YAML ports of the file system and their read types.
pub use obzenflow_infra::systems::file::{CsvFile, CsvLine, CsvPage, CsvRead, YamlFile};

/// `ValuesSource` is the in-process source that owns its values.
pub use obzenflow_adapters::sources::ValuesSource;

/// These items are the list-and-detail port of a pull source, its request and the
/// step that a request asks for.
pub use obzenflow_adapters::sources::{ListDetail, ListDetailRead, ListDetailStep};

/// Finite YAML source and its application decoder contract.
pub use obzenflow_infra::systems::file::yaml::{
    YamlDecodeError, YamlDecoder, YamlLine, YamlNode, YamlPage, YamlRead, YamlRecord,
    YamlSelection, YamlSource, YamlSourceBuilder,
};

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
