// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{FiniteRead, QueryRequest};
use crate::stages::common::handlers::source::{
    Finite, SourceError, SourceRead, TypedAsyncFiniteSourceHandler,
};
use crate::systems::{EffectKind, Port, PortKind, QueryKind};
use async_trait::async_trait;
use obzenflow_core::OneFactOutcome;
use std::sync::Arc;
use std::time::Duration;

const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(30);

type KeptRecord<Record> = fn(Record) -> Result<Record, SourceError>;

/// `query` starts the line that builds a pull source from the request of its first
/// read. `.through(port)` ends the line and returns the source handler.
///
/// ```
/// # use obzenflow_runtime::queries::{query, FiniteRead, QueryContext, QueryError};
/// # use obzenflow_runtime::systems::Port;
/// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// struct Position {
///     next_id: u64,
/// }
///
/// #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// struct Tick {
///     id: u64,
/// }
/// # impl obzenflow_core::TypedPayload for Tick { const EVENT_TYPE: &'static str = "docs.tick"; }
///
/// struct Ticks;
///
/// #[obzenflow_runtime::effects::port]
/// impl Ticks {
///     #[query(type = "docs.ticks.read")]
///     async fn read(
///         &self,
///         from: &Position,
///         _ctx: &QueryContext,
///     ) -> Result<FiniteRead<Tick, Position>, QueryError> {
///         Ok(match from.next_id {
///             3 => FiniteRead::EndOfInput,
///             id => FiniteRead::Batch {
///                 records: vec![Tick { id }],
///                 next: Position { next_id: id + 1 },
///             },
///         })
///     }
/// }
///
/// let source = query(Position { next_id: 0 }).through(Port::to(Ticks));
/// ```
///
/// A value that no `#[query]` method takes does not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::queries::query;
/// let source = query(7_u64);
/// ```
pub fn query<R, Record>(first: R) -> PendingQuery<R, KeptRecord<Record>>
where
    R: QueryRequest<Answer = FiniteRead<Record, R>>,
{
    PendingQuery {
        first,
        decode: Ok,
        poll_timeout: DEFAULT_POLL_TIMEOUT,
    }
}

/// `PendingQuery` is the query line before `.through(port)`. It holds the request of
/// the first read, the function that decodes each record and the deadline of each
/// supplier call.
pub struct PendingQuery<R, D> {
    first: R,
    decode: D,
    poll_timeout: Duration,
}

impl<R: QueryRequest, D> PendingQuery<R, D> {
    /// `decode` sets the function from one recorded record to the output of the stage.
    /// A line whose records are the output does not call `decode`. A function that does
    /// not take the record of the query does not compile:
    ///
    /// ```compile_fail,E0631
    /// # use obzenflow_runtime::queries::{query, FiniteRead, QueryContext, QueryError};
    /// # use obzenflow_runtime::stages::common::handlers::SourceError;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Position;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Tick;
    /// # struct Ticks;
    /// # #[obzenflow_runtime::effects::port]
    /// # impl Ticks {
    /// #     #[query(type = "docs.ticks.read")]
    /// #     async fn read(&self, _: &Position, _: &QueryContext)
    /// #         -> Result<FiniteRead<Tick, Position>, QueryError> {
    /// #         Ok(FiniteRead::EndOfInput)
    /// #     }
    /// # }
    /// let line = query(Position).decode(|text: String| Ok::<_, SourceError>(text));
    /// ```
    pub fn decode<Record, Output, F>(self, decode: F) -> PendingQuery<R, F>
    where
        R: QueryRequest<Answer = FiniteRead<Record, R>>,
        F: Fn(Record) -> Result<Output, SourceError> + Send + Sync + 'static,
    {
        PendingQuery {
            first: self.first,
            decode,
            poll_timeout: self.poll_timeout,
        }
    }

    /// `poll_timeout` sets the deadline of each supplier call. A line without it keeps
    /// a deadline of 30 seconds. After the supplier answers, the stage records the
    /// answer even if the deadline passes.
    pub fn poll_timeout(mut self, deadline: Duration) -> Self {
        self.poll_timeout = deadline;
        self
    }

    /// `through` binds the line to the port of the query's system and returns the
    /// source handler. A port whose methods are `#[effect]` methods does not compile:
    ///
    /// ```compile_fail,E0271
    /// # use obzenflow_runtime::effects::EffectError;
    /// # use obzenflow_runtime::queries::{query, FiniteRead, QueryContext, QueryError};
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Position;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Tick;
    /// # impl obzenflow_core::TypedPayload for Tick { const EVENT_TYPE: &'static str = "docs.tick"; }
    /// # #[obzenflow_runtime::effects::port]
    /// # #[async_trait::async_trait]
    /// # trait TickFeed: Send + Sync {
    /// #     #[query(type = "docs.ticks.read")]
    /// #     async fn read(&self, from: &Position, ctx: &QueryContext)
    /// #         -> Result<FiniteRead<Tick, Position>, QueryError>;
    /// # }
    /// # #[derive(Clone, Debug, serde::Serialize)]
    /// # struct Append;
    /// #[obzenflow_runtime::effects::port]
    /// trait TickLedger: Send + Sync {
    ///     #[effect(type = "docs.ticks.append", exactly_once)]
    ///     fn append(&self, request: &Append) -> Result<Tick, EffectError>;
    /// }
    ///
    /// struct Ledger;
    ///
    /// impl TickLedger for Ledger {
    ///     fn append(&self, _: &Append) -> Result<Tick, EffectError> {
    ///         Ok(Tick)
    ///     }
    /// }
    ///
    /// let ledger: Port<dyn TickLedger> = Port::to(Ledger);
    /// let source = query(Position).through(ledger);
    /// ```
    ///
    /// The event port does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::events::{source_channel, Events};
    /// # use obzenflow_runtime::queries::{query, FiniteRead, QueryContext, QueryError};
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Position;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct Tick;
    /// # impl obzenflow_core::TypedPayload for Tick { const EVENT_TYPE: &'static str = "docs.tick"; }
    /// # #[obzenflow_runtime::effects::port]
    /// # #[async_trait::async_trait]
    /// # trait TickFeed: Send + Sync {
    /// #     #[query(type = "docs.ticks.read")]
    /// #     async fn read(&self, from: &Position, ctx: &QueryContext)
    /// #         -> Result<FiniteRead<Tick, Position>, QueryError>;
    /// # }
    /// let (_, ticks) = source_channel::<Tick>(1);
    /// let events: Port<dyn Events> = Port::to(ticks);
    /// let source = query(Position).through(events);
    /// ```
    pub fn through<P, Record, Output>(self, port: Port<P>) -> Query<Output>
    where
        P: ?Sized + PortKind + QueryLine<R, <P as PortKind>::Kind>,
        R: QueryRequest<Answer = FiniteRead<Record, R>>,
        Record: Send + Sync + 'static,
        D: Fn(Record) -> Result<Output, SourceError> + Send + Sync + 'static,
        Output: Send + Sync + 'static,
    {
        let port = <P as QueryLine<R, P::Kind>>::port(port);
        let decode = Arc::new(self.decode);
        let first = self.first;
        Query {
            read: Box::new(move || {
                let decode = Arc::clone(&decode);
                SourceRead::finite(port.clone(), first.clone(), move |record| decode(record))
            }),
            poll_timeout: self.poll_timeout,
        }
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`query` binds a port whose method is marked `#[query]`; this port delivers events"
)]
pub trait QueryLine<R: QueryRequest, Kind>: Send + Sync + 'static {
    fn port(port: Port<Self>) -> Port<R::Port>;
}

impl<P, R> QueryLine<R, QueryKind> for P
where
    P: ?Sized + PortKind<Kind = QueryKind> + Send + Sync + 'static,
    R: QueryRequest<Port = P>,
{
    fn port(port: Port<P>) -> Port<P> {
        port
    }
}

impl<P, R> QueryLine<R, EffectKind> for P
where
    P: ?Sized + PortKind<Kind = EffectKind> + Send + Sync + 'static,
    R: QueryRequest<Port = P>,
{
    fn port(port: Port<P>) -> Port<P> {
        port
    }
}

/// `Query` is the source handler that a query line builds. `source!` takes it.
pub struct Query<Output> {
    read: Box<dyn Fn() -> SourceRead<Output, Finite> + Send + Sync>,
    poll_timeout: Duration,
}

impl<Output> std::fmt::Debug for Query<Output> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Query")
            .field("poll_timeout", &self.poll_timeout)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<Output: OneFactOutcome + Send + Sync + 'static> TypedAsyncFiniteSourceHandler
    for Query<Output>
{
    type Output = Output;

    fn poll_timeout(&self) -> Option<Duration> {
        Some(self.poll_timeout)
    }

    fn read(&self) -> SourceRead<Output, Finite> {
        (self.read)()
    }
}
