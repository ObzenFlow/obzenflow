// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source authoring and runtime envelope lowering (FLOWIP-134g).

use super::continuation::{
    AsyncFiniteContinuation, AsyncInfiniteContinuation, AsyncReaderOpener, FiniteContinuation,
    InfiniteContinuation, SourceContinuation, SyncReaderOpener,
};
use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite, SealAsyncInfinite, SealFinite,
    SealInfinite, SourceResumeError, UnifiedAsyncFiniteSourceHandler,
    UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler, UnifiedInfiniteSourceHandler,
};
use super::positioned::{PositionedFiniteReader, SourceRecord};
use super::prepared::{
    sealed as admission_sealed, AdmitSource, AsyncSourceReader, ConnectorSource, DirectSource,
    SourceReader, SyncSourceReader,
};
use super::record_de::deserialize_json;
use super::SourceError;
use crate::stages::common::handler_error::StageFatal;
use crate::stages::source::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};
use async_trait::async_trait;
use obzenflow_core::event::observability::{HttpPullMeasurements, HttpPullTelemetry};
use obzenflow_core::event::observability::{NoObservations, ObservationRecorder};

use super::hosted::{HostedAdmission, HostedSubmission};
use obzenflow_core::event::payloads::execution_payload::{
    SourceContinuationRefusal, SourceInputPinnedFact,
};
use obzenflow_core::event::{
    ChainEventFactory, SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode, StageFatalCode,
    StageFatalReason,
};
use obzenflow_core::ingress::{HostedIngressBindingSlot, IngressContext};
use obzenflow_core::source::{
    EncodedSourcePosition, SourcePosition, SourcePositionError, SourceProgress,
};
use obzenflow_core::{ChainEvent, EventId, OneFactStageOutput, TypedPayload, WriterId};
use serde::de::DeserializeOwned;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn configuration_fatal(detail: impl Into<String>) -> StageFatal {
    StageFatal::new(
        StageFatalCode::Configuration,
        StageFatalReason::ConfigurationInvariant,
        detail,
    )
}

pub(super) fn protocol_fatal(detail: impl Into<String>) -> StageFatal {
    StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    )
}

pub(super) fn lower_outputs<O>(
    writer_id: WriterId,
    outputs: Vec<(O, Option<IngressContext>)>,
) -> Result<Vec<ChainEvent>, StageFatal>
where
    O: OneFactStageOutput,
{
    let mut events = Vec::with_capacity(outputs.len());
    for (output, ingress_context) in outputs {
        let mut facts = output.into_facts().map_err(|error| {
            protocol_fatal(format!(
                "typed source output serialization failed for `{}`: {error}",
                std::any::type_name::<O>()
            ))
        })?;
        if facts.len() != 1 {
            return Err(protocol_fatal(format!(
                "one_fact_stage_output: `{}` lowered to {} facts instead of exactly one",
                std::any::type_name::<O>(),
                facts.len(),
            )));
        }
        let fact = facts.pop().expect("length checked above");
        let mut event = fact.into_event(writer_id);
        event.ingress_context = ingress_context;
        events.push(event);
    }
    Ok(events)
}

/// Lower positioned records, each carrying the position after it, so the fact
/// and its progress commit in one append (FLOWIP-134i B11).
fn lower_positioned<O, P>(
    writer_id: WriterId,
    records: Vec<SourceRecord<O, P>>,
) -> Result<Vec<ChainEvent>, StageFatal>
where
    O: OneFactStageOutput,
    P: SourcePosition,
{
    let mut outputs = Vec::with_capacity(records.len());
    let mut progress = Vec::with_capacity(records.len());
    for record in records {
        let after = EncodedSourcePosition::encode(&record.after).map_err(|error| {
            protocol_fatal(format!(
                "source position for `{}`: {error}",
                std::any::type_name::<P>()
            ))
        })?;
        progress.push(SourceProgress { after });
        outputs.push((record.value, None));
    }
    let mut events = lower_outputs(writer_id, outputs)?;
    for (event, progress) in events.iter_mut().zip(progress) {
        event.source_progress = Some(progress);
    }
    Ok(events)
}

fn http_pull_evidence(
    writer_id: WriterId,
    snapshot: HttpPullTelemetry,
    recorder: &dyn ObservationRecorder,
) -> ChainEvent {
    use obzenflow_core::event::observability::ObservationRecord;
    use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, HttpPullStateFact};
    recorder.observe(ObservationRecord::HttpPull(HttpPullMeasurements {
        requests_total: snapshot.requests_total,
        responses_2xx: snapshot.responses_2xx,
        responses_4xx: snapshot.responses_4xx,
        responses_5xx: snapshot.responses_5xx,
        rate_limited_total: snapshot.rate_limited_total,
        retries_total: snapshot.retries_total,
        events_decoded_total: snapshot.events_decoded_total,
        wait_seconds_rate_limit: snapshot.wait_seconds_rate_limit,
        wait_seconds_poll_interval: snapshot.wait_seconds_poll_interval,
        wait_seconds_backoff: snapshot.wait_seconds_backoff,
    }));
    ChainEventFactory::execution_event(
        writer_id,
        ExecutionPayload::HttpPullState(HttpPullStateFact {
            state: snapshot.state,
            wait_reason: snapshot.wait_reason,
            next_wake_unix_secs: snapshot.next_wake_unix_secs,
            last_success_unix_secs: snapshot.last_success_unix_secs,
        }),
    )
}

#[derive(Default)]
struct ObservationState {
    open: bool,
    snapshot: Option<HttpPullTelemetry>,
    violation: Option<String>,
}

/// Runtime-minted, closed source-observation capability.
///
/// It can report one typed HTTP pull snapshot during the current live poll. It
/// exposes no envelope, writer, routing, status, or arbitrary-payload API.
#[doc(hidden)]
#[derive(Clone)]
pub struct SourceObservationSink {
    state: Arc<Mutex<ObservationState>>,
}

impl fmt::Debug for SourceObservationSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceObservationSink")
            .finish_non_exhaustive()
    }
}

impl SourceObservationSink {
    fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(ObservationState::default())),
        }
    }

    /// Report the snapshot for the current poll synchronously.
    ///
    /// Misuse is remembered by the capability and converted by the runtime
    /// adapter into a framework fatal with zero poll output.
    pub fn report_http_pull(&self, snapshot: HttpPullTelemetry) {
        let Ok(mut state) = self.state.lock() else {
            return;
        };
        if !state.open {
            state.violation =
                Some("source observation reported outside the live poll invocation".to_string());
            return;
        }
        if state.snapshot.is_some() {
            state.violation =
                Some("source observation reported more than once in one live poll".to_string());
            return;
        }
        state.snapshot = Some(snapshot);
    }

    fn begin_poll(&self) -> Result<SourceObservationPoll, StageFatal> {
        let mut state = self.state.lock().map_err(|_| {
            protocol_fatal("source observation capability lock was poisoned before poll")
        })?;
        if let Some(violation) = state.violation.take() {
            return Err(protocol_fatal(violation));
        }
        if state.open {
            return Err(protocol_fatal(
                "source observation poll scope was opened while already active",
            ));
        }
        state.open = true;
        state.snapshot = None;
        drop(state);
        Ok(SourceObservationPoll {
            sink: self.clone(),
            finished: false,
        })
    }

    fn finish_poll(&self) -> Result<Option<HttpPullTelemetry>, StageFatal> {
        let mut state = self.state.lock().map_err(|_| {
            protocol_fatal("source observation capability lock was poisoned after poll")
        })?;
        state.open = false;
        if let Some(violation) = state.violation.take() {
            state.snapshot = None;
            return Err(protocol_fatal(violation));
        }
        Ok(state.snapshot.take())
    }

    fn cancel_poll(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.open = false;
            state.snapshot = None;
        }
    }
}

struct SourceObservationPoll {
    sink: SourceObservationSink,
    finished: bool,
}

impl SourceObservationPoll {
    fn finish(mut self) -> Result<Option<HttpPullTelemetry>, StageFatal> {
        let result = self.sink.finish_poll();
        self.finished = true;
        result
    }
}

impl Drop for SourceObservationPoll {
    fn drop(&mut self) {
        if !self.finished {
            self.sink.cancel_poll();
        }
    }
}

/// Pure synchronous finite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed synchronous finite source contract",
    note = "implement TypedFiniteSourceHandler with Output matching the source! arrow (FLOWIP-134g)"
)]
pub trait TypedFiniteSourceHandler: Send + Sync {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError>;
}

/// Pure asynchronous finite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed asynchronous finite source contract",
    note = "implement TypedAsyncFiniteSourceHandler with Output matching the async_source! arrow (FLOWIP-134g)"
)]
#[async_trait]
pub trait TypedAsyncFiniteSourceHandler: Send + Sync {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn poll_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError>;

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }

    /// Runtime-only installation hook for the closed live-poll observation rail.
    #[doc(hidden)]
    fn install_source_observation_sink(&mut self, _sink: SourceObservationSink) {}
}

/// Pure synchronous infinite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed synchronous infinite source contract",
    note = "implement TypedInfiniteSourceHandler with Output matching the infinite_source! arrow (FLOWIP-134g)"
)]
pub trait TypedInfiniteSourceHandler: Send + Sync {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError>;
}

/// Runtime-owned registration sampled once by an async-infinite adapter.
#[doc(hidden)]
#[derive(Clone)]
pub struct SourceRuntimeRegistration {
    pub(super) hosted_ingress_slot: HostedIngressBindingSlot,
}

impl fmt::Debug for SourceRuntimeRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceRuntimeRegistration")
            .field("ingress_key", self.hosted_ingress_slot.ingress_key())
            .finish()
    }
}

impl SourceRuntimeRegistration {
    pub(super) fn hosted(slot: HostedIngressBindingSlot) -> Self {
        Self {
            hosted_ingress_slot: slot,
        }
    }
}

/// One typed async-infinite invocation before runtime envelope lowering.
#[doc(hidden)]
pub struct TypedAsyncInfiniteSourceInvocation<O> {
    outputs: InvocationOutputs<O>,
}

enum InvocationOutputs<O> {
    Facts(Vec<O>),
    /// Admitted hosted members with their receipts (FLOWIP-084l).
    Hosted(Vec<HostedSubmission<O>>),
}

impl<O> TypedAsyncInfiniteSourceInvocation<O> {
    fn facts_only(outputs: Vec<O>) -> Self {
        Self {
            outputs: InvocationOutputs::Facts(outputs),
        }
    }

    pub(super) fn hosted(submissions: Vec<HostedSubmission<O>>) -> Self {
        Self {
            outputs: InvocationOutputs::Hosted(submissions),
        }
    }
}

/// Pure asynchronous infinite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed asynchronous infinite source contract",
    note = "implement TypedAsyncInfiniteSourceHandler with Output matching the async_infinite_source! arrow (FLOWIP-134g)"
)]
#[async_trait]
pub trait TypedAsyncInfiniteSourceHandler: Send + Sync {
    type Output: OneFactStageOutput + Send + Sync + 'static;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError>;

    async fn drain(&mut self) -> Result<(), SourceError> {
        Ok(())
    }

    /// Runtime-only installation hook for the closed live-poll observation rail.
    #[doc(hidden)]
    fn install_source_observation_sink(&mut self, _sink: SourceObservationSink) {}

    /// Runtime-owned hosted-ingress registration, if any.
    #[doc(hidden)]
    fn runtime_registration(&self) -> Option<SourceRuntimeRegistration> {
        None
    }

    /// Runtime-owned invocation hook used to rejoin sealed ingress context.
    #[doc(hidden)]
    async fn next_invocation(
        &mut self,
    ) -> Result<TypedAsyncInfiniteSourceInvocation<Self::Output>, SourceError> {
        self.next()
            .await
            .map(TypedAsyncInfiniteSourceInvocation::facts_only)
    }
}

/// Error returned by an [`IngressDecoder`]. It carries a typed diagnostic only,
/// so a refusal journals a reason and field, never submitted values.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct IngressDecodeError(SourceDiagnostic);

impl IngressDecodeError {
    pub fn missing_field(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::MissingField).within_field(field))
    }

    pub fn invalid_value(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue).within_field(field))
    }

    pub fn invalid_record() -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord))
    }

    /// Nests the path under a schema-declared parent field.
    pub fn within_field(self, name: &'static str) -> Self {
        Self(self.0.within_field(name))
    }

    pub fn code(self, code: SourceErrorCode) -> Self {
        Self(self.0.code(code))
    }

    pub fn into_diagnostic(self) -> SourceDiagnostic {
        self.0
    }
}

/// One submitted ingress body. Exposes neither raw text nor request metadata.
pub struct IngressRecord<'a> {
    data: &'a serde_json::Value,
}

impl<'a> IngressRecord<'a> {
    /// Wraps a body, for example to test a decoder directly.
    pub fn new(data: &'a serde_json::Value) -> Self {
        Self { data }
    }

    /// Deserialize this body with serde. Errors name schema fields only.
    pub fn deserialize<T: DeserializeOwned>(&self) -> Result<T, IngressDecodeError> {
        deserialize_json(self.data).map_err(IngressDecodeError)
    }
}

impl fmt::Debug for IngressRecord<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IngressRecord").finish_non_exhaustive()
    }
}

/// User-owned mapping from one submitted ingress body to one domain output.
///
/// The decoder value owns the source's `Output` contract. The default method
/// deserializes the record with serde; applications override it for an
/// external representation or a record-local rule, rejecting with a typed
/// [`IngressDecodeError`]. Hosted admission runs it exactly once per
/// submission, before anything is queued (FLOWIP-084l B4). Typed
/// `IngressHandle` submissions already have the `Output` shape and bypass it.
pub trait IngressDecoder: Clone + Send + Sync + 'static {
    type Output: TypedPayload + Send + Sync + 'static;

    fn decode(&self, record: IngressRecord<'_>) -> Result<Self::Output, IngressDecodeError> {
        record.deserialize()
    }
}

/// Prove that `value`'s journal form decodes as its own type, returning the
/// decoded copy so the queued value is exactly what replay reconstructs
/// (FLOWIP-084l B4). A failure is `UnrepresentableValue` at the failing field.
pub fn prove_journal_form<T: TypedPayload>(value: T) -> Result<T, IngressDecodeError> {
    let unrepresentable = || {
        IngressDecodeError(SourceDiagnostic::new(
            SourceDiagnosticReason::UnrepresentableValue,
        ))
    };
    let payload = value.into_chain_payload().map_err(|_| unrepresentable())?;
    let body = payload.contract_body().map_err(|_| unrepresentable())?;
    IngressRecord::new(&body)
        .deserialize::<T>()
        .map_err(|error| {
            IngressDecodeError(
                error
                    .into_diagnostic()
                    .with_reason(SourceDiagnosticReason::UnrepresentableValue),
            )
        })
}

/// Runtime adapter for a synchronous typed finite source.
#[doc(hidden)]
pub struct TypedFiniteSourceHandlerAdapter<H> {
    reader: SyncSourceReader<H>,
    writer_id: Option<WriterId>,
}

impl<H> TypedFiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self::with_reader(SourceReader::acquired(handler))
    }

    pub(super) fn cold(open: SyncReaderOpener<H>) -> Self {
        Self::with_reader(SourceReader::cold(open))
    }

    fn with_reader(reader: SyncSourceReader<H>) -> Self {
        Self {
            reader,
            writer_id: None,
        }
    }
}

impl<H: TypedFiniteSourceHandler + 'static>
    admission_sealed::Admitted<dyn UnifiedFiniteSourceHandler, DirectSource> for H
{
}

impl<H: TypedFiniteSourceHandler + 'static>
    AdmitSource<dyn UnifiedFiniteSourceHandler, DirectSource> for H
{
    type Output = H::Output;
    type Handler = TypedFiniteSourceHandlerAdapter<H>;
    const CONTINUATION: SourceContinuation = SourceContinuation::Refuse;

    fn prepare(self) -> Self::Handler {
        TypedFiniteSourceHandlerAdapter::cold(Box::new(move |_| Ok(self)))
    }
}

impl<C: FiniteSourceConnector>
    admission_sealed::Admitted<dyn UnifiedFiniteSourceHandler, ConnectorSource> for C
{
}

impl<C: FiniteSourceConnector> AdmitSource<dyn UnifiedFiniteSourceHandler, ConnectorSource> for C {
    type Output = C::Output;
    type Handler = <C::Continuation as FiniteContinuation<C::Reader>>::Handler;
    const CONTINUATION: SourceContinuation =
        <C::Continuation as FiniteContinuation<C::Reader>>::KIND;

    fn prepare(self) -> Self::Handler {
        <C::Continuation as FiniteContinuation<C::Reader>>::cold(Box::new(move |context| {
            self.open(context)
        }))
    }
}

impl<H: TypedFiniteSourceHandler> SealFinite for TypedFiniteSourceHandlerAdapter<H> {}

impl<H: TypedFiniteSourceHandler> UnifiedFiniteSourceHandler
    for TypedFiniteSourceHandlerAdapter<H>
{
    fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        self.reader.acquire(context)?;
        Ok(())
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(writer_id) = self.writer_id else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "typed source adapter invoked before runtime writer identity installation",
            ));
        };
        let handler = self
            .reader
            .get_mut()
            .expect("the source supervisor must acquire before polling");
        match handler.next() {
            Ok(Some(outputs)) => match lower_outputs(
                writer_id,
                outputs.into_iter().map(|output| (output, None)).collect(),
            ) {
                Ok(events) => ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Batch(events),
                    Vec::new(),
                ),
                Err(fatal) => ErasedSourceInvocation::fatal(fatal),
            },
            Ok(None) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof, Vec::new()),
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }
}

/// Runtime adapter for a positioned synchronous finite reader (FLOWIP-134i B1).
#[doc(hidden)]
pub struct PositionedFiniteSourceHandlerAdapter<R> {
    reader: SyncSourceReader<R>,
    writer_id: Option<WriterId>,
}

impl<R> PositionedFiniteSourceHandlerAdapter<R> {
    pub(super) fn cold(open: SyncReaderOpener<R>) -> Self {
        Self {
            reader: SourceReader::cold(open),
            writer_id: None,
        }
    }
}

impl<R: PositionedFiniteReader> SealFinite for PositionedFiniteSourceHandlerAdapter<R> {}

impl<R: PositionedFiniteReader + 'static> UnifiedFiniteSourceHandler
    for PositionedFiniteSourceHandlerAdapter<R>
{
    fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        self.reader.acquire(context)?;
        Ok(())
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn input_pin(&self) -> Option<Result<SourceInputPinnedFact, SourcePositionError>> {
        let reader = self.reader.get()?;
        Some(
            EncodedSourcePosition::encode(&reader.beginning()).map(|beginning| {
                SourceInputPinnedFact {
                    identity: reader.input_identity().clone(),
                    beginning,
                }
            }),
        )
    }

    fn resume_at(
        &mut self,
        pin: &SourceInputPinnedFact,
        after: Option<&EncodedSourcePosition>,
    ) -> Result<(), SourceResumeError> {
        let reader = self
            .reader
            .get_mut()
            .expect("the source supervisor must acquire before positioning");
        if let Some(part) = reader.input_identity().changed_part(&pin.identity) {
            return Err(SourceResumeError::Refused(
                SourceContinuationRefusal::InputChanged { part },
            ));
        }
        let position = after
            .unwrap_or(&pin.beginning)
            .decode::<R::Position>()
            .map_err(|_| {
                SourceResumeError::Refused(SourceContinuationRefusal::PositionUnreadable)
            })?;
        reader.seek(position).map_err(SourceResumeError::SeekFailed)
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(writer_id) = self.writer_id else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "positioned source adapter invoked before runtime writer identity installation",
            ));
        };
        let reader = self
            .reader
            .get_mut()
            .expect("the source supervisor must acquire before polling");
        match reader.next() {
            Ok(Some(records)) => match lower_positioned(writer_id, records) {
                Ok(events) => ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Batch(events),
                    Vec::new(),
                ),
                Err(fatal) => ErasedSourceInvocation::fatal(fatal),
            },
            Ok(None) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof, Vec::new()),
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }
}

/// Runtime adapter for a synchronous typed infinite source.
#[doc(hidden)]
pub struct TypedInfiniteSourceHandlerAdapter<H> {
    reader: SyncSourceReader<H>,
    writer_id: Option<WriterId>,
}

impl<H> TypedInfiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self::with_reader(SourceReader::acquired(handler))
    }

    pub(super) fn cold(open: SyncReaderOpener<H>) -> Self {
        Self::with_reader(SourceReader::cold(open))
    }

    fn with_reader(reader: SyncSourceReader<H>) -> Self {
        Self {
            reader,
            writer_id: None,
        }
    }
}

impl<H: TypedInfiniteSourceHandler + 'static>
    admission_sealed::Admitted<dyn UnifiedInfiniteSourceHandler, DirectSource> for H
{
}

impl<H: TypedInfiniteSourceHandler + 'static>
    AdmitSource<dyn UnifiedInfiniteSourceHandler, DirectSource> for H
{
    type Output = H::Output;
    type Handler = TypedInfiniteSourceHandlerAdapter<H>;
    const CONTINUATION: SourceContinuation = SourceContinuation::Reattach;

    fn prepare(self) -> Self::Handler {
        TypedInfiniteSourceHandlerAdapter::cold(Box::new(move |_| Ok(self)))
    }
}

impl<C: InfiniteSourceConnector>
    admission_sealed::Admitted<dyn UnifiedInfiniteSourceHandler, ConnectorSource> for C
{
}

impl<C: InfiniteSourceConnector> AdmitSource<dyn UnifiedInfiniteSourceHandler, ConnectorSource>
    for C
{
    type Output = C::Output;
    type Handler = <C::Continuation as InfiniteContinuation<C::Reader>>::Handler;
    const CONTINUATION: SourceContinuation =
        <C::Continuation as InfiniteContinuation<C::Reader>>::KIND;

    fn prepare(self) -> Self::Handler {
        <C::Continuation as InfiniteContinuation<C::Reader>>::cold(Box::new(move |context| {
            self.open(context)
        }))
    }
}

impl<H: TypedInfiniteSourceHandler> SealInfinite for TypedInfiniteSourceHandlerAdapter<H> {}

impl<H> UnifiedInfiniteSourceHandler for TypedInfiniteSourceHandlerAdapter<H>
where
    H: TypedInfiniteSourceHandler + Send + Sync,
{
    fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        self.reader.acquire(context)?;
        Ok(())
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(writer_id) = self.writer_id else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "typed source adapter invoked before runtime writer identity installation",
            ));
        };
        let handler = self
            .reader
            .get_mut()
            .expect("the source supervisor must acquire before polling");
        match handler.next() {
            Ok(outputs) => match lower_outputs(
                writer_id,
                outputs.into_iter().map(|output| (output, None)).collect(),
            ) {
                Ok(events) => ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Batch(events),
                    Vec::new(),
                ),
                Err(fatal) => ErasedSourceInvocation::fatal(fatal),
            },
            Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
        }
    }
}

/// Runtime adapter for an asynchronous typed finite source.
#[doc(hidden)]
pub struct TypedAsyncFiniteSourceHandlerAdapter<H> {
    reader: AsyncSourceReader<H>,
    writer_id: Option<WriterId>,
    observation_sink: SourceObservationSink,
    recorder: Arc<dyn ObservationRecorder>,
}

impl<H: TypedAsyncFiniteSourceHandler> TypedAsyncFiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self::with_reader(SourceReader::acquired(handler))
    }

    pub(super) fn cold(open: AsyncReaderOpener<H>) -> Self {
        Self::with_reader(SourceReader::cold(open))
    }

    fn with_reader(reader: AsyncSourceReader<H>) -> Self {
        let observation_sink = SourceObservationSink::new();
        Self {
            reader,
            writer_id: None,
            observation_sink,
            recorder: Arc::new(NoObservations),
        }
    }
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    admission_sealed::Admitted<dyn UnifiedAsyncFiniteSourceHandler, DirectSource> for H
{
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    AdmitSource<dyn UnifiedAsyncFiniteSourceHandler, DirectSource> for H
{
    type Output = H::Output;
    type Handler = TypedAsyncFiniteSourceHandlerAdapter<H>;
    const CONTINUATION: SourceContinuation = SourceContinuation::Refuse;

    fn prepare(self) -> Self::Handler {
        TypedAsyncFiniteSourceHandlerAdapter::cold(Box::new(move |_| {
            Box::pin(async move { Ok(self) })
        }))
    }
}

impl<C: AsyncFiniteSourceConnector>
    admission_sealed::Admitted<dyn UnifiedAsyncFiniteSourceHandler, ConnectorSource> for C
{
}

impl<C: AsyncFiniteSourceConnector>
    AdmitSource<dyn UnifiedAsyncFiniteSourceHandler, ConnectorSource> for C
{
    type Output = C::Output;
    type Handler = <C::Continuation as AsyncFiniteContinuation<C::Reader>>::Handler;
    const CONTINUATION: SourceContinuation =
        <C::Continuation as AsyncFiniteContinuation<C::Reader>>::KIND;

    fn prepare(self) -> Self::Handler {
        <C::Continuation as AsyncFiniteContinuation<C::Reader>>::cold(Box::new(move |context| {
            Box::pin(async move { self.open(context).await })
        }))
    }
}

impl<H: TypedAsyncFiniteSourceHandler> SealAsyncFinite for TypedAsyncFiniteSourceHandlerAdapter<H> {}

#[async_trait]
impl<H> UnifiedAsyncFiniteSourceHandler for TypedAsyncFiniteSourceHandlerAdapter<H>
where
    H: TypedAsyncFiniteSourceHandler + Send + Sync,
{
    async fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        if let Some(handler) = self.reader.acquire(context).await? {
            if self.writer_id.is_some() {
                handler.install_source_observation_sink(self.observation_sink.clone());
            }
        }
        Ok(())
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
        if let Some(handler) = self.reader.get_mut() {
            handler.install_source_observation_sink(self.observation_sink.clone());
        }
    }

    fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
        self.recorder = recorder;
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.reader
            .get()
            .map_or(Some(Duration::from_secs(30)), |handler| {
                handler.poll_timeout()
            })
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(writer_id) = self.writer_id else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "typed async source adapter invoked before runtime writer identity installation",
            ));
        };
        let scope = match self.observation_sink.begin_poll() {
            Ok(scope) => scope,
            Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
        };
        let result = self
            .reader
            .get_mut()
            .expect("the source supervisor must acquire before polling")
            .next()
            .await;
        let snapshot = match scope.finish() {
            Ok(snapshot) => snapshot,
            Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
        };
        let observations = snapshot
            .map(|snapshot| {
                vec![http_pull_evidence(
                    writer_id,
                    snapshot,
                    self.recorder.as_ref(),
                )]
            })
            .unwrap_or_default();
        match result {
            Ok(Some(outputs)) => match lower_outputs(
                writer_id,
                outputs.into_iter().map(|output| (output, None)).collect(),
            ) {
                Ok(events) => ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Batch(events),
                    observations,
                ),
                Err(fatal) => ErasedSourceInvocation::fatal(fatal),
            },
            Ok(None) => {
                ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof, observations)
            }
            Err(error) => ErasedSourceInvocation::handler_error(error, observations),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        match self.reader.get_mut() {
            Some(handler) => handler.drain().await,
            None => Ok(()),
        }
    }
}

/// Runtime adapter for an asynchronous typed infinite source.
#[doc(hidden)]
pub struct TypedAsyncInfiniteSourceHandlerAdapter<H> {
    reader: AsyncSourceReader<H>,
    writer_id: Option<WriterId>,
    observation_sink: SourceObservationSink,
    recorder: Arc<dyn ObservationRecorder>,
    hosted_slot: Option<HostedIngressBindingSlot>,
    /// Hosted ingress only: the single writer's identity index (FLOWIP-084l).
    admission: Option<HostedAdmission>,
}

impl<H: TypedAsyncInfiniteSourceHandler> TypedAsyncInfiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        let hosted_slot = handler
            .runtime_registration()
            .map(|registration| registration.hosted_ingress_slot);
        Self::with_reader(SourceReader::acquired(handler), hosted_slot)
    }

    /// A cold connector reader; hosted ingress is a direct handler only.
    pub(super) fn cold(open: AsyncReaderOpener<H>) -> Self {
        Self::with_reader(SourceReader::cold(open), None)
    }

    fn with_reader(
        reader: AsyncSourceReader<H>,
        hosted_slot: Option<HostedIngressBindingSlot>,
    ) -> Self {
        let observation_sink = SourceObservationSink::new();
        let admission = hosted_slot.clone().map(HostedAdmission::new);
        Self {
            reader,
            writer_id: None,
            observation_sink,
            recorder: Arc::new(NoObservations),
            hosted_slot,
            admission,
        }
    }
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    admission_sealed::Admitted<dyn UnifiedAsyncInfiniteSourceHandler, DirectSource> for H
{
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    AdmitSource<dyn UnifiedAsyncInfiniteSourceHandler, DirectSource> for H
{
    type Output = H::Output;
    type Handler = TypedAsyncInfiniteSourceHandlerAdapter<H>;
    const CONTINUATION: SourceContinuation = SourceContinuation::Reattach;

    fn prepare(self) -> Self::Handler {
        let hosted_slot = self
            .runtime_registration()
            .map(|registration| registration.hosted_ingress_slot);
        TypedAsyncInfiniteSourceHandlerAdapter::with_reader(
            SourceReader::cold(Box::new(move |_| Box::pin(async move { Ok(self) }))),
            hosted_slot,
        )
    }
}

impl<C: AsyncInfiniteSourceConnector>
    admission_sealed::Admitted<dyn UnifiedAsyncInfiniteSourceHandler, ConnectorSource> for C
{
}

impl<C: AsyncInfiniteSourceConnector>
    AdmitSource<dyn UnifiedAsyncInfiniteSourceHandler, ConnectorSource> for C
{
    type Output = C::Output;
    type Handler = <C::Continuation as AsyncInfiniteContinuation<C::Reader>>::Handler;
    const CONTINUATION: SourceContinuation =
        <C::Continuation as AsyncInfiniteContinuation<C::Reader>>::KIND;

    fn prepare(self) -> Self::Handler {
        <C::Continuation as AsyncInfiniteContinuation<C::Reader>>::cold(Box::new(move |context| {
            Box::pin(async move { self.open(context).await })
        }))
    }
}

impl<H: TypedAsyncInfiniteSourceHandler> SealAsyncInfinite
    for TypedAsyncInfiniteSourceHandlerAdapter<H>
{
}

#[async_trait]
impl<H> UnifiedAsyncInfiniteSourceHandler for TypedAsyncInfiniteSourceHandlerAdapter<H>
where
    H: TypedAsyncInfiniteSourceHandler + Send + Sync,
{
    async fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
        if let Some(handler) = self.reader.acquire(context).await? {
            if self.writer_id.is_some() {
                handler.install_source_observation_sink(self.observation_sink.clone());
            }
        }
        Ok(())
    }

    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
        if let Some(handler) = self.reader.get_mut() {
            handler.install_source_observation_sink(self.observation_sink.clone());
        }
    }

    fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
        self.recorder = recorder;
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.reader.get().and_then(|handler| handler.poll_timeout())
    }

    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        self.hosted_slot.clone()
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(writer_id) = self.writer_id else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "typed async source adapter invoked before runtime writer identity installation",
            ));
        };
        let scope = match self.observation_sink.begin_poll() {
            Ok(scope) => scope,
            Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
        };
        let result = self
            .reader
            .get_mut()
            .expect("the source supervisor must acquire before polling")
            .next_invocation()
            .await;
        let snapshot = match scope.finish() {
            Ok(snapshot) => snapshot,
            Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
        };
        let observations = snapshot
            .map(|snapshot| {
                vec![http_pull_evidence(
                    writer_id,
                    snapshot,
                    self.recorder.as_ref(),
                )]
            })
            .unwrap_or_default();
        let lowered = match result {
            Ok(invocation) => match (invocation.outputs, self.admission.as_mut()) {
                (InvocationOutputs::Facts(outputs), _) => lower_outputs(
                    writer_id,
                    outputs.into_iter().map(|output| (output, None)).collect(),
                ),
                (InvocationOutputs::Hosted(submissions), Some(admission)) => {
                    admission.admit(writer_id, submissions)
                }
                (InvocationOutputs::Hosted(_), None) => Err(configuration_fatal(
                    "hosted ingress submissions reached a source without a hosted registration",
                )),
            },
            Err(error) => return ErasedSourceInvocation::handler_error(error, observations),
        };
        match lowered {
            Ok(events) => ErasedSourceInvocation::completed(
                ErasedSourceCompletion::Batch(events),
                observations,
            ),
            Err(fatal) => ErasedSourceInvocation::fatal(fatal),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        match self.reader.get_mut() {
            Some(handler) => handler.drain().await,
            None => Ok(()),
        }
    }

    fn settle_committed(&mut self, committed: &[EventId]) {
        if let Some(admission) = self.admission.as_mut() {
            admission.settle(committed);
        }
    }

    fn fold_recorded_admission(&mut self, event: &ChainEvent) {
        if let Some(admission) = self.admission.as_mut() {
            admission.fold(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::erased::ErasedSourceOutcome;
    use super::*;
    use crate::execution::{RuntimeExecution, RuntimeMode};
    use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::{FlowId, OneFactStageOutput, StageId, StageOutputFacts, TypedPayload};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Row(u32);

    impl TypedPayload for Row {
        const EVENT_TYPE: &'static str = "typed_source.row";
    }

    #[derive(Clone)]
    struct OffsetIngress {
        offset: u32,
    }

    impl IngressDecoder for OffsetIngress {
        type Output = Row;

        fn decode(&self, record: IngressRecord<'_>) -> Result<Self::Output, IngressDecodeError> {
            let row = record.deserialize::<u32>()?;
            if row == 0 {
                return Err(IngressDecodeError::invalid_value("row"));
            }
            Ok(Row(row + self.offset))
        }
    }

    #[derive(Clone, Debug)]
    struct OneRow;

    impl TypedFiniteSourceHandler for OneRow {
        type Output = Row;

        fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            Ok(Some(vec![Row(7)]))
        }
    }

    #[test]
    fn finite_adapter_owns_canonical_envelope_and_writer() {
        let writer_id = WriterId::from(StageId::new());
        let mut adapter = TypedFiniteSourceHandlerAdapter::new(OneRow);
        UnifiedFiniteSourceHandler::install_writer_id(&mut adapter, writer_id);

        let (outcome, observations) =
            UnifiedFiniteSourceHandler::next_invocation(&mut adapter).into_parts();
        let ErasedSourceOutcome::Completed(ErasedSourceCompletion::Batch(events)) = outcome else {
            panic!("expected batch")
        };
        assert!(observations.is_empty());
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].writer_id, writer_id);
        assert_eq!(events[0].event_type(), Row::event_type_name());
        assert_eq!(Row::from_event(&events[0]), Some(Row(7)));
    }

    #[test]
    fn missing_writer_fails_before_handler_poll() {
        #[derive(Clone, Debug)]
        struct Counted {
            calls: Arc<AtomicUsize>,
        }

        impl TypedFiniteSourceHandler for Counted {
            type Output = Row;

            fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(Some(vec![Row(1)]))
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let mut adapter = TypedFiniteSourceHandlerAdapter::new(Counted {
            calls: calls.clone(),
        });
        let (outcome, observations) =
            UnifiedFiniteSourceHandler::next_invocation(&mut adapter).into_parts();
        let ErasedSourceOutcome::Fatal(fatal) = outcome else {
            panic!("expected fatal")
        };
        assert!(observations.is_empty());
        assert_eq!(fatal.code, StageFatalCode::Configuration);
        assert_eq!(fatal.reason, StageFatalReason::ConfigurationInvariant);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct OtherRow(u32);

    impl TypedPayload for OtherRow {
        const EVENT_TYPE: &'static str = "typed_source.other_row";
    }

    #[derive(Clone, Debug, StageOutputFacts)]
    struct DishonestProduct {
        first: Row,
        second: OtherRow,
    }

    // Deliberately false semantic assertion: the adapter must detect that
    // this value lowers to two facts and commit neither one.
    impl OneFactStageOutput for DishonestProduct {}

    #[derive(Clone, Debug)]
    struct DishonestSource;

    impl TypedFiniteSourceHandler for DishonestSource {
        type Output = DishonestProduct;

        fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            Ok(Some(vec![DishonestProduct {
                first: Row(1),
                second: OtherRow(2),
            }]))
        }
    }

    #[test]
    fn lowering_contradiction_is_atomic_framework_fatal() {
        let mut adapter = TypedFiniteSourceHandlerAdapter::new(DishonestSource);
        UnifiedFiniteSourceHandler::install_writer_id(&mut adapter, WriterId::from(StageId::new()));

        let (outcome, observations) =
            UnifiedFiniteSourceHandler::next_invocation(&mut adapter).into_parts();
        let ErasedSourceOutcome::Fatal(fatal) = outcome else {
            panic!("dishonest one-fact output must fail atomically")
        };
        assert!(observations.is_empty());
        assert_eq!(fatal.code, StageFatalCode::Protocol);
        assert_eq!(fatal.reason, StageFatalReason::ProtocolInputIntegrity);
        assert!(fatal.detail.contains("lowered to 2 facts"));
    }

    #[derive(Clone, Debug)]
    struct SnapshotThenValidation {
        sink: Option<SourceObservationSink>,
    }

    #[async_trait]
    impl TypedAsyncFiniteSourceHandler for SnapshotThenValidation {
        type Output = Row;

        async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            let snapshot = HttpPullTelemetry {
                requests_total: 3,
                ..HttpPullTelemetry::default()
            };
            self.sink
                .as_ref()
                .expect("runtime installs observation sink")
                .report_http_pull(snapshot);
            Err(SourceError::Validation(
                SourceDiagnosticReason::InvalidRecord.into(),
            ))
        }

        fn install_source_observation_sink(&mut self, sink: SourceObservationSink) {
            self.sink = Some(sink);
        }
    }

    #[tokio::test]
    async fn same_poll_snapshot_survives_validation_error() {
        let writer_id = WriterId::from(StageId::new());
        let mut adapter =
            TypedAsyncFiniteSourceHandlerAdapter::new(SnapshotThenValidation { sink: None });
        UnifiedAsyncFiniteSourceHandler::install_writer_id(&mut adapter, writer_id);
        let execution = RuntimeExecution::new(RuntimeMode::Live, None);
        UnifiedAsyncFiniteSourceHandler::install_observation_recorder(
            &mut adapter,
            execution.observation_recorder(FlowId::new(), writer_id),
        );

        let (outcome, observations) =
            UnifiedAsyncFiniteSourceHandler::next_invocation(&mut adapter)
                .await
                .into_parts();
        assert!(matches!(
            outcome,
            ErasedSourceOutcome::HandlerError(SourceError::Validation(ref diagnostic))
                if diagnostic.reason() == SourceDiagnosticReason::InvalidRecord
        ));
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].writer_id, writer_id);
        assert!(matches!(
            &observations[0].payload,
            ChainPayload::Execution(ExecutionPayload::HttpPullState(_))
        ));
        use obzenflow_core::event::observability::{ObservationRecord, ObservationSource};
        assert!(execution.observations().snapshot().iter().flat_map(|packet| &packet.records)
            .any(|record| matches!(record, ObservationRecord::HttpPull(snapshot) if snapshot.requests_total == 3)));
    }

    #[derive(Clone, Debug)]
    struct MisusesCapabilityOnInstall {
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl TypedAsyncFiniteSourceHandler for MisusesCapabilityOnInstall {
        type Output = Row;

        async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(vec![Row(9)]))
        }

        fn install_source_observation_sink(&mut self, sink: SourceObservationSink) {
            sink.report_http_pull(HttpPullTelemetry::default());
        }
    }

    #[tokio::test]
    async fn observation_misuse_fails_before_poll_with_zero_output() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut adapter = TypedAsyncFiniteSourceHandlerAdapter::new(MisusesCapabilityOnInstall {
            calls: calls.clone(),
        });
        UnifiedAsyncFiniteSourceHandler::install_writer_id(
            &mut adapter,
            WriterId::from(StageId::new()),
        );

        let (outcome, observations) =
            UnifiedAsyncFiniteSourceHandler::next_invocation(&mut adapter)
                .await
                .into_parts();
        let ErasedSourceOutcome::Fatal(fatal) = outcome else {
            panic!("out-of-poll capability use must be fatal")
        };
        assert!(observations.is_empty());
        assert_eq!(fatal.code, StageFatalCode::Protocol);
        assert_eq!(fatal.reason, StageFatalReason::ProtocolInputIntegrity);
        assert!(fatal.detail.contains("outside the live poll"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn decoder_rejections_keep_a_typed_diagnostic_without_input_text() {
        let decoder = OffsetIngress { offset: 40 };
        assert_eq!(
            decoder.decode(IngressRecord::new(&serde_json::json!(2))),
            Ok(Row(42))
        );

        let rule = decoder
            .decode(IngressRecord::new(&serde_json::json!(0)))
            .unwrap_err()
            .into_diagnostic();
        assert_eq!(rule.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(
            rule.location().field_path(),
            [obzenflow_core::event::FieldSegment::Field(
                obzenflow_core::event::FieldName::new("row")
            )]
        );

        let malformed = decoder
            .decode(IngressRecord::new(&serde_json::json!("SECRET_VALUE")))
            .unwrap_err()
            .into_diagnostic();
        assert_eq!(malformed.reason(), SourceDiagnosticReason::InvalidValue);
        assert!(!format!("{malformed} {malformed:?}").contains("SECRET"));
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Tagged {
        name: String,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
    }

    impl TypedPayload for Tagged {
        const EVENT_TYPE: &'static str = "typed_source.tagged";
    }

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Measured {
        value: f64,
    }

    impl TypedPayload for Measured {
        const EVENT_TYPE: &'static str = "typed_source.measured";
    }

    #[test]
    fn journal_form_proof_returns_the_decoded_copy() {
        let tagged = Tagged {
            name: "a".into(),
            tags: vec!["x".into()],
        };
        assert_eq!(prove_journal_form(tagged.clone()), Ok(tagged));
    }

    #[test]
    fn unrepresentable_values_name_the_failing_field() {
        let skipped = prove_journal_form(Tagged {
            name: "a".into(),
            tags: Vec::new(),
        })
        .unwrap_err()
        .into_diagnostic();
        assert_eq!(
            skipped.reason(),
            SourceDiagnosticReason::UnrepresentableValue
        );
        assert_eq!(
            skipped.location().field_path(),
            [obzenflow_core::event::FieldSegment::Field(
                obzenflow_core::event::FieldName::new("tags")
            )]
        );

        let non_finite = prove_journal_form(Measured { value: f64::NAN })
            .unwrap_err()
            .into_diagnostic();
        assert_eq!(
            non_finite.reason(),
            SourceDiagnosticReason::UnrepresentableValue
        );
    }
}
