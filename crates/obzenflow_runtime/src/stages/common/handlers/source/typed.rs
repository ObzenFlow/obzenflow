// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed source authoring and runtime envelope lowering (FLOWIP-134g).

use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite, SealAsyncInfinite, SealFinite,
    SealInfinite, UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
    UnifiedFiniteSourceHandler, UnifiedInfiniteSourceHandler,
};
use super::SourceError;
use crate::stages::common::handler_error::StageFatal;
use crate::typing::SourceTyping;
use async_trait::async_trait;
use obzenflow_core::event::observability::HttpPullTelemetry;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{ChainEventFactory, StageFatalCode, StageFatalReason};
use obzenflow_core::ingress::{
    EventSubmission, HostedIngressBindingSlot, IngressContext, SubmissionIngressContext,
    SubmissionPayloadKind,
};
use obzenflow_core::{ChainEvent, OneFactStageOutput, TypedPayload, WriterId};
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

fn protocol_fatal(detail: impl Into<String>) -> StageFatal {
    StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    )
}

fn lower_outputs<O>(
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
        let mut event = ChainEventFactory::data_event(writer_id, fact.event_type, fact.payload);
        event.ingress_context = ingress_context;
        events.push(event);
    }
    Ok(events)
}

fn observation_event(writer_id: WriterId, snapshot: HttpPullTelemetry) -> ChainEvent {
    ChainEventFactory::observability_event(
        writer_id,
        ObservabilityPayload::Metrics(MetricsLifecycle::HttpPullSnapshot { snapshot }),
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
    hosted_ingress_slot: HostedIngressBindingSlot,
}

impl fmt::Debug for SourceRuntimeRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceRuntimeRegistration")
            .field("ingress_key", self.hosted_ingress_slot.ingress_key())
            .finish()
    }
}

impl SourceRuntimeRegistration {
    fn hosted(slot: HostedIngressBindingSlot) -> Self {
        Self {
            hosted_ingress_slot: slot,
        }
    }
}

/// One typed async-infinite invocation before runtime envelope lowering.
#[doc(hidden)]
pub struct TypedAsyncInfiniteSourceInvocation<O> {
    outputs: Vec<(O, Option<IngressContext>)>,
}

impl<O> TypedAsyncInfiniteSourceInvocation<O> {
    fn facts_only(outputs: Vec<O>) -> Self {
        Self {
            outputs: outputs.into_iter().map(|output| (output, None)).collect(),
        }
    }

    fn hosted(outputs: Vec<(O, IngressContext)>) -> Self {
        Self {
            outputs: outputs
                .into_iter()
                .map(|(output, context)| (output, Some(context)))
                .collect(),
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

/// Error returned by an application-owned [`IngressDecoder`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct IngressDecodeError {
    message: String,
}

impl IngressDecodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<serde_json::Error> for IngressDecodeError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(error.to_string())
    }
}

/// User-owned mapping from an admitted ingress body to one domain output.
///
/// The decoder value owns the source's `Output` contract. The default method
/// performs ordinary serde JSON decoding; applications can override it for an
/// external representation that still maps to the declared domain output.
/// Implementations must be deterministic, side-effect free, and behaviourally
/// equivalent across clones: hosted HTTP ingress invokes the decoder once for
/// admission validation and again when the accepted submission reaches the source.
/// Typed `IngressHandle` submissions already have the `Output` shape and bypass
/// this external-representation mapping.
pub trait IngressDecoder: Clone + Send + Sync + 'static {
    type Output: TypedPayload + Send + Sync + 'static;

    fn decode(&self, data: serde_json::Value) -> Result<Self::Output, IngressDecodeError> {
        serde_json::from_value(data).map_err(IngressDecodeError::from)
    }
}

/// Runtime-owned typed hosted-ingress queue core.
pub struct HostedIngressSource<D> {
    rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<EventSubmission>>>,
    slot: HostedIngressBindingSlot,
    max_batch_size: usize,
    decoder: D,
}

impl<D> Clone for HostedIngressSource<D>
where
    D: Clone,
{
    fn clone(&self) -> Self {
        Self {
            rx: Arc::clone(&self.rx),
            slot: self.slot.clone(),
            max_batch_size: self.max_batch_size,
            decoder: self.decoder.clone(),
        }
    }
}

impl<D> fmt::Debug for HostedIngressSource<D>
where
    D: IngressDecoder,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedIngressSource")
            .field("decoder", &std::any::type_name::<D>())
            .field("output", &std::any::type_name::<D::Output>())
            .field("ingress_key", self.slot.ingress_key())
            .field("max_batch_size", &self.max_batch_size)
            .finish_non_exhaustive()
    }
}

impl<D> HostedIngressSource<D> {
    pub fn new(
        decoder: D,
        rx: tokio::sync::mpsc::Receiver<EventSubmission>,
        slot: HostedIngressBindingSlot,
    ) -> Self {
        Self {
            rx: Arc::new(tokio::sync::Mutex::new(rx)),
            slot,
            max_batch_size: 1000,
            decoder,
        }
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size.max(1);
        self
    }
}

impl<D> SourceTyping for HostedIngressSource<D>
where
    D: IngressDecoder,
{
    type Output = D::Output;
}

impl<D> HostedIngressSource<D>
where
    D: IngressDecoder,
{
    async fn receive_batch(&mut self) -> Result<Vec<(D::Output, IngressContext)>, SourceError> {
        let mut rx = self.rx.lock().await;
        let first = rx.recv().await.ok_or_else(|| {
            SourceError::Transport("hosted ingress source channel closed".to_string())
        })?;
        let mut submissions = vec![first];
        while submissions.len() < self.max_batch_size {
            match rx.try_recv() {
                Ok(submission) => submissions.push(submission),
                Err(_) => break,
            }
        }

        let expected_key = self.slot.ingress_key();
        let mut decoded = Vec::with_capacity(submissions.len());
        for submission in submissions {
            if !D::Output::event_type_matches(submission.event_type.as_str()) {
                return Err(SourceError::Validation(format!(
                    "hosted ingress event type `{}` is outside configured output `{}`",
                    submission.event_type,
                    D::Output::versioned_event_type(),
                )));
            }
            let SubmissionIngressContext {
                accepted_at_ns,
                ingress_key,
                batch_index,
                attempt_seq,
                payload_kind,
            } = submission.ingress_handoff.ok_or_else(|| {
                SourceError::Validation(
                    "hosted ingress submission is missing framework ingress context".to_string(),
                )
            })?;
            if &ingress_key != expected_key {
                return Err(SourceError::Validation(format!(
                    "hosted ingress context key `{ingress_key}` does not match configured key `{expected_key}`"
                )));
            }
            let value = match payload_kind {
                SubmissionPayloadKind::External => self.decoder.decode(submission.data),
                SubmissionPayloadKind::TypedOutput => {
                    serde_json::from_value(submission.data).map_err(IngressDecodeError::from)
                }
            }
            .map_err(|error| {
                SourceError::Deserialization(format!(
                    "hosted ingress `{}` decode failed: {error}",
                    D::Output::versioned_event_type()
                ))
            })?;
            decoded.push((
                value,
                IngressContext {
                    accepted_at_ns,
                    ingress_key,
                    batch_index,
                    attempt_seq,
                },
            ));
        }
        Ok(decoded)
    }
}

#[async_trait]
impl<D> TypedAsyncInfiniteSourceHandler for HostedIngressSource<D>
where
    D: IngressDecoder,
{
    type Output = D::Output;

    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(self
            .receive_batch()
            .await?
            .into_iter()
            .map(|(output, _)| output)
            .collect())
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.rx.lock().await.close();
        Ok(())
    }

    fn runtime_registration(&self) -> Option<SourceRuntimeRegistration> {
        Some(SourceRuntimeRegistration::hosted(self.slot.clone()))
    }

    async fn next_invocation(
        &mut self,
    ) -> Result<TypedAsyncInfiniteSourceInvocation<Self::Output>, SourceError> {
        self.receive_batch()
            .await
            .map(TypedAsyncInfiniteSourceInvocation::hosted)
    }
}

macro_rules! sync_adapter {
    ($name:ident, $typed:ident, $seal:ident, $unified:ident, $finite:literal) => {
        #[doc(hidden)]
        #[derive(Clone, Debug)]
        pub struct $name<H> {
            handler: H,
            writer_id: Option<WriterId>,
        }

        impl<H> $name<H> {
            pub fn new(handler: H) -> Self {
                Self {
                    handler,
                    writer_id: None,
                }
            }
        }

        impl<H: $typed> $seal for $name<H> {}

        impl<H> $unified for $name<H>
        where
            H: $typed + Send + Sync,
        {
            fn install_writer_id(&mut self, writer_id: WriterId) {
                self.writer_id = Some(writer_id);
            }

            fn next_invocation(&mut self) -> ErasedSourceInvocation {
                let Some(writer_id) = self.writer_id else {
                    return ErasedSourceInvocation::fatal(configuration_fatal(
                        "typed source adapter invoked before runtime writer identity installation",
                    ));
                };
                let result = self.handler.next();
                if $finite {
                    match result {
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
                        Ok(None) => ErasedSourceInvocation::completed(
                            ErasedSourceCompletion::Eof,
                            Vec::new(),
                        ),
                        Err(error) => ErasedSourceInvocation::handler_error(error, Vec::new()),
                    }
                } else {
                    unreachable!("infinite adapter uses its explicit implementation")
                }
            }
        }
    };
}

// The finite macro keeps the repetitive adapter plumbing small. The infinite
// form has a different return type and is written explicitly below.
sync_adapter!(
    TypedFiniteSourceHandlerAdapter,
    TypedFiniteSourceHandler,
    SealFinite,
    UnifiedFiniteSourceHandler,
    true
);

/// Runtime adapter for a synchronous typed infinite source.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedInfiniteSourceHandlerAdapter<H> {
    handler: H,
    writer_id: Option<WriterId>,
}

impl<H> TypedInfiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            writer_id: None,
        }
    }
}

impl<H: TypedInfiniteSourceHandler> SealInfinite for TypedInfiniteSourceHandlerAdapter<H> {}

impl<H> UnifiedInfiniteSourceHandler for TypedInfiniteSourceHandlerAdapter<H>
where
    H: TypedInfiniteSourceHandler + Send + Sync,
{
    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn next_invocation(&mut self) -> ErasedSourceInvocation {
        let Some(writer_id) = self.writer_id else {
            return ErasedSourceInvocation::fatal(configuration_fatal(
                "typed source adapter invoked before runtime writer identity installation",
            ));
        };
        match self.handler.next() {
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
#[derive(Clone, Debug)]
pub struct TypedAsyncFiniteSourceHandlerAdapter<H> {
    handler: H,
    writer_id: Option<WriterId>,
    observation_sink: SourceObservationSink,
}

impl<H: TypedAsyncFiniteSourceHandler> TypedAsyncFiniteSourceHandlerAdapter<H> {
    pub fn new(mut handler: H) -> Self {
        let observation_sink = SourceObservationSink::new();
        handler.install_source_observation_sink(observation_sink.clone());
        Self {
            handler,
            writer_id: None,
            observation_sink,
        }
    }
}

impl<H: TypedAsyncFiniteSourceHandler> SealAsyncFinite for TypedAsyncFiniteSourceHandlerAdapter<H> {}

#[async_trait]
impl<H> UnifiedAsyncFiniteSourceHandler for TypedAsyncFiniteSourceHandlerAdapter<H>
where
    H: TypedAsyncFiniteSourceHandler + Send + Sync,
{
    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.handler.poll_timeout()
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
        let result = self.handler.next().await;
        let snapshot = match scope.finish() {
            Ok(snapshot) => snapshot,
            Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
        };
        let observations = snapshot
            .map(|snapshot| vec![observation_event(writer_id, snapshot)])
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
        self.handler.drain().await
    }
}

/// Runtime adapter for an asynchronous typed infinite source.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedAsyncInfiniteSourceHandlerAdapter<H> {
    handler: H,
    writer_id: Option<WriterId>,
    observation_sink: SourceObservationSink,
    registration: Option<SourceRuntimeRegistration>,
}

impl<H: TypedAsyncInfiniteSourceHandler> TypedAsyncInfiniteSourceHandlerAdapter<H> {
    pub fn new(mut handler: H) -> Self {
        let registration = handler.runtime_registration();
        let observation_sink = SourceObservationSink::new();
        handler.install_source_observation_sink(observation_sink.clone());
        Self {
            handler,
            writer_id: None,
            observation_sink,
            registration,
        }
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
    fn install_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = Some(writer_id);
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.handler.poll_timeout()
    }

    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        self.registration
            .as_ref()
            .map(|registration| registration.hosted_ingress_slot.clone())
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
        let result = self.handler.next_invocation().await;
        let snapshot = match scope.finish() {
            Ok(snapshot) => snapshot,
            Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
        };
        let observations = snapshot
            .map(|snapshot| vec![observation_event(writer_id, snapshot)])
            .unwrap_or_default();
        match result {
            Ok(invocation) => match lower_outputs(writer_id, invocation.outputs) {
                Ok(events) => ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Batch(events),
                    observations,
                ),
                Err(fatal) => ErasedSourceInvocation::fatal(fatal),
            },
            Err(error) => ErasedSourceInvocation::handler_error(error, observations),
        }
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.handler.drain().await
    }
}

#[cfg(test)]
mod tests {
    use super::super::erased::ErasedSourceOutcome;
    use super::*;
    use obzenflow_core::event::ChainEventContent;
    use obzenflow_core::ingress::{IngressAttemptSeq, IngressKey};
    use obzenflow_core::{OneFactStageOutput, StageId, StageOutputFacts, TypedPayload};
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Row(u32);

    impl TypedPayload for Row {
        const EVENT_TYPE: &'static str = "typed_source.row";
    }

    #[derive(Clone, Debug)]
    struct RowIngress;

    impl IngressDecoder for RowIngress {
        type Output = Row;
    }

    #[derive(Clone)]
    struct OffsetIngress {
        offset: u32,
    }

    impl IngressDecoder for OffsetIngress {
        type Output = Row;

        fn decode(&self, data: serde_json::Value) -> Result<Self::Output, IngressDecodeError> {
            let row = serde_json::from_value::<u32>(data)?;
            Ok(Row(row + self.offset))
        }
    }

    fn assert_row_output<S>(_source: &S)
    where
        S: SourceTyping<Output = Row>,
    {
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
        assert_eq!(events[0].event_type(), Row::versioned_event_type());
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
            Err(SourceError::Validation("bad decoded row".to_string()))
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

        let (outcome, observations) =
            UnifiedAsyncFiniteSourceHandler::next_invocation(&mut adapter)
                .await
                .into_parts();
        assert!(matches!(
            outcome,
            ErasedSourceOutcome::HandlerError(SourceError::Validation(ref message))
                if message == "bad decoded row"
        ));
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].writer_id, writer_id);
        let ChainEventContent::Observability(ObservabilityPayload::Metrics(
            MetricsLifecycle::HttpPullSnapshot { snapshot },
        )) = &observations[0].content
        else {
            panic!("expected typed HTTP pull snapshot")
        };
        assert_eq!(snapshot.requests_total, 3);
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

    fn hosted_submission(
        row: u32,
        accepted_at_ns: u64,
        ingress_key: &str,
        batch_index: Option<usize>,
        attempt_seq: u64,
    ) -> EventSubmission {
        EventSubmission {
            event_type: Row::versioned_event_type().into(),
            data: serde_json::json!(row),
            metadata: None,
            ingress_handoff: Some(SubmissionIngressContext {
                accepted_at_ns,
                ingress_key: IngressKey::from(ingress_key),
                batch_index,
                attempt_seq: IngressAttemptSeq(attempt_seq),
                payload_kind: SubmissionPayloadKind::External,
            }),
        }
    }

    #[tokio::test]
    async fn hosted_ingress_rejoins_exact_single_and_batch_context_at_runtime_lowering() {
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let slot = HostedIngressBindingSlot::new("bank.accounts");
        let source = HostedIngressSource::new(RowIngress, rx, slot.clone());
        let writer_id = WriterId::from(StageId::new());
        let mut adapter = TypedAsyncInfiniteSourceHandlerAdapter::new(source);

        let registered = UnifiedAsyncInfiniteSourceHandler::hosted_ingress_slot(&adapter)
            .expect("hosted registration is sampled by the adapter");
        assert_eq!(registered.ingress_key(), slot.ingress_key());
        UnifiedAsyncInfiniteSourceHandler::install_writer_id(&mut adapter, writer_id);

        tx.send(hosted_submission(7, 101, "bank.accounts", None, 11))
            .await
            .expect("single submission queued");
        let (outcome, observations) =
            UnifiedAsyncInfiniteSourceHandler::next_invocation(&mut adapter)
                .await
                .into_parts();
        let ErasedSourceOutcome::Completed(ErasedSourceCompletion::Batch(single)) = outcome else {
            panic!("single hosted submission lowers to one batch")
        };
        assert!(observations.is_empty());
        assert_eq!(single.len(), 1);
        assert_eq!(single[0].writer_id, writer_id);
        assert_eq!(single[0].event_type(), Row::versioned_event_type());
        assert_eq!(Row::from_event(&single[0]), Some(Row(7)));
        assert_eq!(
            single[0].ingress_context,
            Some(IngressContext {
                accepted_at_ns: 101,
                ingress_key: IngressKey::from("bank.accounts"),
                batch_index: None,
                attempt_seq: IngressAttemptSeq(11),
            })
        );

        tx.send(hosted_submission(8, 202, "bank.accounts", Some(0), 12))
            .await
            .expect("first batch row queued");
        tx.send(hosted_submission(9, 203, "bank.accounts", Some(1), 12))
            .await
            .expect("second batch row queued");
        let (outcome, observations) =
            UnifiedAsyncInfiniteSourceHandler::next_invocation(&mut adapter)
                .await
                .into_parts();
        let ErasedSourceOutcome::Completed(ErasedSourceCompletion::Batch(batch)) = outcome else {
            panic!("hosted batch lowers atomically")
        };
        assert!(observations.is_empty());
        assert_eq!(batch.len(), 2);
        assert_eq!(Row::from_event(&batch[0]), Some(Row(8)));
        assert_eq!(Row::from_event(&batch[1]), Some(Row(9)));
        assert_eq!(
            batch
                .iter()
                .map(|event| event.ingress_context.clone().expect("context retained"))
                .collect::<Vec<_>>(),
            vec![
                IngressContext {
                    accepted_at_ns: 202,
                    ingress_key: IngressKey::from("bank.accounts"),
                    batch_index: Some(0),
                    attempt_seq: IngressAttemptSeq(12),
                },
                IngressContext {
                    accepted_at_ns: 203,
                    ingress_key: IngressKey::from("bank.accounts"),
                    batch_index: Some(1),
                    attempt_seq: IngressAttemptSeq(12),
                },
            ]
        );
        assert!(batch.iter().all(|event| event.writer_id == writer_id));
    }

    #[tokio::test]
    async fn hosted_ingress_decoder_value_owns_and_decodes_output() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let slot = HostedIngressBindingSlot::new("bank.offset");
        let mut source = HostedIngressSource::new(OffsetIngress { offset: 40 }, rx, slot);
        assert_row_output(&source);

        let source_debug = format!("{source:?}");
        assert!(source_debug.contains("OffsetIngress"));
        assert!(!source_debug.contains("offset: 40"));

        tx.send(hosted_submission(2, 301, "bank.offset", None, 21))
            .await
            .expect("submission queued");
        assert_eq!(source.next().await.expect("decoded batch"), vec![Row(42)]);
    }

    #[tokio::test]
    async fn hosted_ingress_rejects_a_mixed_batch_atomically() {
        let (tx, rx) = tokio::sync::mpsc::channel(2);
        let slot = HostedIngressBindingSlot::new("bank.accounts");
        let source = HostedIngressSource::new(RowIngress, rx, slot);
        let mut adapter = TypedAsyncInfiniteSourceHandlerAdapter::new(source);
        UnifiedAsyncInfiniteSourceHandler::install_writer_id(
            &mut adapter,
            WriterId::from(StageId::new()),
        );

        tx.send(hosted_submission(1, 301, "bank.accounts", Some(0), 21))
            .await
            .expect("valid row queued");
        let mut invalid = hosted_submission(2, 302, "bank.accounts", Some(1), 21);
        invalid.event_type = OtherRow::versioned_event_type().into();
        tx.send(invalid).await.expect("invalid row queued");

        let (outcome, observations) =
            UnifiedAsyncInfiniteSourceHandler::next_invocation(&mut adapter)
                .await
                .into_parts();
        assert!(matches!(
            outcome,
            ErasedSourceOutcome::HandlerError(SourceError::Validation(ref message))
                if message.contains("outside configured output")
        ));
        assert!(observations.is_empty(), "no partial batch may escape");
    }
}
