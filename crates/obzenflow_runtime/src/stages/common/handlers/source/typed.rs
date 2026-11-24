// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite, SealAsyncInfinite,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
};
use super::read::{read_failure, Continuous, DrivenRead, Finite, NotReady, ReadEnd, SourceRead};
use super::record_de::deserialize_json;
use super::SourceError;
use crate::stages::common::handler_error::StageFatal;
use async_trait::async_trait;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::{
    SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode, StageFatalCode, StageFatalReason,
};
use obzenflow_core::{ChainEvent, OneFactOutcome, TypedPayload};
use serde::de::DeserializeOwned;
use std::fmt;
use std::time::Duration;

pub(super) fn configuration_fatal(detail: impl Into<String>) -> StageFatal {
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

/// Pure synchronous finite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed synchronous finite source contract",
    note = "implement TypedFiniteSourceHandler with Output matching the source! arrow"
)]
pub trait TypedFiniteSourceHandler: Send + Sync {
    type Output: OneFactOutcome + Send + Sync + 'static;

    /// `read` returns the read that this handler makes through its bound port.
    fn read(&self) -> SourceRead<Self::Output, Finite>;
}

/// Pure asynchronous finite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed asynchronous finite source contract",
    note = "implement TypedAsyncFiniteSourceHandler with Output matching the source! arrow"
)]
#[async_trait]
pub trait TypedAsyncFiniteSourceHandler: Send + Sync {
    type Output: OneFactOutcome + Send + Sync + 'static;

    fn poll_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(30))
    }

    /// `read` returns the read that this handler makes through its bound port.
    fn read(&self) -> SourceRead<Self::Output, Finite>;
}

/// Pure synchronous infinite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed synchronous infinite source contract",
    note = "implement TypedInfiniteSourceHandler with Output matching the source! arrow"
)]
pub trait TypedInfiniteSourceHandler: Send + Sync {
    type Output: OneFactOutcome + Send + Sync + 'static;

    /// `read` returns the read that this handler makes through its bound port.
    fn read(&self) -> SourceRead<Self::Output, Continuous>;
}

/// Pure asynchronous infinite source contract.
#[diagnostic::on_unimplemented(
    message = "this source handler does not witness its arrow contract",
    label = "this handler does not implement the typed asynchronous infinite source contract",
    note = "implement TypedAsyncInfiniteSourceHandler with Output matching the source! arrow"
)]
#[async_trait]
pub trait TypedAsyncInfiniteSourceHandler: Send + Sync {
    type Output: OneFactOutcome + Send + Sync + 'static;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    /// `read` returns the read that this handler makes through its bound port.
    fn read(&self) -> SourceRead<Self::Output, Continuous>;
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

/// Runtime adapter for an asynchronous typed finite source.
#[doc(hidden)]
pub struct TypedAsyncFiniteSourceHandlerAdapter<H: TypedAsyncFiniteSourceHandler> {
    handler: H,
    reads: DrivenReads<H::Output>,
}

impl<H: TypedAsyncFiniteSourceHandler> TypedAsyncFiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        let reads = DrivenReads::new(handler.read().into_driven());
        Self { handler, reads }
    }
}

const SYNC_IDLE_BACKOFF_CAP: Duration = Duration::from_millis(10);

struct DrivenReads<Output> {
    read: DrivenRead<Output>,
    context: Option<crate::effects::EffectInvocationContext>,
    number: u64,
    attempt: obzenflow_core::event::payloads::effect_payload::EffectOrdinal,
    retry_at: Option<tokio::time::Instant>,
    effects: Option<crate::effects::EffectsCore>,
    reproduced: bool,
    replayed: u64,
    committed_rejections: Option<u64>,
    reproduced_rejections: u64,
    recorded: Option<crate::replay::ReplayDriver>,
    emitted: Option<ChainEvent>,
    held: Option<ChainEvent>,
}

impl<Output: OneFactOutcome + Send + Sync + 'static> DrivenReads<Output> {
    fn new(read: DrivenRead<Output>) -> Self {
        Self {
            read,
            context: None,
            number: 0,
            attempt: obzenflow_core::event::payloads::effect_payload::EffectOrdinal::new(0),
            retry_at: None,
            effects: None,
            reproduced: false,
            replayed: 0,
            committed_rejections: None,
            reproduced_rejections: 0,
            recorded: None,
            emitted: None,
            held: None,
        }
    }

    fn has_pending(&self) -> bool {
        self.held.is_some() || self.read.has_pending()
    }

    fn poll_cancelled(&mut self) {
        if let Some(event) = self.emitted.take() {
            self.held = Some(event);
        }
    }

    async fn cross_to_live(&mut self) -> Result<bool, StageFatal> {
        if !self.read.is_continuous() || self.has_pending() {
            return Ok(false);
        }
        let Some(context) = self.context.clone() else {
            return Ok(false);
        };
        if context
            .stage
            .effect_history
            .as_ref()
            .is_some_and(|history| history.actions().next_unreproduced().is_some())
        {
            return Ok(false);
        }
        let execution = context.stage.runtime_execution.clone();
        let stage_id = context.stage.stage_id;
        let mut effects = crate::effects::EffectsCore::new(context);
        match effects.cross_to_live().await {
            Ok(()) => Ok(!matches!(
                execution.source_phase_for(stage_id),
                crate::execution::SourceExecutionPhase::Replaying
            )),
            Err(error) => Err(effects
                .authoring_halt_fatal()
                .unwrap_or_else(|| protocol_fatal(error.to_string()))),
        }
    }

    async fn records(&mut self, error: &SourceError) -> Result<bool, StageFatal> {
        let Some(context) = self.context.as_ref().filter(|_| self.reproduced) else {
            return Ok(true);
        };
        if context.stage.runtime_execution.resume_control().is_none() {
            return Ok(error.is_terminal());
        }
        if matches!(error, SourceError::Validation(_)) {
            let committed = match self.committed_rejections {
                Some(committed) => committed,
                None => {
                    let committed = committed_rejections(context);
                    self.committed_rejections = Some(committed);
                    committed
                }
            };
            self.reproduced_rejections += 1;
            return Ok(self.reproduced_rejections > committed);
        }
        Ok(!error.is_terminal()
            && context
                .stage
                .effect_history
                .as_ref()
                .is_none_or(|history| history.actions().next_unreproduced().is_none()))
    }

    async fn invocation(&mut self) -> ErasedSourceInvocation {
        self.emitted = None;
        if let Some(event) = self.held.take() {
            self.emitted = Some(event.clone());
            return ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(vec![event]));
        }
        loop {
            if let Some(end) = self.next_read().await {
                return end;
            }
            match self.read.next_output() {
                None => {
                    return ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(
                        Vec::new(),
                    ))
                }
                Some(Err(error)) => match self.records(&error).await {
                    Ok(true) => return ErasedSourceInvocation::handler_error(error),
                    Ok(false) => {}
                    Err(fatal) => return ErasedSourceInvocation::fatal(fatal),
                },
                Some(Ok(output)) => return self.output(output).await,
            }
        }
    }

    async fn next_read(&mut self) -> Option<ErasedSourceInvocation> {
        while !self.read.has_pending() {
            let Some(mut context) = self.context.clone() else {
                return Some(ErasedSourceInvocation::fatal(configuration_fatal(
                    "source read before the runtime installed its stage context",
                )));
            };
            let Some(number) = self.number.checked_add(1) else {
                return Some(ErasedSourceInvocation::fatal(protocol_fatal(
                    "source read number exhausted",
                )));
            };
            context.input_seq = crate::messaging::upstream_subscription::StageInputPosition(number);
            if super::read::recording_ends(&context, number, self.attempt) {
                let mut effects = crate::effects::EffectsCore::new(context);
                if let Err(error) = effects.reproduce_trailing_crossing().await {
                    return Some(ErasedSourceInvocation::fatal(
                        effects
                            .authoring_halt_fatal()
                            .unwrap_or_else(|| protocol_fatal(error.to_string())),
                    ));
                }
                return Some(ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Eof,
                ));
            }
            let mut effects = crate::effects::EffectsCore::new(context).starting_at(self.attempt);
            let reproduced = effects.is_replaying();
            if !reproduced {
                if let Some(end) = self.wait_for_live_read(&mut effects).await {
                    return Some(end);
                }
            }
            let outcome = self.read.read(&mut effects).await;
            self.reproduced = reproduced;
            match outcome {
                Ok(end) => {
                    self.number = number;
                    self.attempt =
                        obzenflow_core::event::payloads::effect_payload::EffectOrdinal::new(0);
                    match end {
                        ReadEnd::EndOfInput => {
                            return Some(ErasedSourceInvocation::completed(
                                ErasedSourceCompletion::Eof,
                            ))
                        }
                        ReadEnd::Batch => {
                            self.effects = Some(effects);
                            return None;
                        }
                    }
                }
                Err(error) => {
                    if let Some(fatal) = effects.authoring_halt_fatal() {
                        return Some(ErasedSourceInvocation::fatal(fatal));
                    }
                    let Some(failure) = read_failure(&error) else {
                        return Some(ErasedSourceInvocation::fatal(protocol_fatal(
                            error.to_string(),
                        )));
                    };
                    let Some(attempt) = self.attempt.get().checked_add(1) else {
                        return Some(ErasedSourceInvocation::fatal(protocol_fatal(
                            "source read attempts exhausted",
                        )));
                    };
                    self.attempt =
                        obzenflow_core::event::payloads::effect_payload::EffectOrdinal::new(
                            attempt,
                        );
                    let retry_after = (!reproduced).then(|| error.retry_after()).flatten();
                    self.retry_at = retry_after.map(|delay| tokio::time::Instant::now() + delay);
                    match self.records(&failure).await {
                        Ok(true) => {
                            return Some(ErasedSourceInvocation::rate_limited(failure, retry_after))
                        }
                        Ok(false) => {}
                        Err(fatal) => return Some(ErasedSourceInvocation::fatal(fatal)),
                    }
                }
            }
        }
        None
    }

    async fn wait_for_live_read(
        &mut self,
        effects: &mut crate::effects::EffectsCore,
    ) -> Option<ErasedSourceInvocation> {
        if self.read.is_continuous() {
            if let Err(error) = effects.cross_to_live().await {
                return Some(ErasedSourceInvocation::fatal(
                    effects
                        .authoring_halt_fatal()
                        .unwrap_or_else(|| protocol_fatal(error.to_string())),
                ));
            }
            match self.read.ready(effects).await {
                Ok(()) => {}
                Err(NotReady::Source(error)) => {
                    return Some(ErasedSourceInvocation::handler_error(error))
                }
                Err(NotReady::Effect(error)) => {
                    return Some(ErasedSourceInvocation::fatal(
                        effects
                            .authoring_halt_fatal()
                            .unwrap_or_else(|| protocol_fatal(error.to_string())),
                    ))
                }
            }
        }
        if let Some(at) = self.retry_at {
            tokio::time::sleep_until(at).await;
            self.retry_at = None;
        }
        None
    }

    async fn output(&mut self, output: Output) -> ErasedSourceInvocation {
        let effects = self
            .effects
            .as_mut()
            .expect("a committed read supplied the pending records");
        let fact = match output.into_facts() {
            Ok(mut facts) if facts.len() == 1 => facts.pop().expect("one source fact"),
            Ok(_) => {
                return ErasedSourceInvocation::fatal(protocol_fatal(
                    "a source output must produce one fact",
                ))
            }
            Err(error) => return ErasedSourceInvocation::fatal(protocol_fatal(error.to_string())),
        };
        let event = match effects.prepare_typed_fact(fact, false).await {
            Ok((event, _)) => event,
            Err(error) => {
                return ErasedSourceInvocation::fatal(
                    effects
                        .authoring_halt_fatal()
                        .unwrap_or_else(|| protocol_fatal(error.to_string())),
                )
            }
        };
        if self.reproduced
            && self.context.as_ref().is_some_and(|context| {
                super::read::recording_ends_before(context, self.number, &event.id)
            })
        {
            return match effects.reproduce_trailing_crossing().await {
                Ok(()) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof),
                Err(error) => ErasedSourceInvocation::fatal(
                    effects
                        .authoring_halt_fatal()
                        .unwrap_or_else(|| protocol_fatal(error.to_string())),
                ),
            };
        }
        match self.preserved(event).await {
            Ok(event) => {
                if self.reproduced {
                    self.replayed += 1;
                }
                self.emitted = Some(event.clone());
                ErasedSourceInvocation::completed(ErasedSourceCompletion::Batch(vec![event]))
            }
            Err(fatal) => ErasedSourceInvocation::fatal(fatal),
        }
    }

    async fn preserved(&mut self, event: ChainEvent) -> Result<ChainEvent, StageFatal> {
        preserve_recorded(&mut self.recorded, self.context.as_ref(), event).await
    }
}

fn committed_rejections(context: &crate::effects::EffectInvocationContext) -> u64 {
    let (Some(archive), Some(flow)) = (
        context.stage.runtime_execution.archive_for_io(),
        context.stage.flow_context.as_ref(),
    ) else {
        return 0;
    };
    std::iter::once(archive.clone())
        .chain(archive.ancestors())
        .map(|member| member.recorded_source_rejections(&flow.stage_name))
        .sum()
}

pub(super) async fn preserve_recorded(
    recorded: &mut Option<crate::replay::ReplayDriver>,
    context: Option<&crate::effects::EffectInvocationContext>,
    event: ChainEvent,
) -> Result<ChainEvent, StageFatal> {
    let Some(context) = context else {
        return Ok(event);
    };
    let execution = &context.stage.runtime_execution;
    if event.replay_context.is_none() || execution.resume_control().is_none() {
        return Ok(event);
    }
    if recorded.is_none() {
        let (Some(archive), Some(flow)) = (
            execution.archive_for_io(),
            context.stage.flow_context.as_ref(),
        ) else {
            return Err(protocol_fatal("a resumed source read has no archive"));
        };
        let opened = crate::stages::source::supervision::recorded_input(
            archive,
            &flow.stage_name,
            flow.stage_type,
        )
        .await
        .map_err(|error| protocol_fatal(error.to_string()))?;
        *recorded = Some(opened);
    }
    let recorded = recorded.as_mut().expect("the recorded input is open");
    loop {
        let Some(replayed) = recorded
            .next_replayed_event(event.flow_context.clone())
            .await
            .map_err(|error| protocol_fatal(error.to_string()))?
        else {
            return Err(protocol_fatal(format!(
                "the recording holds no fact {} for the resumed read",
                event.id
            )));
        };
        if !matches!(
            replayed.event.payload,
            obzenflow_core::event::ChainPayload::Fact(_)
                | obzenflow_core::event::ChainPayload::CompositeData(_)
        ) {
            continue;
        }
        if replayed.event.id != event.id {
            return Err(protocol_fatal(format!(
                "the recording holds fact {} where the resumed read reproduced {}",
                replayed.event.id, event.id
            )));
        }
        return replayed
            .admit()
            .map_err(|error| protocol_fatal(error.to_string()));
    }
}

impl<H: TypedAsyncFiniteSourceHandler> SealAsyncFinite for TypedAsyncFiniteSourceHandlerAdapter<H> {}

#[async_trait]
impl<H> UnifiedAsyncFiniteSourceHandler for TypedAsyncFiniteSourceHandlerAdapter<H>
where
    H: TypedAsyncFiniteSourceHandler + Send + Sync,
{
    fn has_pending_records(&self) -> bool {
        self.reads.has_pending()
    }

    fn replayed_facts(&self) -> u64 {
        self.reads.replayed
    }

    fn install_read_context(&mut self, context: crate::effects::EffectInvocationContext) {
        self.reads.context = Some(context);
    }

    fn poll_cancelled(&mut self) {
        self.reads.poll_cancelled();
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.handler.poll_timeout()
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }
}

/// Runtime adapter for an asynchronous typed infinite source.
#[doc(hidden)]
pub struct TypedAsyncInfiniteSourceHandlerAdapter<H: TypedAsyncInfiniteSourceHandler> {
    handler: H,
    reads: DrivenReads<H::Output>,
}

impl<H: TypedAsyncInfiniteSourceHandler> TypedAsyncInfiniteSourceHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        let reads = DrivenReads::new(handler.read().into_driven());
        Self { handler, reads }
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
    fn has_pending_records(&self) -> bool {
        self.reads.has_pending()
    }

    fn install_read_context(&mut self, context: crate::effects::EffectInvocationContext) {
        self.reads.context = Some(context);
    }

    fn poll_cancelled(&mut self) {
        self.reads.poll_cancelled();
    }

    async fn cross_to_live(&mut self) -> Result<bool, StageFatal> {
        self.reads.cross_to_live().await
    }

    fn replayed_facts(&self) -> u64 {
        self.reads.replayed
    }

    fn poll_timeout(&self) -> Option<Duration> {
        self.handler.poll_timeout()
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    super::prepared::sealed::Admitted<dyn UnifiedAsyncFiniteSourceHandler> for H
{
}

impl<H: TypedAsyncFiniteSourceHandler + 'static>
    super::prepared::AdmitSource<dyn UnifiedAsyncFiniteSourceHandler> for H
{
    type Output = H::Output;
    type Handler = TypedAsyncFiniteSourceHandlerAdapter<H>;

    fn admit(self) -> super::prepared::AdmittedSource<Self::Handler> {
        let read = self.read();
        let system = read.source_system();
        let continuation = read.continuation();
        let handler = TypedAsyncFiniteSourceHandlerAdapter {
            reads: DrivenReads::new(read.into_driven()),
            handler: self,
        };
        super::prepared::AdmittedSource {
            handler,
            continuation,
            system,
        }
    }
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    super::prepared::sealed::Admitted<dyn UnifiedAsyncInfiniteSourceHandler> for H
{
}

impl<H: TypedAsyncInfiniteSourceHandler + 'static>
    super::prepared::AdmitSource<dyn UnifiedAsyncInfiniteSourceHandler> for H
{
    type Output = H::Output;
    type Handler = TypedAsyncInfiniteSourceHandlerAdapter<H>;

    fn admit(self) -> super::prepared::AdmittedSource<Self::Handler> {
        let read = self.read();
        let system = read.source_system();
        let continuation = read.continuation();
        let handler = TypedAsyncInfiniteSourceHandlerAdapter {
            reads: DrivenReads::new(read.into_driven()),
            handler: self,
        };
        super::prepared::AdmittedSource {
            handler,
            continuation,
            system,
        }
    }
}

#[doc(hidden)]
pub enum SyncInfiniteReads {}

#[doc(hidden)]
pub struct InfiniteReadAdapter<Output> {
    reads: DrivenReads<Output>,
}

impl<Output> SealAsyncInfinite for InfiniteReadAdapter<Output> {}

#[async_trait]
impl<Output: OneFactOutcome + Send + Sync + 'static> UnifiedAsyncInfiniteSourceHandler
    for InfiniteReadAdapter<Output>
{
    fn has_pending_records(&self) -> bool {
        self.reads.has_pending()
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::InfiniteSource
    }

    fn idle_backoff_cap(&self) -> Duration {
        SYNC_IDLE_BACKOFF_CAP
    }

    fn install_read_context(&mut self, context: crate::effects::EffectInvocationContext) {
        self.reads.context = Some(context);
    }

    fn poll_cancelled(&mut self) {
        self.reads.poll_cancelled();
    }

    async fn cross_to_live(&mut self) -> Result<bool, StageFatal> {
        self.reads.cross_to_live().await
    }

    fn replayed_facts(&self) -> u64 {
        self.reads.replayed
    }

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }
}

impl<H: TypedInfiniteSourceHandler + 'static> super::prepared::sealed::Admitted<SyncInfiniteReads>
    for H
{
}

impl<H: TypedInfiniteSourceHandler + 'static> super::prepared::AdmitSource<SyncInfiniteReads>
    for H
{
    type Output = H::Output;
    type Handler = InfiniteReadAdapter<H::Output>;

    fn admit(self) -> super::prepared::AdmittedSource<Self::Handler> {
        let read = self.read();
        let system = read.source_system();
        let continuation = read.continuation();
        super::prepared::AdmittedSource {
            handler: InfiniteReadAdapter {
                reads: DrivenReads::new(read.into_driven()),
            },
            continuation,
            system,
        }
    }
}

#[doc(hidden)]
pub enum SyncFiniteReads {}

#[doc(hidden)]
pub struct FiniteReadAdapter<Output> {
    reads: DrivenReads<Output>,
}

impl<Output> SealAsyncFinite for FiniteReadAdapter<Output> {}

#[async_trait]
impl<Output: OneFactOutcome + Send + Sync + 'static> UnifiedAsyncFiniteSourceHandler
    for FiniteReadAdapter<Output>
{
    fn has_pending_records(&self) -> bool {
        self.reads.has_pending()
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::FiniteSource
    }

    fn replayed_facts(&self) -> u64 {
        self.reads.replayed
    }

    fn idle_backoff_cap(&self) -> Duration {
        SYNC_IDLE_BACKOFF_CAP
    }

    fn install_read_context(&mut self, context: crate::effects::EffectInvocationContext) {
        self.reads.context = Some(context);
    }

    fn poll_cancelled(&mut self) {
        self.reads.poll_cancelled();
    }

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }
}

impl<H: TypedFiniteSourceHandler + 'static> super::prepared::sealed::Admitted<SyncFiniteReads>
    for H
{
}

impl<H: TypedFiniteSourceHandler + 'static> super::prepared::AdmitSource<SyncFiniteReads> for H {
    type Output = H::Output;
    type Handler = FiniteReadAdapter<H::Output>;

    fn admit(self) -> super::prepared::AdmittedSource<Self::Handler> {
        let read = self.read();
        let system = read.source_system();
        let continuation = read.continuation();
        super::prepared::AdmittedSource {
            handler: FiniteReadAdapter {
                reads: DrivenReads::new(read.into_driven()),
            },
            continuation,
            system,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

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
