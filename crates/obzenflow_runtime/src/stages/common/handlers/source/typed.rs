// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncFinite, SealAsyncInfinite,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
};
use super::read::{
    read_failure, Continuous, DrivenRead, Finite, ReadEnd, SourceOutput, SourceRead,
};
use super::SourceError;
use crate::stages::common::handler_error::StageFatal;
use async_trait::async_trait;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::{ChainEvent, OneFactOutcome};
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

pub(super) fn effect_fatal(
    effects: &crate::effects::EffectsCore,
    error: &crate::effects::EffectError,
) -> StageFatal {
    effects
        .authoring_halt_fatal()
        .unwrap_or_else(|| match error {
            crate::effects::EffectError::Journal(detail) => StageFatal::new(
                StageFatalCode::Journal,
                StageFatalReason::JournalFailure,
                detail.clone(),
            ),
            error => protocol_fatal(error.to_string()),
        })
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

/// Runtime adapter for an asynchronous typed finite source.
#[doc(hidden)]
pub struct TypedAsyncFiniteSourceHandlerAdapter<H: TypedAsyncFiniteSourceHandler> {
    reads: DrivenReads<H::Output>,
}

const SYNC_IDLE_BACKOFF_CAP: Duration = Duration::from_millis(10);

struct DrivenReads<Output> {
    read: DrivenRead<Output>,
    deadline: Option<Duration>,
    context: Option<crate::effects::EffectInvocationContext>,
    number: u64,
    retry_at: Option<tokio::time::Instant>,
    effects: Option<crate::effects::EffectsCore>,
    reproduced: bool,
    replayed: u64,
    recorded: Option<crate::replay::ReplayDriver>,
    emitted: Option<ChainEvent>,
    held: Option<ChainEvent>,
}

impl<Output: OneFactOutcome + Send + Sync + 'static> DrivenReads<Output> {
    fn new(read: DrivenRead<Output>, deadline: Option<Duration>) -> Self {
        Self {
            read,
            deadline,
            context: None,
            number: 0,
            retry_at: None,
            effects: None,
            reproduced: false,
            replayed: 0,
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

    fn install(&mut self, context: crate::effects::EffectInvocationContext) {
        self.read
            .install_context(context.stage.runtime_execution.resume_control().is_some());
        self.context = Some(context);
    }

    async fn cross_to_live(&mut self) -> Result<bool, StageFatal> {
        if !self.read.is_continuous() || self.has_pending() || self.read.holds_receipts() {
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
            Ok(()) => {
                let live = !matches!(
                    execution.source_phase_for(stage_id),
                    crate::execution::SourceExecutionPhase::Replaying
                );
                if live {
                    self.read.crossed_to_live();
                }
                Ok(live)
            }
            Err(error) => Err(effect_fatal(&effects, &error)),
        }
    }

    async fn records(&mut self, error: &SourceError) -> Result<bool, StageFatal> {
        let Some(context) = self.context.as_ref().filter(|_| self.reproduced) else {
            return Ok(true);
        };
        if context.stage.runtime_execution.resume_control().is_none() {
            return Ok(error.is_terminal());
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
            if super::read::recording_ends(&context, number) {
                let mut effects = crate::effects::EffectsCore::new(context);
                if let Err(error) = effects.reproduce_trailing_crossing().await {
                    return Some(ErasedSourceInvocation::fatal(effect_fatal(
                        &effects, &error,
                    )));
                }
                return Some(ErasedSourceInvocation::completed(
                    ErasedSourceCompletion::Eof,
                ));
            }
            let mut effects = crate::effects::EffectsCore::new(context);
            let reproduced = effects.is_replaying();
            if !reproduced {
                if let Some(end) = self.wait_for_live_read(&mut effects).await {
                    return Some(end);
                }
            }
            let outcome = self.read.read(&mut effects, self.deadline).await;
            self.reproduced = reproduced;
            match outcome {
                Ok(end) => {
                    self.number = number;
                    effects.adopt_answer_as_parent();
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
                        return Some(ErasedSourceInvocation::fatal(effect_fatal(
                            &effects, &error,
                        )));
                    };
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
                return Some(ErasedSourceInvocation::fatal(effect_fatal(effects, &error)));
            }
            if let Err(error) = self.read.ready().await {
                return Some(ErasedSourceInvocation::handler_error(error));
            }
        }
        if let Some(at) = self.retry_at {
            tokio::time::sleep_until(at).await;
            self.retry_at = None;
        }
        None
    }

    async fn output(&mut self, output: SourceOutput<Output>) -> ErasedSourceInvocation {
        let effects = self
            .effects
            .as_mut()
            .expect("a committed read supplied the pending records");
        let (fact, admission) = match output {
            SourceOutput::Value(output) => match output.into_facts() {
                Ok(mut facts) if facts.len() == 1 => (facts.pop().expect("one source fact"), None),
                Ok(_) => {
                    return ErasedSourceInvocation::fatal(protocol_fatal(
                        "a source output must produce one fact",
                    ))
                }
                Err(error) => {
                    return ErasedSourceInvocation::fatal(protocol_fatal(error.to_string()))
                }
            },
            SourceOutput::Admitted(staged) => {
                let staged = *staged;
                let fact = match staged.fact.into_typed() {
                    Ok(fact) => fact,
                    Err(error) => {
                        return ErasedSourceInvocation::fatal(protocol_fatal(format!(
                            "a recorded event cannot be decoded: {error}"
                        )))
                    }
                };
                let admission = super::read::Admission {
                    identity: staged.identity,
                    accepted_at_ns: staged.context.accepted_at_ns,
                    receipts: staged.receipts,
                };
                (fact, Some((staged.context, admission)))
            }
        };
        let mut event = match effects.prepare_typed_fact(fact, false).await {
            Ok((event, _)) => event,
            Err(error) => return ErasedSourceInvocation::fatal(effect_fatal(effects, &error)),
        };
        if self.reproduced
            && self.context.as_ref().is_some_and(|context| {
                super::read::recording_ends_before(context, self.number, &event.id)
            })
        {
            return match effects.reproduce_trailing_crossing().await {
                Ok(()) => ErasedSourceInvocation::completed(ErasedSourceCompletion::Eof),
                Err(error) => ErasedSourceInvocation::fatal(effect_fatal(effects, &error)),
            };
        }
        let admission = admission.map(|(context, admission)| {
            event.ingress_context = Some(context);
            admission
        });
        match self.preserved(event).await {
            Ok(event) => {
                if let Some(admission) = admission {
                    self.read.admitted(admission, event.id);
                }
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

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }
}

/// Runtime adapter for an asynchronous typed infinite source.
#[doc(hidden)]
pub struct TypedAsyncInfiniteSourceHandlerAdapter<H: TypedAsyncInfiniteSourceHandler> {
    reads: DrivenReads<H::Output>,
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
        self.reads.install(context);
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

    fn hosted_ingress_slot(&self) -> Option<obzenflow_core::ingress::HostedIngressBindingSlot> {
        self.reads.read.hosted_ingress_slot()
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }

    fn settle_committed(&mut self, committed: &[obzenflow_core::EventId]) {
        self.reads.read.settle_committed(committed);
    }
}

fn admitted<Output, L, H>(
    read: SourceRead<Output, L>,
    deadline: Option<Duration>,
    handler: impl FnOnce(DrivenReads<Output>) -> H,
) -> super::prepared::AdmittedSource<H>
where
    Output: OneFactOutcome + Send + Sync + 'static,
    L: super::read::ReadLifetime,
{
    let declaration = read.declaration();
    let continuation = read.continuation();
    super::prepared::AdmittedSource {
        handler: handler(DrivenReads::new(read.into_driven(), deadline)),
        continuation,
        declaration,
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
        admitted(self.read(), self.poll_timeout(), |reads| {
            TypedAsyncFiniteSourceHandlerAdapter { reads }
        })
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
        admitted(self.read(), self.poll_timeout(), |reads| {
            TypedAsyncInfiniteSourceHandlerAdapter { reads }
        })
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
        self.reads.install(context);
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

    fn hosted_ingress_slot(&self) -> Option<obzenflow_core::ingress::HostedIngressBindingSlot> {
        self.reads.read.hosted_ingress_slot()
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.reads.invocation().await
    }

    fn settle_committed(&mut self, committed: &[obzenflow_core::EventId]) {
        self.reads.read.settle_committed(committed);
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
        admitted(self.read(), None, |reads| InfiniteReadAdapter { reads })
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
        admitted(self.read(), None, |reads| FiniteReadAdapter { reads })
    }
}
