// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{SourceContinuation, SourceError};
use crate::effects::{
    EffectError, EffectsCore, SourceSystem,
};
use crate::queries::{ask, QueryPort, QueryRequest};
use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::{
    ReattachGuarantee, SourcePollContinuation, SourcePollErrorKind,
};
use obzenflow_core::event::{EffectFailureDetail, SourceDiagnosticReason, SourceReadFailure};
use obzenflow_core::WriterId;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

pub(crate) async fn read_stage(
    resources: &crate::stages::StageResources,
    instrumentation: Arc<crate::metrics::instrumentation::StageInstrumentation>,
) -> Result<Arc<crate::effects::StageConstants>, EffectError> {
    use crate::effects::{CurrentRunHistory, EffectHistory, EffectHistoryScan, StageConstants};
    use crate::stages::common::supervision::catch_up::CatchUpHandle;
    let flow = &resources.flow_context;
    let journal = resources.data_journal.clone();
    let execution = resources.runtime_execution.clone();
    let catch_up = CatchUpHandle::producing(
        flow.clone(),
        journal.clone(),
        instrumentation.clone(),
        execution.clone(),
    );
    let history = if let Some(archive) = execution.archive_for_io() {
        let history = Arc::new(
            EffectHistory::load(
                archive,
                &obzenflow_core::StageKey::from(flow.stage_name.clone()),
                EffectHistoryScan::Actions { pass_through: None },
            )
            .await?,
        );
        catch_up.install_history(history.clone()).await;
        Some(history)
    } else {
        None
    };
    Ok(Arc::new(StageConstants {
        flow_id: resources.flow_id,
        stage_id: flow.stage_id,
        stage_key: flow.stage_name.clone(),
        writer_id: WriterId::from(flow.stage_id),
        stage_logic_version: "1".to_owned(),
        data_journal: journal.clone(),
        flow_context: Some(flow.clone()),
        observers: None,
        instrumentation: Some(instrumentation),
        heartbeat_state: None,
        effect_history: history,
        current_run_history: CurrentRunHistory::new(journal),
        runtime_execution: execution,
        effect_ports: resources.effect_ports.clone(),
        effect_declarations: resources.effect_declarations.clone(),
        output_contract: resources.output_contract.clone(),
        backpressure_readers: Vec::new(),
        lineage: resources.lineage_policy,
        catch_up: Some(catch_up),
    }))
}

/// `FiniteRead` is the recorded reply to one finite read. It holds records and the
/// argument of the next read, or the end of the input.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FiniteRead<Record, Next> {
    Batch { records: Vec<Record>, next: Next },
    EndOfInput,
}

/// `ContinuousRead` is the recorded reply to one continuous read. It holds the records
/// that the read took and the argument of the next read.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContinuousRead<Record, Next> {
    pub records: Vec<Record>,
    pub next: Next,
}

/// `Finite` marks a source read whose input ends.
pub enum Finite {}

/// `Continuous` marks a source read whose input stays open until the run stops.
pub enum Continuous {}

mod sealed {
    pub trait Lifetime {}
    impl Lifetime for super::Finite {}
    impl Lifetime for super::Continuous {}
}

/// `ReadLifetime` is the lifetime of a source read, [`Finite`] or [`Continuous`]. The
/// reply type of the port method sets it, and the handler role must agree with it.
pub trait ReadLifetime: sealed::Lifetime + Send + Sync + 'static {}

impl ReadLifetime for Finite {}

impl ReadLifetime for Continuous {}

/// Each system behind a continuous source port implements `InputReadiness`. Make it
/// a supertrait of the port trait, so that every system behind the port supplies it.
/// The framework waits for `ready` before each live read, and the wait is not a read.
#[async_trait]
pub trait InputReadiness: Send + Sync {
    /// `ready` resolves when the system holds input for the next read. It takes
    /// nothing from the system. An error ends the live reads of the stage.
    async fn ready(&self) -> Result<(), SourceError>;
}

/// `SourceRead` is the read that a source handler declares through its bound port.
/// The framework performs each read and keeps the argument of the next read from the
/// recorded reply. `L` is the lifetime of the read.
pub struct SourceRead<Output, L: ReadLifetime = Finite> {
    reads: Box<dyn Reads<Output>>,
    continuation: SourceContinuation,
    lifetime: PhantomData<fn() -> L>,
}

impl<Output: Send + Sync + 'static> SourceRead<Output, Finite> {
    /// `SourceRead::finite` makes a finite read. Each reply gives the argument of the next
    /// read or ends the input.
    pub fn finite<R, Record, F>(port: QueryPort<R::Port>, first: R, output: F) -> Self
    where R: QueryRequest<Answer=FiniteRead<Record,R>>, Record: Send + Sync + 'static,
          F: Fn(Record) -> Result<Output,SourceError> + Send + Sync + 'static {
        Self::pages(port, first, |_, reply| Ok(match reply {
            FiniteRead::Batch { records, next } => Some(((), records, next)),
            FiniteRead::EndOfInput => None,
        }), move |_, record| output(record))
    }

    /// Maps a supplier's page and its metadata into source records and the next request.
    pub fn pages<R, Record, M, Split, F>(port: QueryPort<R::Port>, first: R, page: Split, output: F) -> Self
    where R: QueryRequest, Record: Send + Sync + 'static, M: Send + Sync + 'static,
        Split: Fn(&R,R::Answer) -> Result<Option<(M,Vec<Record>,R)>, crate::queries::QueryError> + Send + Sync + 'static,
        F: Fn(&M,Record) -> Result<Output,SourceError> + Send + Sync + 'static {
        Self {
            reads: Box::new(FiniteReads {port,next:Some(first),metadata:None,pending:VecDeque::new(),page,output}),
            continuation: SourceContinuation::RecordedRead, lifetime: PhantomData,
        }
    }

    /// `in_process` returns the same read for input that only this process holds. The
    /// framework refuses to resume a run that did not finish this input.
    pub fn in_process(mut self) -> Self {
        self.continuation = SourceContinuation::Refuse;
        self
    }
}

impl<Output: Send + Sync + 'static> SourceRead<Output, Continuous> {
    /// `SourceRead::continuous` makes a continuous read. Before each live read, the
    /// framework waits until the system behind the port is ready. A resumed run
    /// continues the live reads under the guarantee of the port method. Declare a
    /// method that takes input destructively `at_most_once`, and a method that reads
    /// the latest state `at_least_once`.
    pub fn continuous<R, Record, F>(port: QueryPort<R::Port>, first: R, output: F) -> Self
    where R: QueryRequest<Answer=ContinuousRead<Record,R>>, R::Port: InputReadiness,
        Record: Send + Sync + 'static,
        F: Fn(Record) -> Result<Output,SourceError> + Send + Sync + 'static,
    {
        let continuation = SourceContinuation::Reattach { guarantee: ReattachGuarantee::LatestState };
        Self {
            reads: Box::new(ContinuousReads {
                port,
                system: OnceLock::new(),
                next: first,
                pending: VecDeque::new(),
                output,
            }),
            continuation,
            lifetime: PhantomData,
        }
    }
}

impl<Output: Send + Sync + 'static, L: ReadLifetime> SourceRead<Output, L> {
    pub(crate) fn continuation(&self) -> SourceContinuation {
        self.continuation
    }

    pub(crate) fn source_system(&self) -> SourceSystem {
        self.reads.source_system()
    }

    pub(crate) fn into_driven(self) -> DrivenRead<Output> {
        DrivenRead { reads: self.reads }
    }
}

pub(crate) struct DrivenRead<Output> {
    reads: Box<dyn Reads<Output>>,
}

impl<Output: Send + Sync + 'static> DrivenRead<Output> {

    pub(crate) fn has_pending(&self) -> bool {
        self.reads.has_pending()
    }

    pub(crate) fn is_continuous(&self) -> bool {
        self.reads.is_continuous()
    }

    pub(crate) async fn ready(&self, effects: &mut EffectsCore) -> Result<(), NotReady> {
        self.reads.ready(effects).await
    }

    pub(crate) fn next_output(&mut self) -> Option<Result<Output, SourceError>> {
        self.reads.next_output()
    }

    pub(crate) async fn read(&mut self, effects: &mut EffectsCore) -> Result<ReadEnd, EffectError> {
        self.reads.read(effects).await
    }
}

pub(crate) enum ReadEnd {
    Batch,
    EndOfInput,
}

pub(crate) enum NotReady {
    Source(SourceError),
    Effect(EffectError),
}

#[async_trait]
trait Reads<Output>: Send + Sync {
    fn source_system(&self) -> SourceSystem;
    fn is_continuous(&self) -> bool;
    fn has_pending(&self) -> bool;
    async fn ready(&self, _effects: &mut EffectsCore) -> Result<(), NotReady> {
        Ok(())
    }
    fn next_output(&mut self) -> Option<Result<Output, SourceError>>;
    async fn read(&mut self, effects: &mut EffectsCore) -> Result<ReadEnd, EffectError>;
}

struct FiniteReads<R:QueryRequest,Record,M,Split,F> {
    port: QueryPort<R::Port>, next:Option<R>, metadata:Option<M>, pending:VecDeque<Record>, page:Split, output:F,
}

#[async_trait]
impl<R,Record,M,Split,Output,F> Reads<Output> for FiniteReads<R,Record,M,Split,F>
where R: QueryRequest, Record: Send + Sync + 'static, M: Send + Sync + 'static, Output: Send + Sync + 'static,
    Split: Fn(&R,R::Answer)->Result<Option<(M,Vec<Record>,R)>,crate::queries::QueryError> + Send + Sync + 'static,
    F: Fn(&M,Record)->Result<Output,SourceError> + Send + Sync + 'static,
{
    fn source_system(&self) -> SourceSystem { self.port.system.clone() }
    fn is_continuous(&self) -> bool { false }
    fn has_pending(&self) -> bool { !self.pending.is_empty() }
    fn next_output(&mut self) -> Option<Result<Output,SourceError>> {
        let record = self.pending.pop_front()?;
        Some((self.output)(self.metadata.as_ref().expect("pending records have page metadata"),record))
    }
    async fn read(&mut self, stage: &mut EffectsCore)->Result<ReadEnd,EffectError>{
        let Some(argument)=self.next.as_ref() else {return Ok(ReadEnd::EndOfInput)};
        let answer=ask(stage,&self.port,argument).await?;
        match (self.page)(argument,answer)? {
            Some((metadata,records,next)) => { self.next=Some(next); self.metadata=Some(metadata); self.pending=records.into(); Ok(ReadEnd::Batch) }
            None => { self.next=None; Ok(ReadEnd::EndOfInput) }
        }
    }
}

struct ContinuousReads<R: QueryRequest, Record, F> {
    port: QueryPort<R::Port>,
    system: OnceLock<Arc<R::Port>>,
    next: R,
    pending: VecDeque<Record>,
    output: F,
}

#[async_trait]
impl<R, Record, Output, F> Reads<Output> for ContinuousReads<R, Record, F>
where
    R: QueryRequest<Answer = ContinuousRead<Record, R>>
        + Clone
        + Send
        + Sync
        + 'static,
    R::Port: InputReadiness,
    Record: Send + Sync + 'static,
    Output: Send + Sync + 'static,
    F: Fn(Record) -> Result<Output, SourceError> + Send + Sync + 'static,
{

    fn source_system(&self) -> SourceSystem {
        self.port.system.clone()
    }

    fn is_continuous(&self) -> bool {
        true
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    async fn ready(&self, effects: &mut EffectsCore) -> Result<(), NotReady> {
        let system = match self.system.get() {
            Some(system) => system,
            None => {
                let resolved = self.port.system.resolve_query(effects.source_registry()).map_err(NotReady::Effect)?.map_err(|error| NotReady::Source(reported(error.failure)))?;
                self.system.get_or_init(|| resolved)
            }
        };
        system.ready().await.map_err(NotReady::Source)
    }

    fn next_output(&mut self) -> Option<Result<Output, SourceError>> {
        self.pending.pop_front().map(&self.output)
    }

    async fn read(&mut self, effects: &mut EffectsCore) -> Result<ReadEnd, EffectError> {
        let ContinuousRead { records, next } = ask(effects, &self.port, &self.next).await?;
        self.pending.extend(records);
        self.next = next;
        Ok(ReadEnd::Batch)
    }
}

pub(crate) fn recording_ends(
    context: &crate::effects::EffectInvocationContext,
    number: u64,
    ordinal: obzenflow_core::event::payloads::effect_payload::EffectOrdinal,
) -> bool {
    use obzenflow_core::event::payloads::effect_payload::EffectCursor;
    let execution = &context.stage.runtime_execution;
    execution.archive_for_io().is_some()
        && execution.resume_control().is_none()
        && context.stage.effect_history.as_ref().is_none_or(|history| {
            history
                .source_inputs.get(&EffectCursor::new(
                    history.recorded_flow_id().clone(),
                    context.stage.stage_key.clone(),
                    number,
                    ordinal,
                ))
                .is_none()
        })
}

pub(crate) fn recording_ends_before(
    context: &crate::effects::EffectInvocationContext,
    number: u64,
    fact: &obzenflow_core::EventId,
) -> bool {
    use obzenflow_core::event::payloads::effect_payload::EffectOrdinal;
    recording_ends(context, number + 1, EffectOrdinal::new(0))
        && context
            .stage
            .effect_history
            .as_ref()
            .is_some_and(|history| !history.actions().holds_fact(fact))
}

pub(crate) fn read_failure(error: &EffectError) -> Option<SourceError> {
    if let Some(EffectFailureDetail::SourceRead(failure)) = error.failure_detail() {
        return Some(reported(failure));
    }
    let transport = |reason: SourceDiagnosticReason| Some(SourceError::Transport(reason.into()));
    let terminal = |kind, reason: SourceDiagnosticReason| {
        Some(SourceError::Terminal {
            kind,
            diagnostic: reason.into(),
        })
    };
    if !matches!(
        error,
        EffectError::Timeout(_)
            | EffectError::Transport(_)
            | EffectError::RateLimited { .. }
            | EffectError::Permanent(_)
            | EffectError::Validation(_)
            | EffectError::Domain(_)
            | EffectError::Execution(_)
            | EffectError::DependencyFailed { .. }
            | EffectError::RecoveryAbandoned { .. }
            | EffectError::BoundaryRejected { .. }
            | EffectError::RecordedFailure { .. }
    ) {
        return None;
    }
    if error
        .failure_cause()
        .is_some_and(|cause| cause.source.as_str() == "replay")
    {
        return None;
    }
    match error.error_type().as_str() {
        "timeout" => Some(SourceError::Timeout(
            SourceDiagnosticReason::TimedOut.into(),
        )),
        "rate_limited" => transport(SourceDiagnosticReason::RateLimited),
        "validation" | "invalid_request" | "unsupported" => terminal(
            SourcePollErrorKind::Validation,
            SourceDiagnosticReason::InvalidRecord,
        ),
        "permanent" | "authentication" => terminal(
            SourcePollErrorKind::Other,
            SourceDiagnosticReason::RemoteRejected,
        ),
        "boundary_rejected" => transport(SourceDiagnosticReason::RemoteRejected),
        "binding_authority_fault"
        | "destination_changed"
        | "serialization"
        | "journal"
        | "missing_recorded_effect"
        | "recording_departure"
        | "recording_ended"
        | "duplicate_recorded_effect"
        | "descriptor_mismatch"
        | "effect_provenance_mismatch"
        | "incomplete_outcome_group"
        | "missing_idempotency_key"
        | "undeclared_effect"
        | "undeclared_output"
        | "emit_unsupported"
        | "completed_without_output"
        | "completed_empty_with_output"
        | "target_invariant_violation"
        | "transactional_commit_missing"
        | "replay_archive" => None,
        _ => transport(SourceDiagnosticReason::RemoteFailed),
    }
}

fn reported(failure: SourceReadFailure) -> SourceError {
    let SourceReadFailure {
        category,
        continuation,
        diagnostic,
    } = failure;
    if continuation == SourcePollContinuation::Terminal {
        return SourceError::Terminal {
            kind: category,
            diagnostic,
        };
    }
    match category {
        SourcePollErrorKind::Timeout => SourceError::Timeout(diagnostic),
        SourcePollErrorKind::Transport => SourceError::Transport(diagnostic),
        SourcePollErrorKind::Deserialization => SourceError::Deserialization(diagnostic),
        SourcePollErrorKind::Validation => SourceError::Validation(diagnostic),
        SourcePollErrorKind::Other => SourceError::Other(diagnostic),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{
        EffectFailureKind, RetryDisposition, SourceDiagnostic, SourceErrorCode,
    };

    fn recorded(live: &EffectError) -> EffectError {
        EffectError::RecordedFailure {
            error_type: live.error_type(),
            error_message: String::new(),
            retry: RetryDisposition::NotRetryable,
            cause: live.failure_cause(),
            detail: live.failure_detail().map(Box::new),
        }
    }

    #[test]
    fn failed_read_returns_remote_reason() {
        let cases = [
            (
                EffectError::Timeout("deadline".into()),
                SourceDiagnosticReason::TimedOut,
            ),
            (
                EffectError::RateLimited {
                    message: "429 Too Many Requests".into(),
                    retry_after: std::time::Duration::from_secs(1),
                },
                SourceDiagnosticReason::RateLimited,
            ),
            (
                EffectError::Permanent("401 Unauthorized".into()),
                SourceDiagnosticReason::RemoteRejected,
            ),
            (
                EffectError::Transport("503 Service Unavailable".into()),
                SourceDiagnosticReason::RemoteFailed,
            ),
        ];
        for (error, reason) in cases {
            let failure = read_failure(&error).expect("a remote failure is reported");
            assert_eq!(failure.diagnostic().reason(), reason, "{error:?}");
            assert!(!format!("{failure} {failure:?}").contains("Unauthorized"));
        }
    }

    #[test]
    fn refused_read_request_fails_terminally() {
        let failure = read_failure(&EffectError::Validation("422 Unprocessable Entity".into()))
            .expect("a refused request is reported");
        assert!(failure.is_terminal(), "{failure:?}");
        assert_eq!(failure.kind(), SourcePollErrorKind::Validation);
        assert_eq!(
            failure.diagnostic().reason(),
            SourceDiagnosticReason::InvalidRecord
        );
    }

    #[test]
    fn recorded_failure_translates_like_live() {
        let unauthorized = EffectError::SourceRead {
            error: Box::new(EffectError::Permanent(
                "401 Unauthorized; token sk_live_canary".into(),
            )),
            failure: Box::new(SourceReadFailure {
                category: SourcePollErrorKind::Validation,
                continuation: SourcePollContinuation::Terminal,
                diagnostic: SourceDiagnostic::new(SourceDiagnosticReason::RemoteRejected)
                    .code(SourceErrorCode::try_new("http.status", "401").unwrap()),
            }),
        };
        for live in [
            unauthorized,
            EffectError::Timeout("deadline".into()),
            EffectError::RateLimited {
                message: "429".into(),
                retry_after: std::time::Duration::from_secs(1),
            },
            EffectError::Transport("503".into()),
            EffectError::Permanent("404".into()),
            EffectError::Validation("422".into()),
            EffectError::Execution("opaque".into()),
        ] {
            let translated = read_failure(&live).expect("a system failure is reported");
            assert_eq!(read_failure(&recorded(&live)), Some(translated), "{live:?}");
        }
        let replayed = recorded(&EffectError::SourceRead {
            error: Box::new(EffectError::Permanent("200 OK".into())),
            failure: Box::new(SourceReadFailure {
                category: SourcePollErrorKind::Deserialization,
                continuation: SourcePollContinuation::Terminal,
                diagnostic: SourceDiagnosticReason::MalformedInput.into(),
            }),
        });
        let failure = read_failure(&replayed).expect("a recorded failure is reported");
        assert!(failure.is_terminal());
        assert_eq!(failure.kind(), SourcePollErrorKind::Deserialization);
        assert_eq!(
            failure.diagnostic().reason(),
            SourceDiagnosticReason::MalformedInput
        );
        assert_eq!(
            read_failure(&EffectError::RecordedFailure {
                error_type: EffectFailureKind::new("descriptor_mismatch"),
                error_message: String::new(),
                retry: RetryDisposition::NotRetryable,
                cause: None,
                detail: None,
            }),
            None,
            "a framework failure is no source error"
        );
    }
}

impl<T: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static> SourceRead<T, Continuous> {
    /// Receive sender-initiated values from a bounded in-process channel.
    pub fn channel(receiver: Arc<super::SourceReceiver<T>>) -> Self {
        let evidence=obzenflow_core::SystemBindingEvidence::named("channel").expect("valid channel binding");
        Self {
            reads: Box::new(ChannelEvents {receiver, system: SourceSystem::event(evidence), pending:None}),
            continuation: SourceContinuation::Reattach {guarantee:ReattachGuarantee::AtMostOnce},
            lifetime:PhantomData,
        }
    }
}

struct ChannelEvents<T> {
    receiver: Arc<super::SourceReceiver<T>>,
    system: SourceSystem,
    pending: Option<T>,
}

#[async_trait]
impl<T: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static> Reads<T> for ChannelEvents<T> {
    fn source_system(&self) -> SourceSystem { self.system.clone() }
    fn is_continuous(&self) -> bool { true }
    fn has_pending(&self) -> bool { self.pending.is_some() }
    async fn ready(&self, _: &mut EffectsCore) -> Result<(),NotReady> {
        self.receiver.ready().await.map_err(|_| NotReady::Source(SourceError::Other(SourceDiagnosticReason::InputClosed.into())))
    }
    fn next_output(&mut self)->Option<Result<T,SourceError>> { self.pending.take().map(Ok) }
    async fn read(&mut self,stage:&mut EffectsCore)->Result<ReadEnd,EffectError>{
        use obzenflow_core::query::SourceInput;
        let input = stage.source_input(|input| matches!(input,SourceInput::IngressMember{..}), || async {
            let item=self.receiver.take(1).pop().ok_or_else(|| EffectError::Execution("ready channel held no value".into()))?;
            let member=serde_json::to_value(item).map_err(|e| EffectError::Serialization(e.to_string()))?;
            Ok(SourceInput::IngressMember{member})
        }).await?;
        let SourceInput::IngressMember{member}=input else { unreachable!("channel received query answer") };
        self.pending=Some(serde_json::from_value(member).map_err(|e|EffectError::Serialization(e.to_string()))?);
        Ok(ReadEnd::Batch)
    }
}
