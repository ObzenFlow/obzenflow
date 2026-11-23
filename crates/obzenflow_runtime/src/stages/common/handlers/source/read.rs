// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::SourceError;
use crate::effects::{
    AnswersEffect, Call, CallRequest, EffectDeclaration, EffectError, EffectsCore, Port,
    PortEffects, RecordedReply, SourceSystem, StageEffectsError,
};
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_core::event::payloads::execution_payload::{ReattachGuarantee, SourcePollErrorKind};
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::WriterId;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;

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

/// `SourceRead` is the read that a source handler declares through its bound port.
/// The framework performs each read and keeps the argument of the next read from the
/// recorded reply.
pub struct SourceRead<Output> {
    reads: Box<dyn Reads<Output>>,
    continuation: super::SourceContinuation,
}

impl<Output: Send + Sync + 'static> SourceRead<Output> {
    /// `SourceRead::finite` makes a finite read. Each reply gives the argument of the next
    /// read or ends the input.
    pub fn finite<R, Record, F>(port: Port<R::Port>, first: R, output: F) -> Self
    where
        R: CallRequest<Outcome = FiniteRead<Record, R>, Records = RecordedReply>
            + Clone
            + Send
            + Sync
            + 'static,
        R::Port: PortEffects,
        Record: Send + Sync + 'static,
        F: Fn(Record) -> Result<Output, SourceError> + Send + Sync + 'static,
    {
        Self::finite_in(port, first, move |_: &R, record| output(record))
    }

    /// `SourceRead::finite_in` makes a finite read whose map also gets the argument of the
    /// next read, for example the columns of a file.
    pub fn finite_in<R, Record, F>(port: Port<R::Port>, first: R, output: F) -> Self
    where
        R: CallRequest<Outcome = FiniteRead<Record, R>, Records = RecordedReply>
            + Clone
            + Send
            + Sync
            + 'static,
        R::Port: PortEffects,
        Record: Send + Sync + 'static,
        F: Fn(&R, Record) -> Result<Output, SourceError> + Send + Sync + 'static,
    {
        Self {
            reads: Box::new(FiniteReads {
                port,
                next: Some(first),
                pending: VecDeque::new(),
                output,
            }),
            continuation: super::SourceContinuation::RecordedRead,
        }
    }

    /// `SourceRead::continuous` makes a continuous read. Each live read waits for `ready`,
    /// which resolves when the system holds input. A resumed run continues the live reads
    /// under `guarantee`.
    pub fn continuous<R, Record, F, Ready>(
        port: Port<R::Port>,
        first: R,
        ready: Ready,
        guarantee: ReattachGuarantee,
        output: F,
    ) -> Self
    where
        R: CallRequest<Outcome = ContinuousRead<Record, R>, Records = RecordedReply>
            + Clone
            + Send
            + Sync
            + 'static,
        R::Port: PortEffects,
        Record: Send + Sync + 'static,
        F: Fn(Record) -> Result<Output, SourceError> + Send + Sync + 'static,
        Ready: Fn() -> BoxFuture<'static, Result<(), SourceError>> + Send + Sync + 'static,
    {
        Self {
            reads: Box::new(ContinuousReads {
                port,
                next: first,
                pending: VecDeque::new(),
                ready,
                output,
            }),
            continuation: super::SourceContinuation::Reattach { guarantee },
        }
    }

    /// `in_process` returns the same read for input that only this process holds. The
    /// framework refuses to resume a run that did not finish this input.
    pub fn in_process(mut self) -> Self {
        self.continuation = super::SourceContinuation::Refuse;
        self
    }

    pub(crate) fn continuation(&self) -> super::SourceContinuation {
        self.continuation
    }

    pub(crate) fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        self.reads.declaration()
    }

    pub(crate) fn source_system(&self) -> SourceSystem {
        self.reads.source_system()
    }

    pub(crate) fn has_pending(&self) -> bool {
        self.reads.has_pending()
    }

    pub(crate) fn ready(&self) -> Option<BoxFuture<'static, Result<(), SourceError>>> {
        self.reads.ready()
    }

    pub(crate) fn is_continuous(&self) -> bool {
        matches!(
            self.continuation,
            super::SourceContinuation::Reattach { .. }
        )
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

#[async_trait]
trait Reads<Output>: Send + Sync {
    fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError>;
    fn source_system(&self) -> SourceSystem;
    fn has_pending(&self) -> bool;
    fn ready(&self) -> Option<BoxFuture<'static, Result<(), SourceError>>> {
        None
    }
    fn next_output(&mut self) -> Option<Result<Output, SourceError>>;
    async fn read(&mut self, effects: &mut EffectsCore) -> Result<ReadEnd, EffectError>;
}

struct FiniteReads<R: CallRequest, Record, F> {
    port: Port<R::Port>,
    next: Option<R>,
    pending: VecDeque<Record>,
    output: F,
}

#[async_trait]
impl<R, Record, Output, F> Reads<Output> for FiniteReads<R, Record, F>
where
    R: CallRequest<Outcome = FiniteRead<Record, R>, Records = RecordedReply>
        + Clone
        + Send
        + Sync
        + 'static,
    R::Port: PortEffects,
    Record: Send + Sync + 'static,
    Output: Send + Sync + 'static,
    F: Fn(&R, Record) -> Result<Output, SourceError> + Send + Sync + 'static,
{
    fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        <Port<R::Port> as AnswersEffect<Call<R>>>::declaration(&self.port)
    }

    fn source_system(&self) -> SourceSystem {
        self.port.source_system()
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn next_output(&mut self) -> Option<Result<Output, SourceError>> {
        let read = self.next.as_ref()?;
        self.pending
            .pop_front()
            .map(|record| (self.output)(read, record))
    }

    async fn read(&mut self, effects: &mut EffectsCore) -> Result<ReadEnd, EffectError> {
        let Some(argument) = self.next.clone() else {
            return Ok(ReadEnd::EndOfInput);
        };
        match effects.perform_call(argument).await? {
            FiniteRead::EndOfInput => {
                self.next = None;
                Ok(ReadEnd::EndOfInput)
            }
            FiniteRead::Batch { records, next } => {
                self.pending.extend(records);
                self.next = Some(next);
                Ok(ReadEnd::Batch)
            }
        }
    }
}

struct ContinuousReads<R: CallRequest, Record, Ready, F> {
    port: Port<R::Port>,
    next: R,
    pending: VecDeque<Record>,
    ready: Ready,
    output: F,
}

#[async_trait]
impl<R, Record, Output, Ready, F> Reads<Output> for ContinuousReads<R, Record, Ready, F>
where
    R: CallRequest<Outcome = ContinuousRead<Record, R>, Records = RecordedReply>
        + Clone
        + Send
        + Sync
        + 'static,
    R::Port: PortEffects,
    Record: Send + Sync + 'static,
    Output: Send + Sync + 'static,
    Ready: Fn() -> BoxFuture<'static, Result<(), SourceError>> + Send + Sync + 'static,
    F: Fn(Record) -> Result<Output, SourceError> + Send + Sync + 'static,
{
    fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        <Port<R::Port> as AnswersEffect<Call<R>>>::declaration(&self.port)
    }

    fn source_system(&self) -> SourceSystem {
        self.port.source_system()
    }

    fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    fn ready(&self) -> Option<BoxFuture<'static, Result<(), SourceError>>> {
        Some((self.ready)())
    }

    fn next_output(&mut self) -> Option<Result<Output, SourceError>> {
        self.pending.pop_front().map(&self.output)
    }

    async fn read(&mut self, effects: &mut EffectsCore) -> Result<ReadEnd, EffectError> {
        let ContinuousRead { records, next } = effects.perform_call(self.next.clone()).await?;
        self.pending.extend(records);
        self.next = next;
        Ok(ReadEnd::Batch)
    }
}

pub(crate) fn recording_ends(
    context: &crate::effects::EffectInvocationContext,
    number: u64,
) -> bool {
    use obzenflow_core::event::payloads::effect_payload::{EffectCursor, EffectOrdinal};
    let execution = &context.stage.runtime_execution;
    execution.archive_for_io().is_some()
        && execution.resume_control().is_none()
        && context.stage.effect_history.as_ref().is_none_or(|history| {
            history
                .recorded_descriptor_hash(&EffectCursor::new(
                    history.recorded_flow_id().clone(),
                    context.stage.stage_key.clone(),
                    number,
                    EffectOrdinal::new(0),
                ))
                .is_none()
        })
}

pub(crate) fn recording_ends_before(
    context: &crate::effects::EffectInvocationContext,
    number: u64,
    fact: &obzenflow_core::EventId,
) -> bool {
    recording_ends(context, number + 1)
        && context
            .stage
            .effect_history
            .as_ref()
            .is_some_and(|history| !history.actions().holds_fact(fact))
}

pub(crate) fn read_failure(error: &EffectError) -> Option<SourceError> {
    let reason = match error {
        EffectError::Timeout(_) => {
            return Some(SourceError::Timeout(
                SourceDiagnosticReason::TimedOut.into(),
            ))
        }
        EffectError::Transport(_) => SourceDiagnosticReason::RemoteFailed,
        EffectError::RateLimited { .. } => SourceDiagnosticReason::RateLimited,
        EffectError::Validation(_) => {
            return Some(SourceError::Terminal {
                kind: SourcePollErrorKind::Validation,
                diagnostic: SourceDiagnosticReason::InvalidRecord.into(),
            })
        }
        EffectError::Permanent(_) => {
            return Some(SourceError::Terminal {
                kind: SourcePollErrorKind::Other,
                diagnostic: SourceDiagnosticReason::RemoteRejected.into(),
            })
        }
        EffectError::Execution(_)
        | EffectError::Domain(_)
        | EffectError::DependencyFailed { .. }
        | EffectError::RecordedFailure { .. }
        | EffectError::RecoveryAbandoned { .. } => SourceDiagnosticReason::RemoteFailed,
        EffectError::BoundaryRejected { .. } if !error.is_replay_rejection() => {
            SourceDiagnosticReason::RemoteRejected
        }
        _ => return None,
    };
    Some(SourceError::Transport(reason.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
