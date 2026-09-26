// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution-owned publication lifetime. This scope retains work, never
//! lifecycle facts or permission to invoke a handler.
//!
//! SupervisorTaskBuilder installs the owner around each shared runner. New
//! tasks must explicitly enter a captured scope or use `commit_in` with that
//! owner; Tokio does not inherit the task-local binding when spawning a task.
//! With neither a bound nor an explicit owner, standalone callers execute
//! inline and cancellation follows the caller's future. Retained publication
//! guarantees require an owner.

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use obzenflow_core::event::payloads::JournalPayload;
use obzenflow_core::event::{CausalFrontier, JournalEvent, JournalRecord};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::{Journal, JournalError};
use std::collections::BTreeMap;
use std::error::Error;
use std::future::Future;
use std::sync::{Arc, Mutex};
use tokio::sync::{oneshot, Semaphore};

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
#[error("publication admission is closed")]
pub(crate) struct AdmissionClosed;

pub(crate) fn is_admission_closed(mut error: &(dyn Error + 'static)) -> bool {
    loop {
        if error.is::<AdmissionClosed>() {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SharedError(Arc<dyn Error + Send + Sync>);

impl std::fmt::Display for SharedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Error for SharedError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.0.as_ref())
    }
}

impl From<BoxError> for SharedError {
    fn from(error: BoxError) -> Self {
        Self(Arc::from(error))
    }
}

pub(crate) fn is_indeterminate(mut error: &(dyn Error + 'static)) -> bool {
    loop {
        if matches!(
            error.downcast_ref::<JournalError>(),
            Some(JournalError::CommitIndeterminate { .. })
        ) {
            return true;
        }
        match error.source() {
            Some(source) => error = source,
            None => return false,
        }
    }
}

/// A known physical commit with inconsistent accounting cannot admit more
/// output or seal a trustworthy frontier. Preserve the originating error.
pub(crate) fn accounting_failed(error: BoxError) -> BoxError {
    if let Some(scope) = PublicationScope::current() {
        scope
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .admission = Admission::Poisoned;
    }
    error
}

type Completion = Shared<BoxFuture<'static, Result<(), SharedError>>>;

/// An observation of the publications accepted at capture time. It neither
/// closes admission nor owns a new task. Every captured join is observed.
pub(crate) type PublicationSettlement = Shared<BoxFuture<'static, Result<(), SharedError>>>;

struct Operation {
    task: tokio::task::AbortHandle,
    completion: Completion,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Admission {
    Open,
    Closed,
    Poisoned,
}

struct State {
    admission: Admission,
    next_id: u64,
    operations: BTreeMap<u64, Operation>,
    tail: Option<Shared<BoxFuture<'static, CausalFrontier>>>,
    inputs: CausalFrontier,
    failure: Option<SharedError>,
}

/// One scope per supervisor, retained by its standard handle through joining.
pub(crate) struct PublicationScope {
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    control_slots: Arc<Semaphore>,
    host_slots: Arc<Semaphore>,
    ordered: bool,
}

#[derive(Clone)]
struct PublicationContext {
    scope: Arc<PublicationScope>,
    accepted: bool,
    frontier: Option<Arc<Mutex<CausalFrontier>>>,
}

tokio::task_local! {
    static CURRENT: PublicationContext;
}

impl PublicationScope {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_ordering(true, 1)
    }

    pub(crate) fn pipeline() -> Arc<Self> {
        Self::with_ordering(true, 2)
    }

    #[cfg(test)]
    pub(crate) fn concurrent() -> Arc<Self> {
        Self::with_ordering(false, 1)
    }

    fn with_ordering(ordered: bool, control_capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            ordered,
            state: Mutex::new(State {
                admission: Admission::Open,
                next_id: 0,
                operations: BTreeMap::new(),
                tail: None,
                inputs: CausalFrontier::default(),
                failure: None,
            }),
            slots: Arc::new(Semaphore::new(64)),
            control_slots: Arc::new(Semaphore::new(control_capacity)),
            host_slots: Arc::new(Semaphore::new(8)),
        })
    }

    pub(crate) fn current() -> Option<Arc<Self>> {
        CURRENT.try_with(|context| context.scope.clone()).ok()
    }

    pub(crate) fn has_capacity(&self, count: usize) -> bool {
        self.slots.available_permits() >= count
    }

    pub(crate) fn wait_for_capacity(
        self: &Arc<Self>,
        count: u32,
    ) -> BoxFuture<'static, Result<(), BoxError>> {
        let slots = self.slots.clone();
        async move {
            let permit = slots
                .acquire_many_owned(count)
                .await
                .map_err(|_| Box::new(AdmissionClosed) as BoxError)?;
            drop(permit);
            Ok(())
        }
        .boxed()
    }

    /// Incorporated inputs only. Prefetch and optional telemetry never call this.
    pub(crate) fn incorporate(&self, input: &CausalFrontier) -> Result<(), JournalError> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .inputs
            .merge(input)?;
        Ok(())
    }

    pub(crate) fn capture(&self) -> CausalFrontier {
        if let Ok(Some(frontier)) = CURRENT.try_with(|context| {
            if std::ptr::eq(context.scope.as_ref(), self) {
                context.frontier.clone()
            } else {
                None
            }
        }) {
            return frontier.lock().unwrap_or_else(|e| e.into_inner()).clone();
        }
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .inputs
            .clone()
    }

    pub(crate) fn enter_sync<T>(self: &Arc<Self>, f: impl FnOnce() -> T) -> T {
        CURRENT.sync_scope(
            PublicationContext {
                scope: self.clone(),
                accepted: false,
                frontier: None,
            },
            f,
        )
    }

    pub(crate) async fn enter<T>(self: &Arc<Self>, future: impl Future<Output = T>) -> T {
        CURRENT
            .scope(
                PublicationContext {
                    scope: self.clone(),
                    accepted: false,
                    frontier: None,
                },
                future,
            )
            .await
    }

    /// Synchronous admission closure always precedes aborting stage execution.
    /// Accepted work is not aborted, including work awaiting its predecessor.
    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.admission == Admission::Open {
            state.admission = Admission::Closed;
        }
        self.slots.close();
        self.control_slots.close();
        self.host_slots.close();
    }

    pub(crate) fn first_failure(&self) -> Option<SharedError> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::reap(&mut state);
        state.failure.clone()
    }

    pub(crate) fn observe_accepted(&self) -> PublicationSettlement {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        Self::reap(&mut state);
        let failure = state.failure.clone();
        let completions: Vec<_> = state
            .operations
            .values()
            .map(|operation| operation.completion.clone())
            .collect();
        async move {
            let results = futures::future::join_all(completions).await;
            match failure.or_else(|| results.into_iter().find_map(Result::err)) {
                Some(error) => Err(error),
                None => Ok(()),
            }
        }
        .boxed()
        .shared()
    }

    fn retain_error(&self, error: BoxError) -> BoxError {
        let error = SharedError::from(error);
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if is_indeterminate(&error) {
            state.admission = Admission::Poisoned;
        }
        state.failure.get_or_insert(error.clone());
        Box::new(error)
    }

    fn reap(state: &mut State) {
        state.operations.retain(|_, operation| {
            if !operation.task.is_finished() {
                return true;
            }
            match operation.completion.clone().now_or_never() {
                Some(result) => {
                    if let Err(error) = result {
                        state.failure.get_or_insert(error);
                    }
                    false
                }
                None => true,
            }
        });
    }

    pub(crate) fn accept<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
    ) -> BoxFuture<'static, Result<T, BoxError>> {
        self.accept_with_frontier(self.capture(), operation)
    }

    fn accept_with_frontier<T: Send + 'static>(
        self: &Arc<Self>,
        snapshot: CausalFrontier,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
    ) -> BoxFuture<'static, Result<T, BoxError>> {
        let scope = self.clone();
        let operation = operation.boxed();
        async move {
            let slot = scope
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| AdmissionClosed)?;
            scope.register(slot, operation, snapshot)?.await
        }
        .boxed()
    }

    /// Host commands share the publication owner and writer tail, with their
    /// own bounded admission pool. They cannot consume the FSM's output
    /// headroom between its admission check and transition actions.
    pub(crate) fn accept_host<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
    ) -> BoxFuture<'static, Result<T, BoxError>> {
        let scope = self.clone();
        let frontier = self.capture();
        async move {
            let slot = scope
                .host_slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| Box::new(AdmissionClosed) as BoxError)?;
            scope.register(slot, operation, frontier)?.await
        }
        .boxed()
    }

    /// Register from the owner before yielding. Pipeline admission uses this
    /// bounded path so task scheduling cannot reorder lifecycle facts.
    pub(crate) fn enqueue<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
    ) -> Result<BoxFuture<'static, Result<T, BoxError>>, BoxError> {
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => Box::new(AdmissionClosed) as BoxError,
                error => Box::new(error) as BoxError,
            })?;
        self.register(slot, operation, self.capture())
    }

    /// Reserved control admission shares the ordinary writer tail. Capacity
    /// bounds admission only; it cannot overtake an accepted blocked write.
    pub(crate) fn enqueue_control<T: Send + 'static>(
        self: &Arc<Self>,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
    ) -> Result<BoxFuture<'static, Result<T, BoxError>>, BoxError> {
        let slot = self
            .control_slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => Box::new(AdmissionClosed) as BoxError,
                error => Box::new(error) as BoxError,
            })?;
        self.register(slot, operation, self.capture())
    }

    fn register<T: Send + 'static>(
        self: &Arc<Self>,
        slot: tokio::sync::OwnedSemaphorePermit,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
        snapshot: CausalFrontier,
    ) -> Result<BoxFuture<'static, Result<T, BoxError>>, BoxError> {
        let operation = operation.boxed();
        let (receipt_tx, receipt_rx) = oneshot::channel();
        {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            Self::reap(&mut state);
            if state.admission != Admission::Open {
                return Err(AdmissionClosed.into());
            }
            let id = state.next_id;
            state.next_id = state
                .next_id
                .checked_add(1)
                .ok_or_else(|| std::io::Error::other("publication operation identity exhausted"))?;
            let previous = if self.ordered {
                state.tail.take()
            } else {
                None
            };
            let (done_tx, done_rx) = oneshot::channel();
            state.tail = Some(
                async move { done_rx.await.unwrap_or_default() }
                    .boxed()
                    .shared(),
            );
            let scope = self.clone();
            let task = tokio::spawn(async move {
                let _slot = slot;
                let mut frontier = snapshot;
                if let Some(previous) = previous {
                    if let Err(error) = frontier.merge(&previous.await) {
                        let _ = scope.retain_error(Box::new(error));
                        scope
                            .state
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .admission = Admission::Poisoned;
                    }
                }
                let frontier = Arc::new(Mutex::new(frontier));
                let poisoned = scope
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .admission
                    == Admission::Poisoned;
                let result = if poisoned {
                    Err(SharedError::from(Box::new(std::io::Error::other(
                        "publication writer is poisoned",
                    )) as BoxError))
                } else {
                    match CURRENT
                        .scope(
                            PublicationContext {
                                scope: scope.clone(),
                                accepted: true,
                                frontier: Some(frontier.clone()),
                            },
                            std::panic::AssertUnwindSafe(operation).catch_unwind(),
                        )
                        .await
                    {
                        Ok(result) => result.map_err(SharedError::from),
                        Err(_) => {
                            scope
                                .state
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .admission = Admission::Poisoned;
                            Err(SharedError::from(Box::new(std::io::Error::other(
                                "publication accounting panicked",
                            ))
                                as BoxError))
                        }
                    }
                };
                let settlement = result.as_ref().map(|_| ()).map_err(Clone::clone);
                if let Err(error) = &settlement {
                    let mut state = scope.state.lock().unwrap_or_else(|e| e.into_inner());
                    if is_indeterminate(error) {
                        state.admission = Admission::Poisoned;
                    }
                    state.failure.get_or_insert(error.clone());
                }
                let _ = receipt_tx.send(result);
                let _ = done_tx.send(frontier.lock().unwrap_or_else(|e| e.into_inner()).clone());
                settlement
            });
            let abort = task.abort_handle();
            let completion = async move {
                task.await
                    .unwrap_or_else(|error| Err(SharedError::from(Box::new(error) as BoxError)))
            }
            .boxed()
            .shared();
            state.operations.insert(
                id,
                Operation {
                    task: abort,
                    completion,
                },
            );
        }
        Ok(async move {
            receipt_rx
                .await
                .map_err(|error| Box::new(error) as BoxError)?
                .map_err(|error| Box::new(error) as BoxError)
        }
        .boxed())
    }

    /// Every waiter uses retained completion clones. Cancelling a join cannot
    /// consume another waiter's capability or release accepted work.
    pub(crate) async fn join(&self) -> Result<(), SharedError> {
        self.close();
        loop {
            let next = {
                let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
                Self::reap(&mut state);
                state
                    .operations
                    .first_key_value()
                    .map(|(id, op)| (*id, op.completion.clone()))
            };
            let Some((id, completion)) = next else {
                break;
            };
            let result = completion.await;
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(error) = result {
                state.failure.get_or_insert(error);
            }
            state.operations.remove(&id);
        }
        match &self.state.lock().unwrap_or_else(|e| e.into_inner()).failure {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }
}

pub(crate) fn commit<T: Send + 'static>(
    operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
) -> BoxFuture<'static, Result<T, BoxError>> {
    // Erase the owned operation at this boundary. Nesting concrete async
    // states through effects, output commitment and admission otherwise
    // multiplies their stack frames before the retained task is spawned.
    let operation = operation.boxed();
    let context = CURRENT.try_with(Clone::clone).ok();
    match context {
        Some(context) if !context.accepted => context.scope.accept(operation),
        Some(context) => async move {
            CURRENT
                .scope(context.clone(), operation)
                .await
                .map_err(|error| context.scope.retain_error(error))
        }
        .boxed(),
        _ => operation,
    }
}

/// The captured owner governs admission, writer order and failure retention.
/// Only a matching task-local owner can supply an already-accepted context.
#[cfg(test)]
pub(crate) fn commit_in<T: Send + 'static>(
    scope: Option<Arc<PublicationScope>>,
    operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
) -> BoxFuture<'static, Result<T, BoxError>> {
    let operation = operation.boxed();
    if let Some(scope) = scope {
        let same_owner = CURRENT
            .try_with(|context| Arc::ptr_eq(&context.scope, &scope))
            .unwrap_or(false);
        if !same_owner {
            return scope.accept(operation);
        }
    }
    commit(operation)
}

/// Deferred effect handles retain their input snapshot across owner/task changes.
/// The destination's completed publication tail is still incorporated at execution.
pub(crate) fn commit_in_with_frontier<T: Send + 'static>(
    scope: Option<Arc<PublicationScope>>,
    frontier: CausalFrontier,
    operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
) -> BoxFuture<'static, Result<T, BoxError>> {
    if let Some(scope) = scope {
        let inline = CURRENT
            .try_with(|context| context.accepted && Arc::ptr_eq(&context.scope, &scope))
            .unwrap_or(false);
        if !inline {
            return scope.accept_with_frontier(frontier, operation);
        }
    }
    commit(with_snapshot(frontier, operation))
}

/// Raw stage/control publications also cross the execution scope. More complex
/// consumers enclose this append and their accounting in one `commit` operation.
pub(crate) fn report(
    journal: &super::SupervisorJournal,
    event: obzenflow_core::event::SystemEvent,
    options: AppendOptions<obzenflow_core::event::SystemEvent>,
) -> BoxFuture<'static, Result<super::SupervisorRecord, BoxError>> {
    let journal = journal.clone();
    commit(async move {
        journal
            .append_inline(event, options)
            .await
            .map_err(Into::into)
    })
}

pub(crate) async fn report_inline(
    journal: &super::SupervisorJournal,
    event: obzenflow_core::event::SystemEvent,
    options: AppendOptions<obzenflow_core::event::SystemEvent>,
) -> Result<super::SupervisorRecord, JournalError> {
    journal.append_inline(event, options).await
}

pub(crate) fn append<T: JournalEvent + 'static>(
    journal: &Arc<dyn Journal<T>>,
    event: T,
    options: obzenflow_core::journal::AppendOptions<T>,
) -> BoxFuture<'static, Result<JournalRecord<T::Payload>, BoxError>> {
    let journal = journal.clone();
    commit(async move {
        append_inline(&journal, event, options)
            .await
            .map_err(Into::into)
    })
}

/// Capture a queued output or cross-owner message before its next wait.
pub(crate) fn capture() -> CausalFrontier {
    CURRENT
        .try_with(|context| match &context.frontier {
            Some(frontier) => frontier.lock().unwrap_or_else(|e| e.into_inner()).clone(),
            None => context.scope.capture(),
        })
        .unwrap_or_else(|_| {
            DETACHED_INPUT
                .try_with(|frontier| frontier.lock().unwrap_or_else(|e| e.into_inner()).clone())
                .unwrap_or_default()
        })
}

/// An explicit frozen input context for deferred work, without new admission,
/// a new writer, or a scheduling wait. Its publication still inherits the tail.
pub(crate) async fn with_snapshot<T>(
    frontier: CausalFrontier,
    future: impl Future<Output = T>,
) -> T {
    if let Ok(mut context) = CURRENT.try_with(Clone::clone) {
        if context.accepted {
            // An inline nested publication keeps the retained operation's
            // receipts. A frozen input cannot replace that completed work.
            admit_command(&frontier);
        } else {
            context.frontier = Some(Arc::new(Mutex::new(frontier)));
        }
        CURRENT.scope(context, future).await
    } else {
        // Detached command delivery still carries evidence through EventSender.
        DETACHED_INPUT
            .scope(Arc::new(Mutex::new(frontier)), future)
            .await
    }
}

tokio::task_local! { static DETACHED_INPUT: Arc<Mutex<CausalFrontier>>; }

pub(crate) fn admit_command(frontier: &CausalFrontier) -> bool {
    match incorporate(frontier) {
        Ok(()) => true,
        Err(error) => {
            if let Some(scope) = PublicationScope::current() {
                let _ = scope.retain_error(Box::new(error));
                scope
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .admission = Admission::Poisoned;
                scope.close();
            }
            false
        }
    }
}

pub(crate) fn incorporate(frontier: &CausalFrontier) -> Result<(), JournalError> {
    CURRENT
        .try_with(|context| match &context.frontier {
            Some(current) => {
                current
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .merge(frontier)?;
                context.scope.incorporate(frontier)
            }
            None => context.scope.incorporate(frontier),
        })
        .unwrap_or_else(|_| {
            DETACHED_INPUT
                .try_with(|current| {
                    current
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .merge(frontier)
                        .map_err(JournalError::from)
                })
                .unwrap_or(Ok(()))
        })
}

pub(crate) fn observe_record<P: JournalPayload>(
    record: &JournalRecord<P>,
) -> Result<(), JournalError> {
    incorporate(&CausalFrontier::from_record(record)?)
}

/// Receipt retention precedes accounting, mirrors and acknowledgement, including
/// on paths where those later steps fail or the original waiter is cancelled.
pub(crate) async fn append_inline<T: JournalEvent + 'static>(
    journal: &Arc<dyn Journal<T>>,
    event: T,
    mut options: AppendOptions<T>,
) -> Result<JournalRecord<T::Payload>, JournalError> {
    if PublicationScope::current().is_some_and(|scope| {
        scope
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .admission
            == Admission::Poisoned
    }) {
        return Err(JournalError::CommitIndeterminate {
            source: "causal publication owner is poisoned".into(),
        });
    }
    options.frontier.merge(&capture())?;
    let written = journal.append(event, options).await?;
    observe_record(&written)?;
    Ok(written)
}

pub(crate) async fn append_group_inline<T: JournalEvent + 'static>(
    journal: &Arc<dyn Journal<T>>,
    group_id: &str,
    events: Vec<T>,
    mut options: AppendOptions<T>,
) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
    if PublicationScope::current().is_some_and(|scope| {
        scope
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .admission
            == Admission::Poisoned
    }) {
        return Err(JournalError::CommitIndeterminate {
            source: "causal publication owner is poisoned".into(),
        });
    }
    options.frontier.merge(&capture())?;
    let written = journal.append_group(group_id, events, options).await?;
    for record in &written {
        observe_record(record)?;
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn causal_input() -> JournalRecord<obzenflow_core::event::ChainPayload> {
        JournalRecord::new(
            obzenflow_core::JournalWriterId::new(),
            obzenflow_core::event::ChainEventFactory::data_event(
                obzenflow_core::StageId::new().into(),
                "publication.input",
                serde_json::json!({}),
            ),
        )
    }

    #[tokio::test]
    async fn creation_snapshot_excludes_later_inputs_but_includes_completed_tail_after_cancellation(
    ) {
        let owner = PublicationScope::new();
        let input = causal_input();
        let committed = causal_input();
        let later = causal_input();
        owner
            .incorporate(&CausalFrontier::from_record(&input).unwrap())
            .unwrap();
        let (release, gate) = oneshot::channel();
        let committed_coordinate = committed.causal_coordinate();
        // Drop acknowledgement immediately: the accepted publication still owns its receipt.
        drop(
            owner
                .enqueue(async move {
                    gate.await?;
                    observe_record(&committed)?;
                    Err::<(), BoxError>(std::io::Error::other("later step rejected").into())
                })
                .unwrap(),
        );
        let next = owner.accept(async { Ok(capture()) });
        owner
            .incorporate(&CausalFrontier::from_record(&later).unwrap())
            .unwrap();
        release.send(()).unwrap();
        let frontier = next.await.unwrap();
        assert_eq!(frontier.clock().get(&input.causal_coordinate()), 1);
        assert_eq!(frontier.clock().get(&committed_coordinate), 1);
        assert_eq!(frontier.clock().get(&later.causal_coordinate()), 0);
        assert!(owner.join().await.is_err());
    }

    #[tokio::test]
    async fn accounting_failure_poison_retains_successful_receipt() {
        let owner = PublicationScope::new();
        let committed = causal_input();
        let coordinate = committed.causal_coordinate();
        let result = owner
            .accept(async move {
                observe_record(&committed)?;
                Err::<(), _>(accounting_failed(
                    std::io::Error::other("accounting failed").into(),
                ))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(owner.capture().clock().get(&coordinate), 1);
        assert!(owner.accept(async { Ok(()) }).await.is_err());
        assert!(owner.join().await.is_err());
    }

    #[tokio::test]
    async fn stalled_publication_does_not_block_an_independent_owner() {
        let stalled = PublicationScope::new();
        let independent = PublicationScope::new();
        let common = causal_input();
        let input = CausalFrontier::from_record(&common).unwrap();
        stalled.incorporate(&input).unwrap();
        independent.incorporate(&input).unwrap();

        let pending_record = causal_input();
        let pending_coordinate = pending_record.causal_coordinate();
        let (release, gate) = oneshot::channel();
        let (started, running) = oneshot::channel();
        let mut pending = stalled
            .enqueue(async move {
                started.send(()).unwrap();
                gate.await?;
                observe_record(&pending_record)?;
                Ok(())
            })
            .unwrap();
        running.await.unwrap();

        // Completion before releasing the other owner proves there is no
        // cross-owner admission or causal-resolution wait. The common input
        // does not make the stalled publication available as evidence.
        let frontier = independent.accept(async { Ok(capture()) }).await.unwrap();
        assert_eq!(frontier, input);
        assert_eq!(frontier.clock().get(&pending_coordinate), 0);
        assert!(futures::poll!(&mut pending).is_pending());

        release.send(()).unwrap();
        pending.await.unwrap();
        assert_eq!(stalled.capture().clock().get(&pending_coordinate), 1);
        assert_eq!(independent.capture(), input);
        stalled.join().await.unwrap();
        independent.join().await.unwrap();
    }

    #[tokio::test]
    async fn captured_owner_keeps_writer_order_and_failure_under_another_accepted_scope() {
        let owner = PublicationScope::new();
        let ambient = PublicationScope::new();
        let order = Arc::new(Mutex::new(Vec::new()));
        let (release, gate) = oneshot::channel();
        let first_order = order.clone();
        let first = owner
            .enqueue(async move {
                gate.await?;
                first_order.lock().unwrap().push("earlier");
                Ok(())
            })
            .unwrap();

        let captured = owner.clone();
        let captured_order = order.clone();
        ambient
            .accept(async move {
                let mut receipt = commit_in(Some(captured), async move {
                    captured_order.lock().unwrap().push("captured");
                    Err::<(), BoxError>(Box::new(JournalError::CommitIndeterminate {
                        source: std::io::Error::other("lost acknowledgement").into(),
                    }))
                });
                // The captured publication must wait for its own writer tail,
                // even though the caller is already accepted by another owner.
                assert!(futures::poll!(&mut receipt).is_pending());
                release.send(()).unwrap();
                assert!(is_indeterminate(receipt.await.unwrap_err().as_ref()));
                Ok(())
            })
            .await
            .unwrap();

        first.await.unwrap();
        assert_eq!(*order.lock().unwrap(), ["earlier", "captured"]);
        ambient.join().await.unwrap();
        assert!(is_indeterminate(&owner.join().await.unwrap_err()));
    }

    #[tokio::test]
    async fn captured_owner_rejects_closed_admission_under_another_scope() {
        let owner = PublicationScope::new();
        let ambient = PublicationScope::new();
        let executed = Arc::new(AtomicUsize::new(0));
        owner.close();

        let count = executed.clone();
        let error = ambient
            .enter(commit_in(Some(owner.clone()), async move {
                count.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }))
            .await
            .unwrap_err();

        assert!(is_admission_closed(error.as_ref()));
        assert_eq!(executed.load(Ordering::Relaxed), 0);
        owner.join().await.unwrap();
        ambient.join().await.unwrap();
    }

    #[tokio::test]
    async fn captured_owner_retains_nested_commits_after_unscoped_child_is_cancelled() {
        let owner = PublicationScope::new();
        let accounted = Arc::new(AtomicUsize::new(0));
        let (started, accepted) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let count = accounted.clone();
        let caller = owner.enter_sync(move || {
            let captured = PublicationScope::current().unwrap();
            tokio::spawn(async move {
                // Tokio does not inherit the parent's task-local binding.
                assert!(PublicationScope::current().is_none());
                let nested_owner = captured.clone();
                commit_in(Some(captured), async move {
                    started.send(()).unwrap();
                    gate.await?;
                    let nested_count = count.clone();
                    commit(async move {
                        nested_count.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    })
                    .await?;
                    // Both nested entry points must finish within the
                    // accepted publication after admission has closed.
                    commit_in(Some(nested_owner), async move {
                        count.fetch_add(1, Ordering::Relaxed);
                        Err::<(), BoxError>(Box::new(JournalError::CommitIndeterminate {
                            source: std::io::Error::other("lost acknowledgement").into(),
                        }))
                    })
                    .await
                })
                .await
            })
        });

        accepted.await.unwrap();
        owner.close();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        let mut settlement = Box::pin(owner.join());
        assert!(futures::poll!(&mut settlement).is_pending());
        release.send(()).unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), settlement)
            .await
            .expect("nested commits must not queue behind their own publication")
            .unwrap_err();
        assert!(is_indeterminate(&error));
        assert_eq!(accounted.load(Ordering::Relaxed), 2);
        assert!(PublicationScope::current().is_none());
    }

    #[tokio::test]
    async fn unscoped_standalone_publication_remains_caller_owned() {
        assert!(PublicationScope::current().is_none());
        assert_eq!(commit_in(None, async { Ok(7) }).await.unwrap(), 7);

        let (release, gate) = oneshot::channel::<()>();
        let mut publication = commit(async move {
            gate.await?;
            Ok(())
        });
        assert!(futures::poll!(&mut publication).is_pending());
        drop(publication);
        assert!(release.send(()).is_err());
    }

    #[tokio::test]
    async fn saturated_writer_reserves_two_controls_in_the_same_order() {
        let scope = PublicationScope::pipeline();
        let (release, gate) = oneshot::channel();
        drop(
            scope
                .enqueue(async move {
                    gate.await?;
                    Ok(())
                })
                .unwrap(),
        );
        let order = Arc::new(Mutex::new(Vec::new()));
        for index in 1..64 {
            let order = order.clone();
            drop(
                scope
                    .enqueue(async move {
                        order.lock().unwrap().push(index);
                        Ok(())
                    })
                    .unwrap(),
            );
        }
        assert!(scope.enqueue(async { Ok(()) }).is_err());
        for index in 64..66 {
            let order = order.clone();
            drop(
                scope
                    .enqueue_control(async move {
                        order.lock().unwrap().push(index);
                        Ok(())
                    })
                    .unwrap(),
            );
        }
        assert!(scope.enqueue_control(async { Ok(()) }).is_err());
        assert!(order.lock().unwrap().is_empty());
        let mut abandoned = Box::pin(scope.observe_accepted());
        assert!(futures::poll!(&mut abandoned).is_pending());
        drop(abandoned);
        release.send(()).unwrap();
        scope.observe_accepted().await.unwrap();
        assert_eq!(*order.lock().unwrap(), (1..66).collect::<Vec<_>>());
        // Observation did not close admission.
        scope.enqueue(async { Ok(()) }).unwrap().await.unwrap();
        scope.join().await.unwrap();
        assert!(scope.enqueue_control(async { Ok(()) }).is_err());
    }

    #[tokio::test]
    async fn observation_captures_only_accepted_writes_and_exposes_failure_before_all_settle() {
        let scope = PublicationScope::concurrent();
        let before = scope.observe_accepted();
        let (release, gate) = oneshot::channel();
        drop(
            scope
                .enqueue(async move {
                    gate.await?;
                    Ok(())
                })
                .unwrap(),
        );
        let failure = scope
            .enqueue(async {
                Err::<(), BoxError>(Box::new(JournalError::CommitIndeterminate {
                    source: std::io::Error::other("lost acknowledgement").into(),
                }))
            })
            .unwrap();
        before.await.unwrap();
        let mut observation = Box::pin(scope.observe_accepted());
        assert!(futures::poll!(&mut observation).is_pending());
        assert!(failure.await.is_err());
        assert!(is_indeterminate(&scope.first_failure().unwrap()));
        assert!(futures::poll!(&mut observation).is_pending());
        release.send(()).unwrap();
        assert!(is_indeterminate(&observation.await.unwrap_err()));
        assert!(is_indeterminate(&scope.join().await.unwrap_err()));
    }

    #[tokio::test]
    async fn dropped_receipt_and_cancelled_join_retain_fifo_accounting() {
        let scope = PublicationScope::new();
        let accounted = Arc::new(AtomicUsize::new(0));
        let (release, gate) = oneshot::channel();
        let first = scope.clone();
        let first_count = accounted.clone();
        let mut receipt = Box::pin(first.accept(async move {
            gate.await?;
            first_count.store(1, Ordering::Release);
            Ok(())
        }));
        assert!(futures::poll!(receipt.as_mut()).is_pending());
        drop(receipt);
        let second = scope.clone();
        let second_count = accounted.clone();
        let mut receipt = Box::pin(second.accept(async move {
            assert_eq!(second_count.load(Ordering::Acquire), 1);
            second_count.store(2, Ordering::Release);
            Ok(())
        }));
        assert!(futures::poll!(receipt.as_mut()).is_pending());
        drop(receipt);
        scope.close();
        assert!(scope.accept(async { Ok(()) }).await.is_err());
        let mut abandoned = Box::pin(scope.join());
        assert!(futures::poll!(abandoned.as_mut()).is_pending());
        drop(abandoned);
        release.send(()).unwrap();
        let (one, two) = tokio::join!(scope.join(), scope.join());
        one.unwrap();
        two.unwrap();
        assert_eq!(accounted.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn uncertain_commit_poison_is_typed_and_never_runs_queued_work() {
        let scope = PublicationScope::new();
        let (release, gate) = oneshot::channel();
        let first = scope.clone();
        let mut receipt = Box::pin(first.accept(async move {
            gate.await?;
            Err::<(), BoxError>(Box::new(JournalError::CommitIndeterminate {
                source: std::io::Error::other("lost acknowledgement").into(),
            }))
        }));
        assert!(futures::poll!(receipt.as_mut()).is_pending());
        drop(receipt);
        let count = Arc::new(AtomicUsize::new(0));
        let pending_count = count.clone();
        let second = scope.clone();
        let mut queued = Box::pin(second.accept(async move {
            pending_count.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }));
        assert!(futures::poll!(queued.as_mut()).is_pending());
        drop(queued);
        release.send(()).unwrap();
        let error = scope.join().await.unwrap_err();
        assert!(is_indeterminate(&error));
        assert_eq!(count.load(Ordering::Relaxed), 0);
        assert!(is_indeterminate(&scope.join().await.unwrap_err()));
    }

    #[tokio::test]
    async fn accounting_panic_is_a_retained_settlement_failure() {
        let scope = PublicationScope::new();
        let error = scope
            .accept(async {
                panic!("accounting failed");
                #[allow(unreachable_code)]
                Ok::<(), BoxError>(())
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("accounting panicked"));
        assert!(scope
            .join()
            .await
            .unwrap_err()
            .to_string()
            .contains("accounting panicked"));
    }
}
