// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Execution-owned publication lifetime. This scope retains work, never
//! lifecycle facts or permission to invoke a handler.

use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use obzenflow_core::event::{EventEnvelope, JournalEvent};
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
    auxiliary: Option<Operation>,
    tail: Option<Shared<BoxFuture<'static, ()>>>,
    failure: Option<SharedError>,
}

/// One scope per supervisor, retained by its standard handle through joining.
pub(crate) struct PublicationScope {
    state: Mutex<State>,
    slots: Arc<Semaphore>,
    ordered: bool,
}

#[derive(Clone)]
struct PublicationContext {
    scope: Arc<PublicationScope>,
    accepted: bool,
}

tokio::task_local! {
    static CURRENT: PublicationContext;
}

impl PublicationScope {
    pub(crate) fn new() -> Arc<Self> {
        Self::with_ordering(true)
    }

    pub(crate) fn concurrent() -> Arc<Self> {
        Self::with_ordering(false)
    }

    fn with_ordering(ordered: bool) -> Arc<Self> {
        Arc::new(Self {
            ordered,
            state: Mutex::new(State {
                admission: Admission::Open,
                next_id: 0,
                operations: BTreeMap::new(),
                auxiliary: None,
                tail: None,
                failure: None,
            }),
            slots: Arc::new(Semaphore::new(64)),
        })
    }

    pub(crate) fn current() -> Option<Arc<Self>> {
        CURRENT.try_with(|context| context.scope.clone()).ok()
    }

    /// Retain the stage's heartbeat task without putting its lifetime ahead
    /// of publications in writer order. Construction installs one heartbeat
    /// before the supervisor starts; closing that supervisor aborts it.
    pub(crate) fn retain_auxiliary(&self, task: tokio::task::JoinHandle<()>) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        assert!(state.admission == Admission::Open);
        assert!(state.auxiliary.is_none());
        let abort = task.abort_handle();
        let completion = async move {
            match task.await {
                Ok(()) => Ok(()),
                Err(error) if error.is_cancelled() => Ok(()),
                Err(error) => Err(SharedError::from(Box::new(error) as BoxError)),
            }
        }
        .boxed()
        .shared();
        state.auxiliary = Some(Operation {
            task: abort,
            completion,
        });
    }

    pub(crate) fn enter_sync<T>(self: &Arc<Self>, f: impl FnOnce() -> T) -> T {
        CURRENT.sync_scope(
            PublicationContext {
                scope: self.clone(),
                accepted: false,
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
        let scope = self.clone();
        let operation = operation.boxed();
        async move {
            let slot = scope
                .slots
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| AdmissionClosed)?;
            scope.register(slot, operation)?.await
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
        self.register(slot, operation)
    }

    fn register<T: Send + 'static>(
        self: &Arc<Self>,
        slot: tokio::sync::OwnedSemaphorePermit,
        operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
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
                async move {
                    let _ = done_rx.await;
                }
                .boxed()
                .shared(),
            );
            let scope = self.clone();
            let task = tokio::spawn(async move {
                let _slot = slot;
                if let Some(previous) = previous {
                    previous.await;
                }
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
                let _ = done_tx.send(());
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
        let auxiliary = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .auxiliary
            .as_ref()
            .map(|operation| operation.completion.clone());
        if let Some(completion) = auxiliary {
            let result = completion.await;
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            if let Err(error) = result {
                state.failure.get_or_insert(error);
            }
            state.auxiliary = None;
        }
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
    async move {
        let context = CURRENT.try_with(Clone::clone).ok();
        match context {
            Some(context) if !context.accepted => context.scope.accept(operation).await,
            Some(context) => operation
                .await
                .map_err(|error| context.scope.retain_error(error)),
            _ => operation.await,
        }
    }
    .boxed()
}

pub(crate) fn commit_in<T: Send + 'static>(
    scope: Option<Arc<PublicationScope>>,
    operation: impl Future<Output = Result<T, BoxError>> + Send + 'static,
) -> BoxFuture<'static, Result<T, BoxError>> {
    let operation = operation.boxed();
    async move {
        if CURRENT.try_with(|_| ()).is_ok() {
            commit(operation).await
        } else if let Some(scope) = scope {
            scope.accept(operation).await
        } else {
            operation.await
        }
    }
    .boxed()
}

/// Raw stage/control publications also cross the execution scope. More complex
/// consumers enclose this append and their accounting in one `commit` operation.
pub(crate) fn append<T: JournalEvent + 'static>(
    journal: &Arc<dyn Journal<T>>,
    event: T,
    parent: Option<&EventEnvelope<T>>,
) -> BoxFuture<'static, Result<EventEnvelope<T>, BoxError>> {
    let journal = journal.clone();
    let parent = parent.cloned();
    commit(async move {
        journal
            .append(event, parent.as_ref())
            .await
            .map_err(Into::into)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
    async fn auxiliary_lifetime_is_joined_without_blocking_writer_order() {
        let scope = PublicationScope::new();
        let (release, gate) = oneshot::channel();
        scope.retain_auxiliary(tokio::spawn(async move {
            gate.await.unwrap();
        }));
        scope.accept(async { Ok(()) }).await.unwrap();
        let mut abandoned = Box::pin(scope.join());
        assert!(futures::poll!(abandoned.as_mut()).is_pending());
        drop(abandoned);
        release.send(()).unwrap();
        scope.join().await.unwrap();
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
