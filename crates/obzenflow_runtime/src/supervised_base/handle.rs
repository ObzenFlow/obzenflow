// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common handle utilities for supervised FSMs
//!
//! This module provides a handle builder that creates properly typed handles
//! with consistent behavior and proper trait implementations.

use super::builder::{EventSender, HandleError, StateWatcher, SupervisorHandle};
use super::publication::PublicationScope;
use futures::future::{BoxFuture, Shared};
use futures::FutureExt;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::task::{AbortHandle, JoinHandle};

type Task = JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

/// Execution task and its accepted publication resources travel together.
#[doc(hidden)]
pub struct SupervisorTask {
    task: Task,
    publications: Arc<PublicationScope>,
}

impl From<Task> for SupervisorTask {
    fn from(task: Task) -> Self {
        Self {
            task,
            publications: PublicationScope::new(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ExecutionCancellation {
    task: AbortHandle,
    publications: Arc<PublicationScope>,
    requested: Arc<std::sync::atomic::AtomicBool>,
}

impl ExecutionCancellation {
    pub(crate) fn abort(&self) {
        self.requested
            .store(true, std::sync::atomic::Ordering::Release);
        self.publications.close();
        self.task.abort();
    }

    fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

// Physical task completion is retained independently of each observer's wait.
// Aborted remains distinct even when emergency teardown accepts that result.
enum SupervisorExit {
    Returned,
    Failed(Arc<dyn std::error::Error + Send + Sync>),
    Panicked(tokio::task::JoinError),
    Aborted,
}

type SupervisorCompletion = Shared<BoxFuture<'static, Arc<SupervisorExit>>>;

impl SupervisorExit {
    fn result(&self) -> Result<(), HandleError> {
        match self {
            Self::Returned => Ok(()),
            Self::Failed(error) => Err(HandleError::SupervisorFailed(error.clone())),
            Self::Panicked(error) => Err(HandleError::SupervisorPanicked(error.to_string())),
            Self::Aborted => Err(HandleError::SupervisorAborted),
        }
    }
}

/// Builder for creating supervisor handles with proper trait implementation
///
/// This builder ensures all handles follow the same pattern and properly
/// implement the SupervisorHandle trait.
pub struct HandleBuilder<E, S> {
    event_sender: Option<EventSender<E>>,
    state_watcher: Option<StateWatcher<S>>,
    supervisor_task: Option<SupervisorTask>,
    _phantom: PhantomData<(E, S)>,
}

impl<E, S> Default for HandleBuilder<E, S> {
    fn default() -> Self {
        Self {
            event_sender: None,
            state_watcher: None,
            supervisor_task: None,
            _phantom: PhantomData,
        }
    }
}

impl<E, S> HandleBuilder<E, S>
where
    E: Debug + Send + 'static,
    S: Clone + Debug + Send + Sync + 'static,
{
    /// Create a new handle builder
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the event sender
    pub fn with_event_sender(mut self, sender: EventSender<E>) -> Self {
        self.event_sender = Some(sender);
        self
    }

    /// Set the state watcher
    pub fn with_state_watcher(mut self, watcher: StateWatcher<S>) -> Self {
        self.state_watcher = Some(watcher);
        self
    }

    /// Set the supervisor task
    pub fn with_supervisor_task(mut self, task: impl Into<SupervisorTask>) -> Self {
        self.supervisor_task = Some(task.into());
        self
    }

    /// Build a standard handle with HandleError as the error type
    pub fn build_standard(self) -> Result<StandardHandle<E, S>, &'static str> {
        let event_sender = self.event_sender.ok_or("Event sender is required")?;
        let state_watcher = self.state_watcher.ok_or("State watcher is required")?;
        let supervisor_task = self.supervisor_task.ok_or("Supervisor task is required")?;

        let SupervisorTask { task, publications } = supervisor_task;
        let abort_requested = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let supervisor_abort = ExecutionCancellation {
            task: task.abort_handle(),
            publications: publications.clone(),
            requested: abort_requested.clone(),
        };
        let completion = async move {
            let exit = match task.await {
                Ok(Ok(())) => SupervisorExit::Returned,
                Ok(Err(error))
                    if abort_requested.load(std::sync::atomic::Ordering::Acquire)
                        && super::publication::is_admission_closed(error.as_ref()) =>
                {
                    SupervisorExit::Aborted
                }
                Ok(Err(error)) => SupervisorExit::Failed(Arc::from(error)),
                Err(error) if error.is_cancelled() => SupervisorExit::Aborted,
                Err(error) => SupervisorExit::Panicked(error),
            };
            let settlement = publications.join().await;
            Arc::new(match (exit, settlement) {
                (SupervisorExit::Returned | SupervisorExit::Aborted, Err(error)) => {
                    SupervisorExit::Failed(Arc::new(error))
                }
                (exit, _) => exit,
            })
        }
        .boxed()
        .shared();
        Ok(StandardHandle {
            event_sender,
            state_watcher,
            supervisor_abort,
            completion,
        })
    }

    /// Build a custom handle with error conversion
    pub fn build_custom<H, F>(self, constructor: F) -> Result<H, &'static str>
    where
        F: FnOnce(EventSender<E>, StateWatcher<S>, SupervisorTask) -> H,
    {
        let event_sender = self.event_sender.ok_or("Event sender is required")?;
        let state_watcher = self.state_watcher.ok_or("State watcher is required")?;
        let supervisor_task = self.supervisor_task.ok_or("Supervisor task is required")?;

        Ok(constructor(event_sender, state_watcher, supervisor_task))
    }
}

/// Standard handle implementation that uses HandleError
pub struct StandardHandle<E, S> {
    event_sender: EventSender<E>,
    state_watcher: StateWatcher<S>,
    supervisor_abort: ExecutionCancellation,
    // Never await this retained clone directly: every caller observes a clone.
    completion: SupervisorCompletion,
}

impl<E, S> StandardHandle<E, S>
where
    E: Debug + Send + 'static,
    S: Clone + Debug + Send + Sync + 'static,
{
    /// Get a receiver for watching state changes
    pub fn state_receiver(&self) -> tokio::sync::watch::Receiver<S> {
        self.state_watcher.subscribe()
    }

    /// Check if the supervisor is still running
    pub fn is_running(&self) -> bool {
        !self.supervisor_abort.is_finished()
    }

    /// Observe the retained physical join. Dropping or parking one wait does not
    /// prevent other observers from completing it, and never aborts the task.
    pub(crate) async fn join(&self) -> Result<(), HandleError> {
        self.completion.clone().await.result()
    }

    /// Abort the supervisor task (best-effort).
    ///
    /// This does not await completion; callers should follow with
    /// `wait_for_completion` when they need deterministic teardown.
    pub(crate) fn abort(&self) {
        self.supervisor_abort.abort();
    }

    pub(crate) fn abort_handle(&self) -> ExecutionCancellation {
        self.supervisor_abort.clone()
    }

    /// Best-effort bounded wait for supervisor completion.
    ///
    /// Returns:
    /// - `Ok(true)` if the supervisor finished within the timeout.
    /// - `Ok(false)` if the wait timed out (task is still running and retained).
    /// - `Err(_)` if the supervisor finished but failed, panicked or was aborted.
    #[cfg(test)]
    pub(crate) async fn try_wait_for_completion(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<bool, HandleError> {
        match tokio::time::timeout(timeout, self.join()).await {
            Ok(result) => result.map(|()| true),
            Err(_) => Ok(false),
        }
    }
}

#[async_trait::async_trait]
impl<E, S> SupervisorHandle for StandardHandle<E, S>
where
    E: Debug + Send + 'static,
    S: Clone + Debug + Send + Sync + 'static,
{
    type Event = E;
    type State = S;
    type Error = HandleError;

    async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error> {
        self.event_sender.send(event).await
    }

    fn current_state(&self) -> Self::State {
        self.state_watcher.current()
    }

    fn request_abort(&self) {
        self.abort();
    }

    async fn publish_pipeline_control(
        &self,
        journal: Arc<dyn obzenflow_core::journal::Journal<obzenflow_core::event::ChainEvent>>,
        event: obzenflow_core::event::ChainEvent,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match self
            .supervisor_abort
            .publications
            .accept(async move {
                journal.append(event, None).await?;
                Ok(())
            })
            .await
        {
            Err(error) if error.is::<super::publication::AdmissionClosed>() => Ok(()),
            result => result,
        }
    }

    async fn wait_for_completion(&self) -> Result<(), Self::Error> {
        self.join().await
    }

    async fn abort_and_wait(&self) -> Result<(), Self::Error> {
        self.abort();
        let exit = self.completion.clone().await;
        match exit.as_ref() {
            SupervisorExit::Aborted => Ok(()),
            _ => exit.result(),
        }
    }
}

/// Builder for creating supervisor tasks with proper error handling
pub struct SupervisorTaskBuilder<S> {
    name: String,
    publications: Arc<PublicationScope>,
    _phantom: std::marker::PhantomData<S>,
}

impl<S> SupervisorTaskBuilder<S>
where
    S: Send + 'static,
{
    /// Create a new task builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            publications: PublicationScope::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    pub(crate) fn with_publications(mut self, publications: Arc<PublicationScope>) -> Self {
        self.publications = publications;
        self
    }

    /// Spawn the supervisor task
    pub fn spawn<F, Fut>(self, supervisor_fn: F) -> SupervisorTask
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>
            + Send
            + 'static,
    {
        let name = self.name;
        let name_clone = name.clone();
        let name_clone2 = name.clone();
        let name_clone3 = name.clone();
        let name_clone4 = name.clone();
        let is_pipeline = name.contains("pipeline");
        tracing::debug!(
            "🚀 SupervisorTaskBuilder::spawn called for {} (is_pipeline: {})",
            name,
            is_pipeline
        );

        // Call supervisor_fn OUTSIDE the spawn to see if that's the issue
        tracing::trace!("🔵 About to call supervisor_fn() for {}", name_clone);
        // Keep concrete supervisor futures out of nested task-local wrappers.
        // Stage contexts can make those futures large in unoptimised builds.
        let future = supervisor_fn().boxed();
        tracing::trace!("🟢 supervisor_fn() returned future for {}", name_clone);
        tracing::trace!("📦 Future size: {} bytes", std::mem::size_of_val(&future));

        tracing::trace!("⚡ About to call tokio::spawn for {}", name_clone2);

        // Wrap the future to add debugging
        let wrapped_future = async move {
            tracing::trace!("🎯 WRAPPER: Task {} started executing!", name_clone3);
            let result = future.await;
            match &result {
                Ok(_) => {
                    tracing::debug!("✅ Supervisor task {} completed successfully", name_clone3);
                }
                Err(e) => {
                    tracing::error!("❌ Supervisor task {} failed: {}", name_clone3, e);
                }
            }
            tracing::trace!("🎯 WRAPPER: Task {} completed!", name_clone3);
            result
        };

        let publications = self.publications;
        let task_publications = publications.clone();
        let handle = tokio::spawn(async move {
            struct CloseOnExit(Arc<PublicationScope>);
            impl Drop for CloseOnExit {
                fn drop(&mut self) {
                    self.0.close();
                }
            }
            let _close = CloseOnExit(task_publications.clone());
            task_publications.enter(wrapped_future).await
        });
        tracing::trace!("⚡ tokio::spawn returned for {}", name_clone2);
        tracing::debug!(
            "🚀 SupervisorTaskBuilder::spawn returning handle for {}",
            name_clone4
        );
        SupervisorTask {
            task: handle,
            publications,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::supervised_base::ChannelBuilder;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn abort_and_wait_joins_the_cancelled_supervisor_task() {
        let (event_sender, _event_receiver, state_watcher) =
            ChannelBuilder::<(), bool>::new().build(false);
        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let task = {
            let dropped = dropped.clone();
            tokio::spawn(async move {
                let _flag = DropFlag(dropped);
                let _ = started_tx.send(());
                std::future::pending::<()>().await;
                #[allow(unreachable_code)]
                Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
            })
        };
        let handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .expect("standard handle");

        started_rx.await.expect("supervisor task should start");
        handle.abort_and_wait().await.expect("abort and join");

        assert!(dropped.load(Ordering::SeqCst));
        assert!(!handle.is_running());
    }
}

#[cfg(test)]
mod completion_tests {
    use super::*;
    use crate::supervised_base::ChannelBuilder;
    use futures::poll;

    #[derive(Clone, Copy, Debug)]
    enum Exit {
        Success,
        Failure,
        Panic,
        Abort,
    }

    #[tokio::test]
    async fn parked_dropped_and_late_observers_share_every_completion_path() {
        for exit in [Exit::Success, Exit::Failure, Exit::Panic, Exit::Abort] {
            let (sender, _receiver, watcher) = ChannelBuilder::<(), bool>::new().build(false);
            let (release, gate) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                let _ = gate.await;
                match exit {
                    Exit::Failure => Err(std::io::Error::other("supervisor failure").into()),
                    Exit::Panic => panic!("supervisor panic"),
                    _ => Ok::<(), Box<dyn std::error::Error + Send + Sync>>(()),
                }
            });
            let mut handle = HandleBuilder::new()
                .with_event_sender(sender)
                .with_state_watcher(watcher)
                .with_supervisor_task(task)
                .build_standard()
                .unwrap();
            assert!(!handle
                .try_wait_for_completion(std::time::Duration::ZERO)
                .await
                .unwrap());
            let mut parked = Box::pin(handle.join());
            assert!(poll!(parked.as_mut()).is_pending());
            let mut dropped = Box::pin(handle.join());
            assert!(poll!(dropped.as_mut()).is_pending());
            drop(dropped);
            if matches!(exit, Exit::Abort) {
                tokio::time::timeout(std::time::Duration::from_secs(1), handle.abort_and_wait())
                    .await
                    .unwrap()
                    .unwrap();
            } else {
                release.send(()).unwrap();
            }
            let result = tokio::time::timeout(std::time::Duration::from_secs(1), handle.join())
                .await
                .unwrap();
            match exit {
                Exit::Success => assert!(result.is_ok()),
                Exit::Failure => assert!(matches!(result, Err(HandleError::SupervisorFailed(_)))),
                Exit::Panic => assert!(matches!(result, Err(HandleError::SupervisorPanicked(_)))),
                Exit::Abort => assert!(matches!(result, Err(HandleError::SupervisorAborted))),
            }
            assert_eq!(format!("{:?}", parked.await), format!("{result:?}"));
            assert!(!handle.is_running());
            assert_eq!(format!("{:?}", handle.join().await), format!("{result:?}"));
            assert_eq!(
                handle
                    .try_wait_for_completion(std::time::Duration::ZERO)
                    .await
                    .is_ok(),
                result.is_ok()
            );
            if matches!(exit, Exit::Abort) {
                handle.abort_and_wait().await.unwrap();
                assert!(matches!(
                    handle.wait_for_completion().await,
                    Err(HandleError::SupervisorAborted)
                ));
            } else {
                assert_eq!(
                    format!("{:?}", handle.wait_for_completion().await),
                    format!("{result:?}")
                );
            }
        }
    }
}
