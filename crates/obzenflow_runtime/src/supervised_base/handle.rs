// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common handle utilities for supervised FSMs
//!
//! This module provides a handle builder that creates properly typed handles
//! with consistent behavior and proper trait implementations.

use super::builder::{EventSender, HandleError, StateWatcher, SupervisorHandle};
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::task::{AbortHandle, JoinHandle};

type SupervisorTask = JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>;

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
    pub fn with_supervisor_task(mut self, task: SupervisorTask) -> Self {
        self.supervisor_task = Some(task);
        self
    }

    /// Build a standard handle with HandleError as the error type
    pub fn build_standard(self) -> Result<StandardHandle<E, S>, &'static str> {
        let event_sender = self.event_sender.ok_or("Event sender is required")?;
        let state_watcher = self.state_watcher.ok_or("State watcher is required")?;
        let supervisor_task = self.supervisor_task.ok_or("Supervisor task is required")?;

        Ok(StandardHandle {
            event_sender,
            state_watcher,
            supervisor_abort: supervisor_task.abort_handle(),
            supervisor_task: tokio::sync::Mutex::new(Some(supervisor_task)),
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
    supervisor_abort: AbortHandle,
    supervisor_task: tokio::sync::Mutex<Option<SupervisorTask>>,
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

    /// Abort the supervisor task (best-effort).
    ///
    /// This does not await completion; callers should follow with
    /// `wait_for_completion` when they need deterministic teardown.
    pub(crate) fn abort(&self) {
        self.supervisor_abort.abort();
    }

    /// Best-effort bounded wait for supervisor completion.
    ///
    /// Returns:
    /// - `Ok(true)` if the supervisor finished within the timeout.
    /// - `Ok(false)` if the wait timed out (task is still running and retained).
    /// - `Err(_)` if the supervisor finished but failed/panicked, or was not running.
    pub(crate) async fn try_wait_for_completion(
        &mut self,
        timeout: std::time::Duration,
    ) -> Result<bool, HandleError> {
        let Some(mut task) = self.supervisor_task.get_mut().take() else {
            return Err(HandleError::SupervisorNotRunning);
        };

        match tokio::time::timeout(timeout, &mut task).await {
            Ok(join_result) => match join_result {
                Ok(Ok(())) => Ok(true),
                Ok(Err(e)) => Err(HandleError::SupervisorFailed(e.to_string())),
                Err(e) => Err(HandleError::SupervisorPanicked(e.to_string())),
            },
            Err(_) => {
                // Timed out. Put the JoinHandle back so callers can abort/await later.
                *self.supervisor_task.get_mut() = Some(task);
                Ok(false)
            }
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

    async fn wait_for_completion(self) -> Result<(), Self::Error> {
        if let Some(task) = self.supervisor_task.into_inner() {
            match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(HandleError::SupervisorFailed(e.to_string())),
                Err(e) => Err(HandleError::SupervisorPanicked(e.to_string())),
            }
        } else {
            Err(HandleError::SupervisorNotRunning)
        }
    }

    async fn abort_and_wait(&self) -> Result<(), Self::Error> {
        self.abort();
        let Some(task) = self.supervisor_task.lock().await.take() else {
            return Err(HandleError::SupervisorNotRunning);
        };
        match task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(HandleError::SupervisorFailed(error.to_string())),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(HandleError::SupervisorPanicked(error.to_string())),
        }
    }
}

/// Builder for creating supervisor tasks with proper error handling
pub struct SupervisorTaskBuilder<S> {
    name: String,
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
            _phantom: std::marker::PhantomData,
        }
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
        let future = supervisor_fn();
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

        let handle = tokio::spawn(wrapped_future);
        tracing::trace!("⚡ tokio::spawn returned for {}", name_clone2);
        tracing::debug!(
            "🚀 SupervisorTaskBuilder::spawn returning handle for {}",
            name_clone4
        );
        handle
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
