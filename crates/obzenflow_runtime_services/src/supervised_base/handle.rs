//! Common handle utilities for supervised FSMs
//!
//! This module provides a handle builder that creates properly typed handles
//! with consistent behavior and proper trait implementations.

use super::builder::{HandleError, SupervisorHandle, EventSender, StateWatcher};
use std::fmt::Debug;
use std::marker::PhantomData;
use tokio::task::JoinHandle;

/// Builder for creating supervisor handles with proper trait implementation
/// 
/// This builder ensures all handles follow the same pattern and properly
/// implement the SupervisorHandle trait.
pub struct HandleBuilder<E, S> {
    event_sender: Option<EventSender<E>>,
    state_watcher: Option<StateWatcher<S>>,
    supervisor_task: Option<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
    _phantom: PhantomData<(E, S)>,
}

impl<E, S> HandleBuilder<E, S>
where
    E: Debug + Send + 'static,
    S: Clone + Debug + Send + Sync + 'static,
{
    /// Create a new handle builder
    pub fn new() -> Self {
        Self {
            event_sender: None,
            state_watcher: None,
            supervisor_task: None,
            _phantom: PhantomData,
        }
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
    pub fn with_supervisor_task(mut self, task: JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>) -> Self {
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
            supervisor_task: Some(supervisor_task),
        })
    }
    
    /// Build a custom handle with error conversion
    pub fn build_custom<H, F>(self, constructor: F) -> Result<H, &'static str>
    where
        F: FnOnce(EventSender<E>, StateWatcher<S>, JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>) -> H,
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
    supervisor_task: Option<JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
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
        self.supervisor_task
            .as_ref()
            .map(|task| !task.is_finished())
            .unwrap_or(false)
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
    
    async fn wait_for_completion(mut self) -> Result<(), Self::Error> {
        if let Some(task) = self.supervisor_task.take() {
            match task.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(HandleError::SupervisorFailed(e.to_string())),
                Err(e) => Err(HandleError::SupervisorPanicked(e.to_string())),
            }
        } else {
            Err(HandleError::SupervisorNotRunning)
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
    pub fn spawn<F, Fut>(self, supervisor_fn: F) -> JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send + 'static,
    {
        let name = self.name;
        tokio::spawn(async move {
            tracing::info!("Starting supervisor: {}", name);
            let result = supervisor_fn().await;
            match &result {
                Ok(()) => tracing::info!("Supervisor {} completed successfully", name),
                Err(e) => tracing::error!("Supervisor {} failed: {}", name, e),
            }
            result
        })
    }
}