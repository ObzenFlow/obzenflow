// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common builder infrastructure for supervised FSMs
//!
//! This module provides the base trait and utilities for implementing
//! the builder pattern consistently across all supervisors.

use std::error::Error;
use std::fmt::Debug;

/// Base trait that all supervisor builders must implement
///
/// The builder pattern ensures:
/// - Supervisors are created and started atomically
/// - Only handles are exposed to users
/// - Consistent error handling
/// - Proper lifecycle management
///
/// # Implementation Guidelines
///
/// 1. **Supervisor Creation**: The supervisor struct should be private to the module
/// 2. **Channel Setup**: Use `ChannelBuilder` to create event/state channels
/// 3. **Task Spawning**: Use `SupervisorTaskBuilder` to spawn the supervisor
/// 4. **Handle Creation**: Use `HandleBuilder` to create the handle
/// 5. **Error Handling**: Convert all errors to your `Self::Error` type
///
/// # Example
///
/// ```ignore
/// #[async_trait]
/// impl SupervisorBuilder for MyBuilder {
///     type Handle = StandardHandle<MyEvent, MyState>;
///     type Error = BuilderError;
///     
///     async fn build(self) -> Result<Self::Handle, Self::Error> {
///         // 1. Register writer ID
///         let writer_id = self.journal.register_writer(...).await
///             .map_err(|e| BuilderError::WriterRegistrationError(e.to_string()))?;
///         
///         // 2. Create context
///         let context = Arc::new(MyContext { ... });
///         
///         // 3. Create channels
///         let (event_sender, event_receiver, state_watcher) =
///             ChannelBuilder::new().build(MyState::Initial);
///         
///         // 4. Create supervisor (private struct!)
///         let supervisor = MySupervisor { ... };
///         
///         // 5. Spawn task
///         let task = SupervisorTaskBuilder::new("my_supervisor")
///             .spawn(|| async { supervisor.run().await });
///         
///         // 6. Build and return handle
///         HandleBuilder::new()
///             .with_event_sender(event_sender)
///             .with_state_watcher(state_watcher)
///             .with_supervisor_task(task)
///             .build_standard()
///             .map_err(|e| BuilderError::Other(e.to_string()))
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait SupervisorBuilder: Sized {
    /// The handle type returned by this builder
    type Handle: SupervisorHandle;

    /// Error type for build failures
    type Error: Error + Send + Sync + 'static;

    /// Build and start the supervisor, returning only a handle
    ///
    /// This method should:
    /// 1. Create any required resources (writer IDs, channels, etc.)
    /// 2. Create the context with all mutable state
    /// 3. Create the supervisor (privately)
    /// 4. Spawn the supervisor task
    /// 5. Return only the handle for external control
    async fn build(self) -> Result<Self::Handle, Self::Error>;
}

/// Common error types for builder operations
#[derive(Debug, thiserror::Error)]
pub enum BuilderError {
    #[error("Failed to register writer: {0}")]
    WriterRegistrationError(String),

    #[error("Failed to create context: {0}")]
    ContextCreationError(String),

    #[error("Failed to spawn supervisor: {0}")]
    SpawnError(String),

    #[error("Channel creation failed")]
    ChannelError,

    #[error("Other error: {0}")]
    Other(String),
}

/// Trait for handle types returned by builders
///
/// This trait defines the interface for all supervisor handles, ensuring
/// consistent event-based control across the system.
///
/// # Implementation Guidelines
///
/// 1. **Use HandleBuilder**: Don't implement this manually, use `HandleBuilder`
/// 2. **Error Conversion**: If you need custom errors, implement conversion in your handle
/// 3. **Additional Methods**: Add domain-specific convenience methods as regular impl methods
/// 4. **State Observation**: Provide additional state watching methods if needed
///
/// # Standard Implementation
///
/// For most use cases, use `StandardHandle<E, S>` which implements this trait
/// with `HandleError` as the error type:
///
/// ```ignore
/// type Handle = StandardHandle<MyEvent, MyState>;
/// ```
///
/// # Custom Implementation
///
/// For custom error types, create your own handle type:
///
/// ```ignore
/// pub struct MyHandle {
///     event_sender: EventSender<MyEvent>,
///     state_watcher: StateWatcher<MyState>,
///     supervisor_task: Option<JoinHandle<...>>,
/// }
///
/// #[async_trait]
/// impl SupervisorHandle for MyHandle {
///     type Event = MyEvent;
///     type State = MyState;
///     type Error = MyCustomError;
///     
///     async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error> {
///         self.event_sender.send(event).await
///             .map_err(|e| MyCustomError::from(e))
///     }
///     
///     fn current_state(&self) -> Self::State {
///         self.state_watcher.current()
///     }
///     
///     async fn wait_for_completion(self) -> Result<(), Self::Error> {
///         // Custom error conversion logic
///     }
/// }
/// ```
#[async_trait::async_trait]
pub trait SupervisorHandle: Send + Sync {
    /// The event type this handle sends
    type Event: Debug + Send + 'static;

    /// The state type this handle observes
    type State: Clone + Debug + Send + Sync + 'static;

    /// Error type for handle operations
    type Error: Error + Send + Sync + 'static;

    /// Send an event to the supervisor
    async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error>;

    /// Get the current state of the supervisor
    fn current_state(&self) -> Self::State;

    /// Wait for the supervisor to complete
    ///
    /// This consumes the handle and waits for the supervisor task to finish
    async fn wait_for_completion(self) -> Result<(), Self::Error>;
}

/// Common error types for handle operations
#[derive(Debug, thiserror::Error)]
pub enum HandleError {
    #[error("Supervisor is not running")]
    SupervisorNotRunning,

    #[error("Failed to send event: {0}")]
    SendError(String),

    #[error("Supervisor task panicked: {0}")]
    SupervisorPanicked(String),

    #[error("Supervisor task failed: {0}")]
    SupervisorFailed(String),
}

/// Utility struct for creating channels with proper types
pub struct ChannelBuilder<E, S> {
    event_buffer: usize,
    _phantom: std::marker::PhantomData<(E, S)>,
}

impl<E, S> Default for ChannelBuilder<E, S> {
    fn default() -> Self {
        Self {
            event_buffer: 100,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<E, S> ChannelBuilder<E, S>
where
    E: Debug + Send + 'static,
    S: Clone + Debug + Send + Sync + 'static,
{
    /// Create a new channel builder with default buffer size
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the event channel buffer size
    pub fn with_event_buffer(mut self, size: usize) -> Self {
        self.event_buffer = size;
        self
    }

    /// Build the channels for supervisor communication
    pub fn build(self, initial_state: S) -> (EventSender<E>, EventReceiver<E>, StateWatcher<S>) {
        let (event_tx, event_rx) = tokio::sync::mpsc::channel(self.event_buffer);
        let (state_tx, state_rx) = tokio::sync::watch::channel(initial_state);

        (
            EventSender { tx: event_tx },
            EventReceiver { rx: event_rx },
            StateWatcher {
                tx: state_tx,
                rx: state_rx,
            },
        )
    }
}

/// Type-safe event sender
pub struct EventSender<E> {
    tx: tokio::sync::mpsc::Sender<E>,
}

impl<E: Debug + Send + 'static> EventSender<E> {
    pub async fn send(&self, event: E) -> Result<(), HandleError> {
        self.tx
            .send(event)
            .await
            .map_err(|_| HandleError::SupervisorNotRunning)
    }

    pub fn try_send(&self, event: E) -> Result<(), HandleError> {
        self.tx
            .try_send(event)
            .map_err(|_| HandleError::SupervisorNotRunning)
    }
}

impl<E> Clone for EventSender<E> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
        }
    }
}

/// Type-safe event receiver
pub struct EventReceiver<E> {
    rx: tokio::sync::mpsc::Receiver<E>,
}

impl<E> EventReceiver<E> {
    pub async fn recv(&mut self) -> Option<E> {
        self.rx.recv().await
    }

    pub fn try_recv(&mut self) -> Result<E, tokio::sync::mpsc::error::TryRecvError> {
        self.rx.try_recv()
    }
}

/// Type-safe state watcher
pub struct StateWatcher<S> {
    tx: tokio::sync::watch::Sender<S>,
    rx: tokio::sync::watch::Receiver<S>,
}

impl<S: Clone> StateWatcher<S> {
    /// Update the state (for supervisor use)
    pub fn update(&self, state: S) -> Result<(), HandleError> {
        self.tx
            .send(state)
            .map_err(|_| HandleError::SupervisorNotRunning)
    }

    /// Get a receiver for watching state (for handle use)
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<S> {
        self.rx.clone()
    }

    /// Get the current state
    pub fn current(&self) -> S {
        self.rx.borrow().clone()
    }
}

impl<S> Clone for StateWatcher<S> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }
}
