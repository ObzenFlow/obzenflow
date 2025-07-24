# Supervised Base Infrastructure

This module provides the foundational patterns for building supervised FSMs that follow our strict architectural requirements.

## Core Components

### 1. SupervisorBuilder Trait
Every supervisor must be created through a builder that implements this trait:
```rust
#[async_trait]
pub trait SupervisorBuilder: Sized {
    type Handle: SupervisorHandle;
    type Error: Error + Send + Sync + 'static;
    
    async fn build(self) -> Result<Self::Handle, Self::Error>;
}
```

### 2. SupervisorHandle Trait
Every handle must implement this trait for event-based control:
```rust
#[async_trait]
pub trait SupervisorHandle: Send + Sync {
    type Event: Debug + Send + 'static;
    type State: Clone + Debug + Send + Sync + 'static;
    type Error: Error + Send + Sync + 'static;
    
    async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error>;
    fn current_state(&self) -> Self::State;
    async fn wait_for_completion(self) -> Result<(), Self::Error>;
}
```

### 3. HandleBuilder
A builder for creating handles with proper trait implementation:

```rust
// For standard handles that use HandleError
let handle = HandleBuilder::new()
    .with_event_sender(event_sender)
    .with_state_watcher(state_watcher)
    .with_supervisor_task(task)
    .build_standard()?;

// For custom handles with special error types
let handle = HandleBuilder::new()
    .with_event_sender(event_sender)
    .with_state_watcher(state_watcher)
    .with_supervisor_task(task)
    .build_custom(|sender, watcher, task| {
        MyCustomHandle::new(sender, watcher, task)
    })?;
```

## Usage Example

### 1. Define Your Types
```rust
#[derive(Clone, Debug)]
pub enum MyEvent { Start, Stop }

#[derive(Clone, Debug)]
pub enum MyState { Idle, Running }
```

### 2. Create Your Builder
```rust
pub struct MyBuilder {
    config: MyConfig,
    resources: StageResources,
}

#[async_trait]
impl SupervisorBuilder for MyBuilder {
    type Handle = StandardHandle<MyEvent, MyState>;
    type Error = BuilderError;
    
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // Create channels
        let (event_sender, event_receiver, state_watcher) = 
            ChannelBuilder::new().build(MyState::Idle);
        
        // Create supervisor (private!)
        let supervisor = MySupervisor { /* ... */ };
        
        // Spawn task
        let task = SupervisorTaskBuilder::new("my_supervisor")
            .spawn(|| async { supervisor.run().await });
        
        // Build handle using HandleBuilder
        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
```

### 3. For Custom Error Types

If you need a custom error type (like FlowError), create a custom handle:

```rust
pub struct MyHandle {
    event_sender: EventSender<MyEvent>,
    state_watcher: StateWatcher<MyState>,
    supervisor_task: Option<JoinHandle<...>>,
}

impl MyHandle {
    fn new(sender: EventSender<MyEvent>, watcher: StateWatcher<MyState>, task: JoinHandle<...>) -> Self {
        Self {
            event_sender: sender,
            state_watcher: watcher,
            supervisor_task: Some(task),
        }
    }
}

#[async_trait]
impl SupervisorHandle for MyHandle {
    type Event = MyEvent;
    type State = MyState;
    type Error = MyCustomError;
    
    async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error> {
        self.event_sender.send(event).await
            .map_err(|e| MyCustomError::from(e))
    }
    
    fn current_state(&self) -> Self::State {
        self.state_watcher.current()
    }
    
    async fn wait_for_completion(mut self) -> Result<(), Self::Error> {
        // Custom implementation with error conversion
    }
}

// In your builder:
async fn build(self) -> Result<Self::Handle, Self::Error> {
    // ... setup channels and task ...
    
    HandleBuilder::new()
        .with_event_sender(event_sender)
        .with_state_watcher(state_watcher)
        .with_supervisor_task(task)
        .build_custom(MyHandle::new)
        .map_err(|e| BuilderError::Other(e.to_string()))
}
```

## Key Principles

1. **No Macros** - Explicit is better than implicit
2. **Builder Enforced** - Can't create handles without proper setup
3. **Type Safety** - All components are properly typed
4. **Error Flexibility** - Support both standard and custom error types
5. **Consistent Pattern** - All supervisors follow the same structure

## What NOT to Do

❌ Don't create handles manually with a `new()` method
❌ Don't expose supervisor structs publicly
❌ Don't implement SupervisorHandle without using HandleBuilder
❌ Don't bypass the builder pattern