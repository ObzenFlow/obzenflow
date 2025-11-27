# ObzenFlow Runtime Services

This crate provides the runtime infrastructure for ObzenFlow, including supervised FSM patterns, event flow, and pipeline orchestration.

## Architecture Overview

### Supervised FSM Pattern

All supervisors in the system follow a strict FSM-driven architecture with these components:

```
┌─────────────────┐
│     Builder     │ ──── Creates ──────┐
└─────────────────┘                    ↓
                                 ┌─────────────────┐
                                 │   Supervisor    │ (Private)
                                 │   + Context     │
                                 │   + FSM         │
                                 └─────────────────┘
                                       ↓
┌─────────────────┐              Spawns Task
│     Handle      │ ←──── Returns ─────┘
│  (Public API)   │
└─────────────────┘
```

### Component Structure

Every supervised FSM must have these files:

```
module/
├── fsm.rs        # States, Events, Actions, Context
├── supervisor.rs # Supervisor implementation (private)
├── builder.rs    # Builder pattern implementation
├── handle.rs     # Public handle for event-based control
├── config.rs     # Configuration types (optional)
└── mod.rs        # Module exports (only builder, handle, types)
```

## Example Implementation

### 1. Define FSM Types (`fsm.rs`)

```rust
use obzenflow_fsm::{StateVariant, EventVariant, FsmAction, FsmContext};

#[derive(Clone, Debug, PartialEq)]
pub enum MyState {
    Created,
    Running,
    Stopped,
}

#[derive(Clone, Debug)]
pub enum MyEvent {
    Start,
    Stop,
}

#[derive(Clone, Debug)]
pub enum MyAction {
    Initialize,
    Cleanup,
}

#[derive(Clone)]
pub struct MyContext {
    // All mutable state goes here
    pub some_state: Arc<RwLock<String>>,
    // Immutable references
    pub journal: Arc<ReactiveJournal>,
    pub writer_id: WriterId,
}

impl StateVariant for MyState { /* ... */ }
impl EventVariant for MyEvent { /* ... */ }
impl FsmContext for MyContext {}

#[async_trait::async_trait]
impl FsmAction for MyAction {
    type Context = MyContext;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            MyAction::Initialize => {
                // Do real work here
                Ok(())
            }
            MyAction::Cleanup => {
                // Do real work here
                Ok(())
            }
        }
    }
}
```

### 2. Implement Supervisor (`supervisor.rs`)

```rust
use crate::supervised_base::{Supervisor, SelfSupervised, EventLoopDirective};

// Note: pub(crate), not pub!
pub(crate) struct MySupervisor {
    pub(crate) name: String,
    pub(crate) context: Arc<MyContext>,
    pub(crate) journal: Arc<ReactiveJournal>,
    pub(crate) writer_id: WriterId,
}

impl Supervisor for MySupervisor {
    type State = MyState;
    type Event = MyEvent;
    type Context = MyContext;
    type Action = MyAction;
    
    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   MyState;
            event:   MyEvent;
            context: MyContext;
            action:  MyAction;
            initial: initial_state;

            state MyState::Created {
                on MyEvent::Start => |_state: &MyState, _event: &MyEvent, _ctx: &mut MyContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MyState::Running,
                            actions: vec![MyAction::Initialize],
                        })
                    })
                };
            }

            // ... more states and transitions
        }
    }
    
    // Required trait methods
    fn name(&self) -> &str { &self.name }
}

#[async_trait::async_trait]
impl SelfSupervised for MySupervisor {
    async fn dispatch_state(&mut self, state: &Self::State) 
        -> Result<EventLoopDirective<Self::Event>, Error> {
        match state {
            MyState::Created => Ok(EventLoopDirective::Continue),
            MyState::Running => Ok(EventLoopDirective::Continue),
            MyState::Stopped => Ok(EventLoopDirective::Terminate),
        }
    }
}
```

### 3. Create Handle (`handle.rs`)

```rust
use crate::supervised_base::{BaseHandle, SupervisorHandle, HandleError};

pub struct MyHandle {
    base: BaseHandle<MyEvent, MyState>,
}

// Use the macro for standard implementation
impl_supervisor_handle!(MyHandle, MyEvent, MyState, HandleError);

// Add domain-specific convenience methods
impl MyHandle {
    pub async fn start(&self) -> Result<(), HandleError> {
        self.base.send_event(MyEvent::Start).await
    }
    
    pub async fn stop(&self) -> Result<(), HandleError> {
        self.base.send_event(MyEvent::Stop).await
    }
}
```

### 4. Implement Builder (`builder.rs`)

```rust
use crate::supervised_base::*;

pub struct MyBuilder {
    config: MyConfig,
    journal: Arc<ReactiveJournal>,
}

impl MyBuilder {
    pub fn new(journal: Arc<ReactiveJournal>, config: MyConfig) -> Self {
        Self { config, journal }
    }
}

#[async_trait::async_trait]
impl SupervisorBuilder for MyBuilder {
    type Handle = MyHandle;
    type Error = BuilderError;
    
    async fn build(self) -> Result<Self::Handle, Self::Error> {
        // 1. Create channels
        let (event_sender, event_receiver, state_watcher) = 
            ChannelBuilder::new().build(MyState::Created);
        
        // 2. Create context
        let context = Arc::new(MyContext {
            some_state: Arc::new(RwLock::new("initial".to_string())),
            journal: self.journal.clone(),
            writer_id: // ... register writer
        });
        
        // 3. Create supervisor (private!)
        let supervisor = MySupervisor {
            name: "my_supervisor".to_string(),
            context: context.clone(),
            journal: self.journal,
            writer_id: context.writer_id.clone(),
        };
        
        // 4. Spawn task
        let task = SupervisorTaskBuilder::new("my_supervisor")
            .spawn(move || async move {
                SelfSupervisedExt::run(supervisor, MyState::Created, context).await
            });
        
        // 5. Return handle
        Ok(MyHandle {
            base: BaseHandle::new(event_sender, state_watcher, task),
        })
    }
}
```

### 5. Module Structure (`mod.rs`)

```rust
pub mod fsm;
pub mod supervisor;
pub mod handle;
pub mod builder;

// Public API - only these are exported!
pub use builder::MyBuilder;
pub use handle::MyHandle;
pub use fsm::{MyState, MyEvent, MyConfig};

// Note: MySupervisor is NOT exported!
```

## Key Principles

### ✅ DO

1. **All mutable state in Context** - Supervisors only hold immutable references
2. **Actions do real work** - Never have no-op action implementations
3. **Events are the only interface** - No public methods beyond traits
4. **Builder spawns the task** - Returns only a running handle
5. **Use provided infrastructure** - BaseHandle, ChannelBuilder, etc.

### ❌ DON'T

1. **No public `new()` on supervisors** - Only builders create supervisors
2. **No direct supervisor access** - Only through handles and events
3. **No custom event loops** - Use SelfSupervisedExt or HandlerSupervisedExt
4. **No boolean flags** - Use FSM states instead
5. **No string event types** - Use enums for type safety

## Common Infrastructure

The `supervised_base` module provides:

- `SupervisorBuilder` - Base trait for builders
- `SupervisorHandle` - Base trait for handles
- `BaseHandle` - Reusable handle implementation
- `ChannelBuilder` - Type-safe channel creation
- `EventSender/Receiver` - Type-safe event channels
- `StateWatcher` - State observation pattern
- `SupervisorTaskBuilder` - Consistent task spawning

## Anti-Patterns

See [FSM-ANTI-PATTERNS.md](../../docs/FSM-ANTI-PATTERNS.md) for detailed examples of what NOT to do.

## Current Implementations

- **Pipeline** (`pipeline/`) - Full implementation with builder and handle
- **Metrics** (`metrics/`) - Supervisor only, builder/handle pending
- **Stages** (`stages/`) - Various stage supervisors, need refactoring

## Migration Guide

To migrate an existing supervisor:

1. Move all mutable state to Context
2. Remove any public methods except trait requirements
3. Create a builder following the pattern above
4. Create a handle for event-based control
5. Update module exports to hide the supervisor

The goal is complete uniformity - every supervisor looks the same structurally, making the codebase predictable and maintainable.
