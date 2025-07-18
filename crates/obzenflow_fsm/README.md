# ObzenFlow FSM

Finite State Machines are at the heart of how ObzenFlow works. This is our own custom implementation of a **Mealy machine**, that makes intention tradeoffs to support async closures for the right balance between correctness and architectural flexibility. Flexibility is required due to ObzenFlow's extremely expressive middleware and monitoring systems. 

This FSM library requires contexts to be wrapped in `Arc`. This is a deliberate design choice to support:

1. **Async closures** - Handlers are async blocks that need to own their data (primary reason)
2. **Multiple FSM instances** - Different FSMs can share resources like journals or message buses
3. **Thread safety** - The FSM can be safely used across thread boundaries
4. **Future flexibility** - Enables concurrent patterns if needed in the future

While this adds some complexity for simple use cases, it ensures the FSM works correctly in all async scenarios without lifetime issues.

## The Problem

Our current design tries to pass `&mut Context` into async blocks:

```rust
.on("Event", |state, event, ctx: &mut Context| async move {
    // This doesn't work! ctx doesn't live long enough
    ctx.something.await;
})
```

## Solution: Shared Ownership with Interior Mutability

Instead of passing `&mut Context`, we'll use `Arc<Context>` where Context provides interior mutability:

```rust
// Context is now cloneable and contains Arc'd resources
#[derive(Clone)]
struct StageContext {
    event_store: Arc<EventStore>,
    pipeline_tx: mpsc::Sender<Message>,  // Already cloneable
    metrics: Arc<RwLock<Metrics>>,
    // etc.
}

// Handler signature changes to:
.on("Event", |state, event, ctx: Arc<Context>| async move {
    // Now we can move ctx into the async block!
    ctx.event_store.write(event).await?;
    ctx.pipeline_tx.send(msg).await?;
})
```

## Updated FSM Builder API

```rust
// Handler type changes to accept Arc<C> instead of &mut C
type TransitionHandler<S, E, C, A> = Arc<
    dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Transition<S, A>, String>> + Send>>
        + Send
        + Sync,
>;

// Usage looks like:
let fsm = FsmBuilder::new(StageState::Created)
    .when("Running")
        .on("ProcessEvent", |state, event, ctx| async move {
            // ctx is Arc<StageContext> - can be moved into async block
            let data = ctx.event_store.read(event.id).await?;
            
            if let Some(metric) = process_data(data) {
                ctx.metrics.write().await.record(metric);
            }
            
            Ok(Transition {
                next_state: state.clone(),
                actions: vec![StageAction::Continue],
            })
        })
        .done()
    .build();
```

## Benefits

1. **No lifetime issues** - Arc can be cloned and moved into async blocks
2. **Natural async/await** - Handlers can use async operations freely  
3. **Shared state** - Multiple handlers can access the same context
4. **Thread safe** - Arc ensures safe sharing across tasks

## Migration Path

1. Change handler signatures to accept `Arc<C>`
2. Make Context types cloneable with Arc'd internals
3. Update tests to use the new pattern
4. Document the pattern clearly

This aligns with Tokio's recommended patterns for shared state in async systems.

## Design Decisions: Why Not Typestate?

You might be wondering - why use runtime state machines when Rust has such powerful type system features? We actually considered the typestate pattern, which would give us compile-time guarantees about valid state transitions. Here's why we stuck with the enum-based approach:

### The Storage Dilemma

In a real async system, you need to store your state machine somewhere - typically as a field in a supervisor or actor. With typestate, the machine's type changes with every transition. This means you'd either need:
- Trait objects (goodbye type safety!)
- An enum wrapper (wait... that's just our current design with extra steps)
- A complete architectural overhaul

### Dynamic vs Static Dispatch

Our FSM thrives on runtime flexibility:
```rust
// This is what we want - configure behavior at runtime
.when("Running")
  .on("UserEvent", handle_user_event)
  .on("SystemEvent", handle_system_event)
  .on_any(log_all_events)
```

Typestate needs everything resolved at compile time. Different philosophies for different needs.

### The Features We'd Lose

Some of our most useful features just don't translate well to typestate:
- **Wildcard transitions** - "From any state, on ErrorEvent, go to Failed"
- **Unhandled event callbacks** - "Log any event we don't have a handler for"
- **Dynamic timeout handlers** - "If we're in Pending for 30s, transition to Timeout"
- **Entry/exit handlers** - Running cleanup code when leaving states

### The Real Insight

We're building for async, event-driven systems where events come from external sources (network, timers, user input). The typestate pattern shines when state transitions are driven by method calls in your own code. Different tools for different jobs!

### Getting Safety Without Typestate

Want similar guarantees? Here's what we recommend:
1. **State-specific event loops** - Only poll certain futures in certain states
2. **Invariant assertions** - `debug_assert!(matches!(self.state, State::Running))` 
3. **Builder patterns for complex states** - Make invalid states harder to construct
4. **Comprehensive tests** - Our test utilities make it easy to verify all transitions

The bottom line: our enum-based FSM is purpose-built for the realities of async Rust services. It's not a compromise - it's the right tool for this particular job.

## Theoretical Foundation: Mealy Machine with Modern Extensions

You might be curious how obzenflow_fsm relates to traditional state machine theory. At its core, this implementation is closest to a **Mealy machine** with some practical adaptations for async Rust.

### Core Mealy Machine Characteristics

1. **Outputs depend on both state AND input**: Our transition handlers take both the current state and event as parameters to produce actions:
   ```rust
   pub type TransitionHandler<S, E, C, A> = Arc<
       dyn Fn(&S, &E, Arc<C>) -> Pin<Box<dyn Future<Output = Result<Transition<S, A>, String>> + Send>>
   ```

2. **Actions occur during transitions**: Actions are produced as part of the transition result, not just from being in a state:
   ```rust
   pub struct Transition<S, A> {
       pub next_state: S,
       pub actions: Vec<A>,  // Actions are part of the transition
   }
   ```

3. **The fundamental equation matches Mealy**: As stated in our design docs:
   > **State(S) × Event(E) → Actions(A), State(S')**
   
   This is exactly the Mealy machine model where outputs (actions) are determined by the combination of current state and input event.

### Why Not a Moore Machine?

In a Moore machine, outputs depend only on the current state, not the input. But in obzenflow_fsm, the transition handlers explicitly receive both state AND event to determine actions. This gives us more flexibility for real-world use cases.

### Modern Extensions Beyond Classical Theory

1. **Entry/Exit actions**: We support state entry and exit handlers, which is more like a UML state machine feature:
   ```rust
   .on_entry("Running", |state, ctx| async {
       // Initialize resources when entering Running state
   })
   .on_exit("Running", |state, ctx| async {
       // Cleanup when leaving Running state
   })
   ```

2. **Async operations**: All handlers are async, which is a practical adaptation for modern systems but not part of classical FSM theory.

3. **Context parameter**: The addition of a context parameter provides shared state across the FSM, enabling complex stateful operations while keeping the FSM itself pure.

4. **Timeout handlers**: State timeouts are supported, allowing automatic transitions based on time:
   ```rust
   .timeout("Pending", Duration::from_secs(30), |state, ctx| async {
       // Transition to Timeout state after 30 seconds
   })
   ```

### The Akka Heritage

Our design was inspired by Akka's classic FSM, which also follows the Mealy machine model. This heritage shows through in:
- The builder API design
- Support for unhandled event handlers
- The focus on actor-like isolation and message passing
- The emphasis on making state transitions explicit and auditable

### Practical Implications

Understanding that this is a Mealy machine helps explain certain design decisions:
- Why actions are returned from transitions (not just state entry)
- Why we pass both state and event to handlers
- Why the same event can produce different actions in different states
- Why we model side effects as explicit actions rather than implicit operations

This theoretical foundation, combined with our practical extensions for async Rust, creates a powerful abstraction for modeling complex stateful systems in a type-safe, testable way.