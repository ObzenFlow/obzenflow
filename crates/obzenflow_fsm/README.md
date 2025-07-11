# Async FSM Design - Solving the Lifetime Issues

## Design Tradeoff: Arc Context Requirement

This FSM library requires contexts to be wrapped in `Arc`. This is a deliberate design choice to support:

1. **Concurrent handlers** - Entry, exit, and transition handlers may run concurrently
2. **Async closures** - Handlers are async blocks that need to own their data
3. **Middleware composition** - Middleware can wrap and modify handler behavior
4. **Thread safety** - The FSM can be used across thread boundaries

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