# Async FSM Design - Solving the Lifetime Issues

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