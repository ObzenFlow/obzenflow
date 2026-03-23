# Middleware System Architecture

## MiddlewareContext Design Philosophy

**The key insight**: `MiddlewareContext` is **ephemeral** - it exists only during the processing of a single event through the middleware chain and is never persisted.

### Why Ephemeral Context?

1. **Event Immutability**: ChainEvent should remain immutable during processing. Middleware shouldn't mutate the event being processed.

2. **Cross-Middleware Communication**: Middleware layers need to communicate with each other during processing (e.g., circuit breaker telling retry middleware it's open).

3. **Separation of Concerns**: 
   - **Ephemeral state** (context) = communication during processing
   - **Durable state** (control events) = what gets persisted to journal

### How It Works

```rust
// 1. Context created fresh for each event
let mut ctx = MiddlewareContext::new();

// 2. Flows through middleware chain
middleware1.pre_handle(&event, &mut ctx)  // Can emit events, set baggage
middleware2.pre_handle(&event, &mut ctx)  // Can see middleware1's events
handler.process(event)                     // Core processing
middleware2.post_handle(&event, &results, &mut ctx)
middleware1.post_handle(&event, &results, &mut ctx)

// 3. Context is discarded after processing
```

### Three Types of Data

1. **`events: Vec<MiddlewareEvent>`** - In-memory communication
   - Circuit breaker emits "opened" → retry middleware sees it and skips
   - Rate limiter emits "throttled" → monitoring middleware counts it
   - Never persisted, only for this processing cycle

2. **`baggage: HashMap<String, Value>`** - Shared state during processing
   - Retry middleware sets "attempt_count" → logging middleware includes it
   - Timing middleware sets "start_time" → can calculate duration later
   - Think of it like thread-local storage for the processing cycle

3. **`control_events: Vec<ChainEvent>`** - Durable events for journal
   - These ARE persisted after processing completes
   - Used for state changes, metrics, anomalies
   - Appended to handler results by MiddlewareTransform

### Example Flow

```rust
// Circuit breaker middleware
fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
    if self.is_open() {
        // Ephemeral - tells other middleware we're rejecting
        ctx.emit_event("circuit_breaker", "rejected", json!({
            "reason": "circuit_open"
        }));
        
        // Durable - goes to journal for metrics
        ctx.write_control_event(ChainEventFactory::circuit_breaker_opened(
            writer_id,
            0.95,  // error_rate
            15,    // failure_count
        ));
        
        return MiddlewareAction::Skip(vec![]);
    }
    MiddlewareAction::Continue
}

// Retry middleware can see circuit breaker's rejection
fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
    if ctx.has_event("circuit_breaker", "rejected") {
        // Don't retry if circuit is open
        return MiddlewareAction::Abort;
    }
    // ... normal retry logic
}
```

### Why This Design Works

1. **No Journal Pollution**: Middleware chatter doesn't create journal entries
2. **Composability**: Middleware can coordinate without tight coupling
3. **Performance**: No serialization/deserialization of ephemeral data
4. **Clear Boundaries**: What's temporary (context) vs what's permanent (control events)

The context is like a "conversation" between middleware during processing, while control events are the "meeting minutes" that get saved.

This design predates the ChainEvent enhancements but remains valid because it serves a different purpose - it's about the processing pipeline, not the data model.

## Control Events and Journal Integration

Control events written via `ctx.write_control_event()` are special ChainEvents that:
- Get appended to handler results by MiddlewareTransform/Sink/Source adapters
- Flow through the journal for durability
- Can contribute to application metrics and durable observability derived from the journal
- Must follow the current ChainEvent structure and requirements

## Error Handling and Dead Letter Pattern

### Transform Behavior with Error Events

As of FLOWIP-082f, transforms automatically skip events marked with `ProcessingStatus::Error`. This creates a dead letter pattern where:

1. **Middleware marks failures**: When detecting unrecoverable errors, middleware can return:
   ```rust
   let mut error_event = event.clone();
   error_event.processing_info.status = ProcessingStatus::error("unrecoverable error");
   return MiddlewareAction::Skip(vec![error_event]);
   ```

2. **Transforms skip error events**: Events with Error status pass through without processing
   - Prevents error propagation and infinite loops
   - Error events flow to sinks for observability
   - Metrics still count error events

3. **Sinks receive all events**: Including error events, allowing for:
   - Dead letter queue implementation
   - Error logging and monitoring
   - Audit trail completeness

Note: cycle protection is implemented in stage supervisors (FLOWIP-051l) because flow control
signals bypass the middleware chain.

See `ChainEventFactory` methods in obzenflow_core for the current control event API. Key factory methods include:
- `circuit_breaker_opened()`, `circuit_breaker_summary()`
- `retry_exhausted()`
- `metrics_state_snapshot()`, `metrics_ready()`, `metrics_drained()`
- `windowing_count_event()`, `windowing_sum_event()`, `windowing_average_event()`

## Function-based Middleware

The middleware system provides utilities for creating middleware from functions/closures instead of implementing the full Middleware trait. This is useful for simple, one-off behaviors.

### FnMiddleware

A struct that implements the Middleware trait by wrapping three functions:
- `pre`: Pre-processing function
- `post`: Post-processing function  
- `error`: Error handling function

### middleware_fn()

A convenience function that creates middleware from just a pre-processing function (with no-op post and default error handling).

Example:
```rust
use obzenflow_adapters::middleware::{middleware_fn, MiddlewareAction};

// Simple filter middleware
let filter = middleware_fn(|event, ctx| {
    if event.event_type() == "important" {
        MiddlewareAction::Continue
    } else {
        MiddlewareAction::Skip(vec![])
    }
});

// Logging middleware
let logger = middleware_fn(|event, ctx| {
    tracing::info!("Processing event: {}", event.id);
    MiddlewareAction::Continue
});
```

This approach eliminates boilerplate for simple middleware behaviors that only need pre-processing logic.
