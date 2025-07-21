# ObzenFlow Adapters

This crate provides the middleware system and monitoring capabilities for ObzenFlow. It follows a clean, composable design inspired by Rust's iterator adapters, allowing middleware to wrap handlers transparently without the runtime being aware of their presence.

## Architecture Overview

The middleware system in ObzenFlow demonstrates excellent separation of concerns:

1. **User provides a handler** - implements one of the handler traits (e.g., `TransformHandler`)
2. **DSL layer wraps with middleware** - applies middleware chain during stage construction
3. **Runtime receives wrapped handler** - sees only the handler trait interface
4. **Runtime calls handler methods** - completely unaware of middleware presence

This design achieves zero-cost abstraction when middleware is not used, and clean composition when it is.

## How Middleware Works

### 1. Handler Traits (Runtime Services)

The runtime defines simple handler traits that stages must implement:

```rust
// In obzenflow_runtime_services
pub trait TransformHandler: Send + Sync {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent>;
    async fn drain(&mut self) -> Result<()>;
}
```

### 2. Middleware Trait (Adapters)

Middleware provides hooks into the processing lifecycle:

```rust
// In obzenflow_adapters
pub trait Middleware: Send + Sync {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction;
    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext);
    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction;
    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext);
}
```

### 3. Middleware Wrappers

Each handler type has a corresponding middleware wrapper that implements the same trait:

```rust
// In obzenflow_adapters
pub struct MiddlewareTransform<H: TransformHandler> {
    inner: H,
    middleware_chain: Vec<Box<dyn Middleware>>,
}

impl<H: TransformHandler> TransformHandler for MiddlewareTransform<H> {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // 1. Run pre_handle on all middleware
        // 2. Call inner.process()
        // 3. Run post_handle on all middleware
        // 4. Run pre_write on each result
        // 5. Return enriched results
    }
}
```

### 4. Building Middleware (DSL Layer)

The DSL layer (`obzenflow_dsl_infra`) is where middleware gets applied:

```rust
// In stage_descriptor.rs
async fn create_stage_handle(&self, config: StageConfig) -> BoxedStageHandle {
    // Create system middleware (timing, enrichment, etc.)
    let mut all_middleware = create_system_middleware(&config);
    
    // Add user middleware from factories
    let user_middleware: Vec<Box<dyn Middleware>> = self.middleware
        .into_iter()
        .map(|factory| factory.create(&config))
        .collect();
    all_middleware.extend(user_middleware);
    
    // Wrap the handler with middleware chain
    let mut builder = self.handler.middleware(writer_id);
    for mw in all_middleware {
        builder = builder.with(mw);
    }
    let handler_with_middleware = builder.build();
    
    // Pass wrapped handler to runtime
    TransformBuilder::new(handler_with_middleware, config, ...)
}
```

### 5. Runtime Usage

The runtime supervisor simply calls handler methods:

```rust
// In transform/supervisor.rs
let handler = self.context.handler.clone();
let result = handler.process(event_to_process);
```

The runtime doesn't know or care whether `handler` is:
- A raw user handler
- A handler wrapped in middleware
- A handler wrapped in multiple layers of middleware

## Middleware Execution Flow

When an event is processed through a transform with middleware:

```
Event arrives
  → Middleware1.pre_handle()
    → Middleware2.pre_handle()
      → Middleware3.pre_handle()
        → Handler.process()
      ← Middleware3.post_handle()
    ← Middleware2.post_handle()
  ← Middleware1.post_handle()
  → Middleware1.pre_write(result1)
  → Middleware2.pre_write(result1)
  → Middleware3.pre_write(result1)
Results written to journal
```

## System Middleware

ObzenFlow automatically applies system middleware to all stages:

1. **TimingMiddleware** - Measures processing duration
2. **SystemEnrichmentMiddleware** - Adds flow context (flow_name, stage_name, etc.)
3. **FsmInstrumentationMiddleware** - Captures FSM state transitions
4. **OutcomeEnrichmentMiddleware** - Adds success/failure outcomes

## Why This Design Works

### 1. **Type Safety**
The wrapper implements the exact same trait as the handler, ensuring type safety throughout.

### 2. **Zero Cost When Not Used**
If no middleware is specified, the raw handler is passed directly to the runtime.

### 3. **Clean Composition**
Middleware can be stacked like iterator adapters:
```rust
handler
    .middleware()
    .with(LoggingMiddleware::new())
    .with(RetryMiddleware::new())
    .with(RateLimiterMiddleware::new())
    .build()
```

### 4. **Runtime Simplicity**
The runtime's FSM-driven event loop remains simple and focused on orchestration:
- Manage state transitions
- Handle control events
- Call handler methods
- Write results to journal

### 5. **Middleware Orthogonality**
Middleware concerns (logging, metrics, rate limiting) are completely separate from runtime concerns (state management, event flow).

## Example: Following a Transform

1. **User defines handler:**
```rust
struct MyTransform;
impl TransformHandler for MyTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![enrich(event)]
    }
}
```

2. **User creates stage with middleware:**
```rust
let stage = transform!("enricher", MyTransform)
    .with_middleware(rate_limiter(100.0));
```

3. **DSL wraps handler:**
```rust
// Inside stage_descriptor.rs
let wrapped = MyTransform
    .middleware(writer_id)
    .with(TimingMiddleware::new())      // System
    .with(RateLimiterMiddleware::new()) // User
    .build();
```

4. **Runtime receives and uses:**
```rust
// Runtime just sees TransformHandler trait
handler.process(event) // Could be wrapped or not!
```

## Summary

The middleware system demonstrates how thoughtful API design enables powerful composition without complexity. By following Rust's iterator adapter pattern and maintaining clean trait boundaries, ObzenFlow achieves:

- **Transparency**: Runtime unaware of middleware
- **Composability**: Stack middleware like building blocks  
- **Performance**: Zero cost when not used
- **Flexibility**: Easy to add new middleware types
- **Simplicity**: Each component has one clear responsibility

This architecture allows users to add cross-cutting concerns (metrics, logging, rate limiting) without modifying their business logic or complicating the runtime engine.