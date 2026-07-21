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
let mut ctx = MiddlewareContext::live_handler();

// 2. Flows through middleware chain
middleware1.pre_handle(&event, &mut ctx)  // Can emit ephemeral events, insert typed slots
middleware2.pre_handle(&event, &mut ctx)  // Can see middleware1's ephemeral events/slots
handler.process(event)                     // Core processing
middleware2.post_handle(&event, &results, &mut ctx)
middleware1.post_handle(&event, &results, &mut ctx)

// 3. Context is discarded after processing
```

### Three Types of Data

1. **Ephemeral events (`Vec<ChainEvent>`)** - Typed in-memory communication
   - Stored as `ChainEventContent::Observability(ObservabilityPayload::Middleware(..))`
   - Never persisted, only for this middleware pass
   - Built-in middleware use `MiddlewareLifecycle::{CircuitBreaker,RateLimiter,...}`
   - Third-party middleware can use `TypedMiddlewareEvent` via `MiddlewareLifecycle::User`

2. **Typed per-pass slots** - Shared state during processing
   - Keyed by `MiddlewareContextKey` (type-level identity, not strings)
   - Used for attempt counters, retry-after hints, per-pass timing, etc.

3. **`control_events: Vec<ChainEvent>`** - Durable events for journal
   - These ARE persisted after processing completes
   - Used for state changes, metrics, anomalies
   - Appended to handler results by MiddlewareTransform

### Example Flow

```rust
// Circuit breaker middleware
fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
    if self.is_open() {
        // Ephemeral - tells other middleware we're rejecting (in-memory only)
        ctx.emit_ephemeral_event(ChainEventFactory::observability_event(
            self.writer_id,
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Rejected {
                    reason: CircuitBreakerRejectionReason::CircuitOpen,
                    cooldown_remaining_ms: Some(100),
                    circuit_open_duration_ms: None,
                },
            )),
        ));
        
        // Durable - goes to journal for metrics
        ctx.write_control_event(ChainEventFactory::circuit_breaker_opened(
            writer_id,
            0.95,  // error_rate
            15,    // failure_count
        ));
        
        return MiddlewareAction::Skip { results: vec![], cause: None };
    }
    MiddlewareAction::Continue
}

// Retry middleware can see circuit breaker's rejection
fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
    let rejected = ctx.ephemeral_events().iter().any(|event| matches!(
        &event.content,
        ChainEventContent::Observability(
            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                CircuitBreakerEvent::Rejected { .. }
            ))
        )
    ));
    if rejected { return MiddlewareAction::Abort; }
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
   return MiddlewareAction::Skip { results: vec![error_event], cause: None };
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

See `ChainEventFactory` methods in `obzenflow_core` for the current control event API.

## Legacy Function-based Middleware

The middleware system still provides utilities for creating legacy handler-shell
middleware from functions/closures instead of implementing the full `Middleware`
trait. This is a compatibility path for simple handler-shell behaviors while
hook-bound observer and control surfaces are split out.

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

// Logging middleware
let logger = middleware_fn(|event, ctx| {
    tracing::info!("Processing event: {}", event.id);
    MiddlewareAction::Continue
});
```

This approach eliminates boilerplate for compatibility middleware that only
needs pre-processing logic. New observe-only behavior should use observer hooks
instead of the legacy `Middleware` shell.

Function middleware is handler-shell compatibility code (FLOWIP-120c H2). It may
not be used for live I/O policy. Filtering belongs in the handler, where
multi-type output contracts make it natural; resilience policy belongs on the
live I/O unit (a source, a declared effect, or sink delivery).

## Hook-bound Observer Middleware

Observer middleware is separate from legacy `Middleware` and from hook-bound
control policy. Public authoring goes through
`obzenflow_adapters::middleware::observer`, which re-exports the observer traits
and contexts from the bounded runtime port module. Declare
`MiddlewareDeclaration::observer(...)` from the factory and materialize one
observer attachment for each supported surface. Observer hooks can inspect
inputs, outputs, delivery outcomes, effect outcomes, lifecycle phases, and
output commits, and may return diagnostics, but they cannot skip, reject,
retry, recover, synthesize, pause, or abort.

Supported observer surfaces are:

- `SourcePoll`
- `Handler`
- `Stateful`
- `Join`
- `Effect`
- `SinkDelivery`
- `OutputCommit`
- `StageLifecycle`

`OutputCommit` is observer-only. It may fail only as an output commit invariant
failure, not as control flow.

Observer diagnostics are wide events. Observer output must be either
value-preserving enrichment on the journal-bound event or explicit diagnostic
facts appended to the stage data journal. User diagnostics are not mirrored to
the system journal and should not be routed through a side-channel telemetry
bus. Logging observers may mirror a journalled diagnostic to `tracing`, but the
journal fact remains the source of truth.

Observer implementations choose replay behavior with
`ObserverDeterminism::Deterministic` or `ObserverDeterminism::LiveOnly`.
Runtime dispatch applies that gate, so observer authors do not need to inspect
`MiddlewareExecutionScope`.

## Hook-bound Control Middleware

Control middleware that can pause, reject, synthesize, or otherwise affect live
I/O must use the hook-bound carrier instead of the legacy handler shell. The
factory declares its surfaces before runtime erasure, and the DSL materializes
one attachment for one protected unit:

- `SourcePoll`: source polling admission and observation.
- `Effect`: one declared effect protected at the effect boundary.
- `SinkDelivery`: one sink-stage delivery attempt before receipt journalling.

Do not attach policy middleware at flow scope. A policy protects a concrete live
I/O unit, so attach it to the source, to a specific effect or single-effect
stage, or to the sink stage.

Minimal factory shape:

```rust
use obzenflow_adapters::middleware::{
    MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};

struct MyControlFamily;

struct MyControlFactory;

impl MiddlewareFactory for MyControlFactory {
    fn label(&self) -> &'static str {
        "my_control"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<MyControlFamily>("my_control")
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(
            self.label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::SinkDelivery,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        _ctx: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                // Return SourcePollAttachment { policy, completion_gate: None }.
                todo!()
            }
            MiddlewareSurface::Effect(_) => {
                // Return EffectPolicyAttachment::neutral(...) or ::event_aware(...).
                todo!()
            }
            MiddlewareSurface::SinkDelivery(_) => {
                // Return Arc<dyn SinkPolicy>.
                todo!()
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                "stage",
                std::io::Error::other(format!("unsupported surface {:?}", other.kind())),
            )),
        }
    }
}
```

Control policies live under `middleware::control::policy`. They are the
authoring contract for middleware that controls a live I/O boundary;
there is no public generic handler-shell factory fallback. Two AI map-reduce
adapters remain on an internal, sealed FLOWIP-128g migration route, which is not
available to third-party factories. Observe-only middleware uses the adapter
observer authoring surface and the bounded neutral ports described above.
Built-in control middleware lives
beside the policy contract under `middleware::control`, so the namespace reads
as one control subsystem rather than a loose set of root modules.

The policies returned from `materialize` are surface-specific:

- `SourcePolicy::admit` returns `SourceAdmission::Admit` or `Reject`; admitted
  policies observe the raw `SourcePollOutcome`.
- `EffectPolicy::admit` is event-neutral. Use `EventAwareEffectPolicy` only when
  the policy has a same-slice need to inspect the parent `ChainEvent`; both
  variants return `PolicyAdmission::Admit`, `Synthesize`, or `Reject`. Rejected
  effects are recorded under the effect cursor and strict replay returns the
  recorded outcome without invoking the live effect hook.
- `SinkPolicy::admit` returns `SinkAdmission::Admit` or `Reject`; rejection is
  recorded as a failed delivery receipt with middleware rejection metadata, not
  a successful `Noop` delivery.

The binder validates the declaration against the requested surface and protected
unit before materialization. If a surface and protected unit both carry an
effect type key, sink target key, or stage id, they must agree. Effect types and
sink targets are stable semantic keys, not GUID-style identities.
`MiddlewareAttachmentId` is a deterministic ULID-backed identity derived from
the declaration plus the attachment request.

See `tests/middleware_hook_binding_e2e_test.rs` for a complete third-party
control middleware that attaches to source poll, effect, and sink delivery,
rejects one effect protected unit, runs live, and strict-replays the same
archive.
