# Typed middleware

Application policies and observers are available through `obzenflow::middleware`.
They attach at defined runtime boundaries.

## Observers

Observers receive immutable views and return nothing. The supported surfaces
are source polling, handlers, stateful processing, joins, effects, sink delivery,
and stage lifecycle.

An observer cannot change outputs, settlement, or framework journals through
its callback. Callbacks run for live work and are suppressed during replay.
Each attachment has its own unwind boundary: its first panic quarantines it for
the rest of the stage run. This does not isolate blocking, process termination,
or side effects performed through application-owned capabilities.

For example, an application can log delivery results:

```rust,ignore
use obzenflow::middleware::{
    sink_delivery_observer, SinkDeliveryObserver, SinkDeliveryObserverContext,
};

struct DeliveryTrace;

impl SinkDeliveryObserver for DeliveryTrace {
    fn after_sink_delivery(&self, ctx: &SinkDeliveryObserverContext<'_>) {
        tracing::info!(
            stage = ctx.stage_name(),
            outcome = ?ctx.outcome(),
            "sink delivery classified"
        );
    }
}

let observer = sink_delivery_observer("delivery-trace", DeliveryTrace);
```

Pass the resulting attachment in the sink's `observers: [...]` clause.
Application diagnostics use ordinary Rust tools such as `tracing`.

## Control policies

Control policies protect a concrete live-I/O operation: a source poll, effect
invocation, sink delivery, or hosted ingress request. Use the corresponding
policy builders from `obzenflow::middleware`.

Retry is configured through `EffectResilienceBuilder::retry` within effect
resilience. It is not a standalone middleware attachment.

## Framework integration

Internal policy factories declare supported surfaces and materialise one typed
attachment for each protected unit. Unsupported requests fail during flow
construction. The ports include `SourcePolicy`/`SourceBoundary`,
`EffectPolicy`/`EffectBoundary`, `SinkPolicy`/`SinkDeliveryBoundary`, and ingress.

`MiddlewareContext` belongs to one ordered policy pass. Policies admit in
declaration order and observe in reverse order over the same context. It holds
typed slots, the boundary's execution scope, and a control-event outbox that
the runtime commits through its journal path.

The context is temporary and stays within that invocation. It is neither
persisted nor passed to handlers or supervisors. See
[the policy implementations](control/policy/mod.rs) for the individual ports.
