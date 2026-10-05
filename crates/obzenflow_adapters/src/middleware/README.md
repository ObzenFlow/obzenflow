# Typed middleware

Application policies and observers are available through `obzenflow::middleware`.
They attach at defined runtime boundaries.

## Observers

Observers receive immutable views. The supported surfaces
are source polling, handlers, stateful processing, joins, effects, sink delivery,
and stage lifecycle.

An observer cannot change outputs, settlement, or framework journals through
its callback. Callbacks run for live work and are suppressed during replay.
Each attachment has its own unwind boundary: its first panic quarantines it for
the rest of the stage run. Sink observers also return `ObserverResult`; an error
warns and quarantines only that attachment, without changing delivery or its receipt.
Other observer surfaces retain unit callbacks. This does not isolate blocking, process termination,
or side effects performed through application-owned capabilities.

For example, given an application's `Order` payload, a typed sink observer can
log each input after its successful non-Noop delivery receipt is committed:

```rust,ignore
use obzenflow::middleware::{
    sink_delivery_observer, ObserverResult, SinkDeliveryObserver,
};

struct DeliveryTrace;

impl SinkDeliveryObserver for DeliveryTrace {
    type Input = Order;

    fn on_delivered(&self, order: &Order) -> ObserverResult {
        tracing::info!(
            order_id = %order.id,
            "order delivered"
        );
        Ok(())
    }
}

let observer = sink_delivery_observer("delivery-trace", DeliveryTrace);
```

Pass the resulting attachment in the sink implementation's `with { ... }` group.
Flow construction checks its input type against the sink connector's input.
For buffered sinks, the callback follows each original input's eventual receipt,
including receipts committed during flush or drain. Use the optional
`on_attempt(&SinkDeliveryObserverContext) -> ObserverResult` hook for attempt
classifications, including buffering, failure and rejection.
Application diagnostics use ordinary Rust tools such as `tracing`.

## Control policies

Control policies protect a concrete live-I/O operation: a source poll, effect
invocation, sink delivery, or hosted ingress request. Use the corresponding
inert `rate_limit(...)`, `circuit_breaker()`, and `retry()` values from
`obzenflow::middleware` in an implementation's `with { ... }` group or a named
effect's `uses Effect with { ... }` group. Flow construction validates the
complete group and allocates each policy's runtime state.

Retry protects a declared effect and can be used alone or alongside a limiter
and breaker. The group resolves into one coordinated effect policy; declaration
order does not set its execution order. Source polling and sink delivery support
limiting and breaking, while ingress supports limiting.

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
