# Typed middleware

Application policies and observers are available through `obzenflow::middleware`.
They attach at defined runtime boundaries.

## Observers

Observers receive immutable views of source polling, handlers, stateful processing,
joins, effects and stage lifecycle. Sink calls use the effect surface.

Callbacks run for live work and are suppressed during replay. A panic quarantines
the attachment for the rest of the stage run. A typed effect observer receives
the stage input after a successful live call. If its callback returns an error,
the framework logs a warning and continues calling the observer.

For example, an application can log the order associated with a successful call.

```rust,ignore
use obzenflow::middleware::{typed_effect_observer, ObserverResult, TypedEffectObserver};

struct DeliveryTrace;

impl TypedEffectObserver for DeliveryTrace {
    type Input = Order;

    fn on_delivered(&self, order: &Order) -> ObserverResult {
        tracing::info!(order_id = %order.id, "order delivered");
        Ok(())
    }
}

let observer = typed_effect_observer("delivery-trace", DeliveryTrace);
```

Attach the observer with `with` on the sink row. Flow construction checks its
input type against the stage input.

## Control policies

Source polling supports rate limiting and circuit breaking. Effect calls,
including sink calls, support rate limiting, circuit breaking and retry.
Hosted ingress supports rate limiting.

Attach `rate_limit(...)`, `circuit_breaker()` and `retry()` from
`obzenflow::middleware` with `with`. Use `.for_effect::<E>()` to select an
effect in a stage that performs several. Flow construction validates the group
and allocates policy state. The effect boundary coordinates the policies in
execution order.

## Framework integration

Internal policy factories declare supported surfaces and materialize one typed
attachment for each protected unit. Unsupported requests fail during flow
construction. Source polling uses `SourcePolicy` and `SourceBoundary`. Effect calls use
`EffectPolicy` and `EffectBoundary`. Hosted ingress uses `IngressBoundaryMiddleware`.

`MiddlewareContext` belongs to one ordered policy pass. Policies admit in
declaration order and observe in reverse order over the same context. It holds
typed slots, the boundary's execution scope, and a control-event outbox that
the runtime commits through its journal path.

The context is temporary and stays within that invocation. It is neither
persisted nor passed to handlers or supervisors. See
[the policy implementations](control/policy/mod.rs) for the individual ports.
