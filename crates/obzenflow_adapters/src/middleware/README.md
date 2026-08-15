# Typed middleware architecture

ObzenFlow middleware attaches to explicit runtime join points. There is no
generic handler wrapper or `pre_handle`/`post_handle` chain.

## Observer hooks

Observe-only behaviour implements a surface-specific trait from
`obzenflow_runtime::stages::observer` and attaches through its matching
`*_observer("label", value)` helper.

Supported observer surfaces are:

- `SourcePoll`
- `Handler`
- `Stateful`
- `Join`
- `Effect`
- `SinkDelivery`
- `StageLifecycle`

Observers receive immutable, runtime-constructed views and return nothing.
They cannot use this contract to replace outputs, publish framework records,
skip, reject, retry, pause, or abort. Runtime dispatches them only for live
occurrences, never while recorded history is reconstructed.

Each attachment is independently protected by an unwind boundary. Its first
panic quarantines it for the rest of the stage run; sibling observers and the
business operation continue. This is not a process or resource sandbox:
blocking, deadlock, abort, process exit, excessive resource use, and side
effects through independently held application capabilities remain possible.

## Control policies

Control behaviour protects a concrete live-I/O unit through one of the typed
ports:

- `SourcePolicy` and `SourceBoundary`
- `EffectPolicy` and `EffectBoundary`
- `SinkPolicy` and `SinkDeliveryBoundary`
- the ingress boundary port

A factory declares the supported surface and materialises exactly one matching
attachment. Unsupported requests fail during flow construction.

Retry is not a standalone middleware surface. It is an
`EffectResilienceBuilder::retry` setting owned by the circuit-breaker recovery
aggregate around a declared effect.

## `MiddlewareContext`

`MiddlewareContext` is an invocation-local carrier used only inside an ordered
typed policy pass. Policies admit in declaration order and admitted policies
observe in reverse order over the same context.

The context contains:

- typed slots identified by `MiddlewareContextKey`;
- a control-event outbox returned through the boundary report and committed by
  the existing runtime journal path;
- the execution scope for the typed boundary invocation.

It is not persisted, shared between concurrent invocations, passed to handlers
or supervisors, or exposed as a replacement generic middleware API.

## Observer shape

```rust,ignore
use obzenflow_adapters::middleware::sink_delivery_observer;
use obzenflow_runtime::stages::observer::{
    SinkDeliveryObserver, SinkDeliveryObserverContext,
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

The observer layer has no logging, measurement, journal, storage, or exporter
API. Application diagnostics use standard Rust `tracing`. Any future telemetry
or SLI producers are owned by the FLOWIP-135 series.
