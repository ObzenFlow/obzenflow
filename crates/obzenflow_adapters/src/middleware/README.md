# Typed middleware architecture

ObzenFlow middleware attaches to explicit runtime join points. There is no
generic handler wrapper or `pre_handle`/`post_handle` chain.

## Observer hooks

Observe-only behaviour implements a surface-specific trait from
`obzenflow_adapters::middleware::observer` and is returned by a
`MiddlewareFactory` as a typed observer attachment.

Supported observer surfaces are:

- `SourcePoll`
- `Handler`
- `Stateful`
- `Join`
- `Effect`
- `SinkDelivery`
- `OutputCommit`
- `StageLifecycle`

Observers can inspect the surface-shaped context and return journalled
diagnostic evidence. They cannot skip, reject, retry, pause, or abort.
`ObserverDeterminism` declares whether runtime dispatch executes the observer
during replay.

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

## Factory shape

```rust,ignore
use obzenflow_adapters::middleware::{
    MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory,
    MiddlewareFactoryResult, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};

struct MyObserverFactory;

impl MiddlewareFactory for MyObserverFactory {
    fn label(&self) -> &'static str {
        "my_observer"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<Self>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(
            self.label(),
            vec![MiddlewareSurfaceKind::Handler],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        // Validate the requested typed surface and return its attachment.
        todo!()
    }
}
```

The built-in indicator, rate-limiter, circuit-breaker, and effect resilience
factories are complete production examples. Application logging uses standard
Rust `tracing` rather than observer middleware.
