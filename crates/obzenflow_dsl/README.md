# ObzenFlow DSL

This crate is an internal implementation detail of the ObzenFlow project. Most users access the DSL through the top-level `obzenflow` crate.

**Layer:** DSL/orchestration (outer). Depends on `obzenflow_adapters`, `obzenflow_runtime`, `obzenflow_core`, and `obzenflow-topology`.

The composition root for flow construction. The `flow!` macro turns a declarative block into a runnable `FlowHandle` by coordinating topology validation, journal allocation, middleware resolution, and stage wiring.

- `flow!` macro and topology parsing helpers
- Stage descriptor macros (`source!`, `transform!`, `sink!`, `stateful!`, `join!`, and async variants)
- Middleware inheritance and override resolution with audit trail
- `FlowDefinition` future wrapper (what `flow!` returns)
- Structured build errors

## Usage

Most applications should run flows through `FlowApplication` rather than awaiting `FlowHandle` directly:

```rust,ignore
use obzenflow_dsl::{flow, source, sink};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;

FlowApplication::run(flow! {
    name: "my_flow",
    journals: disk_journals("target/my-flow-logs".into()),
    middleware: [],

    stages: {
        src = source!(my_source);
        out = sink!(my_sink);
    },

    topology: {
        src |> out;
    }
})
.await?;
```

The DSL has five sections: `name` (flow identifier), `journals` (persistence backend), `middleware` (flow-level defaults), `stages` (let-bindings producing stage descriptors), and `topology` (edges connecting stages with `|>` and `<|` operators).

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
