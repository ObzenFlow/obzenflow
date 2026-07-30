# ObzenFlow DSL

This crate is an internal implementation detail of the ObzenFlow project. Most users access the DSL through the top-level `obzenflow` crate.

**Layer:** DSL/orchestration (outer). Depends on `obzenflow_adapters`, `obzenflow_runtime`, `obzenflow_core`, and `obzenflow-topology`.

The composition root for flow construction. The `flow!` macro turns a declarative block into a runnable `FlowHandle` by coordinating topology validation, journal allocation, middleware resolution, and stage wiring.

- `flow!` macro and topology parsing helpers
- Stage descriptor macros (`source!`, `transform!`, `sink!`, `stateful!`, `join!`, `effectful_transform!`, `effectful_stateful!`, `inference!`, `ai_map_reduce!`, and async variants)
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
        // Every stage declares its types. After FLOWIP-114c, untyped macro
        // forms fail to compile. See `examples/multi_source_ingest_demo/`
        // for the canonical heterogeneous-fan-in pattern.
        src = source!(MyPayload => my_source);
        out = sink!(MyPayload => my_sink);
    },

    topology: {
        src |> out;
    }
})
.await?;
```

The DSL has five sections: `name` (flow identifier), `journals` (persistence backend), `middleware` (flow-level defaults), `stages` (let-bindings producing stage descriptors), and `topology` (edges connecting stages with `|>` and `<|` operators).

## AI stage shapes

Use `inference!` when each input is already bounded and needs exactly one model decision:

```rust,ignore
brief = inference!(
    ReducedEvidence ->{
        at_least_once(ChatCompletion)
            via chat
            with { ai_resilience() }
    } DecisionBrief => brief_role
);
```

Use `ai_map_reduce!` when the input must be token-budgeted, fanned out, collected, and finalised. Its map and reduce roles use the same effect row shown above. The lexical `via chat` operand is a `ChatBindingContract`, not a registry name; normal configuration creates the contract and its consuming live registration together with `ChatEffectBinding::from_config(...).into_parts()`.

AI roles prepare a target-free `ChatRequestSpec`. The generated handler retains that exact value, binds the configured target only at the effect boundary, records `ChatCompletionReply` as framework replay evidence, and passes the retained spec plus reply to interpretation. The reply is not a selectable stage output.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
