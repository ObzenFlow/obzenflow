# ObzenFlow DSL

This crate is an internal implementation detail of the ObzenFlow project. Most users access the DSL through the top-level `obzenflow` crate.

**Layer:** DSL/orchestration (outer). Depends on `obzenflow_adapters`, `obzenflow_runtime`, `obzenflow_core`, and `obzenflow-topology`.

The composition root for flow construction. The `flow!` macro turns a declarative block into a `FlowDefinition`; ordinary crate-owned Rust coordinates topology validation, journal allocation, stage-authored middleware binding, and stage wiring when the host builds it.

- `flow!` macro and topology parsing helpers
- Stage descriptor macros (`source!`, `transform!`, `sink!`, `stateful!`, `join!`, `effectful_transform!`, `effectful_stateful!`, `inference!`, `ai_map_reduce!`, and async variants)
- Stage-authored middleware binding to typed runtime surfaces
- `FlowDefinition` deferred build wrapper (what `flow!` returns)
- Structured build errors

## Usage

Most applications should run flows through `FlowApplication` rather than awaiting `FlowHandle` directly:

```rust,ignore
use obzenflow_dsl::{flow, source, sink, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;

fn build_flow() -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let my_source = build_source();
        let my_sink = build_sink();

        Ok(flow! {
            name: "my_flow",
            journals: disk_journals("target/my-flow-logs".into()),

            stages: {
                // Every stage declares its types. See
                // `examples/multi_source_ingest_demo/` for the canonical
                // heterogeneous-fan-in pattern.
                src = source!(MyPayload => my_source);
                out = sink!(MyPayload => my_sink);
            },

            topology: {
                src |> out;
            }
        })
    })
}

FlowApplication::run(build_flow()).await?;
```

Supported handler and composite-role slots take a local name or identifier-only
qualified path. Construct builder-owned handlers and sink adapters inside the deferred
materialiser immediately above `flow!`; calls, closures, builder chains, and struct
literals are rejected in the slots. Async-source poll timeout is handler
configuration exposed through `poll_timeout()`, not stage syntax.

The DSL has four core sections: optional `name` (flow identifier), `journals` (persistence backend), `stages` (bindings producing stage descriptors), and `topology` (edges connecting stages with `|>` and `<|` operators). Optional flow backpressure and effect-port sections sit between `journals` and `stages`. Middleware is declared only on the stage where it applies.

## AI stage shapes

Use `inference!` when each input is already bounded and needs exactly one model decision:

```rust,ignore
brief = inference!(
    ReducedEvidence -> DecisionBrief
    uses at_least_once(ChatCompletion)
        via chat
        with ai_resilience()
    => generate_brief
);
```

`generate_brief` is a user-owned type implementing the runtime `InferenceHandler`
trait. Its `Input` and `Output` associated types witness the arrow, while its
`prepare` and `interpret` methods provide the scalar inference hooks. The value to the
right of the arrow is therefore an ordinary stage handler, as it is for the other stage
macros. A hidden adapter performs the declared chat effect between those two hooks.

Use `ai_map_reduce!` when the input must be token-budgeted, fanned out, collected, and finalised. Its map and reduce roles use the same trailing `uses` clause shown above. The lexical `via chat` operand is an `EffectBinding<ChatCompletion>`, not a registry name; normal configuration obtains it directly with `ChatEffectBinding::from_config(...)`, and the flow builder collects its private binding package.

Inference handlers and map-reduce roles prepare a target-free `ChatRequestSpec`. The
framework retains that exact value, binds the configured target only at the effect
boundary, records `ChatCompletionReply` as framework replay evidence, and passes the
retained spec plus reply to interpretation. The reply is not a selectable stage output.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
