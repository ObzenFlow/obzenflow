# ObzenFlow DSL (Infrastructure)

This crate is an internal implementation detail of the ObzenFlow project. It provides the `flow!` DSL and orchestration glue for describing pipelines.

ObzenFlow follows an onion architecture. Inner crates define business-domain abstractions, and outer crates provide runtime/infrastructure implementations that depend on inner crates. Implementation details are injected inward via traits and composition.

**Layer:** DSL/orchestration (outer). Depends on `obzenflow_adapters`, `obzenflow_runtime_services`, and `obzenflow_core` (and `obzenflow-topology` for graph validation).

## What this crate contains

`obzenflow_dsl_infra` is the composition root for flow construction. It turns a declarative `flow! { ... }` block into a runnable `FlowHandle` by coordinating:

- Topology construction + validation (`obzenflow-topology`)
- Journal allocation + run-manifest wiring (`FlowJournalFactory`)
- Stage resource wiring (subscriptions, backpressure plan, replay archive)
- Middleware inheritance/override semantics (flow-level + stage-level)
- Stage handle construction by delegating to runtime-services builders

Core building blocks in this crate:

- `flow!` macro and helpers: `src/dsl/dsl.rs`
- Stage descriptor macros (`source!`, `transform!`, …): `src/dsl/stage_macros.rs`
- `StageDescriptor` trait + per-stage descriptor implementations: `src/dsl/stage_descriptor.rs`
- Middleware resolution with audit trail: `src/middleware_resolution.rs`
- `StageHandleAdapter` bridging typed supervisor handles to a unified `StageHandle`: `src/stage_handle_adapter.rs`
- `FlowDefinition` future wrapper (the thing `flow!` returns): `src/dsl/flow_definition.rs`
- Structured build errors: `src/dsl/error.rs`
- Macro prelude imports: `src/prelude.rs`

## DSL surface (users)

Most users access this DSL via the top-level `obzenflow` crate (this crate is the implementation). The DSL is a *runtime-built* flow definition: `flow! { ... }` returns a `FlowDefinition` future that, when awaited, yields a `FlowHandle`.

### Recommended: run with `FlowApplication`

While `flow!` can be awaited directly to get a `FlowHandle`, most applications should **not** call `FlowHandle::run()` themselves. Prefer the application runner:

- `FlowApplication` lives in `obzenflow_infra::application::FlowApplication` (`crates/obzenflow_infra/src/application/flow_application.rs`).
- It handles runtime/observability setup, optional HTTP server startup (`--server`), CLI parsing, and graceful shutdown.

Minimal example (middleware + topology in a few lines):

```rust
use anyhow::Result;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_dsl_infra::{flow, sink, source};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;

#[tokio::main]
async fn main() -> Result<()> {
    FlowApplication::run(flow! {
        name: "basic_flow",
        journals: disk_journals(std::path::PathBuf::from("target/basic_flow")),

        // Flow-level middleware: defaults for every stage (unless overridden by name)
        middleware: [rate_limit(100.0)],

        stages: {
            // Stage-level middleware can override/extend flow-level defaults.
            // When a stage provides the same middleware type, it overrides by `MiddlewareFactory::name()`.
            src = source!("source" => MySource::new(), [rate_limit(10.0)]); // overrides flow default
            snk = sink!("sink" => MySink::new());
        },

        topology: {
            src |> snk;
        }
    })
    .await?;

    Ok(())
}
```

Key DSL sections:

- `journals:` expression that returns a per-flow journal factory (see “Journals + replay”)
- `middleware:` flow-level middleware factories applied to every stage (with overrides)
- `stages:` `let`-bindings that produce `Box<dyn StageDescriptor>` via macros like `source!`, `transform!`, …
- `topology:` edges using `|>` (forward) and `<|` (backward/feedback) operators

### How `flow!` + middleware + `FlowApplication` fit together

At a high level:

- `flow!` (this crate) is the *declarative composition layer*: stages + topology + middleware + journaling wiring.
- Middleware factories (adapters) are the *policy layer*: rate limiting, breakers, backpressure, cycle guards, and other cross-cutting behavior.
- `FlowApplication` (infra) is the *runtime/ops layer*: build + run the flow, expose optional HTTP endpoints, manage shutdown, and configure replay via CLI/env.

This split is what lets an application define a production-grade pipeline in a handful of lines while still allowing deep customization:

- Change behavior by swapping middleware factories (no changes to business handlers).
- Run with different operational modes (`--server`, `--replay-from`, CORS) without changing flow code.
- Keep the core domain (`obzenflow_core`) free of infrastructure while still wiring everything together at the edge.

## Architecture (maintainers)

### Layering overview

`obzenflow_dsl_infra` is the composition root for building flows. It sits outside the execution engine (`obzenflow_runtime_services`) and coordinates:

- Domain types + traits from `obzenflow_core`
- Stage supervisors/pipeline builders from `obzenflow_runtime_services`
- Middleware factories/exporters from `obzenflow_adapters`
- Topology validation and cycle detection from `obzenflow-topology`

This crate is typically consumed by the infrastructure runner (`obzenflow_infra`) rather than used directly.

Detailed design notes (including diagrams): `src/dsl/README.md`.

### `flow!` expansion model (tl;dr)

At a high level, `flow!` collects stage descriptors + topology edges and returns a `FlowDefinition` future. When awaited, it delegates the heavy lifting to `build_typed_flow!` (`src/dsl/dsl.rs`) to:

- Validate topology (including cycle detection and join/reference wiring)
- Allocate system/stage/error journals via `FlowJournalFactory` (and write `run_manifest.json` when supported)
- Build per-stage resources (subscriptions/registries + optional replay archive)
- Resolve middleware (flow-level + stage-level) and create stage handles from descriptors
- Build the pipeline and return a `FlowHandle`

### Flow construction phases

Implementation details live in `src/dsl/dsl.rs` (inside `build_typed_flow!`):

1. **Collect stage descriptors**
   - `stages: { var = <descriptor-expr>; ... }` builds a `HashMap<String, Box<dyn StageDescriptor>>`.
2. **Parse topology edges**
   - `parse_topology!` supports:
     - `a |> b;` forward edges (`EdgeKind::Forward`)
     - `a <| b;` backward edges (`EdgeKind::Backward`) for feedback loops
     - `(reference, stream) |> joiner;` join tuple syntax (emits two forward edges)
3. **Allocate stable stage IDs**
   - Generates `StageId::new()` for each bound stage variable (`src`, `snk`, …).
4. **Join reference resolution**
   - Join stage descriptors can carry a *reference stage variable name* (from `with_ref!`).
   - The DSL resolves that variable to a concrete `StageId` and writes it back via `set_reference_stage_id`.
   - The DSL also conditionally adds the reference edge unless it is already explicit.
5. **Topology validation**
   - Builds `obzenflow_topology::Topology` and fails with `FlowBuildError::TopologyValidationFailed`.
   - Cycle membership is later used to enable supervisor-level cycle protection for transform stages (FLOWIP-051l).
6. **Journal allocation + run manifest**
   - Creates one system journal and per-stage data + error journals.
   - Writes `run_manifest.json` via `FlowJournalFactory::write_run_manifest` (disk-backed factories implement this; others no-op).
   - `descriptor.name()` must be unique across stages (enforced) because it becomes the stable replay key (`FlowBuildError::DuplicateStageName`).
7. **Backpressure plan extraction**
   - If a stage enables `backpressure` middleware, its factory must expose a `config_snapshot()` containing a numeric `"window"` field.
8. **StageResourcesSet build**
   - Delegates wiring (subscriptions, registries, optional replay archive) to `StageResourcesBuilder`.
9. **Stage handle creation**
   - For each descriptor:
     - Pull pre-built `StageResources`.
     - Special-case join stages: prepend the reference journal/stage into upstream lists.
     - Enable supervisor-level cycle protection (default `max_iterations = 10`) when the stage is a cycle-member transform (FLOWIP-051l).
     - Build `MiddlewareStackConfig` per stage from middleware factory snapshots (for UI/observability).
     - Call `StageDescriptor::create_handle_with_flow_middleware(...)`.
10. **Pipeline build**
    - Uses `PipelineBuilder` (runtime-services) and returns a `FlowHandle`.

### Stage descriptors (the “let bindings” approach)

Stage macros like `source!` and `transform!` return a `Box<dyn StageDescriptor>` (`src/dsl/stage_macros.rs`).

Each descriptor:

- Carries the user handler type (e.g. `FiniteSourceHandler`, `TransformHandler`, `SinkHandler`)
- Carries stage-level middleware factories
- Knows how to build a concrete supervisor handle via runtime-services builders (e.g. `FiniteSourceBuilder`, `TransformBuilder`, `JournalSinkBuilder`, `JoinBuilder`)
- Returns a unified `BoxedStageHandle` by wrapping the typed handle in `StageHandleAdapter` (`src/stage_handle_adapter.rs`)

Supported stage descriptor macros:

- Sources: `source!`, `async_source!`, `infinite_source!`, `async_infinite_source!`
  - Async sources support an optional poll timeout (`DEFAULT_ASYNC_SOURCE_POLL_TIMEOUT` is 30s for finite async sources).
- Transforms: `transform!`, `async_transform!`
- Sinks: `sink!`
  - Convenience: `sink!("name" => |event: ChainEvent| { ... })` expands to a typed sink wrapper.
- Stateful: `stateful!` with optional `emit_interval = ...`
- Join: `join!` + `with_ref!` to specify the catalog/reference stage binding

### Middleware resolution + control strategies

Middleware comes from `obzenflow_adapters` as `MiddlewareFactory` values. The DSL merges flow-level and stage-level middleware via `resolve_middleware` (`src/middleware_resolution.rs`), preserving an audit trail:

- `MiddlewareSource::{Flow, Stage, StageOverride{...}}`
- Overrides are recorded as `OverrideRecord`
- Configuration warnings can be emitted (best-effort; some checks are TODO pending richer factory config introspection)

Descriptors apply middleware in this order:

1. **System middleware** (always present): `TimingMiddleware`, `SystemEnrichmentMiddleware`, `OutcomeEnrichmentMiddleware` (`src/dsl/stage_descriptor.rs`)
2. **Resolved user middleware** (flow-level + stage-level)

Control strategies:

- Some middleware can provide a `ControlEventStrategy` via `create_control_strategy()`.
- The descriptor composes strategies with `CompositeStrategy`, or falls back to `JonestownStrategy`.

Safety:

- Descriptors call `validate_middleware_safety(...)` for the stage type; failures are logged.

Cycle protection:

- If `topology.is_in_cycle(stage)` is true, the DSL enables supervisor-level cycle protection (default `max_iterations = 10`) for transform stages (FLOWIP-051l). Non-transform stages in cycles are currently rejected at materialisation time.

### Join stages (reference + stream)

Join stages have two distinct upstream roles:

- **Reference/catalog** input: a stage that provides the lookup dataset
- **Stream** input: events to enrich/join against the reference

DSL responsibilities for joins:

- Resolve `with_ref!(ref_stage_var, handler)` into a concrete `reference_stage_id`
- Ensure the reference edge exists (unless explicitly provided via join tuple syntax)
- Prepend the reference journal and stage id into the join stage’s upstream lists before building the stage
- Build `JoinMetadata` for the pipeline by scanning topology upstreams and separating reference from stream sources

The join descriptor implementation lives in `src/dsl/stage_descriptor.rs` (`JoinDescriptor`).

### Journals + replay

The `journals:` expression passed to `flow!` must evaluate to a provider that yields a `FlowJournalFactory` (runtime-services trait) for a given `FlowId`. The DSL uses that factory to create:

- System journal: `JournalName::System` owned by `JournalOwner::system(pipeline_id)`
- Per-stage data journal: `JournalName::Stage { ... }` owned by `JournalOwner::stage(stage_id)`
- Per-stage error journal: `JournalName::Stage { name: \"<stage>_error\", ... }`

Run manifests (FLOWIP-095a):

- The DSL builds `RunManifest` (from core) and calls `FlowJournalFactory::write_run_manifest`.
- Replay configuration is derived from env vars:
  - `OBZENFLOW_REPLAY_FROM` (path)
  - `OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE` (truthy values like `1`, `true`, `yes`)

Replay archive:

- The DSL calls `FlowJournalFactory::replay_archive_from_env` and passes the result into `StageResourcesBuilder`.

### Shutdown behavior

The unified stage handle returned by descriptors (`StageHandleAdapter`) implements `wait_for_completion()` using a timeout controlled by:

- `OBZENFLOW_SHUTDOWN_TIMEOUT_SECS` (default 30s)

## Extension guide (maintainers)

### Add a new stage kind to the DSL

1. Add a new descriptor implementing `StageDescriptor` in `src/dsl/stage_descriptor.rs`.
2. Use the appropriate runtime-services builder to construct the supervisor handle.
3. Add event translation + state classification functions (for `StageHandleAdapter`).
4. Add a macro in `src/dsl/stage_macros.rs` returning `Box<dyn StageDescriptor>`.
5. Update topology mapping (`to_topology_stage_type`) if a new `StageType` is introduced.
6. Add focused tests under `src/dsl/tests/`.

### Add/modify DSL syntax

- Update `flow!`, `parse_topology!`, or `build_typed_flow!` in `src/dsl/dsl.rs`.
- Prefer additive changes: these macros are widely used by examples/tests.

## Where to look

- DSL macros and build pipeline: `src/dsl/dsl.rs`
- Stage descriptor macros: `src/dsl/stage_macros.rs`
- StageDescriptor implementations (sources/transforms/sinks/stateful/join): `src/dsl/stage_descriptor.rs`
- Middleware inheritance and audit trail: `src/middleware_resolution.rs`
- Handle bridging to `StageHandle`: `src/stage_handle_adapter.rs`
- `FlowDefinition` wrapper: `src/dsl/flow_definition.rs`
- Build errors: `src/dsl/error.rs`
- DSL tests (join tuple syntax, cycle detection): `src/dsl/tests/`

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md`, and `TRADEMARKS.md`.
