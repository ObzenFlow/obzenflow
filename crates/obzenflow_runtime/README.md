# ObzenFlow Runtime

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Runtime/services. Depends on `obzenflow_core`, `obzenflow-topology` (graph structure and cycle detection), and `obzenflow-fsm` (state machine primitives). Outer layers inject implementations via handler traits and composition.

Defines the handler traits that users implement to build the stages of a durable flow, and provides the pipeline orchestration that supervises their execution. When you write a source, transform, join, stateful, effectful, or sink handler, you are implementing traits from this crate.

- **Handler traits** for each stage type (source, transform, join, stateful, effectful transform, effectful stateful, sink, observer), with async variants where applicable.
- **Pipeline orchestration** that wires topology, journals, and stages into a supervised pipeline, returning a handle for lifecycle control.
- **Stage supervision** where each stage runs as an independent task with automatic lifecycle management and drain semantics.
- **Journal and replay interfaces** that let outer layers inject persistence backends and replay previous runs from archived journals.
- **Metrics collection** via wide-event aggregation, piped to whatever exporter the adapters layer provides.
- **Backpressure and contracts** for per-edge flow control and verification between stages.

## Test support

This crate exposes a test-only feature flag:

- `test-support`: enables the `obzenflow_runtime::testing` module (FLOWIP-114h) and pulls in `tokio/test-util` to support paused-time tests (for example, `#[tokio::test(start_paused = true)]`).

This feature is for deterministic CI and integration tests only. Production builds must not enable it.

## Stage types

Each stage type has a corresponding handler trait (and async variant) that users implement:

- **Source** produces events, either finitely (CSV file, bounded query) or infinitely (WebSocket, heartbeat). Finite sources signal EOF when exhausted.
- **Transform** processes events one at a time, producing zero or more outputs per input. Stateless and ordered.
- **Sink** consumes events and writes durable delivery receipts. Supports optional flush and drain hooks for batched backends.
- **Stateful** accumulates state across events and emits aggregated results on a configurable schedule (every N events, on EOF, on a time window, etc.).
- **Effectful transform** performs declared effects and authors their outcomes as typed facts. Effects are declared through the `Effect` contract with an explicit `EffectSafety`, and committed outcomes are reused during replay and resume instead of re-executing the call.
- **Effectful stateful** folds input into state and decides declared effects from that state, driven only by input. Decided effects and their outcomes are journaled facts like any other.
- **Join** combines a reference dataset with a streaming source, supporting inner, left, and strict join strategies.
- **Observer** sees events without modifying them, useful for monitoring and health checks.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
