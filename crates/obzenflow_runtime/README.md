# ObzenFlow Runtime

Runtime owns stage execution, pipeline supervision, and replay. Application
authors implement its handler contracts through `obzenflow::stages` and use
`obzenflow::effects` and `obzenflow::middleware` for effects and observers.

This layer depends on Core, `obzenflow-topology`, and `obzenflow-fsm`.
Outer layers supply journal backends, adapters, and hosting.

## Responsibilities

- Handler contracts for sources, transforms, stateful stages, joins, and sinks.
- Effect execution, typed outcomes, and delivery evidence.
- Pipeline and stage state machines, lifecycle control, and resource settlement.
- Journal publication, replay, and resume through Core's storage contracts.
- Backpressure, edge verification, and deterministic input ordering where required.
- Execution measurements and snapshots for optional reporting.

Observers receive immutable views at defined execution boundaries. They are
attachments to stages, not an additional stage family.

The [supervision guide](https://github.com/obzenflow/obzenflow/blob/main/crates/obzenflow_runtime/src/supervised_base/README.md)
explains the shared run loops, state-machine ownership, and construction rules
for framework contributors.

## Test support

The `test-support` feature exposes `obzenflow_runtime::testing` and enables
`tokio/test-util` for deterministic integration tests, including paused-time
tests. Keep it disabled in production configurations.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
