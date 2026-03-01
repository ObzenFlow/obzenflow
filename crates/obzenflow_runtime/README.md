# ObzenFlow Runtime Services

This crate is an internal implementation detail of the ObzenFlow project. It provides the runtime services layer: supervised execution, stage orchestration, and coordination logic. Most users should depend on the top-level `obzenflow` crate instead.

## Architecture (maintainers)

ObzenFlow follows an onion architecture. Inner crates define business-domain abstractions, and outer crates provide runtime/infrastructure implementations that depend on inner crates. Implementation details are injected inward via traits and composition.

**Layer:** Runtime/services. Depends on `obzenflow_core`; outer layers (adapters/DSL/infra) are injected into this layer via handler traits and composition.

### Supervised FSM pattern (tl;dr)

ObzenFlow models long-running runtime components as supervised finite state machines (FSMs). It helps to separate:

- **Construction-time wiring**: build/spawn the supervisor task and return a handle (control plane).
- **Runtime supervision**: the task loop that drives the FSM; for stages this is also where user handler code is invoked (data plane).

At a glance:
- Builders implement `SupervisorBuilder` and return a `SupervisorHandle` (usually `StandardHandle<E, S>`).
- Handles send typed events over `tokio::sync::mpsc` and observe state via `tokio::sync::watch`.
- Supervisors run in a tokio task, own a mutable `Context`, and drive an `obzenflow_fsm::StateMachine`.
- Stage supervisors (`HandlerSupervised`) poll upstream journals/subscriptions, call the user handler, and append outputs/errors.

Detailed design notes (actor glossary + sequence diagrams): `src/supervised_base/README.md`.

### Best practices

Do:
- Keep supervisors private (`pub(crate)`) and expose only builders/handles.
- Put mutable state in `Context`; keep supervisors mostly immutable references.
- Model control flow with enums (states/events/actions) rather than boolean flags.
- Ensure FSM actions do real work; avoid placeholder/no-op actions.

Avoid:
- Add public supervisor constructors or direct accessors that bypass events.
- Hand-roll event loops when `SelfSupervisedExt` / `HandlerSupervisedExt` fits.

### Common infrastructure

The `supervised_base` module provides the shared building blocks:
- `SupervisorBuilder`, `SupervisorHandle`
- `ChannelBuilder`, `HandleBuilder`, `StandardHandle`
- `SupervisorTaskBuilder`
- `SelfSupervised` / `HandlerSupervised` (+ `*Ext` helpers)

### Where to look

- Pipeline supervisor: `src/pipeline/`
- Metrics supervisor: `src/metrics/`
- Stage supervisors/handlers: `src/stages/`
- Base patterns/utilities: `src/supervised_base/`

## Policies

See `LICENSE-MIT`, `LICENSE-APACHE`, `NOTICE`, `SECURITY.md`, and `TRADEMARKS.md`.
