# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- (FLOWIP-115m Part 2) Seven surface-specific, live-only observer interception helpers with immutable runtime-owned views and checked labelled composition. Each observer attachment is independently panic-quarantined so an unwind cannot suppress siblings or fail the protected business operation.
- (FLOWIP-128a B4) Stable first-class composite read contracts: the pure Moore lifecycle projection now lives at `obzenflow_core::composite`; `CompositeActivationContext` is pinned as additive durable integration metadata; exporter-facing composite snapshot DTOs have constructors; `composite_status` SSE schema v1 and the exact named-port Prometheus families have producer/consumer goldens.
- (FLOWIP-120m/010r) New `obzenflow_core_derive` compiler satellite with `#[derive(EffectOutcomeFacts)]`, re-exported through `obzenflow_core` serde-style: define an effect outcome carrier as an enum (closed sum, one persisted fact per variant) or named-field struct (product, one fact per field) and get an exact, fail-closed `TypedFactSet` implementation. Malformed shapes, generics, and repeated member types fail at compile time with span-precise errors; distinct types colliding on `EVENT_TYPE` are rejected at flow build. A `#[effect_outcome(crate = ...)]` attribute redirects generated paths when a direct Core dependency has been renamed.
- (FLOWIP-120m) `EffectOutcomeFacts`: public supertrait alias over `TypedFactSet`, now the `Effect::Outcome` bound, with `#[diagnostic::on_unimplemented]` notes pointing plain enums at the derive.
- (FLOWIP-115n) Checked `CircuitBreaker` configuration and the single `EffectResilience` aggregate for exact per-effect breaker, retry, and per-attempt limiter policy.
- (FLOWIP-120m) `TypedFactSetError::UnexpectedFact`: recorded fact groups decode exactly; a fact outside the carrier's declared set fails closed instead of being silently ignored.

### Changed
- (FLOWIP-010r) **Breaking**: renamed the unpublished Core compiler satellite from `obzenflow_derive` to `obzenflow_core_derive` and made `obzenflow_core` its sole first-party dependent and supported macro gateway. Runtime's alternate derive re-export and hidden Core escape hatch are removed.
- (FLOWIP-010r/010m) Made the main Cargo workspace one literal version unit: every member, including the unpublished `xtask`, inherits `[workspace.package].version`, and every dependency between workspace members requires exactly that version. Separately versioned ObzenFlow repositories remain outside this lockstep boundary.
- (FLOWIP-128a B4) **Breaking**: workspace packages move to 0.2.0 to establish forward-compatible composite APIs before their first release. Composite status/error, activation and observability context, `AppMetricsSnapshot`, and composite exporter DTOs are `#[non_exhaustive]`; graph-cut builders, accumulators, and metric projection SPI are documentation-hidden. The unpublished `obzenflow_runtime::composite` path is removed rather than retained as a compatibility alias.
- (FLOWIP-120m) **Breaking**: `Effect::Output` renamed to `Effect::Outcome`; `EffectDeclaration::output_fact_types` renamed to `outcome_fact_types`.
- (FLOWIP-120m) Producer-side effect-fact containment (`EffectFactNotInContract`) is validated unconditionally at build time for every effectful stage, before live I/O.
- (FLOWIP-115n) Circuit-open and probe-busy effect outcomes are stable recorded framework failures; health classification is independent from retry eligibility and delay.
- (FLOWIP-120m) The payment-gateway example records `payment.authorized.v1`/`payment.declined.v1` directly as the effect outcome group through an `AuthorizePaymentOutcome` carrier; the persisted `payment.gateway_decision.v1` bridge fact is gone.
- (FLOWIP-114p/115n) Middleware control flow no longer keys off string middleware names. `MiddlewareFactory` declares typed surfaces and materialises exact attachments; configuration defaults use runtime-owned descriptors with durable provenance.
- (FLOWIP-114p) `MiddlewareContext` is now typed and encapsulated: removed the legacy string `MiddlewareEvent` APIs and string-keyed baggage, and added typed per-pass slots via `MiddlewareContextKey`.
- (FLOWIP-114p) DSL middleware resolution is now fallible on same-scope duplicate override families, and topology/backpressure extraction uses the resolved middleware list plus typed factory contributions/slots instead of label matching.

### Removed
- (FLOWIP-115m Part 2) Observer reports/evidence, mutable output-commit observers, replay-time observer dispatch, the built-in observer logger, generic user-middleware event authoring, and the premature indicator/latency/measurement and durable-observation-rail APIs. Ordinary observers now return unit and application diagnostics use standard Rust `tracing`; the FLOWIP-135 series owns any future telemetry and SLI product.
- (FLOWIP-120m) String-only `EffectDeclaration` constructors (`idempotent`, `non_idempotent_with_key`, string `transactional`): their empty fact sets bypassed containment validation; use `EffectDeclaration::of::<E>()`.
- (FLOWIP-115n) Breaker-authored branch facts, typed wrappers, outcome synthesis, time windows, and sentence-shaped opening criteria.

## [0.1.2] - 2026-03-04

### Changed
- Improved rustdoc and crate READMEs across all published crates
- Added `homepage` field to workspace metadata
- Added CI license file validation step

## [0.1.1] - 2026-03-01

### Changed
- Governance files included in every published crate tarball
- Established underscore naming convention for internal workspace crates

## [0.1.0] - 2026-03-01

Initial pre-release of the ObzenFlow event streaming and processing framework.

### Core architecture
- Onion architecture with compile-time dependency enforcement across five workspace crates: `obzenflow_core`, `obzenflow_runtime`, `obzenflow_adapters`, `obzenflow_dsl`, `obzenflow_infra`
- Journal-backed event persistence with at-least-once delivery guarantees
- Deterministic replay from archived source journals
- CRC32 + HMAC integrity verification on journal entries

### Runtime and supervision
- Async stage supervision with FSM-driven lifecycle (idle, running, draining, stopped, failed)
- Circuit breaker with integrated retry, exponential backoff, and half-open probing
- Backpressure contracts to bound journal backlog in complex flows
- Cycle guard with convergence detection, fan-out iteration tracking, and EOF gating
- Stage timers for scheduled stateful emissions
- Idle CPU optimisation with blocking waits and exponential backoff
- Runtime guardrails for file descriptor limits and oversized pipelines

### Processing model
- Stateless transforms (sync and async) with middleware composition
- Stateful accumulators with typed reduce, fold, and windowed aggregation
- Reference joins (inner, left, live-update) with configurable staleness
- Fan-out (1:N) and fan-in (N:1) event routing
- Typed source and sink helpers with backpressure-aware semantics
- Error sinks for dead-letter routing

### DSL
- `flow!` macro for declarative pipeline definition with operator syntax (`|>`, `<|`)
- Stage descriptors with compile-time topology validation
- Flow middleware configuration (rate limiting, circuit breakers, observability)

### Sources and sinks
- CSV source and sink with auto-headers, buffering, and column selection
- HTTP event ingestion source (push)
- HTTP pull source for JSON API ingestion with paging and telemetry
- Console sink with pluggable formatters
- Replay-from mode to re-run flows from archived journals

### Observability
- Per-stage metrics (throughput, latency histograms, error rates, backpressure)
- Topology-aware metrics overlay with cumulative and per-stage views
- Continuous contract evaluation and divergence detection
- Web server endpoints for flow control, topology inspection, and event ingestion

### AI integration (optional features)
- LLM transform primitives with chat and embedding builder patterns
- rig.rs integration for AI provider abstraction
- Token estimation for cost-aware LLM usage

### Governance
- Dual licensed under MIT OR Apache-2.0
- DCO sign-off required for all contributions
- SPDX headers enforced on all Rust source files via CI
- Dependency policy enforced via cargo-deny and cargo-machete
