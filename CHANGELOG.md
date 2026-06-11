# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- (FLOWIP-120m) New `obzenflow_derive` crate with `#[derive(EffectOutcomeFacts)]`, re-exported through `obzenflow_core` serde-style: define an effect outcome carrier as an enum (closed sum, one persisted fact per variant) or named-field struct (product, one fact per field) and get an exact, fail-closed `TypedFactSet` implementation. Malformed shapes, generics, and repeated member types fail at compile time with span-precise errors; distinct types colliding on `EVENT_TYPE` are rejected at flow build. A `#[effect_outcome(crate = ...)]` attribute redirects the generated paths when core is reached through a re-export (for example `obzenflow_runtime::obzenflow_core`, a hidden re-export added for this).
- (FLOWIP-120m) `EffectOutcomeFacts`: public supertrait alias over `TypedFactSet`, now the `Effect::Outcome` bound, with `#[diagnostic::on_unimplemented]` notes pointing plain enums at the derive.
- (FLOWIP-120m) Outcome-shaped circuit-breaker fallback: `with_outcome_fallback` plus `build_outcome::<E>()` declare a breaker that synthesizes the protected effect's own outcome carrier (multi-fact product groups included) while the handler keeps the plain `perform`. One fallback shape per stage; mixing with branch-shaped producers is rejected at build time.
- (FLOWIP-120m) `TypedFactSetError::UnexpectedFact`: recorded fact groups decode exactly; a fact outside the carrier's declared set fails closed instead of being silently ignored.

### Changed
- (FLOWIP-120m) **Breaking**: `Effect::Output` renamed to `Effect::Outcome`; `EffectDeclaration::output_fact_types` renamed to `outcome_fact_types`.
- (FLOWIP-120m) **Breaking**: circuit-breaker fallback methods renamed: `with_typed_fallback` is now `with_fallback_fact`, `typed_outcome_with` is now `with_rejection_fact`.
- (FLOWIP-120m) Producer-side effect-fact containment (`EffectFactNotInContract`) is validated unconditionally at build time for every effectful stage; previously it was skipped for stages without an `output_middleware:` lane and surfaced only at commit, after live I/O.
- (FLOWIP-120m) Replay reconstructs effect-fact origin (`Effect` versus `MiddlewareSynthesized`) from the recorded provenance instead of deriving it from middleware registrations; derivation remains as the fallback for pre-120h journals.
- (FLOWIP-120m) The payment-gateway example records `payment.authorized.v1`/`payment.declined.v1` directly as the effect outcome group through an `AuthorizePaymentOutcome` carrier; the persisted `payment.gateway_decision.v1` bridge fact is gone.
- (FLOWIP-114p) Middleware control flow no longer keys off string middleware names. `Middleware` and `MiddlewareFactory` now require typed responsibilities (`source_phase`, `control_role`, `override_key`, `plan_contribution`, `topology_config_slot`) so routing, planning, and override/de-dupe behavior are compiler-checked.
- (FLOWIP-114p) `MiddlewareContext` is now typed and encapsulated: removed the legacy string `MiddlewareEvent` APIs and string-keyed baggage, added typed per-pass slots via `MiddlewareContextKey`, and added typed user middleware events via `TypedMiddlewareEvent`.
- (FLOWIP-114p) DSL middleware resolution is now fallible on same-scope duplicate override families, and topology/backpressure extraction uses the resolved middleware list plus typed factory contributions/slots instead of label matching.

### Removed
- (FLOWIP-120m) String-only `EffectDeclaration` constructors (`idempotent`, `non_idempotent_with_key`, string `transactional`): their empty fact sets bypassed containment validation; use `EffectDeclaration::of::<E>()`.

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
