# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- (FLOWIP-114p) Middleware control flow no longer keys off string middleware names. `Middleware` and `MiddlewareFactory` now require typed responsibilities (`source_phase`, `control_role`, `override_key`, `plan_contribution`, `topology_config_slot`) so routing, planning, and override/de-dupe behavior are compiler-checked.
- (FLOWIP-114p) `MiddlewareContext` is now typed and encapsulated: removed the legacy string `MiddlewareEvent` APIs and string-keyed baggage, added typed per-pass slots via `MiddlewareContextKey`, and added typed user middleware events via `TypedMiddlewareEvent`.
- (FLOWIP-114p) DSL middleware resolution is now fallible on same-scope duplicate override families, and topology/backpressure extraction uses the resolved middleware list plus typed factory contributions/slots instead of label matching.

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
