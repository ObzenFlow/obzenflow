# ObzenFlow Core Derive

This compiler-host crate is the procedural-macro half of the logical ObzenFlow Core component. Its direct package API is an internal implementation detail; users invoke its derives through `obzenflow_core`.

**Layer:** Core compiler satellite (host-side leaf). No dependencies on other ObzenFlow workspace crates.

Derive macros for Core-owned contracts:

- `#[derive(EffectOutcomeFacts)]` defines an effect outcome carrier (FLOWIP-120m): an enum for a closed sum outcome (exactly one persisted fact per variant) or a named-field struct for a product outcome (one fact per field, recorded together). The derive generates the exact, fail-closed `TypedFactSet` implementation.
- `#[derive(StageOutputFacts)]` defines a typed stage output carrier (FLOWIP-120z) and its Core-owned fact-set projections.

Use it through `obzenflow_core`, which re-exports the derive next to the `EffectOutcomeFacts` trait, the same way serde re-exports its derives:

```rust
use obzenflow_core::{EffectOutcomeFacts, TypedPayload};

#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}
```

Generated code resolves `::obzenflow_core` in the deriving crate. If a direct
Core dependency is renamed in Cargo.toml, the existing path override points at
that extern-prelude name:

```rust
#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(crate = flow_core)]
pub enum Outcome {
    Ok(SomeFact),
}
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
