# ObzenFlow Derive

This crate is an internal implementation detail of the ObzenFlow project. Most users should depend on the top-level `obzenflow` crate instead.

**Layer:** Proc-macro (host-side leaf). No dependencies on other ObzenFlow workspace crates.

Derive macros for the framework's authoring surface:

- `#[derive(EffectOutcomeFacts)]` defines an effect outcome carrier (FLOWIP-120m): an enum for a closed sum outcome (exactly one persisted fact per variant) or a named-field struct for a product outcome (one fact per field, recorded together). The derive generates the exact, fail-closed `TypedFactSet` implementation.

Use it through `obzenflow_core`, which re-exports the derive next to the `EffectOutcomeFacts` trait, the same way serde re-exports its derives:

```rust
use obzenflow_core::{EffectOutcomeFacts, TypedPayload};

#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}
```

Generated code resolves `::obzenflow_core` in the deriving crate. A crate that
reaches the trait through a re-export without a direct `obzenflow_core`
dependency points the derive at the re-exported core instead:

```rust
#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(crate = obzenflow_runtime::obzenflow_core)]
pub enum Outcome {
    Ok(SomeFact),
}
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
