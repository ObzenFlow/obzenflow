# ObzenFlow Core Derive

This crate implements Core's procedural macros. Application authors use the
derives re-exported by `obzenflow::schema`; they do not need a direct dependency
on this compiler crate.

| Derive | Contract |
| --- | --- |
| `EffectOutcomeFacts` | Declares the typed facts an effect outcome can contain. |
| `StageOutputFacts` | Declares a stage output carrier and its fact-set projections. |

## Application use

For application payload types `PaymentAuthorized` and `PaymentDeclined`:

```rust,ignore
use obzenflow::schema::EffectOutcomeFacts;

#[derive(Debug, Clone, EffectOutcomeFacts)]
#[effect_outcome(schema = obzenflow::schema)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}
```

`StageOutputFacts` uses `#[stage_output(schema = obzenflow::schema)]`.
If the facade dependency is renamed to `of`, use `of::schema` in either attribute.

## Compiler integration

The generated implementations use Core's schema contracts. The compiler crate
has no dependency on the facade or other ObzenFlow workspace crates.

Direct Core consumers can use its re-exported derives with the default
`::obzenflow_core` path. A renamed Core dependency retains the
`#[effect_outcome(crate = flow_core)]` and `#[stage_output(crate = flow_core)]`
overrides.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
