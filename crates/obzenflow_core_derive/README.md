# ObzenFlow Core Derive

This crate implements ObzenFlow's procedural macros. Application authors use the
fact derives re-exported by `obzenflow::schema` and the effect macros re-exported
by `obzenflow::effects`; they do not need a direct dependency on this compiler
crate.

| Macro | Contract |
| --- | --- |
| `EffectOutcomeFacts` | Declares the typed facts an effect outcome can contain. |
| `StageOutputFacts` | Declares a stage output carrier and its fact-set projections. |
| `EffectHandler` | Declares the systems an effectful handler calls, each in a `#[system]` field. |
| `#[obzenflow::port]` | Marks a port trait, or the `impl` block of a struct that serves as its own port, so a `System` takes its port type from the value it is built from. Each method marked `#[effect(type = "...", exactly_once)]` or `at_least_once` declares the effect its request names. |

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

The fact derives generate implementations of Core's schema contracts.
`EffectHandler` and the port attribute expand to paths under `::obzenflow::effects`;
code that depends on the runtime crate directly names its effects module with
`#[effect_handler(effects = <path>)]` or `#[port(effects = <path>)]`. The compiler
crate has no dependency on the facade or other ObzenFlow workspace crates.

Direct Core consumers can use its re-exported derives with the default
`::obzenflow_core` path. A renamed Core dependency retains the
`#[effect_outcome(crate = flow_core)]` and `#[stage_output(crate = flow_core)]`
overrides.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
