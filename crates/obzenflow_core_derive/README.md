# ObzenFlow Core Derive

This crate implements ObzenFlow's procedural macros. Application authors use the
fact derives re-exported by `obzenflow::schema` and the effect macros re-exported
by `obzenflow::effects`; they do not need a direct dependency on this compiler
crate.

| Macro | Contract |
| --- | --- |
| `EffectOutcomeFacts` | Declares the typed facts an effect outcome can contain. |
| `StageOutputFacts` | Declares a stage output carrier and its fact-set projections. |
| `#[obzenflow::command]` | Declares the ports an effectful command works through, with `through = Port` on a unit struct or in `#[port]` fields. |
| `#[obzenflow::system]` | Names a system on its `impl` of a port. |
| `#[obzenflow::port]` | Marks a port trait, or the `impl` block of a struct that serves as its own port, so a `Port` takes its port type from the system it is built from. Each method marked `#[effect(type = "...", exactly_once)]` or `at_least_once` declares the effect its request names. |

## Application use

For application payload types `PaymentAuthorized` and `PaymentDeclined`:

```rust,ignore
use obzenflow::schema::EffectOutcomeFacts;

#[derive(Debug, Clone, EffectOutcomeFacts)]
pub enum AuthorizePaymentOutcome {
    Authorized(PaymentAuthorized),
    Declined(PaymentDeclined),
}
```

`StageOutputFacts` is written the same way. If the facade dependency is renamed to `of`,
add `#[effect_outcome(schema = of::schema)]` or `#[stage_output(schema = of::schema)]`.

## Compiler integration

The fact derives generate implementations of Core's schema contracts through paths under
`::obzenflow::schema`, and the command, system and port attributes expand to paths under
`::obzenflow::effects`. Code that depends on Core or the runtime crate directly names the
module with `schema = <path>` or `effects = <path>`. The compiler crate has no dependency
on the facade or other ObzenFlow workspace crates.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
