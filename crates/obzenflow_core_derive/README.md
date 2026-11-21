# ObzenFlow Core Derive

This crate implements ObzenFlow's procedural macros. Application authors use
`outcome![...]` from `obzenflow::schema` and the effect macros re-exported by
`obzenflow::effects`; they do not need a direct dependency on this compiler crate.

| Macro | Contract |
| --- | --- |
| `outcome![...]` | Sorts a list's outcomes by type name, through the hidden macro Core's `outcome!` calls. |
| `#[obzenflow::command]` | Declares the ports an effectful command works through, with `through = Port` on a unit struct or in `#[port]` fields. |
| `#[obzenflow::system]` | Names a system on its `impl` of a port. |
| `#[obzenflow::port]` | Marks a port trait, or the `impl` block of a struct that serves as its own port, so a `Port` takes its port type from the system it is built from. Each `#[effect]` method declares one guarantee (`exactly_once`, `at_least_once` or `at_most_once`). A sink method records its reply with `records = reply`. |

## Application use

For application payload types `PaymentAuthorized` and `PaymentDeclined`, a port method
answers with exactly one of them:

```rust,ignore
use obzenflow::schema::outcome;

#[obzenflow::port]
#[async_trait]
pub trait PaymentGatewayPort {
    #[effect(type = "payment.authorize", exactly_once)]
    async fn authorize(
        &self,
        request: &AuthorizeRequest,
        key: &IdempotencyKey,
        ctx: &EffectContext,
    ) -> Result<outcome![PaymentAuthorized | PaymentDeclined], EffectError>;
}
```

A transform's `Output` is written the same way, and `outcome(x)` builds a value. A renamed
facade dependency needs no attribute.

## Compiler integration

Core's `outcome!` passes its own path to the list macro, so a list expands the same under
any dependency name. The command, system and port attributes resolve through
`::obzenflow::effects` when imported from the facade and `::obzenflow_runtime::effects`
when imported from Runtime. A renamed dependency supplies `effects = <path>`. The
compiler crate has no dependency on the facade or other ObzenFlow workspace crates.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
