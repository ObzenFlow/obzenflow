# Type-and-effect stage grammar

Effectful stages keep the type transformation intact, then declare the
capabilities available to the handler:

```rust,ignore
Input -> Output
uses Effect
=> handler
```

Read this as “input becomes output, using this capability, through this
handler.” `uses` grants authority; it does not claim that the handler performs
the effect for every input.

Pure stages omit the `uses` clause:

```rust,ignore
Input -> Output => handler
```

A singleton effect is always bare. Braces always mean an unordered set of at
least two effects:

```rust,ignore
Input -> Output uses EffectA => handler
Input -> Output uses { EffectA, EffectB } => handler
```

An empty set and a braced singleton are invalid. Multi-fact outputs retain
their existing braces without becoming visually entangled with effects:

```rust,ignore
Input -> { OutputA, OutputB }
uses EffectA
=> handler
```

For wrapped declarations, finish the complete `Input -> Output` signature,
then align `uses` and `=>`. Indent `via` and `with` one level beneath the effect.
Short declarations may stay on one line, but framework examples use the wrapped
layout consistently because `rustfmt` does not format macro interiors.

## Binding and policy operands

A portless effect is named directly. A named effect must select a lexical typed
binding with `via`:

```rust,ignore
Input -> Output
uses {
    LocalEffect,
    at_least_once(ChatCompletion) via chat,
}
=> handler
```

`via chat` receives an `EffectBinding<ChatCompletion>`, not a string or a lookup
key. This remains mandatory for a named effect whose declared slot set is empty:
the evidence and construction family are still part of its authority.

An effect may have one existing boundary policy, introduced by bare `with`:

```rust,ignore
Input -> Output
uses at_least_once(ChatCompletion)
    via chat
    with ai_resilience()
=> handler
```

Policy lists and blocks are not part of this grammar. Transactional execution
uses the same named-binding path; its executor is a reserved typed slot:

```rust,ignore
Input -> Output
uses transactional(LedgerWrite)
    via ledger
=> handler
```

The same `uses` syntax applies to `effectful_transform!` and
`effectful_stateful!`. Stateful policies remain unavailable unless enabled by
their own FLOWIP.

## Constructing named authority

Normal applications select an effect-specific facade, install it, and retain
the lexical binding returned by that operation:

```rust,ignore
let mut effect_ports = EffectPortRegistry::new();
let chat = ChatEffectBinding::from_config(&ai_models)?
    .install_into(&mut effect_ports)?;
```

The facade owns its logical name, typed slots, evidence, metadata projection,
registration construction, and installation. An application-selected chat
implementation uses the same surface:

```rust,ignore
let chat = ChatEffectBinding::from_resolver(chat_target, chat_resolver)?
    .install_into(&mut effect_ports)?;
```

Effect authors and advanced application-local effects may use the public
generic `EffectRegistrationBuilder`. That builder validates the effect's exact
typed slot set and returns a lexical binding plus an opaque registration. Use
`bind_eager` for an already constructed metadata-free implementation and
`bind_deferred` only for bounded, non-suspending local construction. A resolver
is not an async or remote discovery hook. Metadata-bearing integrations keep
their projection inside their outward facade rather than requiring application
code to construct `ResolvedEffectPort` values.

Handlers retain `binding.invocation()` through their effect values. They do not
receive the registration, resolver, registry, or an ambient port lookup. Live
misses receive only their declaration's resolved typed slots through
`EffectContext::port(slot)`. Strict replay hits require no live registration and
never invoke a resolver.

If an implementation detects a target invariant failure, construct it from the
same declared slot token with `EffectError::target_invariant_violation(slot)`.
Raw targets, resolver causes, binding evidence, and credentials are not accepted
by the framework diagnostic surface.

Multi-effect declarations and singleton policy attachments canonicalize by
stable effect identity, so source-order permutations materialize the same
membership.

## Migration from the pre-132a surface

Move every non-empty ordinary declaration into the trailing `uses` clause:

```rust,ignore
// Before
effectful_transform!(Input -> Output => handler, effects: [EffectA]);

// After
effectful_transform!(Input -> Output uses EffectA => handler);
```

Replace string-selected or detached live ports with an effect-specific facade
where one exists. Install the facade at the composition root, pass clones of
the returned binding into declarations and handlers, and use `via binding` in
the relevant `uses` entry. Application-local effects without a facade construct one
`EffectBinding<E>`/`EffectRegistration<E>` pair through the generic builder and
install its registration. Replace
`transactional(Effect, "executor")` with `transactional(Effect) via binding`.

The old raw registry insertion, deferred lookup, and AI-specific binding
contract APIs are removed rather than deprecated. Current run manifests carry
`effect_binding_descriptor: 1`; archives without that capability are rejected
before effect records are decoded. Rebuild test archives with the current
schema instead of expecting pre-132a descriptor compatibility.

## Concrete 132a migrations

### Payment gateway

Payment moves the declaration and its singleton policy from the detached lane
into a trailing capability clause:

```rust,ignore
// Before
ValidatedOrder -> {
    PaymentAuthorized,
    PaymentDeclined,
    OrderCancelled,
    PaymentAuthorizationUnavailable,
} => gateway,
effects: [AuthorizePayment with gateway_resilience]

// After
ValidatedOrder -> {
    PaymentAuthorized,
    PaymentDeclined,
    OrderCancelled,
    PaymentAuthorizationUnavailable,
}
uses AuthorizePayment
    with gateway_resilience
=> gateway
```

### One-shot inference

The one-shot example keeps its `via chat` capability and lets the binding
facade own its consuming registration installation:

```rust,ignore
// Before
let (chat, chat_registration) =
    ChatEffectBinding::from_config(&ai_models)?.into_parts();
let effect_ports =
    chat_registration.install_into(EffectPortRegistry::new())?;

// After
let mut effect_ports = EffectPortRegistry::new();
let chat = ChatEffectBinding::from_config(&ai_models)?
    .install_into(&mut effect_ports)?;

ReducedEvidence -> DecisionBrief
uses at_least_once(ChatCompletion)
    via chat
    with ai_resilience()
=> brief_role
```

### Hacker News digest

HN previously selected its test resolver through the raw port registry. Its
test seam now supplies the same high-level facade used by configured providers;
the example never rebuilds registration or target-metadata machinery:

```rust,ignore
// Before
let (chat, chat_registration) =
    ChatEffectBinding::from_config(&ai_models)?.into_parts();
let chat_target = chat.target().clone();
let effect_ports = if let Some(resolver) = chat_resolver_override {
    EffectPortRegistry::new()
        .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, resolver)
} else {
    chat_registration.install_into(EffectPortRegistry::new())
}?;

// After
let chat_binding = if let Some(binding) = chat_binding_override {
    binding
} else {
    ChatEffectBinding::from_config(&ai_models)?
};
let mut effect_ports = EffectPortRegistry::new();
let chat = chat_binding.install_into(&mut effect_ports)?;
let chat_target = chat.evidence().target().clone();
```

Each generated role now finishes its type transformation before declaring the
same capability:

```rust,ignore
// Before
map: [FormattedStory] -> {
    at_least_once(ChatCompletion) via chat with ai_resilience()
} HnDigestGroupSummary => map_role

// After
map: [FormattedStory] -> HnDigestGroupSummary
uses at_least_once(ChatCompletion)
    via chat
    with ai_resilience()
=> map_role
```

The reduce role uses the identical ordering.

### Standalone chat

Standalone chat replaces the raw name-plus-type resolver and detached effect
list with an effect-specific facade and canonical capability declaration:

```rust,ignore
// Before
let effect_ports = EffectPortRegistry::new()
    .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, chat_resolver)?;
effectful_transform!(
    TicketRaised -> TicketSummarised => chat_handler,
    effects: [at_least_once(ChatCompletion) with ai_resilience()],
);

// After
let chat = ChatEffectBinding::from_resolver(chat_target, chat_resolver)?
    .install_into(&mut effect_ports)?;
effectful_transform!(
    TicketRaised -> TicketSummarised
    uses at_least_once(ChatCompletion)
        via chat
        with ai_resilience()
    => chat_handler,
);
```

### Standalone embedding

Embedding makes the corresponding migration explicitly rather than relying on
a textual substitution from the chat example:

```rust,ignore
// Before
let effect_ports = EffectPortRegistry::new()
    .with_deferred::<dyn EmbeddingClient>(
        EMBEDDING_CLIENT_PORT,
        embedding_resolver,
    )?;
effectful_transform!(
    TicketSummarised -> TicketEmbedded => embedding_handler,
    effects: [at_least_once(EmbeddingGeneration) with ai_resilience()],
);

// After
let embedding = EmbeddingEffectBinding::from_resolver(
    embedding_target,
    embedding_resolver,
)?
.install_into(&mut effect_ports)?;
effectful_transform!(
    TicketSummarised -> TicketEmbedded
    uses at_least_once(EmbeddingGeneration)
        via embedding
        with ai_resilience()
    => embedding_handler,
);
```

### Transactional ledger

Transactional registration moves from a raw string-keyed executor to its
effect's typed transactional slot, while the `uses` clause carries the binding
value:

```rust,ignore
// Before
ports.insert::<dyn TransactionalEffectPort<LedgerEffect>>(
    "ledger_tx",
    ledger_port,
)?;
effectful_transform!(
    ReplayInput -> { ReplayOutput, ReplayEffectValue } => ledger_handler,
    effects: [transactional(LedgerEffect, "ledger_tx") with resilience],
);

// After
let (ledger_binding, registration) =
    EffectRegistrationBuilder::<LedgerEffect>::new(
        LogicalEffectBindingName::new("ledger_tx")?,
        LedgerBindingEvidence,
    )
    .bind_eager(
        transactional_effect_port_slot::<LedgerEffect>(),
        ledger_port,
    )?
    .finish()?;
ports.install(registration)?;
effectful_transform!(
    ReplayInput -> { ReplayOutput, ReplayEffectValue }
    uses transactional(LedgerEffect)
        via ledger_binding
        with resilience
    => ledger_handler,
);
```
