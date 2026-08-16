# Type-and-effect stage grammar

Effectful stages declare their input type, effect row, and output type in one
signature:

```rust,ignore
Input -> { EffectA, at_least_once(EffectB) } Output => handler
```

Pure stages omit the effect row:

```rust,ignore
Input -> Output => handler
```

An empty row is invalid. Multi-fact outputs retain their existing braces, on
the output side of the effect row:

```rust,ignore
Input -> { EffectA } { OutputA, OutputB } => handler
```

## Binding and policy operands

A portless effect is named directly. A named effect must select a lexical typed
binding with `via`:

```rust,ignore
Input -> {
    LocalEffect,
    at_least_once(ChatCompletion) via chat
} Output => handler
```

`via chat` receives an `EffectBinding<ChatCompletion>`, not a string or a lookup
key. This remains mandatory for a named effect whose declared slot set is empty:
the evidence and construction family are still part of its authority.

An effect may have one existing boundary policy, introduced by bare `with`:

```rust,ignore
Input -> {
    at_least_once(ChatCompletion) via chat with ai_resilience()
} Output => handler
```

Policy lists and blocks are not part of this grammar. Transactional execution
uses the same named-binding path; its executor is a reserved typed slot:

```rust,ignore
Input -> { transactional(LedgerWrite) via ledger } Output => handler
```

The same row syntax applies to `effectful_transform!` and
`effectful_stateful!`. Stateful policies remain unavailable unless enabled by
their own FLOWIP.

## Constructing named authority

Provider facades, applications, and test fixtures all use the public generic
builder. It validates the effect's exact typed slot set and returns the lexical
binding together with one opaque, consuming registration:

```rust,ignore
let (chat, chat_registration) =
    EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat")?,
        chat_evidence,
    )
    .bind_deferred(CHAT_CLIENT, chat_resolver)?
    .finish()?;

let mut effect_ports = EffectPortRegistry::new();
effect_ports.install(chat_registration)?;
```

Use `bind_eager` for an already constructed application-local implementation.
Use `bind_deferred` only for bounded, non-suspending local construction. A
resolver returns a typed `Arc<P>` directly; it is not an async or remote
discovery hook.

Handlers retain `binding.invocation()` through their effect values. They do not
receive the registration, resolver, registry, or an ambient port lookup. Live
misses receive only their declaration's resolved typed slots through
`EffectContext::port(slot)`. Strict replay hits require no live registration and
never invoke a resolver.

If an implementation detects a target invariant failure, construct it from the
same declared slot token with `EffectError::target_invariant_violation(slot)`.
Raw targets, resolver causes, binding evidence, and credentials are not accepted
by the framework diagnostic surface.

Effect-row declarations and singleton policy attachments canonicalize by stable
effect identity, so source-order permutations materialize the same membership.

## Migration from the pre-132a surface

Move every non-empty ordinary declaration into the arrow:

```rust,ignore
// Before
effectful_transform!(Input -> Output => handler, effects: [EffectA]);

// After
effectful_transform!(Input -> { EffectA } Output => handler);
```

Replace string-selected or detached live ports with one
`EffectBinding<E>`/`EffectRegistration<E>` pair, install the registration at the
composition root, pass clones of the binding into declarations and handlers,
and use `via binding` in the row. Replace
`transactional(Effect, "executor")` with `transactional(Effect) via binding`.

The old raw registry insertion, deferred lookup, and AI-specific binding
contract APIs are removed rather than deprecated. Current run manifests carry
`effect_binding_descriptor: 1`; archives without that capability are rejected
before effect records are decoded. Rebuild test archives with the current
schema instead of expecting pre-132a descriptor compatibility.

## Concrete 132a migrations

### Payment gateway

Payment moves the declaration and its singleton policy from the detached lane
onto the arrow:

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
    AuthorizePayment with gateway_resilience
} {
    PaymentAuthorized,
    PaymentDeclined,
    OrderCancelled,
    PaymentAuthorizationUnavailable,
} => gateway
```

### One-shot inference

The one-shot example keeps its `via chat` effect row and migrates the binding
facade's result into the generic, consuming installation gateway:

```rust,ignore
// Before
let (chat, chat_registration) =
    ChatEffectBinding::from_config(&ai_models)?.into_parts();
let effect_ports =
    chat_registration.install_into(EffectPortRegistry::new())?;

// After
let (chat, chat_registration) =
    ChatEffectBinding::from_config(&ai_models)?.into_parts()?;
let mut effect_ports = EffectPortRegistry::new();
effect_ports.install(chat_registration)?;

ReducedEvidence -> {
    at_least_once(ChatCompletion) via chat with ai_resilience()
} DecisionBrief => brief_role
```

### Hacker News digest

HN previously selected its test resolver through the raw port registry. It now
rebuilds the same typed authority through the public builder and installs the
result once at the composition root:

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
let (chat, chat_registration) =
    ChatEffectBinding::from_config(&ai_models)?.into_parts()?;
let (chat, chat_registration) = if let Some(resolver) = chat_resolver_override {
    EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat")?,
        chat.evidence().clone(),
    )
    .bind_deferred(CHAT_CLIENT, resolver)?
    .finish()?
} else {
    (chat, chat_registration)
};
let chat_target = chat.evidence().target().clone();
let mut effect_ports = EffectPortRegistry::new();
effect_ports.install(chat_registration)?;
```

### Standalone chat

Standalone chat replaces the raw name-plus-type resolver and detached effect
list with a typed registration and canonical row declaration:

```rust,ignore
// Before
let effect_ports = EffectPortRegistry::new()
    .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, chat_resolver)?;
effectful_transform!(
    TicketRaised -> TicketSummarised => chat_handler,
    effects: [at_least_once(ChatCompletion) with ai_resilience()],
);

// After
let (chat, chat_registration) =
    EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat")?,
        chat_evidence,
    )
    .bind_deferred(CHAT_CLIENT, chat_resolver)?
    .finish()?;
effect_ports.install(chat_registration)?;
effectful_transform!(
    TicketRaised -> {
        at_least_once(ChatCompletion) via chat with ai_resilience()
    } TicketSummarised => chat_handler,
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
let (embedding, embedding_registration) =
    EffectRegistrationBuilder::<EmbeddingGeneration>::new(
        LogicalEffectBindingName::new("embedding")?,
        embedding_evidence,
    )
    .bind_deferred(EMBEDDING_CLIENT, embedding_resolver)?
    .finish()?;
effect_ports.install(embedding_registration)?;
effectful_transform!(
    TicketSummarised -> {
        at_least_once(EmbeddingGeneration) via embedding with ai_resilience()
    } TicketEmbedded => embedding_handler,
);
```

### Transactional ledger

Transactional registration moves from a raw string-keyed executor to its
effect's typed transactional slot, while the row carries the binding value:

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
    ReplayInput -> {
        transactional(LedgerEffect) via ledger_binding with resilience
    } { ReplayOutput, ReplayEffectValue } => ledger_handler,
);
```
