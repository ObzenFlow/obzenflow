# Application facade ledger

The Rust modules in this directory are the explicit symbol ledger: they contain
only modules and named re-exports. These exports retain their defining types and
implementations. Stage construction and external-resource acquisition still occur
inside the existing deferred materialiser.

| Facade path | Defining owner | Feature gate | Supporting types | Consumer proof |
| --- | --- | --- | --- | --- |
| `prelude` | Root capability re-exports | None | Exact common vocabulary listed in `prelude.rs`; no specialised catalogues | Renamed facade fixture |
| `schema` | Core event schema | None | Payload errors, flat fact carriers, member/subset contracts, event identity | Renamed derives; payment and allocation carriers |
| `dsl` | DSL | None | Flow build errors, backpressure clauses | Every example; renamed macro fixture covering every stage family |
| `stages::sources` | Adapters catalogue; Runtime typed contracts; Infra HTTP client composition | `http-pull` for the default HTTP client | Four source traits, source/decode errors, CSV rows, HTTP request/response/configuration/cursor types, concrete functional source return types | Character, CSV, bank, HN, multi-source examples |
| `stages::transforms` | Runtime | None | Pure/effectful handler traits, named mapping/filter/budget helpers | Character, payment, renamed fixture |
| `stages::stateful` | Runtime | None | Pure/effectful handler traits, emissions, accumulator states/snapshots and strategies | Bank, allocation, accumulator catalogue, renamed fixture |
| `stages::joins` | Runtime | None | Reference view/mode, concrete strategy builders | Bank, flight, CSV, product catalogue, renamed fixture |
| `stages::sinks` | Adapters catalogue; Runtime authoring contracts; Core delivery vocabulary | `postgres` for PostgreSQL | Formatters, CSV projection/writer, connector/writer/inline contracts, delivery contexts, settlement outcomes, errors, redelivery safety | Payment, PostgreSQL, HN, CSV, selected-sink fixture |
| `effects` | Runtime; Core binding and failure evidence | None | Effect sets, completion, contexts, named/portless bindings, port registration/resolution, safety/idempotency and failure types | Payment, allocation, renamed effectful handlers and AI fixture |
| `middleware` | Adapters policies/factories; Runtime observer contracts; Core read-only evidence | None | Checked builders/errors, observer contexts, stage type and vector clock | Payment, HN, bank; observer integration tests |
| `journal` | Infra backends/inspection; Core journal and recorded evidence | None | Factories, inspection errors, journal read handles and recorded delivery/effect/lifecycle vocabulary | Allocation replay; payment validation/retry journal tests |
| `application` | Infra application/verification; Runtime resolved configuration; Core configuration vocabulary | `web-host`, `prometheus`, `studio` enable corresponding capabilities | Runner/builder, presentation, run modes, configuration field enums and resolved value/provenance types | All examples; host boundary tests |
| `application::ingress` | Infra ingress composition; Core ingress DTOs | None for submission; `web-host` for serving HTTP | Handles, submission outcomes/errors, authentication, validation, ingress identity/refusal types | Bank; hosted-ingress tests |
| `ai` | Core requests/roles/budgets; Runtime inference; Adapters transforms; Infra provider composition | `ai` for provider bindings and Tiktoken | Model targets, requests/replies, roles, many-result carrier, budgeting/errors | One-shot, HN, renamed AI macro fixture |
| `env`, `error` | Infra environment parsing; Runtime/Core handler errors | None | Typed environment errors; fatal and classification vocabulary | Existing examples and error handling |

The DSL owns its documentation-hidden `__private` module, which re-exports only
the symbols exported macros require. Macros resolve it with `$crate`.
Core owns the narrow schema compiler-support module. Both carrier derives accept
`schema = <path>`, including renamed dependencies, while preserving direct-Core
defaults and `crate = <path>` overrides. Neither the derives nor inner crates
depend on the root facade.

The renamed fixture disables the extern prelude and explicitly imports only the
facade and third-party application dependencies. It catches caller-site crate
leaks within the existing test runner. It does **not** replace the FLOWIP-133i
packaged local-registry and post-publication consumer qualification gates.
