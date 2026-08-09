// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Input;
struct Output;
struct Seed;
struct Item;
struct Partial;
struct Effect;

fn main() {
    // The retired sink clause order remains the primary diagnostic even when
    // the handler is also an expression.
    let _ = obzenflow_dsl::sink!(
        Input => Handler::new(),
        observers: [],
        delivery: idempotent
    );

    // An invalid delivery value is more specific than the expression in the
    // handler slot, just as it was before the slot narrowed.
    let _ = obzenflow_dsl::sink!(
        name: "bad_delivery",
        Input => Handler::new(),
        delivery: retryable,
        observers: []
    );

    // Placeholder recognition also precedes the generic expression fallback;
    // the invalid delivery value remains the selected error.
    let _ = obzenflow_dsl::sink!(
        Input => placeholder!("still sketching"),
        delivery: retryable
    );

    // Malformed explicit-name, middleware, backpressure, and effects clauses
    // must fail while parsing that clause, never at the handler fallback.
    let _ = obzenflow_dsl::source!(
        name: source_name,
        Output => Handler::new()
    );
    let _ = obzenflow_dsl::source!(
        Output => Handler::new(),
        observers: []
    );
    let _ = obzenflow_dsl::transform!(
        Input -> Output => Handler::new(),
        backpressure:
    );
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> Output => Handler::new(),
        observers: [],
        effects: [Effect]
    );

    // The unsupported inference chunking clause remains primary too.
    let _ = obzenflow_dsl::inference!(
        Input -> {
            at_least_once(ChatCompletion) via chat with policy
        } Output => Handler::new(),
        chunking: by_budget { fixture }
    );

    // Effect-row acknowledgement errors also precede role-slot errors.
    let _ = obzenflow_dsl::inference!(
        Input -> {
            ChatCompletion via chat with policy
        } Output => Handler::new()
    );

    let _ = obzenflow_dsl::ai_map_reduce!(
        Seed -> Output => {
            map: [Item] -> {
                ChatCompletion via chat with policy
            } Partial => Handler::new(),
            reduce: (Seed, [Partial]) -> {
                at_least_once(ChatCompletion) via chat with policy
            } Output => finalise_role,
        },
        chunking: by_budget { fixture }
    );
}
