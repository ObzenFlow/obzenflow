// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Input;
struct Output;
struct Reference;
struct Seed;
struct Item;
struct Partial;

struct Handler {
    marker: (),
}

// Keep each spelling as raw token trees. Forwarding an `expr` fragment would
// make it opaque and would not exercise the public token-level path matcher.
macro_rules! reject_in_every_slot {
    ($($bad:tt)+) => {
        let _ = obzenflow_dsl::source!(Output => $($bad)+);
        let _ = obzenflow_dsl::async_source!(Output => $($bad)+);
        let _ = obzenflow_dsl::infinite_source!(Output => $($bad)+);
        let _ = obzenflow_dsl::async_infinite_source!(Output => $($bad)+);
        let _ = obzenflow_dsl::transform!(Input -> Output => $($bad)+);
        let _ = obzenflow_dsl::effectful_transform!(
            Input -> Output => $($bad)+,
            effects: [],
            observers: []
        );
        let _ = obzenflow_dsl::stateful!(Input -> Output => $($bad)+);
        let _ = obzenflow_dsl::effectful_stateful!(
            Input -> Output => $($bad)+,
            effects: [],
            observers: []
        );
        let _ = obzenflow_dsl::join!(
            catalog reference: Reference,
            Input -> Output => $($bad)+
        );
        let _ = obzenflow_dsl::sink!(Input => $($bad)+);
        let _ = obzenflow_dsl::inference!(
            Input -> {
                at_least_once(ChatCompletion) via chat with policy
            } Output => $($bad)+
        );
        let _ = obzenflow_dsl::ai_map_reduce!(
            Seed -> Output => {
                map: [Item] -> {
                    at_least_once(ChatCompletion) via chat with policy
                } Partial => $($bad)+,
                reduce: (Seed, [Partial]) -> {
                    at_least_once(ChatCompletion) via chat with policy
                } Output => finalise_role,
            },
            chunking: by_budget { fixture }
        );
        let _ = obzenflow_dsl::ai_map_reduce!(
            Seed -> Output => {
                map: [Item] -> {
                    at_least_once(ChatCompletion) via chat with policy
                } Partial => map_role,
                reduce: (Seed, [Partial]) -> {
                    at_least_once(ChatCompletion) via chat with policy
                } Output => $($bad)+,
            },
            chunking: by_budget { fixture }
        );
    };
}

fn main() {
    reject_in_every_slot!(make());
    reject_in_every_slot!(Handler::new());
    reject_in_every_slot!(Handler::builder().build());
    reject_in_every_slot!(|| Handler { marker: () });
    reject_in_every_slot!(Handler { marker: () });
}
