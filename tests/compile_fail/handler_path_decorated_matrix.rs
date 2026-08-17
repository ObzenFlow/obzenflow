// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Input;
struct Output;
struct Alternate;
struct Reference;
struct Effect;
struct Handler;

fn main() {
    // Each call uses the family's maximally decorated live form. These cases
    // pin the teaching fallback after contract, explicit-name, attachment,
    // backpressure, effects, emit-interval, delivery, and catalog parsing.
    let _ = obzenflow_dsl::source!(
        name: "source",
        { Output, Alternate } => Handler::new(),
        [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::async_source!(
        name: "async_source",
        { Output, Alternate } => Handler::new(),
        [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::infinite_source!(
        name: "infinite_source",
        { Output, Alternate } => Handler::new(),
        [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::async_infinite_source!(
        name: "async_infinite_source",
        { Output, Alternate } => Handler::new(),
        [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::transform!(
        name: "transform",
        Input -> { Output, Alternate } => Handler::new(),
        observers: [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::effectful_transform!(
        name: "effectful_transform",
        Input -> { Output, Alternate } uses Effect => Handler::new(),
        observers: [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::stateful!(
        name: "stateful",
        Input -> { Output, Alternate } => Handler::new(),
        emit_interval = interval,
        observers: [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::effectful_stateful!(
        name: "effectful_stateful",
        Input -> { Output, Alternate } uses Effect => Handler::new(),
        observers: [middleware],
        backpressure: backpressure
    );
    let _ = obzenflow_dsl::join!(
        name: "join",
        catalog reference: Reference,
        Input -> { Output, Alternate } => Handler::new(),
        observers: [middleware]
    );
    let _ = obzenflow_dsl::sink!(
        name: "sink",
        Input => Handler::new(),
        delivery: idempotent,
        observers: [middleware]
    );
    let _ = obzenflow_dsl::sink!(Input => sinks::json());
}
