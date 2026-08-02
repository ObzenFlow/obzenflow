// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Event;
struct Handler;

fn main() {
    let _ = obzenflow_dsl::async_source!(
        Event => (Handler, std::time::Duration::from_secs(1))
    );
    let _ = obzenflow_dsl::async_infinite_source!(
        name: "events",
        Event => (Handler, std::time::Duration::from_secs(1)),
        []
    );
    let _ = obzenflow_dsl::async_source!(
        name: "constructed",
        Event => (Handler::new(), std::time::Duration::from_secs(1)),
        [],
        backpressure: policy
    );
}
