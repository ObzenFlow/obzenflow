// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::sinks::{ConsoleSink, JsonFormatter};
use obzenflow_runtime::stages::common::handlers::{Delivered, Delivery};
use obzenflow_runtime::stages::sink::{FallibleSinkTyped, SinkTyped, SinkTypedWithDelivery};
use obzenflow_runtime::typing::SinkTyping;

#[path = "../support/typed_sink.rs"]
mod support;
use support::Input;

fn main() {
    let _ = ConsoleSink::<Input, JsonFormatter>::json().include_all();
    let _ = SinkTyped::new(|_input: Input| async move {}).allow_skip();
}
