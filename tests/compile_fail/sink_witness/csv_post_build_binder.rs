// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::sinks::CsvSink;

#[path = "../support/typed_sink.rs"]
mod support;
use support::Input;

fn main() {
    let output = CsvSink::<Input>::new("unused.csv").unwrap();
    let _ = output.typed::<Input>();
}
