// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::sinks::CsvSink;

#[path = "../support/typed_sink.rs"]
mod support;
use support::{Input, OtherInput};

fn main() {
    let output = CsvSink::<OtherInput>::new("unused.csv").unwrap();
    let _ = obzenflow_dsl::sink!(Input => output);
}
