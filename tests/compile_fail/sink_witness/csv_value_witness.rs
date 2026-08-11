// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::sinks::CsvSink;

fn main() {
    let output = CsvSink::<serde_json::Value>::new("unused.csv").unwrap(); // allow-serde-value: FLOWIP-134h negative fixture
    let _ = obzenflow_dsl::sink!(serde_json::Value => output);
}
