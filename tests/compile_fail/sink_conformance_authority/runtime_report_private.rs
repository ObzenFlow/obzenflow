// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::testing::sink::SinkConformanceReport;

fn main() {
    let _ = SinkConformanceReport {
        protocol_version: 1,
        cases: Vec::new(),
    };
}
