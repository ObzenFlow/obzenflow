// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::testing::sink::SinkRunEvidence;
use obzenflow::application::CurrentRunLocator;

fn main() {
    let _ = SinkRunEvidence {
        locator: CurrentRunLocator::new("forged-run".into()),
        eof_kinds: Vec::new(),
        operation_failures: Vec::new(),
        operation_failure_metrics: Vec::new(),
        failure_chains: Vec::new(),
        completed_sink_count: 0,
        failed_sink_count: 0,
    };
}
