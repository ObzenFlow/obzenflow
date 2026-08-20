// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptOutcome, SinkDeliveryPermit,
};

fn observe_twice(
    permit: Box<dyn SinkDeliveryPermit>,
    outcome: &SinkDeliveryAttemptOutcome,
) {
    let _ = permit.observe(outcome);
    let _ = permit.observe(outcome);
}

fn main() {}
