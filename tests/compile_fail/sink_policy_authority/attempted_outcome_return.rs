// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptOutcome, SinkDeliveryPermit,
};

struct ReplacingPermit;

impl SinkDeliveryPermit for ReplacingPermit {
    fn observe(
        self: Box<Self>,
        _outcome: &SinkDeliveryAttemptOutcome,
    ) -> SinkDeliveryAttemptOutcome {
        SinkDeliveryAttemptOutcome::Panicked {
            message: "replacement".into(),
        }
    }
}

fn main() {}
