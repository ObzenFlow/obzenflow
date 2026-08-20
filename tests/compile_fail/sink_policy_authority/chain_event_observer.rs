// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryPermit, SinkPolicyEvidenceBatch,
};

struct ForgingPermit;

impl SinkDeliveryPermit for ForgingPermit {
    fn observe(self: Box<Self>, _outcome: &ChainEvent) -> SinkPolicyEvidenceBatch {
        SinkPolicyEvidenceBatch::new()
    }
}

fn main() {}
