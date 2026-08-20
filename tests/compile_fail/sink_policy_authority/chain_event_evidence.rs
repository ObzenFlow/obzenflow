// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::sink::journal_sink::SinkPolicyEvidenceBatch;

fn forge(batch: &mut SinkPolicyEvidenceBatch, event: ChainEvent) {
    let _ = batch.try_push(event);
}

fn main() {}
