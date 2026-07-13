// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod coordinator;
mod state;

use state::SinkRetryInvocation;

pub(super) async fn run(
    boundary: &super::PerSinkDeliveryPolicyBoundary,
    execute: &mut dyn obzenflow_runtime::stages::sink::journal_sink::SinkDeliveryExecutor,
    stop: obzenflow_runtime::stages::common::BoundaryStopReceiver,
) -> obzenflow_runtime::stages::sink::journal_sink::SinkDeliveryBoundaryReport {
    coordinator::run(boundary, execute, stop).await
}
