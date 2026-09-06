// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Provider-neutral publication of the latest observations for one run.
use super::{AppMetricsSnapshot, InfraMetricsSnapshot};

/// Synchronously replaces the corresponding local snapshot before returning.
///
/// Each publication preserves the other stream and works without readers.
/// Implementations perform only controlled in-memory work: no formatting, I/O,
/// delivery acknowledgement, or unbounded queues. Read views must release locks
/// before formatting or output. Backend lifecycle belongs to the host.
pub trait MetricsSnapshotSink: Send + Sync {
    fn publish_app_snapshot(&self, snapshot: AppMetricsSnapshot);
    fn publish_infra_snapshot(&self, snapshot: InfraMetricsSnapshot);
}
