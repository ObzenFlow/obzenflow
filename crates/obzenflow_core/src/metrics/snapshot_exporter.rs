// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! In-memory export of the latest observations for one run.
use super::{AppMetricsSnapshot, InfraMetricsSnapshot};

/// Publishes snapshots to a per-run in-memory read model.
///
/// Synchronously replaces the corresponding local snapshot before returning.
///
/// Each publication preserves the other stream and works without readers.
/// Implementations perform only controlled in-memory work: no formatting, I/O,
/// delivery acknowledgement, or unbounded queues. Read views must release locks
/// before formatting or output. Application resources and delivery belong to the host.
pub trait MetricsSnapshotExporter: Send + Sync {
    fn publish_app_snapshot(&self, snapshot: AppMetricsSnapshot);
    fn publish_infra_snapshot(&self, snapshot: InfraMetricsSnapshot);
}
