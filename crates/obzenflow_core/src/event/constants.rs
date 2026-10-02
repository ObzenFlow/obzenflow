// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event type constants
//!
//! String constants for event types used throughout the system.
//! These are kept as constants to ensure consistency and make refactoring easier.

/// Control event types (flow through stage journals)
pub mod control {
    pub const EOF: &str = "runtime.stream.end_declared";
    pub const WATERMARK: &str = "runtime.stream.watermark_declared";
    pub const CHECKPOINT: &str = "runtime.stream.checkpoint_declared";
    pub const DRAIN: &str = "runtime.stream.drain_requested";

    pub const SOURCE_PRODUCTION_DECLARED: &str = "runtime.source.production_declared";
    pub const SOURCE_PRODUCTION_FINALIZED: &str = "runtime.source.production_finalized";
    pub const SUBSCRIPTION_PROGRESS: &str = "runtime.subscription.progress_reported";
    pub const SUBSCRIPTION_FINALIZED: &str = "runtime.subscription.consumption_finalized";
}

/// Runtime supervisor occurrences. Stage supervisor names are derived through
/// `supervisor_descriptor::supervisor_event_type` from their canonical names.
pub mod system {
    pub mod pipeline {
        pub const ALL_STAGES_COMPLETED: &str =
            "supervisor.runtime.pipeline_supervisor.milestone.all_stages_completed";
        pub const FINAL_MARKER_PUBLISHED: &str =
            "supervisor.runtime.pipeline_supervisor.milestone.final_marker_published";
        pub const COMPLETED: &str = "supervisor.runtime.pipeline_supervisor.outcome.completed";
        pub const FINALIZE_METRICS_REQUESTED: &str =
            "supervisor.runtime.pipeline_supervisor.command.finalize_metrics.requested";
    }

    pub mod metrics {
        pub const READY: &str = "supervisor.runtime.metrics_aggregator.milestone.ready";
        pub const FINALIZATION_COMPLETED: &str =
            "supervisor.runtime.metrics_aggregator.finalization.completed";
        pub const REFRESH_READERS_STOPPED: &str =
            "supervisor.runtime.metrics_aggregator.milestone.refresh_readers_stopped";
        pub const SNAPSHOT_PUBLISHED: &str =
            "supervisor.runtime.metrics_aggregator.snapshot.published";
    }
}
