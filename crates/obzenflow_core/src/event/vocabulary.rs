// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Canonical runtime event spelling. Existing payload enums select occurrences;
//! these constants confer no routing, publication or supervision authority.
//!
//! Literal fragments, never Rust identifier names, define the journal contract.
//! Fixed names are static; named supervisors use the shared escaping constructor.

macro_rules! event_names {
    (@names $prefix:expr; $($name:ident => $suffix:literal),+ $(,)?) => {
        $(pub const $name: &str = concat!($prefix, ".", $suffix);)+
    };
    ($prefix_name:ident => $prefix:literal; $($family:ident => $subject:literal { $($name:ident => $suffix:literal),+ $(,)? }),+ $(,)?) => {
        pub const $prefix_name: &str = $prefix;
        $(pub mod $family {
            event_names!(@names concat!($prefix, $subject); $($name => $suffix),+);
        })+
    };
}

event_names! {
    RUNTIME_PREFIX => "runtime.";
    stream => "stream" {
        EOF_DECLARED => "eof_declared",
        WATERMARK_DECLARED => "watermark_declared",
        CATCH_UP_COMPLETED => "catch_up_completed",
        CHECKPOINT_DECLARED => "checkpoint_declared",
        DRAIN_REQUESTED => "drain_requested",
    },
    pipeline => "pipeline" {
        ABORT_REQUESTED => "abort_requested",
    },
    source => "source" {
        PRODUCTION_DECLARED => "production_declared",
        PRODUCTION_FINALIZED => "production_finalized",
    },
    subscription => "subscription" {
        PROGRESS_REPORTED => "progress_reported",
        GAP_DETECTED => "gap_detected",
        CONSUMPTION_FINALIZED => "consumption_finalized",
        STALL_DETECTED => "stall_detected",
        AT_LEAST_ONCE_VIOLATED => "at_least_once_violated",
    },
    contract => "contract" {
        POLICY_ACCEPTED => "policy_accepted",
        POLICY_REJECTED => "policy_rejected",
        VERIFICATION_PASSED => "verification_passed",
        VERIFICATION_FAILED => "verification_failed",
        VERIFICATION_PENDING => "verification_pending",
        VERIFICATION_SKIPPED => "verification_skipped",
    },
    circuit_breaker => "circuit_breaker" {
        OPENED => "opened",
        CLOSED => "closed",
        HALF_OPEN_ENTERED => "half_open_entered",
        ADMISSION_REJECTED => "admission_rejected",
        ATTEMPT_ASSESSED => "attempt_assessed",
    },
    retry => "retry" {
        SCHEDULED => "scheduled",
        SUCCEEDED => "succeeded",
        EXHAUSTED => "exhausted",
        STOPPED_NON_RETRYABLE => "stopped_non_retryable",
    },
    resilience => "resilience" {
        EVALUATION_FINISHED => "evaluation_finished",
    },
    rate_limiter => "rate_limiter" {
        WAIT_STARTED => "wait_started",
        MODE_CHANGED => "mode_changed",
        CONFIGURATION_CHANGED => "configuration_changed",
    },
    backpressure => "backpressure" {
        STALL_DETECTED => "stall_detected",
    },
}

/// Supervisor grammar, canonical built-in names and shared occurrence suffixes.
/// The supervisor descriptor supplies the instance's name and supervision mode.
pub mod supervisor {
    pub(crate) const ROOT: &str = "supervisor";
    pub(crate) const RUNTIME: &str = "runtime";
    pub(crate) const STAGE: &str = "stage";

    pub const PIPELINE_NAME: &str = "pipeline_supervisor";
    pub const METRICS_NAME: &str = "metrics_aggregator";

    pub(crate) const REGISTERED: &str = "registered";

    pub(crate) mod milestone {
        event_names!(@names "milestone";
            READY => "ready",
            READY_FOR_RUN => "ready_for_run",
            SOURCES_STARTED => "sources_started",
            DRAIN_STARTED => "drain_started",
            DRAIN_COMPLETED => "drain_completed",
            ALL_STAGES_COMPLETED => "all_stages_completed",
            FINAL_MARKER_PUBLISHED => "final_marker_published",
            REFRESH_READERS_STOPPED => "refresh_readers_stopped",
        );
    }

    pub(crate) mod command {
        event_names!(@names "command";
            START_ADMITTED => "start.admitted",
            GRACEFUL_STOP_ADMITTED => "graceful_stop.admitted",
            CANCEL_ADMITTED => "cancel.admitted",
            FINALIZE_METRICS_REQUESTED => "finalize_metrics.requested",
        );
    }

    pub(crate) mod outcome {
        event_names!(@names "outcome";
            COMPLETED => "completed",
            FAILED => "failed",
            CANCELLED => "cancelled",
            NOT_STARTED => "not_started",
        );
    }

    pub(crate) mod finalization {
        event_names!(@names "finalization"; COMPLETED => "completed");
    }

    pub(crate) mod snapshot {
        event_names!(@names "snapshot"; PUBLISHED => "published");
    }
}
