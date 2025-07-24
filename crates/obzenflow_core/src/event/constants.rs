//! Event type constants
//!
//! String constants for event types used throughout the system.
//! These are kept as constants to ensure consistency and make refactoring easier.

/// Control event types (flow through stage journals)
pub mod control {
    pub const EOF: &str = "control.eof";
    pub const WATERMARK: &str = "control.watermark";
    pub const CHECKPOINT: &str = "control.checkpoint";
    pub const DRAIN: &str = "control.drain";
    
    pub mod middleware {
        pub const STATE: &str = "control.middleware.state";
        pub const SUMMARY: &str = "control.middleware.summary";
        pub const ANOMALY: &str = "control.middleware.anomaly";
    }
    
    pub mod metrics {
        pub const STATE: &str = "control.metrics.state";
        pub const RESOURCE: &str = "control.metrics.resource";
        pub const CUSTOM: &str = "control.metrics.custom";
        pub const ANOMALY: &str = "control.metrics.anomaly";
    }
}

/// System event types (go to control journal)
pub mod system {
    pub mod stage {
        pub const RUNNING: &str = "system.stage.running";
        pub const DRAINING: &str = "system.stage.draining";
        pub const DRAINED: &str = "system.stage.drained";
        pub const COMPLETED: &str = "system.stage.completed";
        pub const FAILED: &str = "system.stage.failed";
    }
    
    pub mod pipeline {
        pub const ALL_STAGES_COMPLETED: &str = "system.pipeline.all_stages_completed";
        pub const DRAIN: &str = "system.pipeline.drain";
        pub const COMPLETED: &str = "system.pipeline.completed";
    }
    
    pub mod metrics {
        pub const READY: &str = "system.metrics.ready";
        pub const DRAIN: &str = "system.metrics.drain";
        pub const DRAINED: &str = "system.metrics.drained";
    }
}