//! Metrics aggregator implementation

pub mod supervisor;
pub mod fsm;
pub mod states;
pub mod default_config;

// Re-export commonly used types
pub use supervisor::MetricsAggregatorSupervisor;
pub use states::{
    MetricsAggregatorState, 
    MetricsAggregatorEvent, 
    MetricsAggregatorAction,
    MetricsAggregatorContext,
    MetricsStore,
    StageMetrics,
};
pub use fsm::{MetricsAggregatorFsm, build_metrics_aggregator_fsm};
pub use default_config::DefaultMetricsConfig;