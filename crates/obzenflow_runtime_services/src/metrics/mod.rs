//! Metrics aggregator implementation

pub mod fsm;
pub mod supervisor;
pub mod config;
pub mod builder;
pub mod handle;

// Re-export commonly used types
// Note: MetricsAggregatorSupervisor is intentionally NOT exported - use MetricsAggregatorBuilder
pub use fsm::{
    MetricsAggregatorState, 
    MetricsAggregatorEvent, 
    MetricsAggregatorAction,
    MetricsAggregatorContext,
    MetricsStore,
    StageMetrics,
};
pub use config::DefaultMetricsConfig;
pub use builder::MetricsAggregatorBuilder;
pub use handle::{MetricsHandle, MetricsHandleExt};