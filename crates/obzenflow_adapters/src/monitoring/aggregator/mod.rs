//! MetricsAggregator - Journal-based metrics collection
//!
//! This module implements FLOWIP-056-666: Wide Events Metrics.
//! The MetricsAggregator subscribes to the journal and derives all metrics
//! from the event stream, treating metrics as queries over events.

mod metrics_aggregator;

pub use metrics_aggregator::{EventMetricsAggregator, MetricsAggregatorFactory, StageKey};