use crate::middleware::{Middleware, MiddlewareFactory, MiddlewareContext, MiddlewareAction};
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::sync::RwLock;
use std::collections::HashMap;

/// Latency SLI (Service Level Indicator) middleware
/// 
/// Computes latency percentiles and indicators from event processing times.
/// 
/// Key metrics:
/// - P50, P90, P95, P99 latencies
/// - Percentage of requests meeting latency targets
/// - Latency budget consumption
pub struct LatencySLI {
    stage_name: String,
    
    // Latency targets (configurable)
    target_p50: Duration,
    target_p90: Duration,
    target_p95: Duration,
    target_p99: Duration,
    
    // Track event start times
    event_start_times: Arc<RwLock<HashMap<String, Instant>>>,
    
    // Histogram for percentile calculation
    latency_histogram: Arc<RwLock<LatencyHistogram>>,
}

/// Simple histogram for latency percentile calculation
struct LatencyHistogram {
    buckets: Vec<HistogramBucket>,
    total_count: u64,
}

struct HistogramBucket {
    upper_bound: Duration,
    count: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        // Create exponential buckets: 1ms, 2ms, 5ms, 10ms, 20ms, 50ms, 100ms, 200ms, 500ms, 1s, 2s, 5s
        let bucket_bounds = vec![1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000];
        let buckets = bucket_bounds.into_iter()
            .map(|ms| HistogramBucket {
                upper_bound: Duration::from_millis(ms),
                count: 0,
            })
            .collect();
        
        Self {
            buckets,
            total_count: 0,
        }
    }
    
    fn record(&mut self, latency: Duration) {
        self.total_count += 1;
        
        // Find the right bucket
        for bucket in &mut self.buckets {
            if latency <= bucket.upper_bound {
                bucket.count += 1;
                return;
            }
        }
        
        // If we get here, it's in the overflow bucket (last one)
        if let Some(last) = self.buckets.last_mut() {
            last.count += 1;
        }
    }
    
    fn percentile(&self, p: f64) -> Duration {
        if self.total_count == 0 {
            return Duration::from_millis(0);
        }
        
        let target_count = (self.total_count as f64 * p / 100.0_f64).ceil() as u64;
        let mut cumulative = 0u64;
        
        for bucket in &self.buckets {
            cumulative += bucket.count;
            if cumulative >= target_count {
                return bucket.upper_bound;
            }
        }
        
        // Return the last bucket bound if we somehow don't find it
        self.buckets.last()
            .map(|b| b.upper_bound)
            .unwrap_or(Duration::from_secs(10))
    }
    
    fn count_below(&self, threshold: Duration) -> u64 {
        let mut count = 0u64;
        
        for bucket in &self.buckets {
            if bucket.upper_bound <= threshold {
                count += bucket.count;
            } else {
                // Buckets are ordered, so we can break early
                break;
            }
        }
        
        count
    }
}

impl LatencySLI {
    pub fn new(stage_name: String) -> Self {
        Self::with_targets(
            stage_name,
            Duration::from_millis(50),   // P50 target
            Duration::from_millis(100),  // P90 target
            Duration::from_millis(200),  // P95 target
            Duration::from_millis(500),  // P99 target
        )
    }
    
    pub fn with_targets(
        stage_name: String,
        target_p50: Duration,
        target_p90: Duration,
        target_p95: Duration,
        target_p99: Duration,
    ) -> Self {
        Self {
            stage_name,
            target_p50,
            target_p90,
            target_p95,
            target_p99,
            event_start_times: Arc::new(RwLock::new(HashMap::new())),
            latency_histogram: Arc::new(RwLock::new(LatencyHistogram::new())),
        }
    }
    
    fn emit_slis(&self, ctx: &mut MiddlewareContext) {
        let histogram = self.latency_histogram.read().unwrap();
        
        if histogram.total_count == 0 {
            return; // No data yet
        }
        
        // Calculate percentiles
        let p50 = histogram.percentile(50.0_f64);
        let p90 = histogram.percentile(90.0_f64);
        let p95 = histogram.percentile(95.0_f64);
        let p99 = histogram.percentile(99.0_f64);
        
        // Emit percentile SLIs
        ctx.emit_event("sli", "latency_percentiles", json!({
            "indicator": format!("{}_latency", self.stage_name),
            "p50_ms": p50.as_millis(),
            "p90_ms": p90.as_millis(),
            "p95_ms": p95.as_millis(),
            "p99_ms": p99.as_millis(),
            "sample_count": histogram.total_count,
        }));
        
        // Calculate percentage meeting targets
        let meeting_p50 = histogram.count_below(self.target_p50) as f64 / histogram.total_count as f64;
        let meeting_p90 = histogram.count_below(self.target_p90) as f64 / histogram.total_count as f64;
        let meeting_p95 = histogram.count_below(self.target_p95) as f64 / histogram.total_count as f64;
        let meeting_p99 = histogram.count_below(self.target_p99) as f64 / histogram.total_count as f64;
        
        ctx.emit_event("sli", "latency_target_compliance", json!({
            "indicator": format!("{}_latency_compliance", self.stage_name),
            "p50_compliance": meeting_p50,
            "p90_compliance": meeting_p90,
            "p95_compliance": meeting_p95,
            "p99_compliance": meeting_p99,
            "targets": {
                "p50_ms": self.target_p50.as_millis(),
                "p90_ms": self.target_p90.as_millis(),
                "p95_ms": self.target_p95.as_millis(),
                "p99_ms": self.target_p99.as_millis(),
            },
        }));
        
        // Check for latency degradation
        if p99 > self.target_p99 {
            let degradation_factor = p99.as_millis() as f64 / self.target_p99.as_millis() as f64;
            ctx.emit_event("sli", "latency_degradation", json!({
                "indicator": format!("{}_latency_health", self.stage_name),
                "percentile": "p99",
                "current_ms": p99.as_millis(),
                "target_ms": self.target_p99.as_millis(),
                "degradation_factor": degradation_factor,
                "severity": if degradation_factor > 2.0_f64 { "critical" } else { "warning" },
            }));
        }
        
        // Emit budget consumption (how much of latency budget is used)
        let budget_consumption = if p95 > self.target_p95 {
            (p95.as_millis() as f64 / self.target_p95.as_millis() as f64) * 100.0_f64
        } else {
            (p95.as_millis() as f64 / self.target_p95.as_millis() as f64) * 100.0_f64
        };
        
        ctx.emit_event("sli", "latency_budget", json!({
            "indicator": format!("{}_latency_budget", self.stage_name),
            "consumed_percentage": budget_consumption,
            "remaining_percentage": (100.0_f64 - budget_consumption).max(0.0_f64),
        }));
    }
}

impl Middleware for LatencySLI {
    fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Record start time
        self.event_start_times.write().unwrap().insert(
            event.id.to_string(),
            Instant::now()
        );
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, _results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        let event_id = event.id.to_string();
        
        // Calculate latency
        let latency = {
            let mut start_times = self.event_start_times.write().unwrap();
            if let Some(start_time) = start_times.remove(&event_id) {
                Instant::now().duration_since(start_time)
            } else {
                // Fallback to processing time if available
                Duration::from_millis(0)
            }
        };
        
        if latency > Duration::from_millis(0) {
            // Record in histogram
            self.latency_histogram.write().unwrap().record(latency);
            
            // Emit per-request latency event for detailed tracking
            if latency > self.target_p95 {
                ctx.emit_event("sli", "slow_request", json!({
                    "indicator": format!("{}_latency", self.stage_name),
                    "latency_ms": latency.as_millis(),
                    "target_p95_ms": self.target_p95.as_millis(),
                    "event_id": event_id,
                }));
            }
        }
        
        // Emit aggregated SLIs
        self.emit_slis(ctx);
    }
}

/// Factory for creating LatencySLI middleware
pub struct LatencySLIFactory {
    target_p50: Duration,
    target_p90: Duration,
    target_p95: Duration,
    target_p99: Duration,
}

impl Default for LatencySLIFactory {
    fn default() -> Self {
        Self {
            target_p50: Duration::from_millis(50),
            target_p90: Duration::from_millis(100),
            target_p95: Duration::from_millis(200),
            target_p99: Duration::from_millis(500),
        }
    }
}

impl MiddlewareFactory for LatencySLIFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(LatencySLI::with_targets(
            config.name.clone(),
            self.target_p50,
            self.target_p90,
            self.target_p95,
            self.target_p99,
        ))
    }
    
    fn name(&self) -> &str {
        "LatencySLI"
    }
}

impl LatencySLI {
    /// Create a factory for this SLI middleware with default targets
    pub fn factory() -> Box<dyn MiddlewareFactory> {
        Box::new(LatencySLIFactory::default())
    }
    
    /// Create a factory with custom latency targets
    pub fn factory_with_targets(
        target_p50: Duration,
        target_p90: Duration,
        target_p95: Duration,
        target_p99: Duration,
    ) -> Box<dyn MiddlewareFactory> {
        Box::new(LatencySLIFactory {
            target_p50,
            target_p90,
            target_p95,
            target_p99,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{WriterId, EventId};
    use serde_json::json;
    
    #[test]
    fn test_histogram_percentiles() {
        let mut histogram = LatencyHistogram::new();
        
        // Add some latencies
        for _ in 0..50 {
            histogram.record(Duration::from_millis(10));
        }
        for _ in 0..40 {
            histogram.record(Duration::from_millis(50));
        }
        for _ in 0..10 {
            histogram.record(Duration::from_millis(200));
        }
        
        // P50 should be around 10ms
        assert!(histogram.percentile(50.0_f64) <= Duration::from_millis(20));
        
        // P90 should be around 50ms
        assert!(histogram.percentile(90.0_f64) <= Duration::from_millis(100));
        
        // P99 should be around 200ms
        assert!(histogram.percentile(99.0_f64) <= Duration::from_millis(500));
    }
    
    #[test]
    fn test_latency_tracking() {
        let sli = LatencySLI::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        let event = ChainEvent::new(
            EventId::new(),
            WriterId::default(),
            "test",
            json!({})
        );
        
        // Start tracking
        sli.pre_handle(&event, &mut ctx);
        
        // Simulate some processing time
        std::thread::sleep(Duration::from_millis(10));
        
        // Complete tracking
        sli.post_handle(&event, &[], &mut ctx);
        
        // Should have emitted SLI events
        assert!(ctx.events.iter().any(|e| 
            e.source == "sli" && e.event_type == "latency_percentiles"
        ));
    }
    
    #[test]
    fn test_compliance_calculation() {
        let histogram = LatencyHistogram {
            buckets: vec![
                HistogramBucket { upper_bound: Duration::from_millis(50), count: 80 },
                HistogramBucket { upper_bound: Duration::from_millis(100), count: 15 },
                HistogramBucket { upper_bound: Duration::from_millis(200), count: 5 },
            ],
            total_count: 100,
        };
        
        // 80% of requests are under 50ms
        assert_eq!(histogram.count_below(Duration::from_millis(50)), 80);
        
        // 95% of requests are under 100ms
        assert_eq!(histogram.count_below(Duration::from_millis(100)), 95);
        
        // 100% of requests are under 200ms
        assert_eq!(histogram.count_below(Duration::from_millis(200)), 100);
    }
}