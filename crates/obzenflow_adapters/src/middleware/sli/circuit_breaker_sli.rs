use crate::middleware::{Middleware, MiddlewareFactory, MiddlewareContext, MiddlewareAction};
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, Duration};
use std::sync::RwLock;

/// Circuit Breaker SLI (Service Level Indicator) middleware
/// 
/// Computes availability indicators from circuit breaker raw events.
/// Availability = (successful requests) / (total requests)
/// 
/// This middleware subscribes to circuit breaker events and emits:
/// - Availability SLI (0.0 to 1.0)
/// - Error budget consumption rate
pub struct CircuitBreakerSLI {
    stage_name: String,
    
    // Tracking windows for SLI computation
    windows: Arc<RwLock<SlidingWindows>>,
    
    // Counters for current window
    current_success: Arc<AtomicU64>,
    current_total: Arc<AtomicU64>,
}

/// Sliding window tracking for accurate SLI computation
struct SlidingWindows {
    window_duration: Duration,
    windows: Vec<Window>,
    current_window_start: Instant,
}

struct Window {
    _start: Instant,
    success_count: u64,
    total_count: u64,
}

impl SlidingWindows {
    fn new(window_duration: Duration) -> Self {
        Self {
            window_duration,
            windows: Vec::new(),
            current_window_start: Instant::now(),
        }
    }
    
    fn rotate_if_needed(&mut self, now: Instant, current_success: u64, current_total: u64) {
        if now.duration_since(self.current_window_start) >= self.window_duration {
            // Save current window
            self.windows.push(Window {
                _start: self.current_window_start,
                success_count: current_success,
                total_count: current_total,
            });
            
            // Keep only last hour of windows (assuming 5min windows = 12 windows)
            if self.windows.len() > 12 {
                self.windows.remove(0);
            }
            
            self.current_window_start = now;
        }
    }
    
    fn calculate_availability(&self, current_success: u64, current_total: u64) -> f64 {
        let mut total_success = current_success;
        let mut total_requests = current_total;
        
        for window in &self.windows {
            total_success += window.success_count;
            total_requests += window.total_count;
        }
        
        if total_requests == 0 {
            1.0_f64 // No requests = 100% availability
        } else {
            total_success as f64 / total_requests as f64
        }
    }
}

impl CircuitBreakerSLI {
    pub fn new(stage_name: String) -> Self {
        Self {
            stage_name,
            windows: Arc::new(RwLock::new(SlidingWindows::new(Duration::from_secs(300)))), // 5 min windows
            current_success: Arc::new(AtomicU64::new(0)),
            current_total: Arc::new(AtomicU64::new(0)),
        }
    }
    
    fn emit_sli(&self, ctx: &mut MiddlewareContext, availability: f64) {
        ctx.emit_event("sli", "availability", json!({
            "indicator": format!("{}_availability", self.stage_name),
            "value": availability,
            "window": "5m",
        }));
        
        // Also emit error budget consumption
        // Assuming 99.9% SLO (0.1% error budget)
        let error_rate = 1.0 - availability;
        let error_budget_consumed = if error_rate > 0.001 {
            (error_rate / 0.001) * 100.0 // Percentage of budget consumed
        } else {
            0.0_f64
        };
        
        ctx.emit_event("sli", "error_budget", json!({
            "indicator": format!("{}_error_budget", self.stage_name),
            "consumed_percentage": error_budget_consumed,
            "error_rate": error_rate,
        }));
    }
}

impl Middleware for CircuitBreakerSLI {
    fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Count all requests
        self.current_total.fetch_add(1, Ordering::Relaxed);
        
        // Check if circuit breaker rejected this request
        let was_rejected = ctx.has_event("circuit_breaker", "rejected");
        
        if !was_rejected {
            // Not rejected = success (from CB perspective)
            self.current_success.fetch_add(1, Ordering::Relaxed);
        }
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        let now = Instant::now();
        
        // Get current counts
        let current_success = self.current_success.load(Ordering::Relaxed);
        let current_total = self.current_total.load(Ordering::Relaxed);
        
        // Rotate windows if needed
        {
            let mut windows = self.windows.write().unwrap();
            windows.rotate_if_needed(now, current_success, current_total);
        }
        
        // Calculate availability SLI
        let availability = {
            let windows = self.windows.read().unwrap();
            windows.calculate_availability(current_success, current_total)
        };
        
        // Always emit SLI events when we have data
        if current_total > 0 {
            self.emit_sli(ctx, availability);
        }
        
        // Check for significant CB events that should trigger immediate SLI updates
        let mut events_to_emit = Vec::new();
        for event in &ctx.events {
            match (event.source.as_str(), event.event_type.as_str()) {
                ("circuit_breaker", "opened") => {
                    // Circuit breaker opened - significant availability impact
                    events_to_emit.push(("availability_degraded", json!({
                        "indicator": format!("{}_availability", self.stage_name),
                        "reason": "circuit_breaker_opened",
                        "current_availability": availability,
                    })));
                }
                ("circuit_breaker", "closed") => {
                    // Circuit breaker closed - availability restored
                    events_to_emit.push(("availability_restored", json!({
                        "indicator": format!("{}_availability", self.stage_name),
                        "reason": "circuit_breaker_closed",
                        "current_availability": availability,
                    })));
                }
                _ => {}
            }
        }
        
        // Emit collected events
        for (event_type, data) in events_to_emit {
            ctx.emit_event("sli", event_type, data);
        }
    }
}

/// Factory for creating CircuitBreakerSLI middleware
pub struct CircuitBreakerSLIFactory;

impl MiddlewareFactory for CircuitBreakerSLIFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(CircuitBreakerSLI::new(config.name.clone()))
    }
    
    fn name(&self) -> &str {
        "CircuitBreakerSLI"
    }
}

impl CircuitBreakerSLI {
    /// Create a factory for this SLI middleware
    pub fn factory() -> Box<dyn MiddlewareFactory> {
        Box::new(CircuitBreakerSLIFactory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_availability_calculation() {
        let windows = SlidingWindows::new(Duration::from_secs(60));
        
        // No requests = 100% availability
        assert_eq!(windows.calculate_availability(0, 0), 1.0_f64);
        
        // All successful
        assert_eq!(windows.calculate_availability(100, 100), 1.0_f64);
        
        // 90% success rate
        assert_eq!(windows.calculate_availability(90, 100), 0.9_f64);
        
        // 99.9% success rate (meets typical SLO)
        assert_eq!(windows.calculate_availability(999, 1000), 0.999);
    }
    
    #[test]
    fn test_error_budget_calculation() {
        let sli = CircuitBreakerSLI::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        
        // 99.9% availability = 0% budget consumed
        sli.emit_sli(&mut ctx, 0.999);
        
        // Check emitted events
        assert!(ctx.events.iter().any(|e| 
            e.source == "sli" && e.event_type == "error_budget"
        ));
        
        // 99% availability = 1000% budget consumed (10x over budget)
        ctx.events.clear();
        sli.emit_sli(&mut ctx, 0.99);
        
        let budget_event = ctx.events.iter()
            .find(|e| e.source == "sli" && e.event_type == "error_budget")
            .unwrap();
        
        let consumed = budget_event.data["consumed_percentage"].as_f64().unwrap();
        assert!(consumed > 900.0); // Should be ~1000%
    }
}