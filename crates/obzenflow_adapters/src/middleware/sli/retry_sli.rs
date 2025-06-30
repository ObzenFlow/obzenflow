use crate::middleware::{Middleware, MiddlewareFactory, MiddlewareContext, MiddlewareAction};
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::control_plane::stages::supervisors::config::StageConfig;
use serde_json::json;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;
use std::collections::HashMap;

/// Retry SLI (Service Level Indicator) middleware
/// 
/// Computes success rate and retry efficiency indicators from retry events.
/// 
/// Key metrics:
/// - First attempt success rate
/// - Overall success rate (including retries)
/// - Retry efficiency (successful retries / total retries)
/// - Mean attempts to success
pub struct RetrySLI {
    stage_name: String,
    
    // Counters for SLI computation
    first_attempt_success: Arc<AtomicU64>,
    first_attempt_total: Arc<AtomicU64>,
    
    retry_success: Arc<AtomicU64>,
    retry_total: Arc<AtomicU64>,
    
    exhausted_count: Arc<AtomicU64>,
    
    // Track attempts per event
    event_attempts: Arc<RwLock<HashMap<String, u32>>>,
}

impl RetrySLI {
    pub fn new(stage_name: String) -> Self {
        Self {
            stage_name,
            first_attempt_success: Arc::new(AtomicU64::new(0)),
            first_attempt_total: Arc::new(AtomicU64::new(0)),
            retry_success: Arc::new(AtomicU64::new(0)),
            retry_total: Arc::new(AtomicU64::new(0)),
            exhausted_count: Arc::new(AtomicU64::new(0)),
            event_attempts: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    fn emit_slis(&self, ctx: &mut MiddlewareContext) {
        let first_total = self.first_attempt_total.load(Ordering::Relaxed);
        let first_success = self.first_attempt_success.load(Ordering::Relaxed);
        let retry_total = self.retry_total.load(Ordering::Relaxed);
        let retry_success = self.retry_success.load(Ordering::Relaxed);
        let exhausted = self.exhausted_count.load(Ordering::Relaxed);
        
        // First attempt success rate
        let first_attempt_rate = if first_total > 0 {
            first_success as f64 / first_total as f64
        } else {
            1.0_f64
        };
        
        ctx.emit_event("sli", "first_attempt_success_rate", json!({
            "indicator": format!("{}_first_attempt_success", self.stage_name),
            "value": first_attempt_rate,
            "success_count": first_success,
            "total_count": first_total,
        }));
        
        // Overall success rate (including retries)
        let total_attempts = first_total + retry_total;
        let total_success = first_success + retry_success;
        let overall_success_rate = if total_attempts > 0 {
            total_success as f64 / (first_total as f64) // Success per unique request
        } else {
            1.0_f64
        };
        
        ctx.emit_event("sli", "overall_success_rate", json!({
            "indicator": format!("{}_overall_success", self.stage_name),
            "value": overall_success_rate,
            "with_retries": total_success,
            "unique_requests": first_total,
        }));
        
        // Retry efficiency (how often retries succeed)
        let retry_efficiency = if retry_total > 0 {
            retry_success as f64 / retry_total as f64
        } else {
            1.0_f64 // No retries = perfect efficiency
        };
        
        ctx.emit_event("sli", "retry_efficiency", json!({
            "indicator": format!("{}_retry_efficiency", self.stage_name),
            "value": retry_efficiency,
            "successful_retries": retry_success,
            "total_retries": retry_total,
        }));
        
        // Mean attempts to success
        let successful_requests = first_success + retry_success;
        let mean_attempts = if successful_requests > 0 {
            total_attempts as f64 / successful_requests as f64
        } else {
            0.0_f64
        };
        
        ctx.emit_event("sli", "mean_attempts_to_success", json!({
            "indicator": format!("{}_mean_attempts", self.stage_name),
            "value": mean_attempts,
        }));
        
        // Exhaustion rate (requests that failed all retries)
        let exhaustion_rate = if first_total > 0 {
            exhausted as f64 / first_total as f64
        } else {
            0.0_f64
        };
        
        ctx.emit_event("sli", "retry_exhaustion_rate", json!({
            "indicator": format!("{}_exhaustion_rate", self.stage_name),
            "value": exhaustion_rate,
            "exhausted_count": exhausted,
            "total_requests": first_total,
        }));
    }
}

impl Middleware for RetrySLI {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Track retry attempts from context
        let mut has_retry_attempt = false;
        let mut exhaustion_alert = None;
        
        for mw_event in &ctx.events {
            match (mw_event.source.as_str(), mw_event.event_type.as_str()) {
                ("retry", "attempt_started") => {
                    has_retry_attempt = true;
                    if let Some(attempt) = mw_event.data["attempt"].as_u64() {
                        let event_id = event.id.to_string();
                        self.event_attempts.write().unwrap().insert(event_id, attempt as u32);
                        
                        if attempt == 1 {
                            // First attempt
                            self.first_attempt_total.fetch_add(1, Ordering::Relaxed);
                        } else {
                            // Retry attempt
                            self.retry_total.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                ("retry", "exhausted") => {
                    self.exhausted_count.fetch_add(1, Ordering::Relaxed);
                    
                    // Save alert for later emission
                    exhaustion_alert = Some(json!({
                        "indicator": format!("{}_retry_health", self.stage_name),
                        "event_id": event.id.to_string(),
                        "severity": "warning",
                    }));
                }
                _ => {}
            }
        }
        
        // Emit alert if needed
        if let Some(alert_data) = exhaustion_alert {
            ctx.emit_event("sli", "retry_exhaustion_alert", alert_data);
        }
        
        // If no retry event, this is a first attempt
        if !has_retry_attempt {
            self.first_attempt_total.fetch_add(1, Ordering::Relaxed);
            self.event_attempts.write().unwrap().insert(event.id.to_string(), 1);
        }
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        let event_id = event.id.to_string();
        let success = !results.is_empty();
        
        // Get attempt number for this event
        let attempt = {
            let attempts = self.event_attempts.read().unwrap();
            attempts.get(&event_id).copied().unwrap_or(1)
        };
        
        if success {
            if attempt == 1 {
                self.first_attempt_success.fetch_add(1, Ordering::Relaxed);
            } else {
                self.retry_success.fetch_add(1, Ordering::Relaxed);
            }
            
            // Clean up tracking for successful events
            self.event_attempts.write().unwrap().remove(&event_id);
            
            // Emit success after retry event if applicable
            if attempt > 1 {
                ctx.emit_event("sli", "retry_recovery", json!({
                    "indicator": format!("{}_retry_recovery", self.stage_name),
                    "attempts_needed": attempt,
                }));
            }
        }
        
        // Emit SLIs
        self.emit_slis(ctx);
        
        // Check for retry patterns that indicate issues
        let retry_total = self.retry_total.load(Ordering::Relaxed);
        let first_total = self.first_attempt_total.load(Ordering::Relaxed);
        
        if first_total > 100 && retry_total as f64 / first_total as f64 > 0.5_f64 {
            // More than 50% of requests need retries - concerning
            ctx.emit_event("sli", "high_retry_rate", json!({
                "indicator": format!("{}_retry_health", self.stage_name),
                "retry_percentage": (retry_total as f64 / first_total as f64) * 100.0_f64,
                "severity": "warning",
            }));
        }
    }
}

/// Factory for creating RetrySLI middleware
pub struct RetrySLIFactory;

impl MiddlewareFactory for RetrySLIFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(RetrySLI::new(config.stage_name.clone()))
    }
    
    fn name(&self) -> &str {
        "RetrySLI"
    }
}

impl RetrySLI {
    /// Create a factory for this SLI middleware
    pub fn factory() -> Box<dyn MiddlewareFactory> {
        Box::new(RetrySLIFactory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{WriterId, EventId};
    
    #[test]
    fn test_first_attempt_tracking() {
        let sli = RetrySLI::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        let event = ChainEvent::new(
            EventId::new(),
            WriterId::default(),
            "test",
            json!({})
        );
        
        // First attempt
        sli.pre_handle(&event, &mut ctx);
        assert_eq!(sli.first_attempt_total.load(Ordering::Relaxed), 1);
        assert_eq!(sli.retry_total.load(Ordering::Relaxed), 0);
        
        // Success
        sli.post_handle(&event, &[event.clone()], &mut ctx);
        assert_eq!(sli.first_attempt_success.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_retry_tracking() {
        let sli = RetrySLI::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        let event = ChainEvent::new(
            EventId::new(),
            WriterId::default(),
            "test",
            json!({})
        );
        
        // Add retry attempt event
        ctx.emit_event("retry", "attempt_started", json!({ "attempt": 2 }));
        
        sli.pre_handle(&event, &mut ctx);
        assert_eq!(sli.first_attempt_total.load(Ordering::Relaxed), 0);
        assert_eq!(sli.retry_total.load(Ordering::Relaxed), 1);
        
        // Retry success
        sli.post_handle(&event, &[event.clone()], &mut ctx);
        assert_eq!(sli.retry_success.load(Ordering::Relaxed), 1);
    }
    
    #[test]
    fn test_exhaustion_tracking() {
        let sli = RetrySLI::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        let event = ChainEvent::new(
            EventId::new(),
            WriterId::default(),
            "test",
            json!({})
        );
        
        // Add exhaustion event
        ctx.emit_event("retry", "exhausted", json!({ "attempts": 3 }));
        
        sli.pre_handle(&event, &mut ctx);
        assert_eq!(sli.exhausted_count.load(Ordering::Relaxed), 1);
    }
}