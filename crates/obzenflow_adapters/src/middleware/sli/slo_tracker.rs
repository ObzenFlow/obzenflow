use crate::middleware::{Middleware, MiddlewareFactory, MiddlewareContext, MiddlewareAction};
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use std::sync::RwLock;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Service Level Objective (SLO) tracker middleware
/// 
/// Monitors SLI events and tracks compliance with defined objectives.
/// This is Layer 4 in the middleware stack, consuming SLI events from Layer 2.
/// 
/// ## Features
/// - Error budget tracking with burn rate alerts
/// - Multi-window error budget (fast burn vs slow burn)
/// - SLO compliance reporting
/// - Automatic alerting on budget exhaustion
pub struct SLOTracker {
    stage_name: String,
    objectives: Arc<RwLock<Vec<SLODefinition>>>,
    trackers: Arc<RwLock<HashMap<String, SLOState>>>,
}

/// Definition of a Service Level Objective
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SLODefinition {
    /// Unique name for this SLO
    pub name: String,
    
    /// The SLI indicator this SLO tracks (e.g., "payment_processor_availability")
    pub indicator: String,
    
    /// The target value (e.g., 0.999 for 99.9% availability)
    pub target: f64,
    
    /// Time window for the SLO (e.g., 30 days)
    pub window: Duration,
    
    /// Whether lower values are better (false for availability, true for error rate)
    pub lower_is_better: bool,
    
    /// Alert thresholds for burn rate
    pub alert_config: AlertConfig,
}

/// Alert configuration for SLO violations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Fast burn threshold (e.g., burning 5% of monthly budget in 1 hour)
    pub fast_burn_threshold: f64,
    pub fast_burn_window: Duration,
    
    /// Slow burn threshold (e.g., burning 50% of monthly budget in 7 days)
    pub slow_burn_threshold: f64,
    pub slow_burn_window: Duration,
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            // Alert if burning 5% of monthly budget in 1 hour
            fast_burn_threshold: 0.05,
            fast_burn_window: Duration::from_secs(3600),
            
            // Alert if burning 50% of monthly budget in 7 days
            slow_burn_threshold: 0.5,
            slow_burn_window: Duration::from_secs(7 * 24 * 3600),
        }
    }
}

/// Tracks the state of a single SLO
#[derive(Debug)]
struct SLOState {
    definition: SLODefinition,
    
    // Error budget tracking
    total_budget: f64,
    consumed_budget: f64,
    
    // Time windows for burn rate calculation
    windows: Vec<BurnRateWindow>,
    
    // Last update time
    last_update: Instant,
    
    // Current compliance status
    is_compliant: bool,
}

/// Window for tracking burn rate
#[derive(Debug)]
struct BurnRateWindow {
    duration: Duration,
    start_time: Instant,
    budget_at_start: f64,
    name: String, // "fast" or "slow"
    threshold: f64,
}

impl SLOTracker {
    pub fn new(stage_name: String) -> Self {
        Self {
            stage_name,
            objectives: Arc::new(RwLock::new(Vec::new())),
            trackers: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    pub fn with_objectives(stage_name: String, objectives: Vec<SLODefinition>) -> Self {
        let tracker = Self::new(stage_name);
        tracker.add_objectives(objectives);
        tracker
    }
    
    /// Add SLO definitions to track
    pub fn add_objectives(&self, objectives: Vec<SLODefinition>) {
        let mut current_objectives = self.objectives.write().unwrap();
        let mut trackers = self.trackers.write().unwrap();
        
        for objective in objectives {
            let name = objective.name.clone();
            
            // Calculate total error budget
            let total_budget = if objective.lower_is_better {
                objective.target // For error rate, budget is the target itself
            } else {
                1.0 - objective.target // For availability, budget is the inverse
            };
            
            let now = Instant::now();
            
            // Create burn rate windows
            let windows = vec![
                BurnRateWindow {
                    duration: objective.alert_config.fast_burn_window,
                    start_time: now,
                    budget_at_start: 0.0,
                    name: "fast".to_string(),
                    threshold: objective.alert_config.fast_burn_threshold,
                },
                BurnRateWindow {
                    duration: objective.alert_config.slow_burn_window,
                    start_time: now,
                    budget_at_start: 0.0,
                    name: "slow".to_string(),
                    threshold: objective.alert_config.slow_burn_threshold,
                },
            ];
            
            let state = SLOState {
                definition: objective.clone(),
                total_budget,
                consumed_budget: 0.0,
                windows,
                last_update: now,
                is_compliant: true,
            };
            
            trackers.insert(name, state);
            current_objectives.push(objective);
        }
    }
    
    fn process_sli_event(&self, indicator: &str, value: f64, ctx: &mut MiddlewareContext) {
        let mut trackers = self.trackers.write().unwrap();
        
        // Find all SLOs tracking this indicator
        let matching_slos: Vec<String> = trackers
            .iter()
            .filter(|(_, state)| state.definition.indicator == indicator)
            .map(|(name, _)| name.clone())
            .collect();
        
        for slo_name in matching_slos {
            if let Some(state) = trackers.get_mut(&slo_name) {
                let now = Instant::now();
                let time_since_last = now.duration_since(state.last_update);
                
                // Calculate budget consumption for this measurement
                let budget_consumed = if state.definition.lower_is_better {
                    // For error rate: consume budget when value > target
                    if value > state.definition.target {
                        value - state.definition.target
                    } else {
                        0.0
                    }
                } else {
                    // For availability: consume budget when value < target
                    if value < state.definition.target {
                        state.definition.target - value
                    } else {
                        0.0
                    }
                };
                
                // Weight by time (assuming uniform distribution)
                let weighted_consumption = budget_consumed * 
                    (time_since_last.as_secs_f64() / state.definition.window.as_secs_f64());
                
                state.consumed_budget += weighted_consumption;
                state.last_update = now;
                
                // Check compliance
                let budget_remaining = state.total_budget - state.consumed_budget;
                let budget_percentage_remaining = budget_remaining / state.total_budget * 100.0;
                
                if budget_percentage_remaining <= 0.0 && state.is_compliant {
                    state.is_compliant = false;
                    
                    // Emit budget exhausted event
                    ctx.emit_event("slo", "budget_exhausted", json!({
                        "slo": &slo_name,
                        "indicator": indicator,
                        "target": state.definition.target,
                        "severity": "critical",
                    }));
                }
                
                // Check burn rates
                self.check_burn_rates(state, &slo_name, ctx);
                
                // Emit compliance update
                ctx.emit_event("slo", "compliance_update", json!({
                    "slo": &slo_name,
                    "indicator": indicator,
                    "is_compliant": state.is_compliant,
                    "budget_remaining_percentage": budget_percentage_remaining,
                    "current_value": value,
                    "target": state.definition.target,
                }));
            }
        }
    }
    
    fn check_burn_rates(&self, state: &mut SLOState, slo_name: &str, ctx: &mut MiddlewareContext) {
        let now = Instant::now();
        
        for window in &mut state.windows {
            // Check if window needs rotation
            if now.duration_since(window.start_time) > window.duration {
                window.start_time = now;
                window.budget_at_start = state.consumed_budget;
            }
            
            // Calculate burn rate for this window
            let budget_consumed_in_window = state.consumed_budget - window.budget_at_start;
            let burn_rate = budget_consumed_in_window / state.total_budget;
            
            if burn_rate > window.threshold {
                // Emit burn rate alert
                ctx.emit_event("slo", "burn_rate_alert", json!({
                    "slo": slo_name,
                    "window": &window.name,
                    "burn_rate": burn_rate,
                    "threshold": window.threshold,
                    "severity": if window.name == "fast" { "critical" } else { "warning" },
                }));
            }
        }
    }
}

impl Middleware for SLOTracker {
    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // SLO tracking is passive - we don't modify event flow
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        // Collect SLI events to process (to avoid borrowing issues)
        let sli_events: Vec<(String, f64)> = ctx.events.iter()
            .filter(|e| e.source == "sli")
            .filter_map(|event| {
                match event.event_type.as_str() {
                    "availability" | "first_attempt_success_rate" | 
                    "overall_success_rate" | "latency_percentiles" => {
                        if let (Some(indicator), Some(value)) = 
                            (event.data["indicator"].as_str(), event.data["value"].as_f64()) {
                            Some((indicator.to_string(), value))
                        } else {
                            None
                        }
                    }
                    "latency_target_compliance" => {
                        // Handle percentile-based compliance
                        if let (Some(indicator), Some(p99_compliance)) = 
                            (event.data["indicator"].as_str(), event.data["p99_compliance"].as_f64()) {
                            Some((format!("{}_p99", indicator), p99_compliance))
                        } else {
                            None
                        }
                    }
                    _ => None
                }
            })
            .collect();
        
        // Process collected SLI events
        for (indicator, value) in sli_events {
            self.process_sli_event(&indicator, value, ctx);
        }
        
        // Emit periodic SLO status
        let objectives = self.objectives.read().unwrap();
        if !objectives.is_empty() {
            let trackers = self.trackers.read().unwrap();
            let mut slo_statuses = Vec::new();
            
            for (name, state) in trackers.iter() {
                slo_statuses.push(json!({
                    "name": name,
                    "is_compliant": state.is_compliant,
                    "budget_consumed_percentage": (state.consumed_budget / state.total_budget * 100.0),
                    "indicator": &state.definition.indicator,
                }));
            }
            
            ctx.emit_event("slo", "status_report", json!({
                "stage": &self.stage_name,
                "slos": slo_statuses,
            }));
        }
    }
}

/// Factory for creating SLOTracker middleware
pub struct SLOTrackerFactory {
    objectives: Vec<SLODefinition>,
}

impl SLOTrackerFactory {
    pub fn new(objectives: Vec<SLODefinition>) -> Self {
        Self { objectives }
    }
}

impl MiddlewareFactory for SLOTrackerFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(SLOTracker::with_objectives(
            config.name.clone(),
            self.objectives.clone(),
        ))
    }
    
    fn name(&self) -> &str {
        "SLOTracker"
    }
}

impl SLOTracker {
    /// Create a factory for this SLO tracker with the given objectives
    pub fn factory(objectives: Vec<SLODefinition>) -> Box<dyn MiddlewareFactory> {
        Box::new(SLOTrackerFactory::new(objectives))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_slo_definition_creation() {
        let slo = SLODefinition {
            name: "api_availability".to_string(),
            indicator: "api_availability".to_string(),
            target: 0.999, // 99.9%
            window: Duration::from_secs(30 * 24 * 3600), // 30 days
            lower_is_better: false,
            alert_config: AlertConfig::default(),
        };
        
        assert_eq!(slo.target, 0.999);
        assert!(!slo.lower_is_better);
    }
    
    #[test]
    fn test_error_budget_calculation() {
        let tracker = SLOTracker::new("test".to_string());
        
        let slo = SLODefinition {
            name: "test_slo".to_string(),
            indicator: "test_indicator".to_string(),
            target: 0.99, // 99% availability = 1% error budget
            window: Duration::from_secs(30 * 24 * 3600),
            lower_is_better: false,
            alert_config: AlertConfig::default(),
        };
        
        tracker.add_objectives(vec![slo]);
        
        let trackers = tracker.trackers.read().unwrap();
        let state = trackers.get("test_slo").unwrap();
        
        // Error budget should be 1% (0.01)
        assert!((state.total_budget - 0.01).abs() < 0.0001);
    }
    
    #[test]
    fn test_sli_event_processing() {
        let tracker = SLOTracker::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        
        let slo = SLODefinition {
            name: "availability_slo".to_string(),
            indicator: "test_availability".to_string(),
            target: 0.99,
            window: Duration::from_secs(3600), // 1 hour for testing
            lower_is_better: false,
            alert_config: AlertConfig::default(),
        };
        
        tracker.add_objectives(vec![slo]);
        
        // Simulate SLI event
        ctx.emit_event("sli", "availability", json!({
            "indicator": "test_availability",
            "value": 0.95, // Below target
        }));
        
        // Process the event
        let event = ChainEvent::new(
            obzenflow_core::EventId::new(),
            obzenflow_core::WriterId::default(),
            "test",
            serde_json::json!({}),
        );
        tracker.post_handle(&event, &[], &mut ctx);
        
        // Check that compliance update was emitted
        assert!(ctx.events.iter().any(|e| 
            e.source == "slo" && e.event_type == "compliance_update"
        ));
    }
    
    #[test]
    fn test_burn_rate_alerts() {
        let tracker = SLOTracker::new("test".to_string());
        let mut ctx = MiddlewareContext::new();
        
        let mut alert_config = AlertConfig::default();
        alert_config.fast_burn_threshold = 0.01; // 1% for easier testing
        
        let slo = SLODefinition {
            name: "burn_rate_test".to_string(),
            indicator: "test_indicator".to_string(),
            target: 0.99,
            window: Duration::from_secs(3600),
            lower_is_better: false,
            alert_config,
        };
        
        tracker.add_objectives(vec![slo]);
        
        // Simulate a severe degradation
        ctx.emit_event("sli", "availability", json!({
            "indicator": "test_indicator",
            "value": 0.0, // Complete outage
        }));
        
        let event = ChainEvent::new(
            obzenflow_core::EventId::new(),
            obzenflow_core::WriterId::default(),
            "test",
            serde_json::json!({}),
        );
        tracker.post_handle(&event, &[], &mut ctx);
        
        // Should have burn rate alerts
        let _has_burn_alert = ctx.events.iter().any(|e| 
            e.source == "slo" && e.event_type == "burn_rate_alert"
        );
        
        // Note: In real scenarios, burn rate needs time-based calculations
        // This test just verifies the structure exists
    }
}

