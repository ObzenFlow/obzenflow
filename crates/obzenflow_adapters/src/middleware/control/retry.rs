//! Retry middleware with configurable strategies and jitter
//!
//! This middleware implements retry logic with exponential backoff and jitter
//! to prevent thundering herds. It emits raw events that can be consumed by
//! monitoring and SLI middleware.

use crate::middleware::{Middleware, MiddlewareAction, ErrorAction, MiddlewareContext};
use obzenflow_core::{
    event::chain_event::ChainEvent,
    EventId,
};
use serde_json::json;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Retry strategies with configurable backoff
#[derive(Debug, Clone)]
pub enum RetryStrategy {
    /// Fixed delay between attempts
    Fixed {
        delay: Duration,
    },
    
    /// Exponential backoff with optional jitter
    Exponential {
        initial: Duration,
        max: Duration,
        factor: f64,
        jitter: bool,
    },
}

impl RetryStrategy {
    /// Calculate the delay for a given attempt number (0-indexed)
    fn calculate_delay(&self, attempt: u32) -> Duration {
        match self {
            RetryStrategy::Fixed { delay } => *delay,
            
            RetryStrategy::Exponential { initial, max, factor, jitter } => {
                // Calculate base delay with exponential backoff
                let base_ms = initial.as_millis() as f64 * factor.powi(attempt as i32);
                let capped_ms = base_ms.min(max.as_millis() as f64);
                
                // Apply jitter if enabled (critical for thundering herd prevention!)
                let final_ms = if *jitter {
                    use rand::Rng;
                    let mut rng = rand::rng();
                    // Jitter between 0.5x and 1.5x the base delay
                    capped_ms * rng.random_range(0.5..1.5)
                } else {
                    capped_ms
                };
                
                Duration::from_millis(final_ms as u64)
            }
        }
    }
}

/// Tracks retry state for individual events
#[derive(Debug, Clone)]
struct RetryState {
    attempts: u32,
    last_error: Option<String>,
}

/// Retry middleware that handles transient failures
pub struct RetryMiddleware {
    max_attempts: u32,
    strategy: RetryStrategy,
    /// Per-event retry state
    event_states: Arc<Mutex<HashMap<EventId, RetryState>>>,
}

impl RetryMiddleware {
    /// Create retry middleware with fixed delay
    pub fn fixed(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            strategy: RetryStrategy::Fixed { delay },
            event_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create retry middleware with exponential backoff
    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            strategy: RetryStrategy::Exponential {
                initial: Duration::from_millis(100),
                max: Duration::from_secs(10),
                factor: 2.0,
                jitter: true, // Default to jitter enabled
            },
            event_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Create retry middleware with custom exponential backoff settings
    pub fn exponential_custom(
        max_attempts: u32,
        initial: Duration,
        max: Duration,
        factor: f64,
        jitter: bool,
    ) -> Self {
        Self {
            max_attempts,
            strategy: RetryStrategy::Exponential { initial, max, factor, jitter },
            event_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    /// Clean up old event states to prevent memory leaks
    fn cleanup_old_states(&self) {
        // In a real implementation, we'd periodically clean up states for events
        // that haven't been seen in a while. For now, we'll keep it simple.
    }
}

impl Middleware for RetryMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        let mut states = self.event_states.lock().unwrap();
        let state = states.entry(event.id.clone()).or_insert(RetryState {
            attempts: 0,
            last_error: None,
        });
        
        if state.attempts >= self.max_attempts {
            // Emit exhausted event
            ctx.emit_event("retry", "exhausted", json!({
                "event_id": event.id.as_str(),
                "attempts": state.attempts,
                "max_attempts": self.max_attempts,
                "last_error": state.last_error.as_ref().unwrap_or(&"Unknown".to_string()),
                "strategy": match &self.strategy {
                    RetryStrategy::Fixed { .. } => "fixed",
                    RetryStrategy::Exponential { .. } => "exponential",
                }
            }));
            
            // Remove the state since we're done with this event
            states.remove(&event.id);
            
            return MiddlewareAction::Abort;
        }
        
        if state.attempts > 0 {
            // This is a retry attempt
            let delay = self.strategy.calculate_delay(state.attempts - 1);
            
            // Emit retry attempt event
            ctx.emit_event("retry", "attempt_started", json!({
                "event_id": event.id.as_str(),
                "attempt": state.attempts + 1,
                "max_attempts": self.max_attempts,
                "delay_ms": delay.as_millis(),
                "strategy": match &self.strategy {
                    RetryStrategy::Fixed { .. } => "fixed",
                    RetryStrategy::Exponential { jitter, .. } => {
                        if *jitter {
                            "exponential_with_jitter"
                        } else {
                            "exponential"
                        }
                    }
                }
            }));
            
            // In a real async implementation, we would sleep here
            // For now, we just log the delay
            tracing::debug!(
                "Retry middleware would delay for {:?} before attempt {} for event {}",
                delay,
                state.attempts + 1,
                event.id.as_str()
            );
        }
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        let mut states = self.event_states.lock().unwrap();
        
        if results.is_empty() {
            // Processing failed - increment retry count
            if let Some(state) = states.get_mut(&event.id) {
                state.attempts += 1;
                state.last_error = Some("Processing returned no results".to_string());
                
                // Emit failure event
                ctx.emit_event("retry", "attempt_failed", json!({
                    "event_id": event.id.as_str(),
                    "attempt": state.attempts,
                    "max_attempts": self.max_attempts,
                    "will_retry": state.attempts < self.max_attempts,
                }));
            }
        } else {
            // Success - clean up state
            if let Some(state) = states.remove(&event.id) {
                if state.attempts > 0 {
                    // Emit success after retry event
                    ctx.emit_event("retry", "succeeded_after_retry", json!({
                        "event_id": event.id.as_str(),
                        "attempts_needed": state.attempts + 1,
                        "max_attempts": self.max_attempts,
                    }));
                }
            }
        }
    }
    
    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        let mut states = self.event_states.lock().unwrap();
        
        let attempts = {
            let state = states.entry(event.id.clone()).or_insert(RetryState {
                attempts: 0,
                last_error: None,
            });
            
            if state.attempts < self.max_attempts {
                // We have retries left
                state.attempts += 1;
                state.last_error = Some("Handler error".to_string());
                let current_attempts = state.attempts;
                
                // Emit error event
                ctx.emit_event("retry", "error_will_retry", json!({
                    "event_id": event.id.as_str(),
                    "attempt": current_attempts,
                    "max_attempts": self.max_attempts,
                }));
                
                return ErrorAction::Retry;
            } else {
                state.attempts
            }
        };
        
        // No retries left
        states.remove(&event.id);
        
        // Emit final error event
        ctx.emit_event("retry", "error_exhausted", json!({
            "event_id": event.id.as_str(),
            "attempts": attempts,
            "max_attempts": self.max_attempts,
        }));
        
        ErrorAction::Propagate
    }
}

/// Builder for retry middleware
pub struct RetryBuilder {
    max_attempts: u32,
    strategy_type: RetryStrategyType,
    initial_delay: Duration,
    max_delay: Duration,
    backoff_factor: f64,
    with_jitter: bool,
}

enum RetryStrategyType {
    Fixed(Duration),
    Exponential,
}

impl RetryBuilder {
    /// Create a new retry builder with exponential backoff
    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            strategy_type: RetryStrategyType::Exponential,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
            with_jitter: true,
        }
    }
    
    /// Create a new retry builder with fixed delay
    pub fn fixed(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            strategy_type: RetryStrategyType::Fixed(delay),
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
            with_jitter: false,
        }
    }
    
    /// Set the initial delay for exponential backoff
    pub fn initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }
    
    /// Set the maximum delay for exponential backoff
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }
    
    /// Set the backoff factor for exponential backoff
    pub fn backoff_factor(mut self, factor: f64) -> Self {
        self.backoff_factor = factor;
        self
    }
    
    /// Enable jitter for exponential backoff (enabled by default)
    pub fn with_jitter(mut self) -> Self {
        self.with_jitter = true;
        self
    }
    
    /// Disable jitter for exponential backoff
    pub fn without_jitter(mut self) -> Self {
        self.with_jitter = false;
        self
    }
    
    /// Build the retry middleware
    pub fn build(self) -> Box<dyn Middleware> {
        match self.strategy_type {
            RetryStrategyType::Fixed(delay) => {
                Box::new(RetryMiddleware::fixed(self.max_attempts, delay))
            }
            RetryStrategyType::Exponential => {
                Box::new(RetryMiddleware::exponential_custom(
                    self.max_attempts,
                    self.initial_delay,
                    self.max_delay,
                    self.backoff_factor,
                    self.with_jitter,
                ))
            }
        }
    }
}

/// Helper functions for creating retry middleware
pub mod retry {
    use super::*;
    
    /// Create retry middleware with exponential backoff and jitter
    pub fn exponential(max_attempts: u32) -> RetryBuilder {
        RetryBuilder::exponential(max_attempts)
    }
    
    /// Create retry middleware with fixed delay
    pub fn fixed(max_attempts: u32, delay: Duration) -> RetryBuilder {
        RetryBuilder::fixed(max_attempts, delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fixed_delay_calculation() {
        let strategy = RetryStrategy::Fixed { 
            delay: Duration::from_millis(100) 
        };
        
        assert_eq!(strategy.calculate_delay(0), Duration::from_millis(100));
        assert_eq!(strategy.calculate_delay(1), Duration::from_millis(100));
        assert_eq!(strategy.calculate_delay(5), Duration::from_millis(100));
    }
    
    #[test]
    fn test_exponential_delay_without_jitter() {
        let strategy = RetryStrategy::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(1),
            factor: 2.0,
            jitter: false,
        };
        
        assert_eq!(strategy.calculate_delay(0), Duration::from_millis(100));
        assert_eq!(strategy.calculate_delay(1), Duration::from_millis(200));
        assert_eq!(strategy.calculate_delay(2), Duration::from_millis(400));
        assert_eq!(strategy.calculate_delay(3), Duration::from_millis(800));
        // Should be capped at max
        assert_eq!(strategy.calculate_delay(4), Duration::from_secs(1));
    }
    
    #[test]
    fn test_exponential_delay_with_jitter() {
        let strategy = RetryStrategy::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(10),
            factor: 2.0,
            jitter: true,
        };
        
        // With jitter, delays should be within expected range
        for attempt in 0..5 {
            let delay = strategy.calculate_delay(attempt);
            let base = 100.0 * 2.0_f64.powi(attempt as i32);
            let min = Duration::from_millis((base * 0.5) as u64);
            let max = Duration::from_millis((base * 1.5) as u64);
            
            assert!(delay >= min && delay <= max,
                "Delay {:?} should be between {:?} and {:?} for attempt {}",
                delay, min, max, attempt);
        }
    }
}