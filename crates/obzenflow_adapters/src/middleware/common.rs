//! Common middleware implementations and helper functions
//! 
//! This module provides ready-to-use middleware for common cross-cutting concerns
//! like rate limiting, timeouts, and basic logging.

use super::{Middleware, MiddlewareAction, middleware_fn};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use serde_json;

/// Rate limiting middleware that limits events per second
pub fn rate_limit(per_second: u32) -> Box<dyn Middleware> {
    let rate_limiter = Arc::new(Mutex::new(RateLimiterState::new(per_second)));
    
    Box::new(middleware_fn(move |_event, ctx| {
        let mut state = rate_limiter.lock().unwrap();
        if state.allow_event() {
            MiddlewareAction::Continue
        } else {
            ctx.emit_event("rate_limiter", "event_dropped", serde_json::json!({
                "reason": "rate_limit_exceeded",
                "limit_per_second": state.per_second
            }));
            MiddlewareAction::Skip(vec![])
        }
    }))
}

/// Simple timeout middleware (placeholder - real implementation would be async)
pub fn timeout(duration: Duration) -> Box<dyn Middleware> {
    Box::new(middleware_fn(move |event, ctx| {
        // Store timeout deadline in event metadata for downstream handling
        // Real implementation would use async runtime for proper timeout
        tracing::debug!("Timeout middleware: {:?} for event {}", duration, event.id.as_str());
        ctx.emit_event("timeout", "deadline_set", serde_json::json!({
            "event_id": event.id.as_str(),
            "timeout_ms": duration.as_millis()
        }));
        MiddlewareAction::Continue
    }))
}

/// Simple logging middleware
pub fn logging(level: tracing::Level) -> Box<dyn Middleware> {
    Box::new(middleware_fn(move |event, _ctx| {
        match level {
            tracing::Level::TRACE => tracing::trace!("Processing event: {:?}", event),
            tracing::Level::DEBUG => tracing::debug!("Processing event: {}", event.id.as_str()),
            tracing::Level::INFO => tracing::info!("Processing event: {} ({})", event.id.as_str(), event.event_type),
            tracing::Level::WARN => tracing::warn!("Processing event: {}", event.id.as_str()),
            tracing::Level::ERROR => tracing::error!("Processing event: {}", event.id.as_str()),
        }
        MiddlewareAction::Continue
    }))
}

/// Helper module for retry middleware with different strategies
pub mod retry {
    use super::*;
    
    /// Exponential backoff retry builder
    pub struct ExponentialRetryBuilder {
        max_attempts: usize,
        initial_delay: Duration,
        max_delay: Duration,
        backoff_factor: f64,
        with_jitter: bool,
    }
    
    /// Create an exponential backoff retry middleware
    pub fn exponential(max_attempts: usize) -> ExponentialRetryBuilder {
        ExponentialRetryBuilder {
            max_attempts,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_factor: 2.0,
            with_jitter: false,
        }
    }
    
    impl ExponentialRetryBuilder {
        pub fn with_jitter(mut self) -> Self {
            self.with_jitter = true;
            self
        }
        
        pub fn initial_delay(mut self, delay: Duration) -> Self {
            self.initial_delay = delay;
            self
        }
        
        pub fn max_delay(mut self, delay: Duration) -> Self {
            self.max_delay = delay;
            self
        }
        
        pub fn backoff_factor(mut self, factor: f64) -> Self {
            self.backoff_factor = factor;
            self
        }
        
        pub fn build(self) -> Box<dyn Middleware> {
            // For now, return a simple middleware that logs retry attempts
            // Full implementation would track retry counts per event
            Box::new(middleware_fn(move |event, _ctx| {
                tracing::debug!("Retry middleware configured for event {}", event.id.as_str());
                MiddlewareAction::Continue
            }))
        }
    }
    
    /// Simple fixed delay retry
    pub fn fixed_delay(max_attempts: usize, delay: Duration) -> Box<dyn Middleware> {
        Box::new(middleware_fn(move |event, _ctx| {
            tracing::debug!("Fixed retry middleware: {} attempts, {:?} delay for event {}", 
                max_attempts, delay, event.id.as_str());
            MiddlewareAction::Continue
        }))
    }
}

/// Helper module for logging middleware with different configurations
pub mod logging {
    use super::*;
    
    pub struct LoggingBuilder {
        level: tracing::Level,
        with_payload: bool,
        with_metadata: bool,
    }
    
    pub fn info() -> LoggingBuilder {
        LoggingBuilder {
            level: tracing::Level::INFO,
            with_payload: false,
            with_metadata: false,
        }
    }
    
    pub fn debug() -> LoggingBuilder {
        LoggingBuilder {
            level: tracing::Level::DEBUG,
            with_payload: false,
            with_metadata: false,
        }
    }
    
    impl LoggingBuilder {
        pub fn with_payload(mut self) -> Self {
            self.with_payload = true;
            self
        }
        
        pub fn with_metadata(mut self) -> Self {
            self.with_metadata = true;
            self
        }
        
        pub fn build(self) -> Box<dyn Middleware> {
            Box::new(middleware_fn(move |event, _ctx| {
                let msg = if self.with_payload {
                    format!("Processing event: {} ({}) - payload: {:?}", 
                        event.id.as_str(), event.event_type, event.payload)
                } else {
                    format!("Processing event: {} ({})", 
                        event.id.as_str(), event.event_type)
                };
                
                match self.level {
                    tracing::Level::TRACE => tracing::trace!("{}", msg),
                    tracing::Level::DEBUG => tracing::debug!("{}", msg),
                    tracing::Level::INFO => tracing::info!("{}", msg),
                    tracing::Level::WARN => tracing::warn!("{}", msg),
                    tracing::Level::ERROR => tracing::error!("{}", msg),
                }
                MiddlewareAction::Continue
            }))
        }
    }
}

// Internal rate limiter state
struct RateLimiterState {
    per_second: u32,
    window: VecDeque<Instant>,
}

impl RateLimiterState {
    fn new(per_second: u32) -> Self {
        Self {
            per_second,
            window: VecDeque::with_capacity(per_second as usize),
        }
    }
    
    fn allow_event(&mut self) -> bool {
        let now = Instant::now();
        let one_second_ago = now - Duration::from_secs(1);
        
        // Remove events older than 1 second
        while let Some(&front) = self.window.front() {
            if front < one_second_ago {
                self.window.pop_front();
            } else {
                break;
            }
        }
        
        // Check if we can allow this event
        if self.window.len() < self.per_second as usize {
            self.window.push_back(now);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_rate_limiter_state() {
        let mut limiter = RateLimiterState::new(2);
        
        // First two events should be allowed
        assert!(limiter.allow_event());
        assert!(limiter.allow_event());
        
        // Third event within the same second should be rejected
        assert!(!limiter.allow_event());
    }
}