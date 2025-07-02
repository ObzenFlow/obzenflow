//! Common middleware implementations and helper functions
//! 
//! This module provides ready-to-use middleware for common cross-cutting concerns
//! like rate limiting, timeouts, and basic logging.

use super::{Middleware, MiddlewareAction, MiddlewareFactory, middleware_fn};
use super::windowing::WindowingMiddlewareFactory;
use super::rate_limiter::{rate_limit as create_rate_limiter};
use std::sync::Arc;
use std::time::Duration;
use serde_json;
use obzenflow_core::event::chain_event::ChainEvent;

/// Rate limiting middleware factory that limits events per second
pub fn rate_limit(per_second: f64) -> Box<dyn MiddlewareFactory> {
    create_rate_limiter(per_second)
}

/// Rate limiting with burst capacity
pub use super::rate_limiter::rate_limit_with_burst;

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


/// Helper module for windowing middleware with different aggregations
pub mod windowing {
    use super::*;
    
    /// Create a tumbling window that counts events
    pub fn count(window_duration: Duration) -> Box<dyn MiddlewareFactory> {
        Box::new(WindowingMiddlewareFactory::tumbling_count(window_duration))
    }
    
    /// Create a tumbling window that sums a numeric field
    pub fn sum(window_duration: Duration, field: impl Into<String>) -> Box<dyn MiddlewareFactory> {
        Box::new(WindowingMiddlewareFactory::tumbling_sum(window_duration, field.into()))
    }
    
    /// Create a tumbling window that averages a numeric field
    pub fn average(window_duration: Duration, field: impl Into<String>) -> Box<dyn MiddlewareFactory> {
        let field = field.into();
        Box::new(WindowingMiddlewareFactory::tumbling_custom(
            window_duration,
            Arc::new(move |events: Vec<ChainEvent>| {
                let values: Vec<f64> = events.iter()
                    .filter_map(|e| e.payload.get(&field))
                    .filter_map(|v| v.as_f64())
                    .collect();
                
                let avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };
                
                let mut result = ChainEvent::new(
                    obzenflow_core::event::event_id::EventId::new(),
                    obzenflow_core::journal::writer_id::WriterId::new(),
                    "windowing.average",
                    serde_json::Value::Null,
                );
                result.payload = serde_json::json!({
                    "average": avg,
                    "field": field,
                    "value_count": values.len(),
                    "event_count": events.len(),
                    "window_end_ms": std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_millis(),
                });
                result
            })
        ))
    }
    
    /// Create a tumbling window with custom aggregation
    pub fn custom<F>(window_duration: Duration, aggregation_fn: F) -> Box<dyn MiddlewareFactory>
    where
        F: Fn(Vec<ChainEvent>) -> ChainEvent + Send + Sync + 'static,
    {
        Box::new(WindowingMiddlewareFactory::tumbling_custom(
            window_duration,
            Arc::new(aggregation_fn),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
}