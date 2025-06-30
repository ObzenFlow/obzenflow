//! Rate limiting middleware with control strategy integration
//!
//! This middleware demonstrates how rate limiting can use delay strategies
//! to ensure rate limits are respected even during shutdown.

use crate::middleware::{
    Middleware, MiddlewareFactory, MiddlewareAction, MiddlewareContext,
    ControlStrategyRequirement, ErrorAction, MiddlewareSafety,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime_services::control_plane::stages::supervisors::{
    config::StageConfig,
    stage_handle::StageType,
};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::collections::VecDeque;
use serde_json::json;

/// Token bucket rate limiter implementation
#[derive(Debug)]
struct TokenBucket {
    /// Maximum tokens (burst capacity)
    capacity: f64,
    /// Current tokens available
    tokens: f64,
    /// Tokens added per second
    refill_rate: f64,
    /// Last time tokens were refilled
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            tokens: capacity, // Start full
            refill_rate,
            last_refill: Instant::now(),
        }
    }
    
    /// Try to consume tokens, returns true if successful
    fn try_consume(&mut self, tokens: f64) -> bool {
        self.refill();
        
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }
    
    /// Get time until enough tokens are available
    fn time_until_available(&mut self, tokens: f64) -> Option<Duration> {
        self.refill();
        
        if self.tokens >= tokens {
            return None; // Already available
        }
        
        let needed = tokens - self.tokens;
        let seconds_needed = needed / self.refill_rate;
        Some(Duration::from_secs_f64(seconds_needed))
    }
    
    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);
        
        let tokens_to_add = elapsed.as_secs_f64() * self.refill_rate;
        self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
        self.last_refill = now;
    }
    
    /// Get current token count (for monitoring)
    fn available_tokens(&mut self) -> f64 {
        self.refill();
        self.tokens
    }
}

/// Rate limiting middleware using token bucket algorithm
pub struct RateLimiterMiddleware {
    bucket: Arc<Mutex<TokenBucket>>,
    /// Events that were delayed and need to be emitted
    delayed_events: Arc<Mutex<VecDeque<ChainEvent>>>,
    /// Cost per event (default 1.0)
    cost_per_event: f64,
}

impl RateLimiterMiddleware {
    fn new(events_per_second: f64, burst_capacity: Option<f64>, cost_per_event: f64) -> Self {
        let capacity = burst_capacity.unwrap_or(events_per_second);
        let bucket = TokenBucket::new(capacity, events_per_second);
        
        Self {
            bucket: Arc::new(Mutex::new(bucket)),
            delayed_events: Arc::new(Mutex::new(VecDeque::new())),
            cost_per_event,
        }
    }
    
    /// Check and emit any delayed events that can now be processed
    fn emit_delayed_events(&self, ctx: &mut MiddlewareContext) -> Vec<ChainEvent> {
        let mut result = Vec::new();
        let mut delayed = self.delayed_events.lock().unwrap();
        let mut bucket = self.bucket.lock().unwrap();
        
        // Try to emit as many delayed events as possible
        while let Some(_) = delayed.front() {
            if bucket.try_consume(self.cost_per_event) {
                let event = delayed.pop_front().unwrap();
                let event_type = event.event_type.clone();
                result.push(event);
                ctx.emit_event("rate_limiter", "delayed_event_released", json!({
                    "event_type": event_type,
                }));
            } else {
                break; // No more tokens available
            }
        }
        
        result
    }
}

impl Middleware for RateLimiterMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Always let control events through
        if event.is_control() {
            // But first, try to flush delayed events
            let released = self.emit_delayed_events(ctx);
            if !released.is_empty() {
                ctx.emit_event("rate_limiter", "flushing_before_control", json!({
                    "released_count": released.len(),
                    "control_type": &event.event_type,
                }));
                // Prepend released events before the control event
                let mut all_events = released;
                all_events.push(event.clone());
                return MiddlewareAction::Skip(all_events);
            }
            return MiddlewareAction::Continue;
        }
        
        // Check if we have tokens for this event
        let mut bucket = self.bucket.lock().unwrap();
        
        if bucket.try_consume(self.cost_per_event) {
            // We have capacity - emit any delayed events first
            drop(bucket); // Release lock
            let mut released = self.emit_delayed_events(ctx);
            
            if released.is_empty() {
                // No delayed events, just continue
                ctx.emit_event("rate_limiter", "event_allowed", json!({
                    "available_tokens": self.bucket.lock().unwrap().available_tokens(),
                }));
                MiddlewareAction::Continue
            } else {
                // Emit delayed events along with current event
                released.push(event.clone());
                MiddlewareAction::Skip(released)
            }
        } else {
            // No tokens - delay this event
            let wait_time = bucket.time_until_available(self.cost_per_event)
                .unwrap_or(Duration::from_millis(100));
            
            ctx.emit_event("rate_limiter", "event_delayed", json!({
                "wait_time_ms": wait_time.as_millis(),
                "available_tokens": bucket.available_tokens(),
                "delayed_queue_size": self.delayed_events.lock().unwrap().len() + 1,
            }));
            
            // Add to delayed queue
            drop(bucket); // Release lock
            self.delayed_events.lock().unwrap().push_back(event.clone());
            
            // Skip this event for now
            MiddlewareAction::Skip(vec![])
        }
    }
    
    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Don't consume tokens for errors
        ErrorAction::Propagate
    }
}

/// Factory for creating rate limiter middleware
pub struct RateLimiterFactory {
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
}

impl RateLimiterFactory {
    /// Create a basic rate limiter
    pub fn new(events_per_second: f64) -> Self {
        Self {
            events_per_second,
            burst_capacity: None,
            cost_per_event: 1.0,
        }
    }
    
    /// Set burst capacity (defaults to events_per_second)
    pub fn with_burst(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }
    
    /// Set cost per event (for weighted rate limiting)
    pub fn with_cost(mut self, cost: f64) -> Self {
        self.cost_per_event = cost;
        self
    }
}

impl MiddlewareFactory for RateLimiterFactory {
    fn create(&self, _config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(RateLimiterMiddleware::new(
            self.events_per_second,
            self.burst_capacity,
            self.cost_per_event,
        ))
    }
    
    fn name(&self) -> &str {
        "rate_limiter"
    }
    
    fn required_control_strategy(&self) -> Option<ControlStrategyRequirement> {
        // Need delay strategy to ensure we can flush delayed events before EOF
        // Calculate max delay based on burst capacity and rate
        let capacity = self.burst_capacity.unwrap_or(self.events_per_second);
        let max_drain_time = capacity / self.events_per_second;
        
        Some(ControlStrategyRequirement::Windowing {
            window_duration: Duration::from_secs_f64(max_drain_time),
        })
    }
    
    fn supported_stage_types(&self) -> &[StageType] {
        // Rate limiting makes sense for all stage types
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
        ]
    }
    
    fn safety_level(&self) -> MiddlewareSafety {
        // Rate limiting on sinks can cause backpressure
        MiddlewareSafety::Advanced
    }
}

/// Helper function for common module
pub fn rate_limit(events_per_second: f64) -> Box<dyn MiddlewareFactory> {
    Box::new(RateLimiterFactory::new(events_per_second))
}

/// Helper function with burst capacity
pub fn rate_limit_with_burst(events_per_second: f64, burst: f64) -> Box<dyn MiddlewareFactory> {
    Box::new(RateLimiterFactory::new(events_per_second).with_burst(burst))
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::event_id::EventId;
    use obzenflow_core::journal::writer_id::WriterId;
    
    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10.0, 5.0); // 10 capacity, 5/sec refill
        
        // Should start full
        assert!(bucket.try_consume(5.0));
        assert!(bucket.try_consume(5.0));
        assert!(!bucket.try_consume(5.0)); // Should fail
        
        // Wait a bit and check refill
        std::thread::sleep(Duration::from_millis(200)); // 0.2 sec = 1 token
        assert!(bucket.try_consume(1.0)); // Should succeed
        assert!(!bucket.try_consume(1.0)); // Should fail again
    }
    
    #[test]
    fn test_token_bucket_time_until_available() {
        let mut bucket = TokenBucket::new(10.0, 2.0); // 10 capacity, 2/sec refill
        
        // Consume all tokens
        assert!(bucket.try_consume(10.0));
        
        // Should need 2.5 seconds to get 5 tokens
        let wait = bucket.time_until_available(5.0).unwrap();
        assert!((wait.as_secs_f64() - 2.5).abs() < 0.1);
    }
    
    #[test]
    fn test_rate_limiter_allows_bursts() {
        // Create middleware directly since the factory doesn't use the config
        let middleware = RateLimiterMiddleware::new(10.0, Some(20.0), 1.0);
        
        let mut ctx = MiddlewareContext::new();
        
        // Should allow burst of 20 events
        for i in 0..20 {
            let event = ChainEvent::new(
                EventId::new(),
                WriterId::new(),
                "test.event",
                json!({ "index": i }),
            );
            
            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => {},
                other => panic!("Expected Continue for event {}, got {:?}", i, other),
            }
        }
        
        // 21st event should be delayed
        let event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "test.event",
            json!({ "index": 20 }),
        );
        
        match middleware.pre_handle(&event, &mut ctx) {
            MiddlewareAction::Skip(events) => {
                assert!(events.is_empty()); // Event was delayed
            },
            other => panic!("Expected Skip for 21st event, got {:?}", other),
        }
    }
    
    #[test]
    fn test_rate_limiter_control_events_pass_through() {
        // Create middleware directly since the factory doesn't use the config
        let middleware = RateLimiterMiddleware::new(1.0, None, 1.0);
        
        let mut ctx = MiddlewareContext::new();
        
        // Consume the one available token
        let data_event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "test.event",
            json!({}),
        );
        middleware.pre_handle(&data_event, &mut ctx);
        
        // Control event should still pass through
        let eof = ChainEvent::eof(EventId::new(), WriterId::new(), true);
        match middleware.pre_handle(&eof, &mut ctx) {
            MiddlewareAction::Continue => {},
            other => panic!("Expected Continue for EOF, got {:?}", other),
        }
    }
    
    #[test]
    fn test_rate_limiter_strategy_requirement() {
        let factory = RateLimiterFactory::new(100.0).with_burst(500.0);
        
        let requirement = factory.required_control_strategy().unwrap();
        match requirement {
            ControlStrategyRequirement::Windowing { window_duration } => {
                // Should be 500/100 = 5 seconds to drain
                assert_eq!(window_duration.as_secs(), 5);
            },
            other => panic!("Expected Windowing requirement, got {:?}", other),
        }
    }
}