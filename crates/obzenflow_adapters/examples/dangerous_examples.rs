//! Example dangerous middleware implementations that demonstrate safety guardrails
//!
//! These middleware are intentionally dangerous and should only be used with
//! full understanding of their implications.

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareFactory, MiddlewareAction, MiddlewareSafety,
    MiddlewareContext, ErrorAction, ControlStrategyRequirement, BackoffConfig,
};
use obzenflow_core::ChainEvent;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_runtime_services::stages::common::stage_handle::StageType;

/// Middleware that skips all control events - EXTREMELY DANGEROUS for sinks!
pub struct SkipControlEventsMiddleware;

impl Middleware for SkipControlEventsMiddleware {
    fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        if event.is_control() {
            // Skip all control events - this will cause sinks to never drain!
            MiddlewareAction::Skip(vec![])
        } else {
            MiddlewareAction::Continue
        }
    }
}

pub struct SkipControlEventsFactory;

impl MiddlewareFactory for SkipControlEventsFactory {
    fn create(&self, _config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(SkipControlEventsMiddleware)
    }
    
    fn name(&self) -> &str {
        "skip_control_events"
    }
    
    fn supported_stage_types(&self) -> &[StageType] {
        // Only "safe" for transforms, and even then it's questionable
        &[StageType::Transform]
    }
    
    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Dangerous
    }
    
    fn required_control_strategy(&self) -> Option<ControlStrategyRequirement> {
        // This middleware bypasses all control strategies!
        Some(ControlStrategyRequirement::Custom {
            strategy_id: "skip_all_control".to_string(),
            config: serde_json::json!({
                "warning": "This middleware skips ALL control events!"
            }),
        })
    }
}

/// Middleware with unbounded batching - dangerous for sinks without timeout
pub struct UnboundedBatchingMiddleware {
    batch_size: usize,
    timeout: Option<std::time::Duration>,
    buffer: std::sync::Arc<std::sync::Mutex<Vec<ChainEvent>>>,
}

impl Middleware for UnboundedBatchingMiddleware {
    fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        if event.is_control() {
            // Let control events through
            return MiddlewareAction::Continue;
        }
        
        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(event.clone());
        
        if buffer.len() >= self.batch_size {
            // Emit batch
            let batch = buffer.drain(..).collect::<Vec<_>>();
            MiddlewareAction::Skip(batch)
        } else if self.timeout.is_none() {
            // No timeout - events can be stuck forever!
            MiddlewareAction::Skip(vec![])
        } else {
            // Has timeout - safer
            MiddlewareAction::Skip(vec![])
        }
    }
}

pub struct UnboundedBatchingFactory {
    batch_size: usize,
    timeout: Option<std::time::Duration>,
}

impl UnboundedBatchingFactory {
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            timeout: None,
        }
    }
    
    pub fn with_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl MiddlewareFactory for UnboundedBatchingFactory {
    fn create(&self, _config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(UnboundedBatchingMiddleware {
            batch_size: self.batch_size,
            timeout: self.timeout,
            buffer: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }
    
    fn name(&self) -> &str {
        if self.timeout.is_none() {
            "unbounded_batching_no_timeout"
        } else {
            "bounded_batching_with_timeout"
        }
    }
    
    fn supported_stage_types(&self) -> &[StageType] {
        if self.timeout.is_none() {
            // Without timeout, only safe for transforms
            &[StageType::Transform]
        } else {
            // With timeout, can be used on sinks too
            &[StageType::Transform, StageType::Sink]
        }
    }
    
    fn safety_level(&self) -> MiddlewareSafety {
        if self.timeout.is_none() {
            MiddlewareSafety::Dangerous
        } else {
            MiddlewareSafety::Advanced
        }
    }
}

/// Middleware that infinitely retries - dangerous for sources
pub struct InfiniteRetryMiddleware;

impl Middleware for InfiniteRetryMiddleware {
    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Always retry, forever!
        ErrorAction::Retry
    }
}

pub struct InfiniteRetryFactory;

impl MiddlewareFactory for InfiniteRetryFactory {
    fn create(&self, _config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(InfiniteRetryMiddleware)
    }
    
    fn name(&self) -> &str {
        "infinite_retry"
    }
    
    fn supported_stage_types(&self) -> &[StageType] {
        // Never safe for sources (they generate data, not receive it)
        &[StageType::Transform, StageType::Sink]
    }
    
    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Dangerous
    }
    
    fn required_control_strategy(&self) -> Option<ControlStrategyRequirement> {
        // Infinite retry with no backoff!
        Some(ControlStrategyRequirement::Retry {
            max_attempts: usize::MAX,
            backoff: BackoffConfig::Fixed {
                delay: std::time::Duration::from_millis(0),
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_dangerous_middleware_safety_levels() {
        let skip_factory = SkipControlEventsFactory;
        assert_eq!(skip_factory.safety_level(), MiddlewareSafety::Dangerous);
        assert_eq!(skip_factory.supported_stage_types(), &[StageType::Transform]);
        
        let unbounded_factory = UnboundedBatchingFactory::new(100);
        assert_eq!(unbounded_factory.safety_level(), MiddlewareSafety::Dangerous);
        
        let bounded_factory = UnboundedBatchingFactory::new(100)
            .with_timeout(std::time::Duration::from_secs(1));
        assert_eq!(bounded_factory.safety_level(), MiddlewareSafety::Advanced);
        
        let infinite_retry = InfiniteRetryFactory;
        assert_eq!(infinite_retry.safety_level(), MiddlewareSafety::Dangerous);
        assert!(!infinite_retry.supported_stage_types().contains(&StageType::FiniteSource));
        assert!(!infinite_retry.supported_stage_types().contains(&StageType::InfiniteSource));
    }
}

fn main() {
    println!("Dangerous middleware examples - these are for demonstration only!");
    println!("\nChecking safety levels of dangerous middleware:");
    
    let skip_factory = SkipControlEventsFactory;
    println!("- SkipControlEventsFactory: {:?}", skip_factory.safety_level());
    println!("  Supported stages: {:?}", skip_factory.supported_stage_types());
    
    let unbounded_factory = UnboundedBatchingFactory::new(100);
    println!("- UnboundedBatchingFactory (no timeout): {:?}", unbounded_factory.safety_level());
    
    let bounded_factory = UnboundedBatchingFactory::new(100)
        .with_timeout(std::time::Duration::from_secs(1));
    println!("- UnboundedBatchingFactory (with timeout): {:?}", bounded_factory.safety_level());
    
    let infinite_retry = InfiniteRetryFactory;
    println!("- InfiniteRetryFactory: {:?}", infinite_retry.safety_level());
    println!("  Supported stages: {:?}", infinite_retry.supported_stage_types());
    
    println!("\nWARNING: These middleware examples are dangerous and should not be used in production!");
}