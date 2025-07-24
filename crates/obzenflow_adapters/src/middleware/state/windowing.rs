//! Windowing middleware for time-based event aggregation
//!
//! This middleware demonstrates how to use control strategies to ensure
//! time windows complete before accepting EOF.

use crate::middleware::{
    Middleware, MiddlewareFactory, MiddlewareAction, MiddlewareContext,
    ErrorAction,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::EventId;
use obzenflow_core::{WriterId, StageId};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_core::event::context::StageType;
use obzenflow_runtime_services::stages::common::control_strategies::{
    ControlEventStrategy, WindowingStrategy,
};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use serde_json::json;

/// Windowing middleware that aggregates events over time windows
///
/// This middleware buffers events until a time window completes, then
/// emits an aggregated result. It requires a delay strategy to ensure
/// the final window has a chance to emit before EOF.
pub struct WindowingMiddleware {
    window_duration: Duration,
    window_start: Arc<RwLock<Option<Instant>>>,
    buffer: Arc<RwLock<Vec<ChainEvent>>>,
    aggregation_fn: Arc<dyn Fn(Vec<ChainEvent>) -> ChainEvent + Send + Sync>,
}

impl WindowingMiddleware {
    fn new(
        window_duration: Duration,
        aggregation_fn: Arc<dyn Fn(Vec<ChainEvent>) -> ChainEvent + Send + Sync>,
    ) -> Self {
        Self {
            window_duration,
            window_start: Arc::new(RwLock::new(None)),
            buffer: Arc::new(RwLock::new(Vec::new())),
            aggregation_fn,
        }
    }
    
    /// Check if the current window is complete
    fn is_window_complete(&self) -> bool {
        match *self.window_start.read().unwrap() {
            None => false, // No window started yet
            Some(start) => start.elapsed() >= self.window_duration,
        }
    }
    
    /// Emit the current window's aggregated result
    fn emit_window(&self, ctx: &mut MiddlewareContext) -> Option<ChainEvent> {
        let mut buffer = self.buffer.write().unwrap();
        if buffer.is_empty() {
            return None;
        }
        
        // Create aggregated event
        let events = buffer.drain(..).collect::<Vec<_>>();
        let count = events.len();
        let aggregated = (self.aggregation_fn)(events);
        
        // Reset window start
        *self.window_start.write().unwrap() = Some(Instant::now());
        
        // Emit metrics
        ctx.emit_event("windowing", "window_emitted", json!({
            "event_count": count,
            "window_duration_ms": self.window_duration.as_millis(),
        }));
        
        Some(aggregated)
    }
}

impl Middleware for WindowingMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Let control events through unchanged
        if event.is_control() {
            // If we have a partial window when EOF arrives, emit it
            if event.is_eof() {
                if let Some(aggregated) = self.emit_window(ctx) {
                    // Emit the final window before EOF
                    return MiddlewareAction::Skip(vec![aggregated]);
                }
            }
            return MiddlewareAction::Continue;
        }
        
        // Start window on first event
        if self.window_start.read().unwrap().is_none() {
            *self.window_start.write().unwrap() = Some(Instant::now());
        }
        
        // Check if window is complete
        if self.is_window_complete() {
            // Emit current window and start new one
            let mut results = Vec::new();
            if let Some(aggregated) = self.emit_window(ctx) {
                results.push(aggregated);
            }
            
            // Buffer this event for the new window
            self.buffer.write().unwrap().push(event.clone());
            
            // Skip original event, return aggregated result
            MiddlewareAction::Skip(results)
        } else {
            // Buffer event and skip it (will be included in aggregation)
            self.buffer.write().unwrap().push(event.clone());
            MiddlewareAction::Skip(vec![])
        }
    }
    
    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Continue buffering on errors - let aggregation handle them
        ErrorAction::Propagate
    }
}

/// Factory for creating windowing middleware
pub struct WindowingMiddlewareFactory {
    window_duration: Duration,
    aggregation_type: AggregationType,
}

#[derive(Clone)]
pub enum AggregationType {
    /// Count events in the window
    Count,
    /// Sum numeric values
    Sum(String), // field name
    /// Average numeric values  
    Average(String), // field name
    /// Custom aggregation function
    Custom(Arc<dyn Fn(Vec<ChainEvent>) -> ChainEvent + Send + Sync>),
}

impl WindowingMiddlewareFactory {
    /// Create a tumbling window that counts events
    pub fn tumbling_count(window_duration: Duration) -> Self {
        Self {
            window_duration,
            aggregation_type: AggregationType::Count,
        }
    }
    
    /// Create a tumbling window that sums a numeric field
    pub fn tumbling_sum(window_duration: Duration, field: String) -> Self {
        Self {
            window_duration,
            aggregation_type: AggregationType::Sum(field),
        }
    }
    
    /// Create a tumbling window with custom aggregation
    pub fn tumbling_custom(
        window_duration: Duration,
        aggregation_fn: Arc<dyn Fn(Vec<ChainEvent>) -> ChainEvent + Send + Sync>,
    ) -> Self {
        Self {
            window_duration,
            aggregation_type: AggregationType::Custom(aggregation_fn),
        }
    }
    
    fn create_aggregation_fn(&self) -> Arc<dyn Fn(Vec<ChainEvent>) -> ChainEvent + Send + Sync> {
        match &self.aggregation_type {
            AggregationType::Count => {
                Arc::new(|events: Vec<ChainEvent>| {
                    let count = events.len();
                    let result = ChainEventFactory::windowing_count_event(
                        WriterId::from(StageId::new()),
                        count,
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    );
                    result
                })
            }
            AggregationType::Sum(field) => {
                let field = field.clone();
                Arc::new(move |events: Vec<ChainEvent>| {
                    let values: Vec<f64> = events.iter()
                        .filter_map(|e| {
                            let payload = e.payload();
                            payload.get(&field).and_then(|v| v.as_f64())
                        })
                        .collect();
                    
                    let sum: f64 = values.iter().sum();
                    
                    let result = ChainEventFactory::windowing_sum_event(
                        WriterId::from(StageId::new()),
                        sum,
                        field.clone(),
                        values.len(),
                        events.len(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    );
                    result
                })
            }
            AggregationType::Average(field) => {
                let field = field.clone();
                Arc::new(move |events: Vec<ChainEvent>| {
                    let values: Vec<f64> = events.iter()
                        .filter_map(|e| {
                            let payload = e.payload();
                            payload.get(&field).and_then(|v| v.as_f64())
                        })
                        .collect();
                    
                    let avg = if values.is_empty() {
                        0.0
                    } else {
                        values.iter().sum::<f64>() / values.len() as f64
                    };
                    
                    let result = ChainEventFactory::windowing_average_event(
                        WriterId::from(StageId::new()),
                        avg,
                        field.clone(),
                        values.len(),
                        events.len(),
                        std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis(),
                    );
                    result
                })
            }
            AggregationType::Custom(f) => f.clone(),
        }
    }
}

impl MiddlewareFactory for WindowingMiddlewareFactory {
    fn create(&self, _config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(WindowingMiddleware::new(
            self.window_duration,
            self.create_aggregation_fn(),
        ))
    }
    
    fn name(&self) -> &str {
        match &self.aggregation_type {
            AggregationType::Count => "windowing_count",
            AggregationType::Sum(_) => "windowing_sum",
            AggregationType::Average(_) => "windowing_average",
            AggregationType::Custom(_) => "windowing_custom",
        }
    }
    
    fn create_control_strategy(&self) -> Option<Box<dyn ControlEventStrategy>> {
        // Need windowing strategy to delay EOF until window completes
        Some(Box::new(WindowingStrategy::new(self.window_duration)))
    }
    
    fn supported_stage_types(&self) -> &[StageType] {
        // Windowing makes sense for transforms
        // Could work for sinks that aggregate before writing
        &[StageType::Transform, StageType::Sink]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_windowing_factory_creates_middleware() {
        let factory = WindowingMiddlewareFactory::tumbling_count(Duration::from_secs(60));
        assert_eq!(factory.name(), "windowing_count");
        
        let strategy = factory.create_control_strategy();
        assert!(strategy.is_some(), "Expected windowing strategy");
    }
    
    #[test]
    fn test_windowing_skips_data_events_until_window_complete() {
        let factory = WindowingMiddlewareFactory::tumbling_count(Duration::from_millis(100));
        // Create middleware directly without StageConfig
        let middleware = Box::new(WindowingMiddleware::new(
            Duration::from_millis(100),
            factory.create_aggregation_fn(),
        ));
        
        let mut ctx = MiddlewareContext::new();
        let mut event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"value": 42}),
        );
        
        // First event starts window and is buffered
        let action = middleware.pre_handle(&event, &mut ctx);
        match action {
            MiddlewareAction::Skip(events) => assert!(events.is_empty()),
            _ => panic!("Expected Skip with no events"),
        }
        
        // Second event is also buffered
        let action = middleware.pre_handle(&event, &mut ctx);
        match action {
            MiddlewareAction::Skip(events) => assert!(events.is_empty()),
            _ => panic!("Expected Skip with no events"),
        }
    }
    
    #[test]
    fn test_windowing_forwards_control_events() {
        let factory = WindowingMiddlewareFactory::tumbling_count(Duration::from_secs(60));
        // Create middleware directly without StageConfig
        let middleware = Box::new(WindowingMiddleware::new(
            Duration::from_secs(60),
            factory.create_aggregation_fn(),
        ));
        
        let mut ctx = MiddlewareContext::new();
        let eof = ChainEventFactory::eof_event(
            WriterId::from(StageId::new()),
            true
        );
        
        let action = middleware.pre_handle(&eof, &mut ctx);
        assert!(matches!(action, MiddlewareAction::Continue));
    }
}