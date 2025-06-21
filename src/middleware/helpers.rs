//! Helper functions for creating middleware

use super::{Middleware, MonitoringMiddleware};
use crate::monitoring::Taxonomy;

/// Function for use in flow! macro - creates monitoring middleware
/// 
/// Usage in flow! macro:
/// ```rust
/// flow! {
///     ("source" => MySource::new(), [monitoring(RED)])
/// }
/// ```
pub fn monitoring<T: Taxonomy>(_taxonomy: T) -> Box<dyn Middleware>
where
    T: 'static,
    T::Metrics: super::monitoring::MetricRecorder + 'static,
{
    // Default name - will be overridden by MonitoringWrapper
    Box::new(MonitoringMiddleware::<T>::new("step"))
}

/// Wrapper that updates monitoring middleware with the correct stage name
pub struct MonitoringWrapper {
    inner: Box<dyn Middleware>,
    stage_name: String,
}

impl MonitoringWrapper {
    pub fn new(middleware: Box<dyn Middleware>, stage_name: String) -> Self {
        Self { inner: middleware, stage_name }
    }
}

impl Middleware for MonitoringWrapper {
    fn pre_handle(&self, event: &crate::chain_event::ChainEvent) -> super::MiddlewareAction {
        self.inner.pre_handle(event)
    }
    
    fn post_handle(&self, event: &crate::chain_event::ChainEvent, results: &mut Vec<crate::chain_event::ChainEvent>) {
        self.inner.post_handle(event, results)
    }
    
    fn on_error(&self, event: &crate::chain_event::ChainEvent, error: &super::StepError) -> super::ErrorAction {
        self.inner.on_error(event, error)
    }
}