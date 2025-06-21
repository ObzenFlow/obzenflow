//! Flow control trait for managing backpressure and rate limiting

use std::time::Duration;

/// Components that implement flow control mechanisms
pub trait FlowControlled: Send + Sync {
    /// Check if processing should pause due to backpressure
    /// Called before processing each batch
    fn should_pause(&self) -> bool {
        false // Default: never pause
    }
    
    /// Check if component can process more events (rate limiting)
    /// Called before processing each event
    fn can_process(&self) -> bool {
        true // Default: always process
    }
    
    /// Get current circuit breaker state
    /// Used to stop processing during error conditions
    fn circuit_state(&self) -> CircuitState {
        CircuitState::Closed // Default: always operational
    }
    
    /// Record processing result for flow control decisions
    /// Called after each event is processed
    fn record_result(&mut self, _success: bool, _latency: Duration) {
        // Default: no tracking
    }
    
    /// Get recommended batch size based on current conditions
    fn recommended_batch_size(&self) -> usize {
        100 // Default batch size
    }
}

/// State of a circuit breaker
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Normal operation - requests are allowed
    Closed,
    /// Rejecting all requests due to errors
    Open,
    /// Testing if service has recovered
    HalfOpen,
}