//! Control strategy requirements that middleware can declare

use std::time::Duration;

/// Requirements that middleware can declare for control event handling
#[derive(Debug, Clone)]
pub enum ControlStrategyRequirement {
    /// Middleware needs retry capability (e.g., circuit breakers)
    Retry {
        /// Maximum number of retry attempts
        max_attempts: usize,
        /// Backoff strategy between retries
        backoff: BackoffConfig,
    },
    
    /// Middleware needs windowing/delay capability
    Windowing {
        /// Duration of the time window
        window_duration: Duration,
    },
    
    /// Custom strategy requirement (advanced use)
    Custom {
        /// Unique identifier for the custom strategy
        strategy_id: String,
        /// Configuration data for the custom strategy
        config: serde_json::Value,
    },
}

/// Backoff configuration for retry strategies
#[derive(Debug, Clone)]
pub enum BackoffConfig {
    /// Fixed delay between attempts
    Fixed {
        delay: Duration,
    },
    
    /// Exponential backoff with optional jitter
    Exponential {
        /// Initial delay
        initial: Duration,
        /// Maximum delay
        max: Duration,
        /// Multiplication factor per attempt
        factor: f64,
        /// Whether to add jitter to prevent thundering herd
        jitter: bool,
    },
}

impl BackoffConfig {
    /// Create a simple exponential backoff with sensible defaults
    pub fn exponential() -> Self {
        Self::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            factor: 2.0,
            jitter: true,
        }
    }
    
    /// Create a fixed delay backoff
    pub fn fixed(delay: Duration) -> Self {
        Self::Fixed { delay }
    }
}