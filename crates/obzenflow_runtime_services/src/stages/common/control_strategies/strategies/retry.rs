//! Retry strategy for control events - used by circuit breakers and retry middleware

use super::super::{ControlEventAction, ControlEventStrategy, ProcessingContext};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;
use std::time::Duration;

/// Strategy that retries processing before accepting EOF
///
/// This strategy is used by circuit breakers and retry middleware to prevent
/// stage termination during recovery attempts. It gives the stage time to
/// heal before accepting shutdown.
pub struct RetryStrategy {
    /// Maximum number of retry attempts before giving up
    pub max_attempts: usize,

    /// Backoff strategy for retries
    pub backoff: BackoffStrategy,
}

/// Backoff strategies for retry delays
#[derive(Debug, Clone)]
pub enum BackoffStrategy {
    /// Fixed delay between attempts
    Fixed { delay: Duration },

    /// Exponential backoff with optional jitter
    Exponential {
        initial: Duration,
        max: Duration,
        factor: f64,
        jitter: bool,
    },
}

impl BackoffStrategy {
    /// Calculate delay for a given attempt number (0-indexed)
    pub fn calculate_delay(&self, attempt: usize) -> Duration {
        match self {
            BackoffStrategy::Fixed { delay } => *delay,

            BackoffStrategy::Exponential {
                initial,
                max,
                factor,
                jitter,
            } => {
                // Calculate exponential delay
                let base_delay = initial.as_millis() as f64 * factor.powi(attempt as i32);
                let capped_delay = base_delay.min(max.as_millis() as f64);

                // Apply jitter if requested (prevents thundering herd)
                let final_delay = if *jitter {
                    let jitter_factor = 1.0 + (rand::random::<f64>() - 0.5) * 0.2; // ±10%
                    capped_delay * jitter_factor
                } else {
                    capped_delay
                };

                Duration::from_millis(final_delay as u64)
            }
        }
    }
}

impl ControlEventStrategy for RetryStrategy {
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        if ctx.eof_attempts < self.max_attempts {
            // Still have attempts left
            ctx.eof_attempts += 1;

            // Calculate backoff delay
            let delay = self.backoff.calculate_delay(ctx.eof_attempts - 1);

            tracing::info!(
                "Retry strategy: attempt {}/{}, delaying {}ms before retry",
                ctx.eof_attempts,
                self.max_attempts,
                delay.as_millis()
            );

            // Return Retry to keep the stage alive
            ControlEventAction::Retry
        } else {
            // Exhausted retries - give up and forward EOF
            tracing::warn!(
                "Retry strategy: exhausted {} attempts, forwarding EOF",
                self.max_attempts
            );
            ControlEventAction::Forward
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_backoff() {
        let backoff = BackoffStrategy::Fixed {
            delay: Duration::from_millis(100),
        };

        assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));
        assert_eq!(backoff.calculate_delay(5), Duration::from_millis(100));
    }

    #[test]
    fn test_exponential_backoff_without_jitter() {
        let backoff = BackoffStrategy::Exponential {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(10),
            factor: 2.0,
            jitter: false,
        };

        assert_eq!(backoff.calculate_delay(0), Duration::from_millis(100));
        assert_eq!(backoff.calculate_delay(1), Duration::from_millis(200));
        assert_eq!(backoff.calculate_delay(2), Duration::from_millis(400));
        assert_eq!(backoff.calculate_delay(3), Duration::from_millis(800));
    }

    #[test]
    fn test_exponential_backoff_respects_max() {
        let backoff = BackoffStrategy::Exponential {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(5),
            factor: 2.0,
            jitter: false,
        };

        // Should cap at 5 seconds
        assert_eq!(backoff.calculate_delay(10), Duration::from_secs(5));
    }
}
