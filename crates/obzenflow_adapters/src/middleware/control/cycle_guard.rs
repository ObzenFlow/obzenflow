//! Cycle guard middleware to prevent infinite loops in topologies with cycles
//!
//! This middleware tracks how many times an event has been processed through
//! a cycle and aborts processing if it exceeds the configured limit.

use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext};
use obzenflow_core::{event::chain_event::ChainEvent, event::CorrelationId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Entry in the cycle tracking map with timestamp for TTL
#[derive(Debug, Clone)]
struct CycleEntry {
    /// Number of iterations for this correlation
    iterations: usize,
    /// Last access time for TTL cleanup
    last_accessed: Instant,
}

/// Middleware that prevents infinite loops in cyclic topologies
pub struct CycleGuardMiddleware {
    /// Maximum iterations allowed before aborting
    max_iterations: usize,
    /// Track how many times each correlation has cycled through this stage
    cycle_tracking: Arc<Mutex<HashMap<CorrelationId, CycleEntry>>>,
    /// Stage name for logging
    stage_name: String,
    /// TTL for entries (default: 5 minutes)
    entry_ttl: Duration,
    /// How often to run cleanup (track via last_cleanup)
    last_cleanup: Arc<Mutex<Instant>>,
}

impl CycleGuardMiddleware {
    /// Create a new cycle guard with specified iteration limit
    pub fn new(max_iterations: usize, stage_name: impl Into<String>) -> Self {
        Self {
            max_iterations,
            cycle_tracking: Arc::new(Mutex::new(HashMap::new())),
            stage_name: stage_name.into(),
            entry_ttl: Duration::from_secs(300), // 5 minutes default
            last_cleanup: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Create a new cycle guard with custom TTL
    pub fn with_ttl(max_iterations: usize, stage_name: impl Into<String>, ttl: Duration) -> Self {
        Self {
            max_iterations,
            cycle_tracking: Arc::new(Mutex::new(HashMap::new())),
            stage_name: stage_name.into(),
            entry_ttl: ttl,
            last_cleanup: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Clean up expired entries
    fn cleanup_expired_entries(&self) {
        let mut tracking = self.cycle_tracking.lock().unwrap();
        let now = Instant::now();

        // Remove entries older than TTL
        tracking.retain(|correlation_id, entry| {
            let age = now.duration_since(entry.last_accessed);
            if age > self.entry_ttl {
                debug!(
                    "CycleGuard[{}]: Removing expired entry for correlation {} (age: {:?})",
                    self.stage_name, correlation_id, age
                );
                false
            } else {
                true
            }
        });

        debug!(
            "CycleGuard[{}]: Cleanup complete, {} entries remaining",
            self.stage_name,
            tracking.len()
        );
    }
}

impl Middleware for CycleGuardMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Only track events with correlation IDs (skip control events)
        let Some(correlation_id) = &event.correlation_id else {
            return MiddlewareAction::Continue;
        };

        // Check if we should run cleanup (every 60 seconds)
        {
            let mut last_cleanup = self.last_cleanup.lock().unwrap();
            let now = Instant::now();
            if now.duration_since(*last_cleanup) > Duration::from_secs(60) {
                debug!("CycleGuard[{}]: Running periodic cleanup", self.stage_name);
                drop(last_cleanup); // Release lock before cleanup
                self.cleanup_expired_entries();
                *self.last_cleanup.lock().unwrap() = now;
            }
        }

        let mut tracking = self.cycle_tracking.lock().unwrap();
        let now = Instant::now();

        // Update or create entry
        let entry = tracking
            .entry(*correlation_id)
            .and_modify(|e| {
                e.iterations += 1;
                e.last_accessed = now;
            })
            .or_insert_with(|| CycleEntry {
                iterations: 1,
                last_accessed: now,
            });

        let iterations = entry.iterations;

        debug!(
            "CycleGuard[{}]: Processing event {} with correlation {} (iteration {} of max {})",
            self.stage_name, event.id, correlation_id, iterations, self.max_iterations
        );

        // Check if we've exceeded the limit
        if iterations > self.max_iterations {
            warn!(
                "CycleGuard[{}]: Aborting event {} - cycle exceeded max iterations ({} > {}) for correlation {}",
                self.stage_name,
                event.id,
                iterations,
                self.max_iterations,
                correlation_id
            );

            // Return the ORIGINAL event with error status to break the cycle
            let mut error_event = event.clone();

            // Set the error status with detailed information
            error_event.processing_info.status =
                obzenflow_core::event::status::processing_status::ProcessingStatus::error(
                    format!(
                        "Cycle limit exceeded after {} iterations (max: {}) for correlation {} in stage {}",
                        iterations,
                        self.max_iterations,
                        correlation_id,
                        self.stage_name
                    ),
                );

            // Return the original event with error status - transforms will skip it
            return MiddlewareAction::Skip(vec![error_event]);
        }

        // Continue processing
        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
        // Cleanup is now handled periodically in pre_handle with TTL
    }
}

/// Factory for creating CycleGuardMiddleware instances
pub struct CycleGuardMiddlewareFactory {
    max_iterations: usize,
}

impl CycleGuardMiddlewareFactory {
    /// Create a factory with default settings (10 iterations max)
    pub fn new() -> Self {
        Self { max_iterations: 10 }
    }

    /// Create a factory with custom iteration limit
    pub fn with_limit(max_iterations: usize) -> Self {
        Self { max_iterations }
    }
}

impl Default for CycleGuardMiddlewareFactory {
    fn default() -> Self {
        Self::new()
    }
}

use crate::middleware::MiddlewareFactory;
use obzenflow_runtime_services::pipeline::config::StageConfig;

impl MiddlewareFactory for CycleGuardMiddlewareFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(CycleGuardMiddleware::new(self.max_iterations, &config.name))
    }

    fn name(&self) -> &str {
        "cycle_guard"
    }
}

/// Convenience function to create a cycle guard middleware factory
pub fn cycle_guard(max_iterations: usize) -> Box<dyn MiddlewareFactory> {
    Box::new(CycleGuardMiddlewareFactory::with_limit(max_iterations))
}
