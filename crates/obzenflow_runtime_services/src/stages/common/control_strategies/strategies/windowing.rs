//! Windowing strategy for time-based event aggregation

use std::time::{Duration, Instant};
use super::super::{ControlEventStrategy, ControlEventAction, ProcessingContext};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;

/// Strategy that delays EOF to allow time-based windows to complete
/// 
/// This strategy is used by windowing middleware to ensure that all
/// time windows have a chance to emit their final aggregated results
/// before the stage terminates.
pub struct WindowingStrategy {
    /// Duration of the time window
    pub window_duration: Duration,
    
    /// When the current window started (set by middleware)
    pub window_start: Option<Instant>,
}

impl WindowingStrategy {
    pub fn new(window_duration: Duration) -> Self {
        Self {
            window_duration,
            window_start: None,
        }
    }
    
    /// Check if the current window is complete
    fn is_window_complete(&self) -> bool {
        match self.window_start {
            None => true, // No window active
            Some(start) => start.elapsed() >= self.window_duration,
        }
    }
    
    /// Calculate remaining time in current window
    fn remaining_window_time(&self) -> Option<Duration> {
        self.window_start.map(|start| {
            let elapsed = start.elapsed();
            if elapsed < self.window_duration {
                self.window_duration - elapsed
            } else {
                Duration::ZERO
            }
        })
    }
}

impl ControlEventStrategy for WindowingStrategy {
    fn handle_eof(&self, _envelope: &EventEnvelope<ChainEvent>, ctx: &mut ProcessingContext) -> ControlEventAction {
        if !self.is_window_complete() {
            if let Some(remaining) = self.remaining_window_time() {
                tracing::info!(
                    "Windowing strategy: delaying EOF for {}ms to complete window",
                    remaining.as_millis()
                );
                
                // Mark that we're in a delay period
                ctx.in_delay = true;
                
                ControlEventAction::Delay(remaining)
            } else {
                // Window complete or no active window
                ControlEventAction::Forward
            }
        } else {
            // Window already complete
            tracing::debug!("Windowing strategy: window complete, forwarding EOF");
            ControlEventAction::Forward
        }
    }
}