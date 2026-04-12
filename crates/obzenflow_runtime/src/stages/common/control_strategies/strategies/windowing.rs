// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Windowing strategy for time-based event aggregation

use super::super::{ControlEventAction, ControlEventStrategy, ProcessingContext};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Strategy that delays EOF to allow time-based windows to complete
///
/// This strategy is used by windowing middleware to ensure that all
/// time windows have a chance to emit their final aggregated results
/// before the stage terminates.
pub struct WindowingStrategy {
    /// Duration of the time window
    pub window_duration: Duration,

    /// When the current window started (shared with middleware)
    pub window_start: Arc<RwLock<Option<Instant>>>,
}

impl WindowingStrategy {
    pub fn new(window_duration: Duration) -> Self {
        Self::with_shared_window_start(window_duration, Arc::new(RwLock::new(None)))
    }

    pub fn with_shared_window_start(
        window_duration: Duration,
        window_start: Arc<RwLock<Option<Instant>>>,
    ) -> Self {
        Self {
            window_duration,
            window_start,
        }
    }

    /// Check if the current window is complete
    fn is_window_complete(&self) -> bool {
        let window_start = self.window_start.read().unwrap();
        match *window_start {
            None => true, // No window active
            Some(start) => start.elapsed() >= self.window_duration,
        }
    }

    /// Calculate remaining time in current window
    fn remaining_window_time(&self) -> Option<Duration> {
        let window_start = self.window_start.read().unwrap();
        (*window_start).map(|start| {
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
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
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

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};

    #[test]
    fn windowing_strategy_delays_eof_when_shared_window_is_active() {
        let shared = Arc::new(RwLock::new(Some(Instant::now())));
        let strategy =
            WindowingStrategy::with_shared_window_start(Duration::from_millis(50), shared);
        let eof = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);
        let envelope = EventEnvelope::new(obzenflow_core::JournalWriterId::new(), eof);
        let mut ctx = ProcessingContext::new();

        let action = strategy.handle_eof(&envelope, &mut ctx);

        match action {
            ControlEventAction::Delay(delay) => {
                assert!(delay > Duration::ZERO);
                assert!(delay <= Duration::from_millis(50));
            }
            other => panic!("expected Delay while window is active, got {other:?}"),
        }
    }
}
