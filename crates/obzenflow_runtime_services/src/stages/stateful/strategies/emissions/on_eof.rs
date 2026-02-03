// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: OnEOF Emission Strategy
//
// Emits results only when EOF is received, used for batch processing
// and final aggregations.

use super::EmissionStrategy;
use std::time::Instant;

/// Emit only on EOF (completion).
///
/// This strategy accumulates all results and emits them only when the
/// stream completes. Useful for batch processing and final aggregations.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime_services::stages::stateful::strategies::emissions::OnEOF;
///
/// let mut strategy = OnEOF::new();
/// assert!(!strategy.should_emit(100, None));  // Not at EOF
/// strategy.notify_eof();
/// assert!(strategy.should_emit(100, None));   // At EOF, emit now
/// ```
#[derive(Debug, Clone)]
pub struct OnEOF {
    eof_received: bool,
}

impl OnEOF {
    /// Create a new OnEOF emission strategy.
    pub fn new() -> Self {
        Self {
            eof_received: false,
        }
    }

    /// Mark that EOF has been received.
    ///
    /// This method is typically called by the supervisor when an EOF
    /// control event is received from upstream.
    pub fn set_eof(&mut self) {
        self.eof_received = true;
    }
}

impl Default for OnEOF {
    fn default() -> Self {
        Self::new()
    }
}

impl EmissionStrategy for OnEOF {
    fn should_emit(&mut self, _events_seen: u64, _last_emit: Option<Instant>) -> bool {
        self.eof_received
    }

    fn reset(&mut self) {
        // OnEOF only emits once at the end, no reset needed
        // After EOF, the pipeline is complete
    }

    fn notify_eof(&mut self) {
        self.set_eof();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_on_eof_does_not_emit_without_eof() {
        let mut strategy = OnEOF::new();
        assert!(!strategy.should_emit(0, None));
        assert!(!strategy.should_emit(100, None));
        assert!(!strategy.should_emit(1000, None));
    }

    #[test]
    fn test_on_eof_emits_when_eof_set() {
        let mut strategy = OnEOF::new();
        assert!(!strategy.should_emit(100, None));

        strategy.set_eof();
        assert!(strategy.should_emit(100, None));
        assert!(strategy.should_emit(200, None)); // Continues to return true
    }

    #[test]
    fn test_on_eof_notify() {
        let mut strategy = OnEOF::new();
        assert!(!strategy.should_emit(100, None));

        strategy.notify_eof();
        assert!(strategy.should_emit(100, None));
    }
}
