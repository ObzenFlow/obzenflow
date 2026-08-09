// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// FLOWIP-080c: OnEOF Emission Strategy
//
// Emits results only when EOF is received, used for batch processing
// and final aggregations.

use super::EmissionStrategy;
use std::time::Duration;

/// Emit only on EOF (completion).
///
/// This strategy accumulates all results and emits them only when the
/// stream completes. Useful for batch processing and final aggregations.
///
/// # Examples
///
/// ```ignore
/// use obzenflow_runtime::stages::stateful::strategies::emissions::OnEOF;
///
/// let strategy = OnEOF::new();
/// assert!(!strategy.should_emit(100, None));
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct OnEOF;

impl OnEOF {
    /// Create a new OnEOF emission strategy.
    pub fn new() -> Self {
        Self
    }
}

impl EmissionStrategy for OnEOF {
    fn should_emit(&self, _events_seen: u64, _period_elapsed: Option<Duration>) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_on_eof_does_not_emit_without_eof() {
        let strategy = OnEOF::new();
        assert!(!strategy.should_emit(0, None));
        assert!(!strategy.should_emit(100, None));
        assert!(!strategy.should_emit(1000, None));
    }

    #[test]
    fn test_on_eof_is_terminal_only() {
        let strategy = OnEOF::new();
        assert!(!strategy.should_emit(100, None));
        assert!(!strategy.should_emit(200, Some(Duration::from_secs(1))));
    }
}
