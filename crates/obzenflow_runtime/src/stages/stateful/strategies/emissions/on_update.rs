// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::EmissionStrategy;
use std::time::Duration;

/// Emits after every input, publishing only the groups that input updated.
///
/// Only [`GroupBy::emit_on_update`](crate::stages::stateful::GroupBy::emit_on_update)
/// builds one, so no other accumulator can select it:
///
/// ```compile_fail,E0423
/// use obzenflow_runtime::stages::stateful::OnUpdate;
///
/// let _ = OnUpdate(());
/// ```
///
/// ```compile_fail,E0624
/// use obzenflow_runtime::stages::stateful::OnUpdate;
///
/// let _ = OnUpdate::new();
/// ```
#[derive(Debug, Clone, Copy)]
pub struct OnUpdate(());

impl OnUpdate {
    pub(crate) fn new() -> Self {
        Self(())
    }
}

impl EmissionStrategy for OnUpdate {
    fn should_emit(&self, events_seen: u64, _period_elapsed: Option<Duration>) -> bool {
        events_seen > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emits_after_every_input_and_keeps_the_state() {
        let strategy = OnUpdate::new();

        assert!(!strategy.should_emit(0, None));
        assert!(strategy.should_emit(1, None));
        assert!(strategy.should_emit(1, Some(Duration::ZERO)));
        assert!(!strategy.resets_accumulator_on_emit());
        assert_eq!(strategy.emit_interval_hint(), None);
    }
}
