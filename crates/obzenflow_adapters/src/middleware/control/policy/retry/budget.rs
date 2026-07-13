// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::BoundaryRetryPolicy;
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use std::num::NonZeroU32;
use std::time::Duration;

/// One invocation's hard retry budget, anchored before initial admission.
pub(crate) struct RetryBudget {
    pub(crate) policy: BoundaryRetryPolicy,
    pub(super) started: tokio::time::Instant,
    deadline: tokio::time::Instant,
}

impl RetryBudget {
    pub(crate) fn new(policy: BoundaryRetryPolicy) -> Option<Self> {
        let started = tokio::time::Instant::now();
        let deadline = started.checked_add(policy.max_total_wall_time)?;
        Some(Self {
            policy,
            started,
            deadline,
        })
    }

    pub(crate) fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    pub(crate) fn can_start_execution(&self) -> bool {
        tokio::time::Instant::now() < self.deadline
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis().min(u64::MAX as u128) as u64
    }

    pub(crate) fn remaining_ms(&self) -> u64 {
        self.deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or_default()
            .as_millis()
            .min(u64::MAX as u128) as u64
    }

    pub(crate) fn next_attempt(&self, completed: NonZeroU32) -> Option<NonZeroU32> {
        let next = NonZeroU32::new(completed.get().checked_add(1)?)?;
        (next <= self.policy.max_attempts).then_some(next)
    }

    pub(crate) fn delay_after(
        &self,
        completed: NonZeroU32,
        retry_after: Option<Duration>,
    ) -> Result<Duration, RetryExhaustionCause> {
        if retry_after.is_some_and(|hint| hint > self.policy.max_single_delay) {
            return Err(RetryExhaustionCause::RetryHintExceedsLimit);
        }
        let configured = self.policy.configured_delay_after(completed);
        let delay = retry_after.map_or(configured, |hint| configured.max(hint));
        let now = tokio::time::Instant::now();
        if let Some(hint) = retry_after {
            let Some(hinted_start) = now.checked_add(hint) else {
                return Err(RetryExhaustionCause::RetryHintExceedsLimit);
            };
            if hinted_start >= self.deadline {
                return Err(RetryExhaustionCause::RetryHintExceedsLimit);
            }
        }
        let Some(start_at) = now.checked_add(delay) else {
            return Err(RetryExhaustionCause::TotalWallTime);
        };
        if start_at >= self.deadline {
            return Err(RetryExhaustionCause::TotalWallTime);
        }
        Ok(delay)
    }
}
