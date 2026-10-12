// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: acknowledgement deadline and idempotency window.

use std::time::Duration;

/// `Retry-After` advised with every `outcome_unknown` answer.
pub const OUTCOME_UNKNOWN_RETRY_AFTER: Duration = Duration::from_secs(1);

/// How long admission waits for a receipt, and how long a committed request
/// identity deduplicates retries. The window always covers one advised retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngressTiming {
    ack_deadline: Duration,
    idempotency_window: Duration,
}

/// A timing pair the contract cannot honour.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidIngressTiming {
    #[error("the acknowledgement deadline must be at least 1 second")]
    DeadlineTooShort,
    #[error(
        "the idempotency window must exceed the acknowledgement deadline plus the \
         outcome_unknown Retry-After"
    )]
    WindowTooShort,
}

impl IngressTiming {
    pub const DEFAULT_ACK_DEADLINE: Duration = Duration::from_secs(10);
    pub const DEFAULT_IDEMPOTENCY_WINDOW: Duration = Duration::from_secs(86_400);

    pub fn new(
        ack_deadline: Duration,
        idempotency_window: Duration,
    ) -> Result<Self, InvalidIngressTiming> {
        if ack_deadline < Duration::from_secs(1) {
            return Err(InvalidIngressTiming::DeadlineTooShort);
        }
        if idempotency_window <= ack_deadline.saturating_add(OUTCOME_UNKNOWN_RETRY_AFTER) {
            return Err(InvalidIngressTiming::WindowTooShort);
        }
        Ok(Self {
            ack_deadline,
            idempotency_window,
        })
    }

    pub fn ack_deadline(&self) -> Duration {
        self.ack_deadline
    }

    pub fn idempotency_window(&self) -> Duration {
        self.idempotency_window
    }
}

impl Default for IngressTiming {
    fn default() -> Self {
        Self {
            ack_deadline: Self::DEFAULT_ACK_DEADLINE,
            idempotency_window: Self::DEFAULT_IDEMPOTENCY_WINDOW,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(value: u64) -> Duration {
        Duration::from_secs(value)
    }

    #[test]
    fn window_must_cover_one_advised_retry() {
        assert_eq!(
            IngressTiming::new(secs(10), secs(11)),
            Err(InvalidIngressTiming::WindowTooShort)
        );
        assert!(IngressTiming::new(secs(10), secs(12)).is_ok());
        assert_eq!(
            IngressTiming::new(Duration::from_millis(999), secs(60)),
            Err(InvalidIngressTiming::DeadlineTooShort)
        );
    }

    #[test]
    fn defaults_satisfy_the_contract() {
        let timing = IngressTiming::default();
        assert_eq!(
            IngressTiming::new(timing.ack_deadline(), timing.idempotency_window()),
            Ok(timing)
        );
    }
}
