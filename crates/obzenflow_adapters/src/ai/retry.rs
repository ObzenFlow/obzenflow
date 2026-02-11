// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::AiClientError;
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AiRetryDecision {
    RetryAfter(Duration),
    DoNotRetry,
}

/// Retry/backoff policy for outer AI provider call paths.
#[derive(Debug, Clone)]
pub struct AiRetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub rate_limit_max_wait: Duration,
}

impl Default for AiRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(4),
            rate_limit_max_wait: Duration::from_secs(10),
        }
    }
}

impl AiRetryPolicy {
    /// Disable this retry layer (single attempt).
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            ..Self::default()
        }
    }

    pub fn decision_for(&self, err: &AiClientError, attempt: u32) -> AiRetryDecision {
        if attempt >= self.max_attempts {
            return AiRetryDecision::DoNotRetry;
        }

        match err {
            AiClientError::Timeout { .. } | AiClientError::Remote { .. } => {
                AiRetryDecision::RetryAfter(self.backoff_for_attempt(attempt))
            }
            AiClientError::RateLimited { retry_after, .. } => {
                let wait = retry_after.unwrap_or(self.backoff_for_attempt(attempt));
                if wait > self.rate_limit_max_wait {
                    AiRetryDecision::DoNotRetry
                } else {
                    AiRetryDecision::RetryAfter(wait)
                }
            }
            AiClientError::Auth { .. }
            | AiClientError::InvalidRequest { .. }
            | AiClientError::Unsupported { .. }
            | AiClientError::Other { .. } => AiRetryDecision::DoNotRetry,
        }
    }

    fn backoff_for_attempt(&self, attempt: u32) -> Duration {
        let step = attempt.saturating_sub(1);
        let multiplier = if step >= 20 {
            1u128 << 20
        } else {
            1u128 << step
        };
        let base = self.initial_backoff.as_millis();
        let delay_ms = base.saturating_mul(multiplier);
        let capped = delay_ms.min(self.max_backoff.as_millis());
        Duration::from_millis(capped as u64)
    }
}

/// Execute an async operation with retry/backoff according to `policy`.
///
/// `operation` receives the 1-based attempt number.
pub async fn execute_with_retry<T, F, Fut>(
    policy: &AiRetryPolicy,
    mut operation: F,
) -> Result<T, AiClientError>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T, AiClientError>>,
{
    let mut attempt = 1u32;

    loop {
        match operation(attempt).await {
            Ok(value) => return Ok(value),
            Err(err) => match policy.decision_for(&err, attempt) {
                AiRetryDecision::RetryAfter(wait) => {
                    tracing::debug!(
                        attempt,
                        wait_ms = wait.as_millis() as u64,
                        "retrying ai provider call"
                    );
                    tokio::time::sleep(wait).await;
                    attempt = attempt.saturating_add(1);
                }
                AiRetryDecision::DoNotRetry => return Err(err),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn invalid_request_is_not_retryable() {
        let policy = AiRetryPolicy::default();
        let decision = policy.decision_for(
            &AiClientError::InvalidRequest {
                message: "bad payload".to_string(),
            },
            1,
        );
        assert_eq!(decision, AiRetryDecision::DoNotRetry);
    }

    #[tokio::test]
    async fn retries_remote_until_success() {
        let policy = AiRetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(2),
            rate_limit_max_wait: Duration::from_millis(5),
        };

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();

        let result = execute_with_retry(&policy, move |_attempt| {
            let calls = calls_clone.clone();
            async move {
                let n = calls.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    Err(AiClientError::Remote {
                        message: "transient".to_string(),
                    })
                } else {
                    Ok(42)
                }
            }
        })
        .await
        .expect("operation should eventually succeed");

        assert_eq!(result, 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }
}
