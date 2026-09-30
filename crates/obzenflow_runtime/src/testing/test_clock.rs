// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! [`TestClock`] is a thin wrapper over Tokio paused time (FLOWIP-114h).
//!
//! Its job is to make tests read clearly and fail loudly when used outside a
//! paused runtime. Construction outside `#[tokio::test(start_paused = true)]`
//! returns a [`TestClockError`].

use futures::FutureExt;
use std::time::Duration;
use thiserror::Error;

/// Failure modes for [`TestClock`].
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TestClockError {
    /// `TestClock` was constructed or driven outside a paused Tokio runtime.
    /// Pause the runtime with `#[tokio::test(start_paused = true)]` or
    /// `tokio::time::pause()` before constructing the clock.
    #[error(
        "TestClock requires a paused Tokio runtime; use \
         `#[tokio::test(start_paused = true)]` or `tokio::time::pause()`"
    )]
    RuntimeNotPaused,
}

/// Verified-paused wrapper over `tokio::time::pause` / `advance`.
///
/// All construction and drive paths assert the Tokio runtime is paused. If
/// the assertion fails, the error message names the fix.
#[derive(Debug)]
pub struct TestClock {
    _private: (),
}

impl TestClock {
    /// Construct a `TestClock`. The Tokio runtime must already be paused.
    pub async fn new() -> Result<Self, TestClockError> {
        verify_paused().await?;
        Ok(Self { _private: () })
    }

    /// Advance virtual time by `duration`. Returns an error if the runtime
    /// is no longer paused (for example, somebody called
    /// `tokio::time::resume()` between construction and this call).
    pub async fn advance(&self, duration: Duration) -> Result<(), TestClockError> {
        verify_paused().await?;
        tokio::time::advance(duration).await;
        Ok(())
    }

    /// Sleep for `duration` under paused time. Wakes the calling task when
    /// the runtime advances past `duration`.
    pub async fn sleep(&self, duration: Duration) -> Result<(), TestClockError> {
        verify_paused().await?;
        tokio::time::sleep(duration).await;
        Ok(())
    }
}

/// Verify the Tokio runtime is currently paused.
///
/// Tokio exposes no `is_paused` query. As a probe, we call
/// `tokio::time::advance(Duration::ZERO)` inside `catch_unwind`: it panics when
/// time is not paused, and succeeds when the runtime clock is paused.
///
/// FLOWIP-114h's contract is "fail loudly outside a paused runtime"; we choose
/// a `Result` instead of a panic for that failure mode.
async fn verify_paused() -> Result<(), TestClockError> {
    if tokio::runtime::Handle::try_current().is_err() {
        return Err(TestClockError::RuntimeNotPaused);
    }

    let probe = std::panic::AssertUnwindSafe(tokio::time::advance(Duration::ZERO))
        .catch_unwind()
        .await;

    match probe {
        Ok(()) => Ok(()),
        Err(_) => Err(TestClockError::RuntimeNotPaused),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn new_succeeds_under_paused_runtime() {
        let clock = TestClock::new()
            .await
            .expect("paused runtime should allow TestClock::new");
        // Advance is a no-op observable through `sleep` waking promptly.
        clock
            .advance(Duration::from_millis(100))
            .await
            .expect("advance under paused runtime should succeed");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn new_fails_when_runtime_is_not_paused() {
        let result = TestClock::new().await;
        assert!(
            matches!(result, Err(TestClockError::RuntimeNotPaused)),
            "expected RuntimeNotPaused outside a paused runtime, got {result:?}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sleep_resolves_after_advance() {
        let clock = TestClock::new().await.expect("paused runtime");
        let target = Duration::from_secs(60);

        let sleeper = tokio::time::sleep(target);
        tokio::pin!(sleeper);
        // Poll the actual timer before driving its semantic boundary.
        assert!(futures::poll!(sleeper.as_mut()).is_pending());
        clock
            .advance(target - Duration::from_secs(1))
            .await
            .expect("advance before deadline");
        assert!(futures::poll!(sleeper.as_mut()).is_pending());
        clock
            .advance(Duration::from_secs(1))
            .await
            .expect("advance to deadline");
        sleeper.await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sleep_uses_the_paused_clock() {
        let clock = TestClock::new().await.expect("paused runtime");
        let started = tokio::time::Instant::now();
        clock.sleep(Duration::from_secs(60)).await.unwrap();
        assert_eq!(started.elapsed(), Duration::from_secs(60));
    }
}
