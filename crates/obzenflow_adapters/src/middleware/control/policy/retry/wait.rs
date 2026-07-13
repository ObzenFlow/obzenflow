// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::stages::common::{BoundaryStopIntent, BoundaryStopReceiver};
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryWaitError {
    Deadline,
    Drain,
    Abort,
}

/// Await admission or backoff. Drain and abort both prevent a new execution.
pub(crate) async fn await_before_execution<F, T>(
    future: F,
    deadline: tokio::time::Instant,
    stop: &mut BoundaryStopReceiver,
) -> Result<T, RetryWaitError>
where
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(RetryWaitError::Deadline);
    }
    match stop.intent() {
        BoundaryStopIntent::Drain => return Err(RetryWaitError::Drain),
        BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
        BoundaryStopIntent::Running => {}
    }
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(RetryWaitError::Deadline),
            intent = stop.changed() => match intent {
                BoundaryStopIntent::Running => {}
                BoundaryStopIntent::Drain => return Err(RetryWaitError::Drain),
                BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
            },
            output = &mut future => return Ok(output),
        }
    }
}

/// Await an already-started physical call. Drain is remembered but does not
/// cancel it; deadline and force abort drop the local future promptly.
pub(crate) async fn await_active_execution<S, F, T>(
    start: S,
    deadline: tokio::time::Instant,
    stop: &mut BoundaryStopReceiver,
) -> Result<(T, bool), RetryWaitError>
where
    S: FnOnce() -> F,
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(RetryWaitError::Deadline);
    }
    match stop.intent() {
        BoundaryStopIntent::Running => {}
        BoundaryStopIntent::Drain => return Err(RetryWaitError::Drain),
        BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
    }
    let future = start();
    let mut drain_requested = false;
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(RetryWaitError::Deadline),
            intent = stop.changed() => match intent {
                BoundaryStopIntent::Running => {}
                BoundaryStopIntent::Drain => drain_requested = true,
                BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
            },
            output = &mut future => return Ok((output, drain_requested)),
        }
    }
}

/// Await asynchronous post-call settlement for a result that is already
/// known. Graceful drain is remembered but does not cancel settlement;
/// deadline and force abort remain terminal interruption points.
pub(crate) async fn await_settlement<F, T>(
    future: F,
    deadline: tokio::time::Instant,
    stop: &mut BoundaryStopReceiver,
) -> Result<(T, bool), RetryWaitError>
where
    F: Future<Output = T>,
{
    if tokio::time::Instant::now() >= deadline {
        return Err(RetryWaitError::Deadline);
    }
    let mut drain_requested = matches!(stop.intent(), BoundaryStopIntent::Drain);
    if matches!(stop.intent(), BoundaryStopIntent::Abort) {
        return Err(RetryWaitError::Abort);
    }
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = tokio::time::sleep_until(deadline) => return Err(RetryWaitError::Deadline),
            intent = stop.changed() => match intent {
                BoundaryStopIntent::Running => {}
                BoundaryStopIntent::Drain => drain_requested = true,
                BoundaryStopIntent::Abort => return Err(RetryWaitError::Abort),
            },
            output = &mut future => return Ok((output, drain_requested)),
        }
    }
}
