// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Blocking destination I/O for async sink writers (FLOWIP-134i B21).

use obzenflow_runtime::stages::common::handlers::{SinkOperationError, SinkOperationResult};

/// Runs blocking destination I/O on Tokio's blocking pool. Writer methods run
/// on async workers, where a blocked syscall stalls the worker and cannot be
/// aborted by a stop.
pub(crate) async fn blocking_io<T: Send + 'static>(
    io: impl FnOnce() -> T + Send + 'static,
) -> SinkOperationResult<T> {
    match tokio::task::spawn_blocking(io).await {
        Ok(value) => Ok(value),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(SinkOperationError::other(format!(
            "sink I/O did not run: {error}"
        ))),
    }
}
