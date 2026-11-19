// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Blocking destination I/O for a sink's system.

use obzenflow_runtime::effects::EffectError;

pub(crate) async fn blocking_io<T: Send + 'static>(
    io: impl FnOnce() -> T + Send + 'static,
) -> Result<T, EffectError> {
    match tokio::task::spawn_blocking(io).await {
        Ok(value) => Ok(value),
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(EffectError::Execution(format!(
            "sink I/O did not run: {error}"
        ))),
    }
}
