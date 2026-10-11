// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Connector code that may block runs on the blocking pool, never on an async
//! worker (FLOWIP-134i B18, B21). The runtime owner bounds how long exit waits
//! for a call that never returns (B20).

use crate::stages::common::handler_error::StageFatal;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};

/// The connector call a stage runs on the blocking pool.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ConnectorCall {
    Open,
    Seek,
    Poll,
    Release,
}

/// Reports a caller dropped mid-call, as a stop deadline's abort does. The
/// call keeps its blocking-pool thread until it returns.
struct AbandonedCall<'a> {
    stage_name: &'a str,
    call: ConnectorCall,
    settled: bool,
}

impl Drop for AbandonedCall<'_> {
    fn drop(&mut self) {
        if !self.settled {
            tracing::warn!(
                stage_name = %self.stage_name,
                call = ?self.call,
                "connector call abandoned in progress; it holds a blocking-pool thread until it \
                 returns"
            );
        }
    }
}

/// Runs one synchronous reader call on the blocking pool, so connector code
/// never blocks an async worker. The reader moves into the call and back, so
/// calls on one reader never overlap.
pub(crate) async fn on_blocking_pool<H, T>(
    handler: &mut Option<H>,
    stage_name: &str,
    kind: ConnectorCall,
    call: impl FnOnce(&mut H) -> T + Send + 'static,
) -> Result<T, StageFatal>
where
    H: Send + 'static,
    T: Send + 'static,
{
    let mut reader = handler.take().expect("handler available before cleanup");
    let mut in_flight = AbandonedCall {
        stage_name,
        call: kind,
        settled: false,
    };
    let joined = tokio::task::spawn_blocking(move || {
        let result = call(&mut reader);
        (reader, result)
    })
    .await;
    in_flight.settled = true;
    match joined {
        Ok((reader, result)) => {
            *handler = Some(reader);
            Ok(result)
        }
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(StageFatal::new(
            StageFatalCode::Coordination,
            StageFatalReason::CoordinationFailure,
            format!("synchronous source call did not run: {error}"),
        )),
    }
}

/// Drops a source reader or sink writer on the blocking pool, because `Drop`
/// is connector code, such as closing a file.
pub(crate) async fn release_on_blocking_pool<H: Send + 'static>(handler: H, stage_name: &str) {
    let mut in_flight = AbandonedCall {
        stage_name,
        call: ConnectorCall::Release,
        settled: false,
    };
    let released = tokio::task::spawn_blocking(move || drop(handler)).await;
    in_flight.settled = true;
    if let Err(error) = released {
        // A cancelled release means runtime shutdown already dropped the handler.
        if error.is_panic() {
            std::panic::resume_unwind(error.into_panic());
        }
    }
}
