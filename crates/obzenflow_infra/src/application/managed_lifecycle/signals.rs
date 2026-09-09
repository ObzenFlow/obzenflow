// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::machine::StopReason;
use crate::application::{flow_application::ShutdownSignal, ApplicationError};

pub(super) struct Signals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(test)]
    injected: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
    #[cfg(test)]
    test_only: bool,
}

impl Signals {
    pub fn new(
        #[cfg(test)] injected: Option<tokio::sync::oneshot::Receiver<ShutdownSignal>>,
    ) -> Result<Self, ApplicationError> {
        Ok(Self {
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
            #[cfg(test)]
            test_only: injected.is_some(),
            #[cfg(test)]
            injected,
        })
    }

    async fn recv(&mut self) -> ShutdownSignal {
        #[cfg(test)]
        if self.test_only {
            return match &mut self.injected {
                Some(receiver) => {
                    let signal = receiver.await.unwrap_or(ShutdownSignal::Sigint);
                    self.injected.take();
                    signal
                }
                None => std::future::pending().await,
            };
        }
        #[cfg(unix)]
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => ShutdownSignal::Sigint,
                _ = self.terminate.recv() => ShutdownSignal::Sigterm,
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
            ShutdownSignal::Sigint
        }
    }
}

pub(super) async fn next(signals: &mut Option<Signals>) -> StopReason {
    match signals {
        Some(signals) => match signals.recv().await {
            ShutdownSignal::Sigint => StopReason::Cancel,
            ShutdownSignal::Sigterm => StopReason::Graceful,
        },
        None => std::future::pending().await,
    }
}
