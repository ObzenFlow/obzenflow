// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ephemeral stop intent shared with live-I/O boundary coordinators.
//!
//! This channel carries runtime task intent only. It is not durable domain
//! state, is never journalled, and is never consulted during replay.

use std::sync::Arc;
use tokio::sync::watch;

/// Monotonic stop intent observed by an active live boundary invocation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum BoundaryStopIntent {
    /// Normal execution may admit and start physical attempts.
    #[default]
    Running,
    /// Let an active attempt settle, but cancel admission/backoff and start no
    /// further physical attempt.
    Drain,
    /// Drop active asynchronous work promptly. No synthetic terminal outcome
    /// is implied by this local cancellation.
    Abort,
}

/// Runtime-owned writer for a boundary stop channel.
#[derive(Debug, Clone)]
pub struct BoundaryStopController {
    sender: Arc<watch::Sender<BoundaryStopIntent>>,
}

/// Cloneable receiver handed to a live boundary invocation.
#[derive(Debug, Clone)]
pub struct BoundaryStopReceiver {
    receiver: watch::Receiver<BoundaryStopIntent>,
    // Keep the channel open even when an invocation is constructed without a
    // long-lived controller. Channel closure must not masquerade as Abort.
    _keepalive: Arc<watch::Sender<BoundaryStopIntent>>,
}

/// Create one ephemeral, monotonic boundary stop channel.
pub fn boundary_stop_channel() -> (BoundaryStopController, BoundaryStopReceiver) {
    let (sender, receiver) = watch::channel(BoundaryStopIntent::Running);
    let sender = Arc::new(sender);
    (
        BoundaryStopController {
            sender: sender.clone(),
        },
        BoundaryStopReceiver {
            receiver,
            _keepalive: sender,
        },
    )
}

impl Default for BoundaryStopReceiver {
    fn default() -> Self {
        boundary_stop_channel().1
    }
}

impl BoundaryStopController {
    /// Request graceful drain. An existing abort intent is never downgraded.
    pub fn request_drain(&self) {
        self.advance(BoundaryStopIntent::Drain);
    }

    /// Request immediate cancellation. Abort is terminal for this channel.
    pub fn request_abort(&self) {
        self.advance(BoundaryStopIntent::Abort);
    }

    /// Inspect the current intent without subscribing.
    pub fn intent(&self) -> BoundaryStopIntent {
        *self.sender.borrow()
    }

    fn advance(&self, requested: BoundaryStopIntent) {
        self.sender.send_if_modified(|current| {
            let next = match (*current, requested) {
                (BoundaryStopIntent::Abort, _) => BoundaryStopIntent::Abort,
                (_, BoundaryStopIntent::Abort) => BoundaryStopIntent::Abort,
                (BoundaryStopIntent::Drain, _) => BoundaryStopIntent::Drain,
                (_, BoundaryStopIntent::Drain) => BoundaryStopIntent::Drain,
                (BoundaryStopIntent::Running, BoundaryStopIntent::Running) => {
                    BoundaryStopIntent::Running
                }
            };
            if *current == next {
                false
            } else {
                *current = next;
                true
            }
        });
    }
}

impl BoundaryStopReceiver {
    /// Inspect the current intent without waiting.
    pub fn intent(&self) -> BoundaryStopIntent {
        *self.receiver.borrow()
    }

    /// Wait until the intent changes and return its new value.
    pub async fn changed(&mut self) -> BoundaryStopIntent {
        // `_keepalive` means closure is not expected while this receiver
        // exists. Treat it conservatively as Abort if the implementation ever
        // changes and closure becomes possible.
        if self.receiver.changed().await.is_err() {
            return BoundaryStopIntent::Abort;
        }
        self.intent()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stop_intent_is_observable_and_monotonic() {
        let (controller, mut receiver) = boundary_stop_channel();
        assert_eq!(receiver.intent(), BoundaryStopIntent::Running);

        controller.request_drain();
        assert_eq!(receiver.changed().await, BoundaryStopIntent::Drain);

        controller.request_abort();
        assert_eq!(receiver.changed().await, BoundaryStopIntent::Abort);

        controller.request_drain();
        assert_eq!(controller.intent(), BoundaryStopIntent::Abort);
        assert_eq!(receiver.intent(), BoundaryStopIntent::Abort);
    }

    #[test]
    fn default_receiver_stays_running_without_an_external_controller() {
        let receiver = BoundaryStopReceiver::default();
        assert_eq!(receiver.intent(), BoundaryStopIntent::Running);
    }
}
