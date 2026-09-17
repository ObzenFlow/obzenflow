// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::JournalEvent;
use std::time::Duration;

/// Optional journal attachments are dense unless explicitly throttled.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ObservabilityPolicy {
    #[default]
    EveryRecord,
    Periodic {
        interval: Duration,
    },
}

/// A selected journal record may construct its diagnostics lazily. The member
/// index lets an atomic group project the correct committed prefix.
pub type ObservationCapture<T> = Box<dyn FnMut(usize, T) -> T + Send>;

pub enum JournalCapture<T: JournalEvent> {
    Live(Option<ObservationCapture<T>>),
    /// Republish recorded attachments without sampling or changing their stamps.
    Historical,
}

impl<T: JournalEvent> Default for JournalCapture<T> {
    fn default() -> Self {
        Self::Live(None)
    }
}
