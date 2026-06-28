// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cursor-based reader for `MemoryJournal`.

use async_trait::async_trait;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use std::sync::{Arc, Mutex};

use super::journal::MemoryJournalState;

/// Reader for `MemoryJournal`.
///
/// Iterates over the journal as events are appended (tail-like semantics). When
/// it reaches the current end it returns `Ok(None)` for that call, but
/// subsequent calls observe newly appended events.
pub struct MemoryJournalReader<T: JournalEvent> {
    state: Arc<Mutex<MemoryJournalState<T>>>,
    position: u64,
}

impl<T: JournalEvent> MemoryJournalReader<T> {
    pub(super) fn new(state: Arc<Mutex<MemoryJournalState<T>>>, position: u64) -> Self {
        let len = state.lock().unwrap().events.len() as u64;
        let clamped = position.min(len);
        Self {
            state,
            position: clamped,
        }
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> JournalReader<T> for MemoryJournalReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let env = {
            let state = self.state.lock().unwrap();
            state.events.get(self.position as usize).cloned()
        };

        if env.is_some() {
            self.position += 1;
        } else {
            // This reader is frequently polled inside tight async loops that rely on timers.
            // Without an `.await` point here, `next()` can complete immediately forever and
            // starve the executor (preventing timeouts/other tasks from making progress).
            tokio::task::yield_now().await;
        }
        Ok(env)
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn is_at_end(&self) -> bool {
        self.position as usize >= self.state.lock().unwrap().events.len()
    }
}
