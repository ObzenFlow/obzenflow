// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cursor-based reader for `MemoryJournal`.

use async_trait::async_trait;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::journal::journal_error::JournalError;
use std::sync::{Arc, Mutex};

use super::journal::MemoryJournalState;
use obzenflow_core::journal::reader::{
    JournalReportStorageReader, ReportScan, ReportScanBudget, ReportScanItem,
};
use obzenflow_core::JournalPayload;

/// Reader for `MemoryJournal`.
///
/// Iterates over the journal as events are appended (tail-like semantics). When
/// it reaches the current end it returns `Ok(None)` for that call, but
/// subsequent calls observe newly appended events.
pub struct MemoryJournalReader<T: JournalEvent> {
    state: Arc<Mutex<MemoryJournalState<T>>>,
    position: u64,
    initial_len: u64,
}

impl<T: JournalEvent> MemoryJournalReader<T> {
    pub(super) fn new(state: Arc<Mutex<MemoryJournalState<T>>>, position: u64) -> Self {
        let len = state.lock().unwrap().events.len() as u64;
        let clamped = position.min(len);
        Self {
            state,
            position: clamped,
            initial_len: len,
        }
    }
}

#[async_trait]
impl<T: JournalEvent> JournalReportStorageReader<T> for MemoryJournalReader<T> {
    async fn storage_next_report(
        &mut self,
        budget: ReportScanBudget,
    ) -> Result<ReportScan<T::Payload>, JournalError> {
        // Yield before cursor mutation, so cancellation cannot lose a report.
        tokio::task::yield_now().await;
        let item = {
            let state = self.state.lock().unwrap();
            let end = (state.events.len() as u64)
                .min(self.position.saturating_add(budget.records.max(1) as u64));
            let end = if self.position < self.initial_len {
                end.min(self.initial_len)
            } else {
                end
            };
            let from = self.position;
            let mut selected = None;
            while self.position < end {
                let row = &state.events[self.position as usize];
                self.position += 1;
                if row.payload.is_supervision_candidate() {
                    // The stored group was validated atomically at append. Skip
                    // unselected records without cloning payloads or provenance.
                    selected = Some(row.clone());
                    break;
                }
            }
            match selected {
                Some(row) => ReportScanItem::Record(Box::new(row)),
                None if self.position > from => ReportScanItem::Progress,
                None => ReportScanItem::Tail {
                    committed_end: true,
                },
            }
        };
        Ok(ReportScan {
            item,
            scanned_bytes: 0,
        })
    }
    fn storage_position(&self) -> u64 {
        self.position
    }
    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Ok(self.position >= self.initial_len)
    }
    fn storage_is_at_end(&self) -> bool {
        self.position >= self.state.lock().unwrap().events.len() as u64
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> obzenflow_core::journal::JournalStorageReader<T>
    for MemoryJournalReader<T>
{
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
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

    fn storage_position(&self) -> u64 {
        self.position
    }

    fn storage_is_at_end(&self) -> bool {
        self.position as usize >= self.state.lock().unwrap().events.len()
    }

    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Ok(self.position >= self.initial_len)
    }
}
