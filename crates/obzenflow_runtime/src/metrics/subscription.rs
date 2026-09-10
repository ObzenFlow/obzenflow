// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Observation cursors use physical journal positions. Stage EOF and reader
//! credits belong to dataflow consumers, never to this metrics observer.

use crate::messaging::PollResult;
use obzenflow_core::journal::{Journal, JournalError, JournalReader};
use obzenflow_core::{ChainEvent, StageId};
use std::sync::Arc;

pub(crate) struct MetricsSubscription {
    readers: Vec<(StageId, Box<dyn JournalReader<ChainEvent>>)>,
    next: usize,
    last_stage: Option<StageId>,
}

impl MetricsSubscription {
    pub(crate) async fn new(
        journals: &[(StageId, Arc<dyn Journal<ChainEvent>>)],
    ) -> Result<Self, JournalError> {
        let mut readers = Vec::with_capacity(journals.len());
        for (id, journal) in journals {
            readers.push((*id, journal.reader().await?));
        }
        Ok(Self {
            readers,
            next: 0,
            last_stage: None,
        })
    }

    pub(crate) fn last_delivered_upstream_stage(&self) -> Option<StageId> {
        self.last_stage
    }

    pub(crate) async fn poll_next(&mut self) -> PollResult<ChainEvent> {
        let len = self.readers.len();
        for _ in 0..len {
            let index = self.next;
            let (stage, reader) = &mut self.readers[index];
            // Advance round-robin only after the read settles. Dropping a read
            // cannot silently move the journal cursor to a different record.
            let result = reader.next().await;
            self.next = (index + 1) % len;
            match result {
                Ok(Some(envelope)) => {
                    self.last_stage = Some(*stage);
                    return PollResult::Event(envelope);
                }
                Ok(None) => {}
                Err(error) => return PollResult::Error(Box::new(error)),
            }
        }
        PollResult::NoEvents
    }
}
