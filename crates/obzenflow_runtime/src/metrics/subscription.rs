// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Physical observation cursors. Stage EOF and reader credits belong to
//! dataflow consumers, never to this metrics observer.

use obzenflow_core::journal::{Journal, JournalError, JournalReader};
use obzenflow_core::{ChainEvent, EventEnvelope, JournalId, StageId};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

pub(super) const IDLE_BACKOFF: Duration = Duration::from_millis(10);
const MAX_BATCH_RECORDS: usize = 64;
const BATCH_QUANTUM: Duration = Duration::from_millis(4);

struct Cursor {
    journal: JournalId,
    stage: StageId,
    reader: Box<dyn JournalReader<ChainEvent>>,
    retry_at: Option<Instant>,
    end_observed: bool,
}

pub(crate) struct MetricsBatch {
    pub(crate) stage: StageId,
    pub(crate) events: Vec<EventEnvelope<ChainEvent>>,
}

pub(crate) struct MetricsSubscription {
    readers: Vec<Cursor>,
    next: usize,
    terminal_observed: bool,
    pending_error: Option<JournalError>,
}

impl MetricsSubscription {
    pub(crate) async fn new(
        journals: &[(StageId, Arc<dyn Journal<ChainEvent>>)],
    ) -> Result<Self, JournalError> {
        let mut readers = Vec::with_capacity(journals.len());
        for (stage, journal) in journals {
            readers.push(Cursor {
                journal: *journal.id(),
                stage: *stage,
                reader: journal.reader().await?,
                retry_at: None,
                end_observed: false,
            });
        }
        Ok(Self {
            readers,
            next: 0,
            terminal_observed: false,
            pending_error: None,
        })
    }

    /// Called only after folding the current pipeline writer's terminal fact.
    pub(crate) fn observe_terminal(&mut self) {
        if self.terminal_observed {
            return;
        }
        self.terminal_observed = true;
        for cursor in &mut self.readers {
            cursor.retry_at = None;
            cursor.end_observed = false;
        }
    }

    /// The owner calls this only between completely folded batches.
    pub(crate) fn is_complete(&self) -> bool {
        self.terminal_observed
            && self.pending_error.is_none()
            && self.readers.iter().all(|cursor| cursor.end_observed)
    }

    pub(crate) fn next_probe(&self) -> Option<Instant> {
        self.readers
            .iter()
            .filter(|cursor| !cursor.end_observed)
            .map(|cursor| cursor.retry_at.unwrap_or_else(Instant::now))
            .min()
    }

    pub(crate) async fn poll_batch(&mut self) -> Result<Option<MetricsBatch>, JournalError> {
        // A successful prefix has already been returned for folding. Deliver
        // its retained failure before any later read on this subscription.
        if let Some(error) = self.pending_error.take() {
            return Err(error);
        }
        let len = self.readers.len();
        for _ in 0..len {
            let index = self.next;
            let cursor = &mut self.readers[index];
            if cursor.end_observed || cursor.retry_at.is_some_and(|at| at > Instant::now()) {
                self.next = (index + 1) % len;
                continue;
            }
            let started = Instant::now();
            let mut events = Vec::with_capacity(MAX_BATCH_RECORDS);
            loop {
                // Never time out a pending read to enforce the batch quantum.
                let result = cursor.reader.next().await;
                self.next = (index + 1) % len;
                match result {
                    Ok(Some(row)) => {
                        cursor.retry_at = None;
                        events.push(row);
                    }
                    Ok(None) => {
                        cursor.retry_at = Some(Instant::now() + IDLE_BACKOFF);
                        cursor.end_observed = self.terminal_observed && cursor.reader.is_at_end();
                        if cursor.end_observed {
                            tracing::debug!(journal_id = %cursor.journal, stage_id = %cursor.stage,
                                reader_position = cursor.reader.position(), "Metrics physical journal reached post-terminal end");
                        }
                        break;
                    }
                    Err(error) => {
                        if events.is_empty() {
                            return Err(error);
                        }
                        self.pending_error = Some(error);
                        break;
                    }
                }
                if events.len() >= MAX_BATCH_RECORDS || started.elapsed() >= BATCH_QUANTUM {
                    break;
                }
            }
            if !events.is_empty() {
                return Ok(Some(MetricsBatch {
                    stage: cursor.stage,
                    events,
                }));
            }
        }
        Ok(None)
    }
}
