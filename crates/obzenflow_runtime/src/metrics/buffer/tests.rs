// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::journal::{AppendOptions, JournalError, JournalReader, JournalStorage};
use obzenflow_core::{ChainEvent, EventId, JournalOwner};
use std::sync::atomic::{AtomicUsize, Ordering};

struct GatedJournal {
    id: JournalId,
    started: tokio::sync::Notify,
    active: Arc<AtomicUsize>,
}

struct Reading(Arc<AtomicUsize>);

impl Drop for Reading {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl JournalStorage<ChainEvent> for GatedJournal {
    fn storage_id(&self) -> &JournalId {
        &self.id
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        None
    }

    async fn storage_append(
        &self,
        _: ChainEvent,
        _: AppendOptions<ChainEvent>,
    ) -> Result<JournalRecord<ChainPayload>, JournalError> {
        unreachable!("tail refresh must not append")
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        unreachable!("tail refresh must not scan")
    }

    async fn storage_read_event(
        &self,
        _: &EventId,
    ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        unreachable!("tail refresh uses its dedicated lookup")
    }

    async fn storage_reader_from(
        &self,
        _: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        unreachable!("tail refresh must not open a reader")
    }

    async fn storage_read_last_n(
        &self,
        _: usize,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        unreachable!("tail refresh must not search history")
    }

    async fn storage_read_metrics_tail(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        async {
            self.active.fetch_add(1, Ordering::SeqCst);
            let _reading = Reading(self.active.clone());
            self.started.notify_one();
            std::future::pending().await
        }
        .await
    }
}

#[tokio::test]
async fn tail_reads_cancel_with_their_owner() {
    for drop_owner in [false, true] {
        let journals = [(); 3].map(|()| {
            Arc::new(GatedJournal {
                id: JournalId::new(),
                started: Default::default(),
                active: Arc::default(),
            })
        });
        let mut readers = TailReaders::default();
        for journal in &journals {
            readers.spawn(journal.clone(), Arc::default(), |_, _| {});
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            for journal in &journals {
                journal.started.notified().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(readers.len(), 3);
        assert!(journals
            .iter()
            .all(|j| j.active.load(Ordering::SeqCst) == 1));
        if drop_owner {
            drop(readers);
        } else {
            readers.stop().await;
            assert_eq!(readers.len(), 0);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while journals
                .iter()
                .any(|j| j.active.load(Ordering::SeqCst) != 0)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}
