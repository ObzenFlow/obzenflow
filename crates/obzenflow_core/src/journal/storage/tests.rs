// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::{
    CausalFrontier, ChainEvent, ChainEventFactory, ChainPayload, PreparedCausalCommit,
};
use std::future::{pending, poll_fn, Future};
use std::sync::Mutex;
use std::task::Poll;

#[derive(Clone, Copy)]
enum Outcome {
    Rejected,
    Indeterminate,
    Pending,
    InvalidReceipt,
}

// Retain private candidates so the tests can attempt the original bypass even
// after a rejected, uncertain or abandoned storage operation.
struct Storage {
    id: JournalId,
    outcome: Outcome,
    candidates: Mutex<Vec<JournalRecord<ChainPayload>>>,
}

#[async_trait]
impl JournalStorage<ChainEvent> for Storage {
    fn storage_id(&self) -> &JournalId {
        &self.id
    }
    fn storage_owner(&self) -> Option<&JournalOwner> {
        None
    }
    async fn storage_append(
        &self,
        event: ChainEvent,
        options: AppendOptions<ChainEvent>,
    ) -> Result<JournalRecord<ChainPayload>, JournalError> {
        Ok(self
            .storage_append_group("single", vec![event], options)
            .await?
            .remove(0))
    }
    async fn storage_append_group(
        &self,
        _: &str,
        events: Vec<ChainEvent>,
        _: AppendOptions<ChainEvent>,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        let records: Vec<_> = events
            .into_iter()
            .map(|event| JournalRecord::new(self.id.into(), event))
            .collect();
        *self.candidates.lock().unwrap() = records.clone();
        match self.outcome {
            Outcome::Rejected => Err(JournalError::Full),
            Outcome::Indeterminate => Err(JournalError::CommitIndeterminate {
                source: "injected uncertain commit".into(),
            }),
            Outcome::Pending => pending().await,
            Outcome::InvalidReceipt => {
                let mut records = records;
                // Simulate a provider violating its success contract. The
                // facade must not describe this as a confirmed non-commit.
                records
                    .last_mut()
                    .unwrap()
                    .envelope
                    .provenance
                    .journal
                    .vector_clock
                    .clocks
                    .clear();
                Ok(records)
            }
        }
    }
    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        Ok(Vec::new())
    }
    async fn storage_read_event(
        &self,
        _: &EventId,
    ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        Ok(None)
    }
    async fn storage_read_last_n(
        &self,
        _: usize,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        Ok(Vec::new())
    }
    async fn storage_reader_from(
        &self,
        _: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Err(JournalError::InitialPrefixUnsupported)
    }
}

fn event() -> ChainEvent {
    ChainEventFactory::data_event(
        crate::StageId::new().into(),
        "candidate",
        serde_json::json!({}),
    )
}

fn assert_private(storage: &Storage, count: usize) {
    let candidates = storage.candidates.lock().unwrap();
    assert_eq!(candidates.len(), count);
    for record in &*candidates {
        PreparedCausalCommit::from_record(record).unwrap();
        assert_eq!(
            CausalFrontier::from_record(record).unwrap_err(),
            CausalError::UnadmittedRecord
        );
    }
}

#[tokio::test]
async fn failed_and_indeterminate_appends_never_admit_their_candidates() {
    for outcome in [Outcome::Rejected, Outcome::Indeterminate] {
        let storage = Storage {
            id: JournalId::new(),
            outcome,
            candidates: Mutex::default(),
        };
        let error = storage
            .append(event(), Default::default())
            .await
            .unwrap_err();
        assert!(matches!(
            (outcome, error),
            (Outcome::Rejected, JournalError::Full)
                | (
                    Outcome::Indeterminate,
                    JournalError::CommitIndeterminate { .. }
                )
        ));
        assert_private(&storage, 1);
        assert!(storage
            .append_group("failed", vec![event(), event()], Default::default())
            .await
            .is_err());
        assert_private(&storage, 2);
    }
}

#[tokio::test]
async fn dropping_pending_appends_never_admits_private_candidates() {
    let storage = Storage {
        id: JournalId::new(),
        outcome: Outcome::Pending,
        candidates: Mutex::default(),
    };
    {
        let mut append = Box::pin(storage.append(event(), Default::default()));
        poll_fn(|cx| {
            assert!(append.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_private(&storage, 1);
    {
        let mut append =
            Box::pin(storage.append_group("pending", vec![event(), event()], Default::default()));
        poll_fn(|cx| {
            assert!(append.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    assert_private(&storage, 2);
}

#[tokio::test]
async fn invalid_success_receipts_are_indeterminate_and_groups_return_no_members() {
    let storage = Storage {
        id: JournalId::new(),
        outcome: Outcome::InvalidReceipt,
        candidates: Mutex::default(),
    };
    assert!(matches!(
        storage.append(event(), Default::default()).await,
        Err(JournalError::CommitIndeterminate { .. })
    ));
    assert_private(&storage, 1);
    assert!(matches!(
        storage
            .append_group("invalid", vec![event(), event()], Default::default())
            .await,
        Err(JournalError::CommitIndeterminate { .. })
    ));
    assert_private(&storage, 2);
}
