// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: the hosted-ingress queue core and its request-identity index.
//!
//! Admission queues decoded, proven values, each with a receipt sender. The
//! source's single writer decides every identified member against the index of
//! committed identities and resolves receipts only after the member's own row
//! commits. The index is a projection of the source's own rows, so resume
//! rebuilds it by folding re-admitted rows before the slot goes live.

use super::typed::{
    lower_outputs, protocol_fatal, IngressDecoder, SourceRuntimeRegistration,
    TypedAsyncInfiniteSourceHandler, TypedAsyncInfiniteSourceInvocation,
};
use super::SourceError;
use crate::stages::common::handler_error::StageFatal;
use crate::typing::SourceTyping;
use async_trait::async_trait;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::ingress::{
    HostedIngressBindingSlot, IngressContentDigest, IngressContext, IngressReceipt,
    IngressRequestId,
};
use obzenflow_core::{ChainEvent, EventId, OneFactStageOutput, WriterId};
use ring::digest::{digest, SHA256};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, oneshot};

/// Resolves one submitted member's commit evidence.
pub type IngressReceiptSender = oneshot::Sender<IngressReceipt>;

/// One admitted member waiting for the source's single writer.
pub struct HostedSubmission<T> {
    value: T,
    context: IngressContext,
    receipt: IngressReceiptSender,
}

impl<T> HostedSubmission<T> {
    pub fn new(value: T, context: IngressContext, receipt: IngressReceiptSender) -> Self {
        Self {
            value,
            context,
            receipt,
        }
    }
}

impl<T> fmt::Debug for HostedSubmission<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedSubmission")
            .field("context", &self.context)
            .finish_non_exhaustive()
    }
}

/// Runtime-owned typed hosted-ingress queue core.
pub struct HostedIngressSource<D: IngressDecoder> {
    rx: mpsc::Receiver<HostedSubmission<D::Output>>,
    slot: HostedIngressBindingSlot,
    max_batch_size: usize,
    decoder: PhantomData<fn() -> D>,
}

impl<D: IngressDecoder> fmt::Debug for HostedIngressSource<D> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedIngressSource")
            .field("decoder", &std::any::type_name::<D>())
            .field("output", &std::any::type_name::<D::Output>())
            .field("ingress_key", self.slot.ingress_key())
            .field("max_batch_size", &self.max_batch_size)
            .finish_non_exhaustive()
    }
}

impl<D: IngressDecoder> HostedIngressSource<D> {
    pub fn new(
        rx: mpsc::Receiver<HostedSubmission<D::Output>>,
        slot: HostedIngressBindingSlot,
    ) -> Self {
        Self {
            rx,
            slot,
            max_batch_size: 1000,
            decoder: PhantomData,
        }
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size.max(1);
        self
    }

    async fn receive_batch(&mut self) -> Result<Vec<HostedSubmission<D::Output>>, SourceError> {
        let first = self.rx.recv().await.ok_or(SourceError::Transport(
            SourceDiagnosticReason::InputClosed.into(),
        ))?;
        let mut submissions = vec![first];
        while submissions.len() < self.max_batch_size {
            match self.rx.try_recv() {
                Ok(submission) => submissions.push(submission),
                Err(_) => break,
            }
        }
        Ok(submissions)
    }
}

impl<D: IngressDecoder> SourceTyping for HostedIngressSource<D> {
    type Output = D::Output;
}

#[async_trait]
impl<D: IngressDecoder> TypedAsyncInfiniteSourceHandler for HostedIngressSource<D> {
    type Output = D::Output;

    /// Values only; the runtime polls `next_invocation`, which keeps receipts.
    async fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(self
            .receive_batch()
            .await?
            .into_iter()
            .map(|submission| submission.value)
            .collect())
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.rx.close();
        Ok(())
    }

    fn runtime_registration(&self) -> Option<SourceRuntimeRegistration> {
        Some(SourceRuntimeRegistration::hosted(self.slot.clone()))
    }

    async fn next_invocation(
        &mut self,
    ) -> Result<TypedAsyncInfiniteSourceInvocation<Self::Output>, SourceError> {
        self.receive_batch()
            .await
            .map(TypedAsyncInfiniteSourceInvocation::hosted)
    }
}

/// Fixed-size index key: a 128-bit SHA-256 prefix of request ID and batch index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct IdentityKey(u128);

impl IdentityKey {
    fn of(id: &IngressRequestId, batch_index: Option<usize>) -> Self {
        let mut bytes = Vec::with_capacity(id.as_str().len() + 9);
        bytes.extend_from_slice(id.as_str().as_bytes());
        match batch_index {
            Some(index) => {
                bytes.push(1);
                bytes.extend_from_slice(&(index as u64).to_le_bytes());
            }
            None => bytes.push(0),
        }
        let hash = digest(&SHA256, &bytes);
        let mut prefix = [0_u8; 16];
        prefix.copy_from_slice(&hash.as_ref()[..16]);
        Self(u128::from_le_bytes(prefix))
    }
}

#[derive(Debug, Clone, Copy)]
struct CommittedIdentity {
    digest: IngressContentDigest,
    event_id: EventId,
    accepted_at_ns: u64,
}

struct PendingReceipts {
    original: IngressReceiptSender,
    joined: Vec<IngressReceiptSender>,
}

/// Admitted in this poll, before lowering assigns the event ID.
struct Staged<O> {
    value: O,
    context: IngressContext,
    receipts: PendingReceipts,
    identity: Option<(IdentityKey, IngressContentDigest)>,
}

/// The source's single-writer admission decision and receipt settlement.
pub(super) struct HostedAdmission {
    slot: HostedIngressBindingSlot,
    committed: HashMap<IdentityKey, CommittedIdentity>,
    /// Insertion order of committed identities, for window eviction.
    expiry: VecDeque<(u64, IdentityKey)>,
    pending: HashMap<EventId, PendingReceipts>,
}

impl HostedAdmission {
    pub(super) fn new(slot: HostedIngressBindingSlot) -> Self {
        Self {
            slot,
            committed: HashMap::new(),
            expiry: VecDeque::new(),
            pending: HashMap::new(),
        }
    }

    fn window_ns(&self) -> u64 {
        duration_ns(self.slot.timing().idempotency_window())
    }

    /// Decide every member, lower the new ones, and hold their receipts until
    /// commit. Duplicates and conflicts resolve immediately. Joins only occur
    /// within one poll: the source polls again only after this poll commits.
    pub(super) fn admit<O: OneFactStageOutput>(
        &mut self,
        writer_id: WriterId,
        submissions: Vec<HostedSubmission<O>>,
    ) -> Result<Vec<ChainEvent>, StageFatal> {
        let window_ns = self.window_ns();
        let mut staged: Vec<Staged<O>> = Vec::with_capacity(submissions.len());
        let mut staged_keys: HashMap<IdentityKey, usize> = HashMap::new();
        for submission in submissions {
            if &submission.context.ingress_key != self.slot.ingress_key() {
                return Err(protocol_fatal(format!(
                    "hosted ingress submission for '{}' reached source '{}'",
                    submission.context.ingress_key,
                    self.slot.ingress_key()
                )));
            }
            let accepted_at_ns = submission.context.accepted_at_ns;
            self.evict_before(accepted_at_ns.saturating_sub(window_ns));
            let identity = submission.context.request.as_ref().map(|request| {
                (
                    IdentityKey::of(&request.id, submission.context.batch_index),
                    request.digest,
                )
            });
            let Some((key, digest)) = identity else {
                staged.push(Staged::new(submission, None));
                continue;
            };
            if let Some(&position) = staged_keys.get(&key) {
                let original = &mut staged[position];
                if original.identity.map(|(_, staged_digest)| staged_digest) == Some(digest) {
                    original.receipts.joined.push(submission.receipt);
                } else {
                    let _ = submission.receipt.send(IngressReceipt::Conflict);
                }
                continue;
            }
            match self.live_identity(key, accepted_at_ns, window_ns) {
                Some(entry) if entry.digest == digest => {
                    let _ = submission.receipt.send(IngressReceipt::Duplicate {
                        event_id: entry.event_id,
                    });
                }
                Some(_) => {
                    let _ = submission.receipt.send(IngressReceipt::Conflict);
                }
                None => {
                    staged_keys.insert(key, staged.len());
                    staged.push(Staged::new(submission, Some((key, digest))));
                }
            }
        }

        let mut held = Vec::with_capacity(staged.len());
        let mut outputs = Vec::with_capacity(staged.len());
        for entry in staged {
            held.push((entry.receipts, entry.identity, entry.context.accepted_at_ns));
            outputs.push((entry.value, Some(entry.context)));
        }
        let events = lower_outputs(writer_id, outputs)?;
        for (event, (receipts, identity, accepted_at_ns)) in events.iter().zip(held) {
            if let Some((key, digest)) = identity {
                self.remember(key, digest, event.id, accepted_at_ns);
            }
            self.pending.insert(event.id, receipts);
        }
        self.publish();
        Ok(events)
    }

    /// Resolve receipts for rows the journal committed.
    pub(super) fn settle(&mut self, committed: &[EventId]) {
        for event_id in committed {
            if let Some(receipts) = self.pending.remove(event_id) {
                let _ = receipts.original.send(IngressReceipt::Committed {
                    event_id: *event_id,
                });
                for joined in receipts.joined {
                    let _ = joined.send(IngressReceipt::Duplicate {
                        event_id: *event_id,
                    });
                }
            }
        }
    }

    /// Fold one re-admitted row of this source into the index (resume).
    pub(super) fn fold(&mut self, event: &ChainEvent) {
        let Some(context) = event.ingress_context.as_ref() else {
            return;
        };
        let Some(request) = context.request.as_ref() else {
            return;
        };
        if unix_now_ns().saturating_sub(context.accepted_at_ns) > self.window_ns() {
            return;
        }
        let key = IdentityKey::of(&request.id, context.batch_index);
        self.remember(key, request.digest, event.id, context.accepted_at_ns);
        self.publish();
    }

    fn live_identity(
        &mut self,
        key: IdentityKey,
        accepted_at_ns: u64,
        window_ns: u64,
    ) -> Option<CommittedIdentity> {
        let entry = *self.committed.get(&key)?;
        if accepted_at_ns.saturating_sub(entry.accepted_at_ns) > window_ns {
            self.committed.remove(&key);
            return None;
        }
        Some(entry)
    }

    fn remember(
        &mut self,
        key: IdentityKey,
        digest: IngressContentDigest,
        event_id: EventId,
        accepted_at_ns: u64,
    ) {
        self.committed.insert(
            key,
            CommittedIdentity {
                digest,
                event_id,
                accepted_at_ns,
            },
        );
        self.expiry.push_back((accepted_at_ns, key));
    }

    /// Drop identities accepted before `cutoff_ns`, oldest first.
    fn evict_before(&mut self, cutoff_ns: u64) {
        while let Some(&(accepted_at_ns, key)) = self.expiry.front() {
            if accepted_at_ns >= cutoff_ns {
                break;
            }
            self.expiry.pop_front();
            if self
                .committed
                .get(&key)
                .is_some_and(|entry| entry.accepted_at_ns == accepted_at_ns)
            {
                self.committed.remove(&key);
            }
        }
    }

    fn publish(&self) {
        self.slot
            .publish_idempotency_entries(self.committed.len() as u64);
    }
}

impl<O> Staged<O> {
    fn new(
        submission: HostedSubmission<O>,
        identity: Option<(IdentityKey, IngressContentDigest)>,
    ) -> Self {
        Self {
            value: submission.value,
            context: submission.context,
            receipts: PendingReceipts {
                original: submission.receipt,
                joined: Vec::new(),
            },
            identity,
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(duration_ns)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::super::erased::{
        ErasedSourceCompletion, ErasedSourceOutcome, UnifiedAsyncInfiniteSourceHandler,
    };
    use super::super::typed::TypedAsyncInfiniteSourceHandlerAdapter;
    use super::*;
    use obzenflow_core::event::StageFatalCode;
    use obzenflow_core::ingress::{
        IngressAttemptSeq, IngressKey, IngressRequestIdentity, IngressTiming,
    };
    use obzenflow_core::{StageId, TypedPayload};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tokio::sync::oneshot::error::TryRecvError;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Row(u32);

    impl TypedPayload for Row {
        const EVENT_TYPE: &'static str = "hosted.row";
    }

    #[derive(Clone, Debug)]
    struct RowIngress;

    impl IngressDecoder for RowIngress {
        type Output = Row;
    }

    const KEY: &str = "bank.tx";
    const SECOND_NS: u64 = 1_000_000_000;

    struct Harness {
        tx: mpsc::Sender<HostedSubmission<Row>>,
        slot: HostedIngressBindingSlot,
        adapter: TypedAsyncInfiniteSourceHandlerAdapter<HostedIngressSource<RowIngress>>,
    }

    impl Harness {
        fn new() -> Self {
            let (tx, rx) = mpsc::channel(16);
            let slot = HostedIngressBindingSlot::new(KEY);
            slot.install_timing(
                IngressTiming::new(Duration::from_secs(1), Duration::from_secs(5))
                    .expect("valid timing"),
            )
            .expect("first install");
            let mut adapter = TypedAsyncInfiniteSourceHandlerAdapter::new(
                HostedIngressSource::new(rx, slot.clone()),
            );
            UnifiedAsyncInfiniteSourceHandler::install_writer_id(
                &mut adapter,
                WriterId::from(StageId::new()),
            );
            Self { tx, slot, adapter }
        }

        async fn submit(
            &self,
            row: u32,
            accepted_at_s: u64,
            batch_index: Option<usize>,
            request: Option<&str>,
        ) -> oneshot::Receiver<IngressReceipt> {
            let (receipt, answer) = oneshot::channel();
            let context = context(accepted_at_s, KEY, batch_index, request, row);
            self.tx
                .send(HostedSubmission::new(Row(row), context, receipt))
                .await
                .expect("queued");
            answer
        }

        /// Queue a member whose answer the test does not read.
        async fn enqueue(
            &self,
            row: u32,
            accepted_at_s: u64,
            batch_index: Option<usize>,
            request: Option<&str>,
        ) {
            drop(self.submit(row, accepted_at_s, batch_index, request).await);
        }

        async fn poll(&mut self) -> Vec<ChainEvent> {
            let (outcome, _) =
                UnifiedAsyncInfiniteSourceHandler::next_invocation(&mut self.adapter)
                    .await
                    .into_parts();
            let ErasedSourceOutcome::Completed(ErasedSourceCompletion::Batch(events)) = outcome
            else {
                panic!("expected a lowered batch, got {outcome:?}")
            };
            events
        }

        fn commit(&mut self, events: &[ChainEvent]) {
            let ids: Vec<EventId> = events.iter().map(|event| event.id).collect();
            UnifiedAsyncInfiniteSourceHandler::settle_committed(&mut self.adapter, &ids);
        }
    }

    fn context(
        accepted_at_s: u64,
        ingress_key: &str,
        batch_index: Option<usize>,
        request: Option<&str>,
        row: u32,
    ) -> IngressContext {
        IngressContext {
            accepted_at_ns: accepted_at_s * SECOND_NS,
            ingress_key: IngressKey::from(ingress_key),
            batch_index,
            attempt_seq: IngressAttemptSeq(u64::from(row)),
            request: request.map(|id| IngressRequestIdentity {
                id: IngressRequestId::parse(id).expect("valid id"),
                digest: IngressContentDigest::of_submission(Row::EVENT_TYPE, &json!(row)),
            }),
        }
    }

    fn answered(answer: &mut oneshot::Receiver<IngressReceipt>) -> Option<IngressReceipt> {
        match answer.try_recv() {
            Ok(receipt) => Some(receipt),
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Closed) => panic!("receipt dropped"),
        }
    }

    #[tokio::test]
    async fn receipts_resolve_only_after_their_own_row_commits() {
        let mut harness = Harness::new();
        let mut answer = harness.submit(7, 100, None, None).await;
        let events = harness.poll().await;
        assert_eq!(events.len(), 1);
        assert_eq!(Row::from_event(&events[0]), Some(Row(7)));
        assert_eq!(answered(&mut answer), None, "staged is not acceptance");

        harness.commit(&events);
        assert_eq!(
            answered(&mut answer),
            Some(IngressReceipt::Committed {
                event_id: events[0].id
            })
        );
    }

    #[tokio::test]
    async fn lowering_keeps_the_exact_context_and_identity() {
        let mut harness = Harness::new();
        harness.enqueue(8, 100, Some(0), Some("batch-1")).await;
        harness.enqueue(9, 100, Some(1), Some("batch-1")).await;
        let events = harness.poll().await;
        let contexts: Vec<_> = events
            .iter()
            .map(|event| event.ingress_context.clone().expect("context"))
            .collect();
        assert_eq!(
            contexts,
            vec![
                context(100, KEY, Some(0), Some("batch-1"), 8),
                context(100, KEY, Some(1), Some("batch-1"), 9),
            ]
        );
    }

    #[tokio::test]
    async fn concurrent_identical_requests_share_one_row() {
        let mut harness = Harness::new();
        let mut original = harness.submit(7, 100, None, Some("tx-1")).await;
        let mut retry = harness.submit(7, 100, None, Some("tx-1")).await;
        let events = harness.poll().await;
        assert_eq!(events.len(), 1, "the retry joins the staged original");
        assert_eq!(answered(&mut retry), None);

        harness.commit(&events);
        let event_id = events[0].id;
        assert_eq!(
            answered(&mut original),
            Some(IngressReceipt::Committed { event_id })
        );
        assert_eq!(
            answered(&mut retry),
            Some(IngressReceipt::Duplicate { event_id })
        );
    }

    #[tokio::test]
    async fn committed_identities_answer_duplicates_and_conflicts_without_rows() {
        let mut harness = Harness::new();
        harness.enqueue(7, 100, None, Some("tx-1")).await;
        let events = harness.poll().await;
        harness.commit(&events);

        let mut duplicate = harness.submit(7, 101, None, Some("tx-1")).await;
        let mut conflict = harness.submit(8, 101, None, Some("tx-1")).await;
        let mut fresh = harness.submit(8, 101, None, Some("tx-2")).await;
        let later = harness.poll().await;
        assert_eq!(later.len(), 1, "only the new identity lowers");
        assert_eq!(
            answered(&mut duplicate),
            Some(IngressReceipt::Duplicate {
                event_id: events[0].id
            })
        );
        assert_eq!(answered(&mut conflict), Some(IngressReceipt::Conflict));
        harness.commit(&later);
        assert_eq!(
            answered(&mut fresh),
            Some(IngressReceipt::Committed {
                event_id: later[0].id
            })
        );
    }

    #[tokio::test]
    async fn unequal_content_in_one_poll_is_a_conflict() {
        let mut harness = Harness::new();
        harness.enqueue(7, 100, None, Some("tx-1")).await;
        let mut conflict = harness.submit(8, 100, None, Some("tx-1")).await;
        let events = harness.poll().await;
        assert_eq!(events.len(), 1);
        assert_eq!(answered(&mut conflict), Some(IngressReceipt::Conflict));
    }

    #[tokio::test]
    async fn batch_members_are_identified_by_index() {
        let mut harness = Harness::new();
        harness.enqueue(7, 100, Some(0), Some("batch-1")).await;
        let events = harness.poll().await;
        harness.commit(&events);

        let mut member_zero = harness.submit(7, 101, Some(0), Some("batch-1")).await;
        harness.enqueue(8, 101, Some(1), Some("batch-1")).await;
        let later = harness.poll().await;
        assert_eq!(later.len(), 1, "only the uncommitted member lowers");
        assert_eq!(
            answered(&mut member_zero),
            Some(IngressReceipt::Duplicate {
                event_id: events[0].id
            })
        );
    }

    #[tokio::test]
    async fn retries_accepted_after_the_window_are_admitted_again() {
        let mut harness = Harness::new();
        harness.enqueue(7, 100, None, Some("tx-1")).await;
        let events = harness.poll().await;
        harness.commit(&events);

        // The window is measured between acceptance times, not at dequeue.
        let mut within = harness.submit(7, 105, None, Some("tx-1")).await;
        assert!(harness.poll().await.is_empty());
        assert!(matches!(
            answered(&mut within),
            Some(IngressReceipt::Duplicate { .. })
        ));

        harness.enqueue(7, 106, None, Some("tx-1")).await;
        assert_eq!(harness.poll().await.len(), 1);
    }

    #[tokio::test]
    async fn the_index_reports_its_size() {
        let mut harness = Harness::new();
        harness.enqueue(7, 100, None, Some("tx-1")).await;
        harness.enqueue(8, 100, None, None).await;
        let events = harness.poll().await;
        harness.commit(&events);
        assert_eq!(harness.slot.idempotency_entries(), 1);

        harness.enqueue(9, 200, None, Some("tx-2")).await;
        harness.poll().await;
        assert_eq!(
            harness.slot.idempotency_entries(),
            1,
            "expired identities leave the index"
        );
    }

    #[tokio::test]
    async fn resume_folds_recorded_identities_inside_the_window() {
        let mut recorded = Harness::new();
        let now_s = unix_now_ns() / SECOND_NS;
        recorded.enqueue(7, now_s, None, Some("tx-1")).await;
        recorded.enqueue(8, now_s - 60, None, Some("tx-old")).await;
        let rows = recorded.poll().await;

        let mut resumed = Harness::new();
        for row in &rows {
            UnifiedAsyncInfiniteSourceHandler::fold_recorded_admission(&mut resumed.adapter, row);
        }
        assert_eq!(
            resumed.slot.idempotency_entries(),
            1,
            "stale rows are skipped"
        );

        let mut retry = resumed.submit(7, now_s + 1, None, Some("tx-1")).await;
        assert!(resumed.poll().await.is_empty());
        assert_eq!(
            answered(&mut retry),
            Some(IngressReceipt::Duplicate {
                event_id: rows[0].id
            })
        );
    }

    #[tokio::test]
    async fn a_foreign_ingress_context_fails_the_stage_without_receipts() {
        let mut harness = Harness::new();
        let (receipt, mut answer) = oneshot::channel();
        harness
            .tx
            .send(HostedSubmission::new(
                Row(1),
                context(100, "other.key", None, None, 1),
                receipt,
            ))
            .await
            .expect("queued");
        let (outcome, _) = UnifiedAsyncInfiniteSourceHandler::next_invocation(&mut harness.adapter)
            .await
            .into_parts();
        let ErasedSourceOutcome::Fatal(fatal) = outcome else {
            panic!("a foreign context is a framework invariant")
        };
        assert_eq!(fatal.code, StageFatalCode::Protocol);
        assert!(matches!(answer.try_recv(), Err(TryRecvError::Closed)));
    }
}
