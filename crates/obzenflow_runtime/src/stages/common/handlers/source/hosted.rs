// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::SourceContinuation;
use crate::events::IngressReceiptSender;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::event::schema::TypedFact;
use obzenflow_core::event::{ChainPayload, EventKind};
use obzenflow_core::ingress::{
    HostedIngressBindingSlot, IngressAttemptSeq, IngressContentDigest, IngressContext,
    IngressReceipt, IngressRequestId,
};
use obzenflow_core::{EventId, EventType};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HostedMember {
    pub(crate) fact: HostedFact,
    pub(crate) context: IngressContext,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct HostedFact {
    kind: EventKind,
    event_type: EventType,
    schema_version: NonZeroU32,
    payload: serde_json::Value,
}

impl HostedFact {
    pub(crate) fn of(fact: TypedFact) -> Result<Self, String> {
        Ok(Self {
            kind: fact.payload.kind(),
            payload: serde_json::to_value(&fact.payload).map_err(|error| error.to_string())?,
            event_type: fact.event_type,
            schema_version: fact.payload_schema_version,
        })
    }

    pub(crate) fn into_typed(self) -> Result<TypedFact, serde_json::Error> {
        Ok(TypedFact {
            payload: ChainPayload::decode(
                self.kind,
                self.event_type.as_str(),
                self.schema_version,
                self.payload,
            )?,
            event_type: self.event_type,
            payload_schema_version: self.schema_version,
        })
    }
}

pub(crate) type ReceiptKey = (IngressAttemptSeq, Option<usize>);

pub(crate) fn receipt_key(context: &IngressContext) -> ReceiptKey {
    (context.attempt_seq, context.batch_index)
}

/// Fixed-size index key: a 128-bit SHA-256 prefix of request ID and batch index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct IdentityKey(u128);

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

pub(crate) struct PendingReceipts {
    original: IngressReceiptSender,
    joined: Vec<IngressReceiptSender>,
}

pub(crate) struct Staged {
    pub(crate) fact: HostedFact,
    pub(crate) context: IngressContext,
    pub(crate) receipts: Option<PendingReceipts>,
    pub(crate) identity: Option<(IdentityKey, IngressContentDigest)>,
}

impl Staged {
    fn new(
        member: HostedMember,
        receipt: Option<IngressReceiptSender>,
        identity: Option<(IdentityKey, IngressContentDigest)>,
    ) -> Self {
        Self {
            fact: member.fact,
            context: member.context,
            receipts: receipt.map(|original| PendingReceipts {
                original,
                joined: Vec::new(),
            }),
            identity,
        }
    }
}

/// The source's single-writer admission decision and receipt settlement.
pub(crate) struct HostedAdmission {
    slot: HostedIngressBindingSlot,
    committed: HashMap<IdentityKey, CommittedIdentity>,
    expiry: VecDeque<(u64, IdentityKey)>,
    pub(crate) pending: HashMap<EventId, PendingReceipts>,
}

impl HostedAdmission {
    pub(crate) fn continuation() -> SourceContinuation {
        SourceContinuation::Reattach {
            guarantee: ReattachGuarantee::SenderRedelivers,
        }
    }

    pub(crate) fn new(slot: HostedIngressBindingSlot) -> Self {
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

    pub(crate) fn foreign_member(&self, members: &[HostedMember]) -> Option<String> {
        members
            .iter()
            .find(|member| &member.context.ingress_key != self.slot.ingress_key())
            .map(|member| {
                format!(
                    "hosted ingress submission for '{}' reached source '{}'",
                    member.context.ingress_key,
                    self.slot.ingress_key()
                )
            })
    }

    pub(crate) fn decide(
        &mut self,
        members: Vec<HostedMember>,
        mut receipt: impl FnMut(&IngressContext) -> Option<IngressReceiptSender>,
    ) -> Vec<Staged> {
        let window_ns = self.window_ns();
        let mut staged: Vec<Staged> = Vec::with_capacity(members.len());
        let mut staged_keys: HashMap<IdentityKey, usize> = HashMap::new();
        for member in members {
            let sender = receipt(&member.context);
            let accepted_at_ns = member.context.accepted_at_ns;
            self.evict_before(accepted_at_ns.saturating_sub(window_ns));
            let identity = member.context.request.as_ref().map(|request| {
                (
                    IdentityKey::of(&request.id, member.context.batch_index),
                    request.digest,
                )
            });
            let Some((key, digest)) = identity else {
                staged.push(Staged::new(member, sender, None));
                continue;
            };
            if let Some(&position) = staged_keys.get(&key) {
                let original = &mut staged[position];
                if original.identity.map(|(_, staged_digest)| staged_digest) == Some(digest) {
                    if let (Some(receipts), Some(sender)) = (original.receipts.as_mut(), sender) {
                        receipts.joined.push(sender);
                    }
                } else if let Some(sender) = sender {
                    deliver_receipt(sender, IngressReceipt::Conflict);
                }
                continue;
            }
            match self.live_identity(key, accepted_at_ns, window_ns) {
                Some(entry) if entry.digest == digest => {
                    if let Some(sender) = sender {
                        deliver_receipt(
                            sender,
                            IngressReceipt::Duplicate {
                                event_id: entry.event_id,
                            },
                        );
                    }
                }
                Some(_) => {
                    if let Some(sender) = sender {
                        deliver_receipt(sender, IngressReceipt::Conflict);
                    }
                }
                None => {
                    staged_keys.insert(key, staged.len());
                    staged.push(Staged::new(member, sender, Some((key, digest))));
                }
            }
        }
        staged
    }

    pub(crate) fn admitted(
        &mut self,
        identity: Option<(IdentityKey, IngressContentDigest)>,
        accepted_at_ns: u64,
        receipts: Option<PendingReceipts>,
        event_id: EventId,
    ) {
        if let Some((key, digest)) = identity {
            self.remember(key, digest, event_id, accepted_at_ns);
        }
        if let Some(receipts) = receipts {
            self.pending.insert(event_id, receipts);
        }
        self.publish();
    }

    pub(crate) fn settle(&mut self, committed: &[EventId]) {
        for event_id in committed {
            if let Some(receipts) = self.pending.remove(event_id) {
                deliver_receipt(
                    receipts.original,
                    IngressReceipt::Committed {
                        event_id: *event_id,
                    },
                );
                for joined in receipts.joined {
                    deliver_receipt(
                        joined,
                        IngressReceipt::Duplicate {
                            event_id: *event_id,
                        },
                    );
                }
            }
        }
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

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn deliver_receipt(sender: IngressReceiptSender, receipt: IngressReceipt) {
    if sender.send(receipt).is_err() {
        tracing::debug!("ingress client left before its receipt");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ingress::{IngressKey, IngressRequestIdentity, IngressTiming};
    use obzenflow_core::TypedPayload;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tokio::sync::oneshot;
    use tokio::sync::oneshot::error::TryRecvError;

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Row(u32);

    impl TypedPayload for Row {
        const EVENT_TYPE: &'static str = "hosted.row";
    }

    const KEY: &str = "bank.tx";
    const SECOND_NS: u64 = 1_000_000_000;

    struct Harness {
        slot: HostedIngressBindingSlot,
        admission: HostedAdmission,
        attempt: u64,
    }

    impl Harness {
        fn new() -> Self {
            let slot = HostedIngressBindingSlot::new(KEY);
            slot.install_timing(
                IngressTiming::new(Duration::from_secs(1), Duration::from_secs(5))
                    .expect("valid timing"),
            )
            .expect("first install");
            Self {
                admission: HostedAdmission::new(slot.clone()),
                slot,
                attempt: 0,
            }
        }

        fn member(
            &mut self,
            row: u32,
            accepted_at_s: u64,
            batch_index: Option<usize>,
            request: Option<&str>,
        ) -> (
            HostedMember,
            oneshot::Receiver<IngressReceipt>,
            IngressReceiptSender,
        ) {
            self.attempt += 1;
            let (sender, answer) = oneshot::channel();
            let member = HostedMember {
                fact: HostedFact::of(crate::events::one_fact(Row(row)).expect("a row lowers"))
                    .expect("a row records"),
                context: context(accepted_at_s, KEY, batch_index, request, row, self.attempt),
            };
            (member, answer, sender)
        }

        fn read(
            &mut self,
            members: Vec<(HostedMember, IngressReceiptSender)>,
        ) -> Vec<(EventId, IngressContext)> {
            let (members, senders): (Vec<_>, Vec<_>) = members.into_iter().unzip();
            let mut senders: VecDeque<_> = senders.into();
            let staged = self.admission.decide(members, |_| senders.pop_front());
            staged
                .into_iter()
                .map(|staged| {
                    let event_id = EventId::new();
                    let context = staged.context.clone();
                    self.admission.admitted(
                        staged.identity,
                        staged.context.accepted_at_ns,
                        staged.receipts,
                        event_id,
                    );
                    (event_id, context)
                })
                .collect()
        }

        fn commit(&mut self, rows: &[(EventId, IngressContext)]) {
            let ids: Vec<EventId> = rows.iter().map(|(event_id, _)| *event_id).collect();
            self.admission.settle(&ids);
        }
    }

    fn context(
        accepted_at_s: u64,
        ingress_key: &str,
        batch_index: Option<usize>,
        request: Option<&str>,
        row: u32,
        attempt: u64,
    ) -> IngressContext {
        IngressContext {
            accepted_at_ns: accepted_at_s * SECOND_NS,
            ingress_key: IngressKey::from(ingress_key),
            batch_index,
            attempt_seq: IngressAttemptSeq(attempt),
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

    #[test]
    fn receipt_resolves_after_row_commit() {
        let mut harness = Harness::new();
        let (member, mut answer, sender) = harness.member(7, 100, None, None);
        let rows = harness.read(vec![(member, sender)]);
        assert_eq!(rows.len(), 1);
        assert_eq!(answered(&mut answer), None, "staged is not acceptance");

        harness.commit(&rows);
        assert_eq!(
            answered(&mut answer),
            Some(IngressReceipt::Committed {
                event_id: rows[0].0
            })
        );
    }

    #[test]
    fn recorded_member_reads_back_unchanged() {
        let mut harness = Harness::new();
        let (member, _answer, _sender) = harness.member(8, 100, Some(0), Some("batch-1"));
        let recorded: HostedMember =
            serde_json::from_value(serde_json::to_value(&member).expect("records"))
                .expect("reads back");
        assert_eq!(recorded.context, member.context);
        let fact = recorded.fact.into_typed().expect("decodes");
        assert_eq!(fact.event_type.as_str(), Row::EVENT_TYPE);
        assert_eq!(fact.payload, ChainPayload::Fact(json!(8)));
    }

    #[test]
    fn concurrent_identical_requests_share_one_row() {
        let mut harness = Harness::new();
        let (original, mut original_answer, original_sender) =
            harness.member(7, 100, None, Some("tx-1"));
        let (retry, mut retry_answer, retry_sender) = harness.member(7, 100, None, Some("tx-1"));
        let rows = harness.read(vec![(original, original_sender), (retry, retry_sender)]);
        assert_eq!(rows.len(), 1, "the retry joins the staged original");
        assert_eq!(answered(&mut retry_answer), None);

        harness.commit(&rows);
        let event_id = rows[0].0;
        assert_eq!(
            answered(&mut original_answer),
            Some(IngressReceipt::Committed { event_id })
        );
        assert_eq!(
            answered(&mut retry_answer),
            Some(IngressReceipt::Duplicate { event_id })
        );
    }

    #[test]
    fn committed_identity_answers_duplicate_without_row() {
        let mut harness = Harness::new();
        let (member, _answer, sender) = harness.member(7, 100, None, Some("tx-1"));
        let rows = harness.read(vec![(member, sender)]);
        harness.commit(&rows);

        let (duplicate, mut duplicate_answer, duplicate_sender) =
            harness.member(7, 101, None, Some("tx-1"));
        let (conflict, mut conflict_answer, conflict_sender) =
            harness.member(8, 101, None, Some("tx-1"));
        let (fresh, mut fresh_answer, fresh_sender) = harness.member(8, 101, None, Some("tx-2"));
        let later = harness.read(vec![
            (duplicate, duplicate_sender),
            (conflict, conflict_sender),
            (fresh, fresh_sender),
        ]);
        assert_eq!(later.len(), 1, "only the new identity lowers");
        assert_eq!(
            answered(&mut duplicate_answer),
            Some(IngressReceipt::Duplicate {
                event_id: rows[0].0
            })
        );
        assert_eq!(
            answered(&mut conflict_answer),
            Some(IngressReceipt::Conflict)
        );
        harness.commit(&later);
        assert_eq!(
            answered(&mut fresh_answer),
            Some(IngressReceipt::Committed {
                event_id: later[0].0
            })
        );
    }

    #[test]
    fn unequal_content_same_key_returns_conflict() {
        let mut harness = Harness::new();
        let (first, _first_answer, first_sender) = harness.member(7, 100, None, Some("tx-1"));
        let (second, mut conflict, second_sender) = harness.member(8, 100, None, Some("tx-1"));
        let rows = harness.read(vec![(first, first_sender), (second, second_sender)]);
        assert_eq!(rows.len(), 1);
        assert_eq!(answered(&mut conflict), Some(IngressReceipt::Conflict));
    }

    #[test]
    fn batch_members_identified_by_index() {
        let mut harness = Harness::new();
        let (member, _answer, sender) = harness.member(7, 100, Some(0), Some("batch-1"));
        let rows = harness.read(vec![(member, sender)]);
        harness.commit(&rows);

        let (zero, mut zero_answer, zero_sender) = harness.member(7, 101, Some(0), Some("batch-1"));
        let (one, _one_answer, one_sender) = harness.member(8, 101, Some(1), Some("batch-1"));
        let later = harness.read(vec![(zero, zero_sender), (one, one_sender)]);
        assert_eq!(later.len(), 1, "only the uncommitted member lowers");
        assert_eq!(
            answered(&mut zero_answer),
            Some(IngressReceipt::Duplicate {
                event_id: rows[0].0
            })
        );
    }

    #[test]
    fn retry_after_window_admits_row() {
        let mut harness = Harness::new();
        let (member, _answer, sender) = harness.member(7, 100, None, Some("tx-1"));
        let rows = harness.read(vec![(member, sender)]);
        harness.commit(&rows);

        let (within, mut within_answer, within_sender) = harness.member(7, 105, None, Some("tx-1"));
        assert!(harness.read(vec![(within, within_sender)]).is_empty());
        assert!(matches!(
            answered(&mut within_answer),
            Some(IngressReceipt::Duplicate { .. })
        ));

        let (after, _after_answer, after_sender) = harness.member(7, 106, None, Some("tx-1"));
        assert_eq!(harness.read(vec![(after, after_sender)]).len(), 1);
    }

    #[test]
    fn index_size_excludes_expired_identities() {
        let mut harness = Harness::new();
        let (keyed, _keyed_answer, keyed_sender) = harness.member(7, 100, None, Some("tx-1"));
        let (plain, _plain_answer, plain_sender) = harness.member(8, 100, None, None);
        let rows = harness.read(vec![(keyed, keyed_sender), (plain, plain_sender)]);
        harness.commit(&rows);
        assert_eq!(harness.slot.idempotency_entries(), 1);

        let (later, _later_answer, later_sender) = harness.member(9, 200, None, Some("tx-2"));
        harness.read(vec![(later, later_sender)]);
        assert_eq!(
            harness.slot.idempotency_entries(),
            1,
            "expired identities leave the index"
        );
    }

    #[test]
    fn reproduced_members_rebuild_index_without_receipts() {
        let mut harness = Harness::new();
        let (member, _answer, _sender) = harness.member(7, 100, None, Some("tx-1"));
        let staged = harness.admission.decide(vec![member], |_| None);
        let original = EventId::new();
        for staged in staged {
            assert!(
                staged.receipts.is_none(),
                "a reproduced member has no sender"
            );
            harness.admission.admitted(
                staged.identity,
                staged.context.accepted_at_ns,
                staged.receipts,
                original,
            );
        }

        let (retry, mut retry_answer, retry_sender) = harness.member(7, 101, None, Some("tx-1"));
        assert!(harness.read(vec![(retry, retry_sender)]).is_empty());
        assert_eq!(
            answered(&mut retry_answer),
            Some(IngressReceipt::Duplicate { event_id: original })
        );
    }

    #[test]
    fn foreign_ingress_context_fails_stage() {
        let mut harness = Harness::new();
        let (mut member, _answer, _sender) = harness.member(1, 100, None, None);
        member.context.ingress_key = IngressKey::from("other.key");
        let fatal = harness
            .admission
            .foreign_member(&[member])
            .expect("a foreign context is a framework invariant");
        assert!(fatal.contains("other.key"), "{fatal}");
    }
}
