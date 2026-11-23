// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The hosted-ingress queue, the stage's read of it and the request-identity
//! index.
//!
//! Admission queues decoded, proven values, each with a receipt sender. The
//! stage reads the queue through its port. The recorded reply holds each
//! member's journal form and ingress identity, and the receipt senders stay
//! with the host. The stage's single writer decides every identified member
//! against the index of committed identities and resolves receipts only after
//! the member's own row commits. Replay and resume rebuild the index from the
//! recorded replies before the slot goes live.

use super::continuation::SourceContinuation;
use super::erased::{
    ErasedSourceCompletion, ErasedSourceInvocation, SealAsyncInfinite,
    UnifiedAsyncInfiniteSourceHandler,
};
use super::prepared::{sealed as admission_sealed, AdmitSource, AdmittedSource};
use super::read::{read_failure, recording_ends, ContinuousRead};
use super::typed::{configuration_fatal, preserve_recorded, protocol_fatal};
use super::SourceError;
use crate::effects::{
    AnswersEffect, Call, EffectContext, EffectDeclaration, EffectError, EffectInvocationContext,
    EffectsCore, Port, StageEffectsError,
};
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::stages::common::handler_error::StageFatal;
use async_trait::async_trait;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_core::event::schema::TypedFact;
use obzenflow_core::event::{ChainPayload, EventKind, SourceDiagnosticReason};
use obzenflow_core::ingress::{
    HostedIngressBindingSlot, IngressAttemptSeq, IngressContentDigest, IngressContext,
    IngressReceipt, IngressRequestId,
};
use obzenflow_core::{ChainEvent, EventId, EventType, OneFactOutcome, WriterId};
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// Resolves one submitted member's commit evidence.
pub type IngressReceiptSender = oneshot::Sender<IngressReceipt>;

/// One admitted member waiting for the stage's read.
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

/// The argument of one read of a hosted queue: at most `batch` admitted
/// members.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostedRead {
    batch: usize,
}

/// One admitted member as its read recorded it: the validated journal form of
/// its value and its durable ingress identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HostedMember {
    fact: HostedFact,
    context: IngressContext,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct HostedFact {
    kind: EventKind,
    event_type: EventType,
    schema_version: NonZeroU32,
    payload: serde_json::Value,
}

impl HostedFact {
    fn of<T: OneFactOutcome>(value: T) -> Result<Self, String> {
        let mut facts = value.into_facts().map_err(|error| {
            format!(
                "typed source output serialization failed for `{}`: {error}",
                std::any::type_name::<T>()
            )
        })?;
        if facts.len() != 1 {
            return Err(format!(
                "one_fact_outcome: `{}` lowered to {} facts instead of exactly one",
                std::any::type_name::<T>(),
                facts.len(),
            ));
        }
        let fact = facts.pop().expect("length checked above");
        Ok(Self {
            kind: fact.payload.kind(),
            payload: serde_json::to_value(&fact.payload).map_err(|error| error.to_string())?,
            event_type: fact.event_type,
            schema_version: fact.payload_schema_version,
        })
    }

    fn into_typed(self) -> Result<TypedFact, serde_json::Error> {
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

/// `HostedQueue` is the port of a hosted queue. Only Runtime implements it.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait HostedQueue: Send + Sync {
    #[effect(type = "obzenflow.hosted.queue_read", at_most_once, records = reply)]
    async fn read(
        &self,
        read: &HostedRead,
        ctx: &EffectContext,
    ) -> Result<ContinuousRead<HostedMember, HostedRead>, EffectError>;
}

type ReceiptKey = (IngressAttemptSeq, Option<usize>);

fn receipt_key(context: &IngressContext) -> ReceiptKey {
    (context.attempt_seq, context.batch_index)
}

struct QueueState<T> {
    rx: tokio::sync::Mutex<mpsc::Receiver<HostedSubmission<T>>>,
    front: Mutex<Option<HostedSubmission<T>>>,
    receipts: Mutex<HashMap<ReceiptKey, IngressReceiptSender>>,
}

impl<T> QueueState<T> {
    async fn ready(&self) -> Result<(), SourceError> {
        if self
            .front
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
        {
            return Ok(());
        }
        let first = self
            .rx
            .lock()
            .await
            .recv()
            .await
            .ok_or(SourceError::Transport(
                SourceDiagnosticReason::InputClosed.into(),
            ))?;
        *self.front.lock().unwrap_or_else(PoisonError::into_inner) = Some(first);
        Ok(())
    }

    fn take_receipt(&self, context: &IngressContext) -> Option<IngressReceiptSender> {
        self.receipts
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&receipt_key(context))
    }
}

struct QueueSystem<T> {
    queue: Arc<QueueState<T>>,
}

#[async_trait]
impl<T: OneFactOutcome + Send + Sync + 'static> HostedQueue for QueueSystem<T> {
    async fn read(
        &self,
        read: &HostedRead,
        _ctx: &EffectContext,
    ) -> Result<ContinuousRead<HostedMember, HostedRead>, EffectError> {
        let mut submissions: Vec<HostedSubmission<T>> = self
            .queue
            .front
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .into_iter()
            .collect();
        {
            let mut rx = self.queue.rx.lock().await;
            while submissions.len() < read.batch {
                match rx.try_recv() {
                    Ok(submission) => submissions.push(submission),
                    Err(_) => break,
                }
            }
        }
        let mut records = Vec::with_capacity(submissions.len());
        for submission in submissions {
            let fact = HostedFact::of(submission.value).map_err(EffectError::Serialization)?;
            self.queue
                .receipts
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(receipt_key(&submission.context), submission.receipt);
            records.push(HostedMember {
                fact,
                context: submission.context,
            });
        }
        Ok(ContinuousRead {
            records,
            next: read.clone(),
        })
    }
}

/// `HostedSource` is the handler of a hosted source stage. It holds the read
/// port of its endpoint's queue. The host keeps the queue's submission handle.
pub struct HostedSource<T> {
    port: Port<dyn HostedQueue>,
    queue: Arc<QueueState<T>>,
    slot: HostedIngressBindingSlot,
    batch: usize,
}

impl<T> fmt::Debug for HostedSource<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostedSource")
            .field("output", &std::any::type_name::<T>())
            .field("system", &self.port.system_name())
            .field("ingress_key", self.slot.ingress_key())
            .field("max_batch_size", &self.batch)
            .finish_non_exhaustive()
    }
}

impl<T: OneFactOutcome + Send + Sync + 'static> HostedSource<T> {
    /// A source of the submissions that framework-managed HTTP ingress admits.
    /// Its system is `http-ingress`.
    pub fn http(rx: mpsc::Receiver<HostedSubmission<T>>, slot: HostedIngressBindingSlot) -> Self {
        Self::named("http-ingress", rx, slot)
    }

    /// A source of the submissions that protocol-neutral ingress admits. Its
    /// system is `ingress`.
    pub fn ingress(
        rx: mpsc::Receiver<HostedSubmission<T>>,
        slot: HostedIngressBindingSlot,
    ) -> Self {
        Self::named("ingress", rx, slot)
    }

    fn named(
        name: &'static str,
        rx: mpsc::Receiver<HostedSubmission<T>>,
        slot: HostedIngressBindingSlot,
    ) -> Self {
        let queue = Arc::new(QueueState {
            rx: tokio::sync::Mutex::new(rx),
            front: Mutex::new(None),
            receipts: Mutex::new(HashMap::new()),
        });
        let system: Arc<dyn HostedQueue> = Arc::new(QueueSystem {
            queue: queue.clone(),
        });
        Self {
            port: Port::framework_system(name, system).instance(slot.ingress_key().as_str()),
            queue,
            slot,
            batch: 1000,
        }
    }

    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.batch = max_batch_size.max(1);
        self
    }

    /// The name of the system that admits this source's submissions.
    pub fn system_name(&self) -> &str {
        self.port.binding().name()
    }
}

const SENDER_REDELIVERS: SourceContinuation = SourceContinuation::Reattach {
    guarantee: ReattachGuarantee::SenderRedelivers,
};

impl<Out: OneFactOutcome + Send + Sync + 'static>
    admission_sealed::Admitted<dyn UnifiedAsyncInfiniteSourceHandler> for HostedSource<Out>
{
}

impl<Out: OneFactOutcome + Send + Sync + 'static> AdmitSource<dyn UnifiedAsyncInfiniteSourceHandler>
    for HostedSource<Out>
{
    type Output = Out;
    type Handler = HostedReadAdapter<Out>;

    fn admit(self) -> AdmittedSource<Self::Handler> {
        let system = self.port.source_system();
        AdmittedSource {
            handler: HostedReadAdapter::new(self),
            continuation: SENDER_REDELIVERS,
            system,
        }
    }
}

/// Runtime adapter for a hosted source. It reads the queue through its port,
/// decides each recorded member, and resolves receipts only after commit.
#[doc(hidden)]
pub struct HostedReadAdapter<T> {
    port: Port<dyn HostedQueue>,
    queue: Arc<QueueState<T>>,
    slot: HostedIngressBindingSlot,
    batch: usize,
    admission: HostedAdmission,
    context: Option<EffectInvocationContext>,
    number: u64,
    effects: Option<EffectsCore>,
    staged: VecDeque<Staged>,
    reproduced: bool,
    replayed: u64,
    recorded: Option<crate::replay::ReplayDriver>,
    emitted: Option<ChainEvent>,
    held: Option<ChainEvent>,
}

impl<T: OneFactOutcome + Send + Sync + 'static> HostedReadAdapter<T> {
    fn new(source: HostedSource<T>) -> Self {
        Self {
            admission: HostedAdmission::new(source.slot.clone()),
            port: source.port,
            queue: source.queue,
            slot: source.slot,
            batch: source.batch,
            context: None,
            number: 0,
            effects: None,
            staged: VecDeque::new(),
            reproduced: false,
            replayed: 0,
            recorded: None,
            emitted: None,
            held: None,
        }
    }

    async fn invocation(&mut self) -> ErasedSourceInvocation {
        self.emitted = None;
        if let Some(event) = self.held.take() {
            self.emitted = Some(event.clone());
            return ErasedSourceInvocation::completed(
                ErasedSourceCompletion::Batch(vec![event]),
                Vec::new(),
            );
        }
        loop {
            if let Some(staged) = self.staged.pop_front() {
                return match self.materialize(staged).await {
                    Ok(event) => {
                        self.emitted = Some(event.clone());
                        ErasedSourceInvocation::completed(
                            ErasedSourceCompletion::Batch(vec![event]),
                            Vec::new(),
                        )
                    }
                    Err(fatal) => ErasedSourceInvocation::fatal(fatal),
                };
            }
            if let Some(end) = self.read().await {
                return end;
            }
        }
    }

    async fn read(&mut self) -> Option<ErasedSourceInvocation> {
        let Some(mut context) = self.context.clone() else {
            return Some(ErasedSourceInvocation::fatal(configuration_fatal(
                "hosted ingress read before the runtime installed its stage context",
            )));
        };
        let Some(number) = self.number.checked_add(1) else {
            return Some(ErasedSourceInvocation::fatal(protocol_fatal(
                "hosted read number exhausted",
            )));
        };
        context.input_seq = StageInputPosition(number);
        if recording_ends(&context, number) {
            let mut effects = EffectsCore::new(context);
            if let Err(error) = effects.reproduce_trailing_crossing().await {
                return Some(ErasedSourceInvocation::fatal(
                    effects
                        .authoring_halt_fatal()
                        .unwrap_or_else(|| protocol_fatal(error.to_string())),
                ));
            }
            return Some(ErasedSourceInvocation::completed(
                ErasedSourceCompletion::Eof,
                Vec::new(),
            ));
        }
        let mut effects = EffectsCore::new(context);
        let reproduced = effects.is_replaying();
        if !reproduced {
            if let Err(error) = effects.cross_to_live().await {
                return Some(ErasedSourceInvocation::fatal(
                    effects
                        .authoring_halt_fatal()
                        .unwrap_or_else(|| protocol_fatal(error.to_string())),
                ));
            }
            if let Err(error) = self.queue.ready().await {
                return Some(ErasedSourceInvocation::handler_error(error, Vec::new()));
            }
        }
        let outcome = effects.perform_call(HostedRead { batch: self.batch }).await;
        self.number = number;
        self.reproduced = reproduced;
        match outcome {
            Ok(read) => {
                if !reproduced {
                    if let Some(fatal) = self.admission.foreign_member(&read.records) {
                        return Some(ErasedSourceInvocation::fatal(fatal));
                    }
                }
                let queue = &self.queue;
                let staged = self.admission.decide(read.records, |context| {
                    (!reproduced).then(|| queue.take_receipt(context)).flatten()
                });
                self.staged.extend(staged);
                self.effects = Some(effects);
                None
            }
            Err(error) => {
                if let Some(fatal) = effects.authoring_halt_fatal() {
                    return Some(ErasedSourceInvocation::fatal(fatal));
                }
                Some(match read_failure(&error) {
                    Some(failure) => ErasedSourceInvocation::handler_error(failure, Vec::new()),
                    None => ErasedSourceInvocation::fatal(protocol_fatal(error.to_string())),
                })
            }
        }
    }

    async fn materialize(&mut self, staged: Staged) -> Result<ChainEvent, StageFatal> {
        let Staged {
            fact,
            context,
            receipts,
            identity,
        } = staged;
        let effects = self
            .effects
            .as_mut()
            .expect("a committed read supplied the staged members");
        let fact = fact.into_typed().map_err(|error| {
            protocol_fatal(format!(
                "a recorded hosted member cannot be decoded: {error}"
            ))
        })?;
        let (mut event, _) = match effects.prepare_typed_fact(fact, false).await {
            Ok(prepared) => prepared,
            Err(error) => {
                return Err(effects
                    .authoring_halt_fatal()
                    .unwrap_or_else(|| protocol_fatal(error.to_string())))
            }
        };
        let accepted_at_ns = context.accepted_at_ns;
        event.ingress_context = Some(context);
        let event = preserve_recorded(&mut self.recorded, self.context.as_ref(), event).await?;
        self.admission
            .admitted(identity, accepted_at_ns, receipts, event.id);
        if self.reproduced {
            self.replayed += 1;
        }
        Ok(event)
    }
}

impl<T: OneFactOutcome + Send + Sync + 'static> SealAsyncInfinite for HostedReadAdapter<T> {}

#[async_trait]
impl<T: OneFactOutcome + Send + Sync + 'static> UnifiedAsyncInfiniteSourceHandler
    for HostedReadAdapter<T>
{
    fn read_declaration(&self) -> Option<Result<EffectDeclaration, StageEffectsError>> {
        Some(<Port<dyn HostedQueue> as AnswersEffect<Call<HostedRead>>>::declaration(&self.port))
    }

    fn install_read_context(&mut self, context: EffectInvocationContext) {
        self.context = Some(context);
    }

    fn has_pending_records(&self) -> bool {
        self.held.is_some() || !self.staged.is_empty()
    }

    fn poll_cancelled(&mut self) {
        if let Some(event) = self.emitted.take() {
            self.held = Some(event);
        }
    }

    fn replayed_facts(&self) -> u64 {
        self.replayed
    }

    async fn cross_to_live(&mut self) -> Result<bool, StageFatal> {
        if self.has_pending_records() {
            return Ok(false);
        }
        let Some(context) = self.context.clone() else {
            return Ok(false);
        };
        if context
            .stage
            .effect_history
            .as_ref()
            .is_some_and(|history| history.actions().next_unreproduced().is_some())
        {
            return Ok(false);
        }
        let execution = context.stage.runtime_execution.clone();
        let stage_id = context.stage.stage_id;
        let mut effects = EffectsCore::new(context);
        match effects.cross_to_live().await {
            Ok(()) => Ok(!matches!(
                execution.source_phase_for(stage_id),
                crate::execution::SourceExecutionPhase::Replaying
            )),
            Err(error) => Err(effects
                .authoring_halt_fatal()
                .unwrap_or_else(|| protocol_fatal(error.to_string()))),
        }
    }

    fn install_writer_id(&mut self, _writer_id: WriterId) {}

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> {
        Some(self.slot.clone())
    }

    async fn next_invocation(&mut self) -> ErasedSourceInvocation {
        self.invocation().await
    }

    async fn drain(&mut self) -> Result<(), SourceError> {
        self.queue.rx.lock().await.close();
        Ok(())
    }

    fn settle_committed(&mut self, committed: &[EventId]) {
        self.admission.settle(committed);
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

struct Staged {
    fact: HostedFact,
    context: IngressContext,
    receipts: Option<PendingReceipts>,
    identity: Option<(IdentityKey, IngressContentDigest)>,
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
pub(super) struct HostedAdmission {
    slot: HostedIngressBindingSlot,
    committed: HashMap<IdentityKey, CommittedIdentity>,
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

    fn foreign_member(&self, members: &[HostedMember]) -> Option<StageFatal> {
        members
            .iter()
            .find(|member| &member.context.ingress_key != self.slot.ingress_key())
            .map(|member| {
                protocol_fatal(format!(
                    "hosted ingress submission for '{}' reached source '{}'",
                    member.context.ingress_key,
                    self.slot.ingress_key()
                ))
            })
    }

    fn decide(
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

    fn admitted(
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

    pub(super) fn settle(&mut self, committed: &[EventId]) {
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
    use obzenflow_core::event::StageFatalCode;
    use obzenflow_core::ingress::{IngressKey, IngressRequestIdentity, IngressTiming};
    use obzenflow_core::TypedPayload;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
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
                fact: HostedFact::of(Row(row)).expect("a row lowers"),
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
    fn hosted_source_admission_records_sender_redelivers() {
        let (_tx, rx) = mpsc::channel::<HostedSubmission<Row>>(1);
        let admitted = HostedSource::ingress(rx, HostedIngressBindingSlot::new(KEY)).admit();
        assert_eq!(
            admitted.continuation,
            SourceContinuation::Reattach {
                guarantee: ReattachGuarantee::SenderRedelivers
            }
        );
        assert_eq!(admitted.system.name(), "ingress");
        assert_eq!(
            admitted
                .handler
                .hosted_ingress_slot()
                .map(|slot| slot.ingress_key().clone()),
            Some(IngressKey::from(KEY))
        );
    }

    #[test]
    fn receipts_resolve_only_after_their_own_row_commits() {
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
    fn recorded_member_keeps_context_and_journal_form() {
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
    fn unequal_content_in_one_read_is_a_conflict() {
        let mut harness = Harness::new();
        let (first, _first_answer, first_sender) = harness.member(7, 100, None, Some("tx-1"));
        let (second, mut conflict, second_sender) = harness.member(8, 100, None, Some("tx-1"));
        let rows = harness.read(vec![(first, first_sender), (second, second_sender)]);
        assert_eq!(rows.len(), 1);
        assert_eq!(answered(&mut conflict), Some(IngressReceipt::Conflict));
    }

    #[test]
    fn batch_members_are_identified_by_index() {
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
    fn retries_accepted_after_the_window_are_admitted_again() {
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
    fn the_index_reports_its_size() {
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
    fn reproduced_members_rebuild_the_index_without_receipts() {
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
        assert_eq!(fatal.code, StageFatalCode::Protocol);
    }
}
