// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    ContractConfig, ContractStatus, ContractsWiring, PollResult, ReaderProgress,
    UpstreamSubscription,
};
use crate::messaging::upstream_subscription_policy::build_policy_stack_for_upstream;
use async_trait::async_trait;
use obzenflow_core::control_middleware::{CircuitBreakerSnapshotter, RateLimiterSnapshotter};
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::journal_event::JournalEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::system_event::{
    ContractResultStatusLabel, SystemEvent, SystemEventType,
};
use obzenflow_core::event::types::{
    Count, DurationMs, SeqNo, ViolationCause as EventViolationCause,
};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEvent, ChainEventContent, ChainEventFactory};
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::{
    CircuitBreakerContractInfo, CircuitBreakerContractMode, ControlMiddlewareProvider, EventId,
    NoControlMiddleware, StageId, TransportContract, WriterId,
};
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::time::Instant;

#[derive(Debug, Default)]
struct TestControlMiddlewareProvider {
    breaker_contracts: RwLock<HashMap<StageId, CircuitBreakerContractInfo>>,
}

impl TestControlMiddlewareProvider {
    fn new() -> Self {
        Self::default()
    }

    fn register_stage_mode(
        &self,
        stage_id: StageId,
        mode: CircuitBreakerContractMode,
        has_fallback: bool,
    ) {
        let mut reg = self
            .breaker_contracts
            .write()
            .expect("TestControlMiddlewareProvider: poisoned lock");
        reg.insert(
            stage_id,
            CircuitBreakerContractInfo {
                mode,
                has_opened_since_registration: false,
                has_fallback_configured: has_fallback,
            },
        );
    }
}

impl ControlMiddlewareProvider for TestControlMiddlewareProvider {
    fn circuit_breaker_snapshotter(&self, _: &StageId) -> Option<Arc<CircuitBreakerSnapshotter>> {
        None
    }

    fn rate_limiter_snapshotter(&self, _: &StageId) -> Option<Arc<RateLimiterSnapshotter>> {
        None
    }

    fn circuit_breaker_state(&self, _: &StageId) -> Option<Arc<AtomicU8>> {
        None
    }

    fn circuit_breaker_contract_info(
        &self,
        stage_id: &StageId,
    ) -> Option<CircuitBreakerContractInfo> {
        self.breaker_contracts
            .read()
            .expect("TestControlMiddlewareProvider: poisoned lock")
            .get(stage_id)
            .copied()
    }

    fn mark_circuit_breaker_opened(&self, stage_id: &StageId) {
        let mut reg = self
            .breaker_contracts
            .write()
            .expect("TestControlMiddlewareProvider: poisoned lock");
        if let Some(info) = reg.get_mut(stage_id) {
            info.has_opened_since_registration = true;
        }
    }
}

/// Minimal in-memory journal implementation for tests.
struct TestJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
}

impl<T: JournalEvent> TestJournal<T> {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

/// In-memory journal with configurable append failures.
type AppendFailurePredicate<T> = dyn Fn(&T, usize) -> bool + Send + Sync;

struct ControlledJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    append_calls: AtomicUsize,
    should_fail: Arc<AppendFailurePredicate<T>>,
}

impl<T: JournalEvent> ControlledJournal<T> {
    fn new(owner: JournalOwner, should_fail: Arc<AppendFailurePredicate<T>>) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
            append_calls: AtomicUsize::new(0),
            should_fail,
        }
    }
}

struct TestJournalReader<T: JournalEvent> {
    events: Vec<EventEnvelope<T>>,
    pos: usize,
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for TestJournal<T> {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> std::result::Result<EventEnvelope<T>, JournalError> {
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        let mut guard = self.events.lock().unwrap();
        guard.push(envelope.clone());
        Ok(envelope)
    }

    async fn read_causally_ordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(guard.clone())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn reader(&self) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(Box::new(TestJournalReader {
            events: guard.clone(),
            pos: 0,
        }))
    }

    async fn reader_from(
        &self,
        position: u64,
    ) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(Box::new(TestJournalReader {
            events: guard.clone(),
            pos: position as usize,
        }))
    }

    async fn read_last_n(
        &self,
        count: usize,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        let len = guard.len();
        let start = len.saturating_sub(count);
        // Return most recent first, matching Journal contract.
        Ok(guard[start..].iter().rev().cloned().collect())
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for ControlledJournal<T> {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> std::result::Result<EventEnvelope<T>, JournalError> {
        let call_index = self.append_calls.fetch_add(1, Ordering::Relaxed);
        if (self.should_fail)(&event, call_index) {
            return Err(JournalError::Implementation {
                message: "append failed".to_string(),
                source: "append failed".into(),
            });
        }

        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        let mut guard = self.events.lock().unwrap();
        guard.push(envelope.clone());
        Ok(envelope)
    }

    async fn read_causally_ordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(guard.clone())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn reader(&self) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(Box::new(TestJournalReader {
            events: guard.clone(),
            pos: 0,
        }))
    }

    async fn reader_from(
        &self,
        position: u64,
    ) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(Box::new(TestJournalReader {
            events: guard.clone(),
            pos: position as usize,
        }))
    }

    async fn read_last_n(
        &self,
        count: usize,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        let len = guard.len();
        let start = len.saturating_sub(count);
        // Return most recent first, matching Journal contract.
        Ok(guard[start..].iter().rev().cloned().collect())
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> JournalReader<T> for TestJournalReader<T> {
    async fn next(&mut self) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
        if self.pos >= self.events.len() {
            Ok(None)
        } else {
            let envelope = self.events.get(self.pos).cloned();
            self.pos += 1;
            Ok(envelope)
        }
    }

    async fn skip(&mut self, n: u64) -> std::result::Result<u64, JournalError> {
        let start = self.pos as u64;
        self.pos = (self.pos as u64 + n) as usize;
        Ok(self.pos as u64 - start)
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.events.len()
    }
}

#[cfg(unix)]
struct EmfileJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    _phantom: std::marker::PhantomData<T>,
}

#[cfg(unix)]
impl<T: JournalEvent> EmfileJournal<T> {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            _phantom: std::marker::PhantomData,
        }
    }
}

#[cfg(unix)]
#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for EmfileJournal<T> {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        _event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> std::result::Result<EventEnvelope<T>, JournalError> {
        Err(JournalError::Implementation {
            message: "append not supported".to_string(),
            source: "append not supported".into(),
        })
    }

    async fn read_causally_ordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn reader(&self) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
        Err(JournalError::Implementation {
            message: "open failed".to_string(),
            source: Box::new(io::Error::from_raw_os_error(libc::EMFILE)),
        })
    }

    async fn reader_from(
        &self,
        _position: u64,
    ) -> std::result::Result<Box<dyn JournalReader<T>>, JournalError> {
        self.reader().await
    }

    async fn read_last_n(
        &self,
        _count: usize,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }
}

#[tokio::test]
#[cfg(unix)]
async fn fails_fast_on_too_many_open_files() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);

    let upstream_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(EmfileJournal::new(upstream_owner));

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let err = UpstreamSubscription::<ChainEvent>::new_with_names_from_positions(
        "downstream",
        &upstreams,
        &[0u64],
    )
    .await
    .err()
    .expect("Expected Too many open files error")
    .to_string();

    assert!(err.contains("Too many open files"));
}

#[tokio::test]
async fn progress_append_failure_does_not_advance_progress_state() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(ControlledJournal::new(
        contract_owner,
        Arc::new(|_event: &ChainEvent, _call| true),
    ));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal,
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].reader_seq = SeqNo(1);
    reader_progress[0].last_progress_seq = SeqNo(0);

    let _status = subscription.check_contracts(&mut reader_progress).await;

    assert_eq!(reader_progress[0].last_progress_seq, SeqNo(0));
    assert!(reader_progress[0].last_progress_instant.is_none());
}

#[tokio::test]
async fn final_append_failure_keeps_final_emitted_false() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(ControlledJournal::new(
        contract_owner,
        Arc::new(|event: &ChainEvent, _call| {
            matches!(
                &event.content,
                ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal { .. })
            )
        }),
    ));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    subscription.state.mark_reader_eof(0);

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];

    let _status = subscription.check_contracts(&mut reader_progress).await;

    assert!(!reader_progress[0].final_emitted);
    assert!(!reader_progress[0].contract_violated);

    let events = contract_journal.read_causally_ordered().await.unwrap();
    assert!(
        !events.iter().any(|env| matches!(
            &env.event.content,
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal { .. })
        )),
        "expected final event append to have failed"
    );
}

#[tokio::test]
async fn diagnostics_only_eof_check_does_not_emit_final_or_latch_state() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(contract_owner));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: true,
        cycle_guard_config: None,
    });

    subscription.state.mark_reader_eof(0);

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].reader_seq = SeqNo(1);
    reader_progress[0].receipted_seq = SeqNo(1);

    let _status = subscription
        .check_contracts_diagnostics_only(&mut reader_progress)
        .await;

    assert!(!reader_progress[0].final_emitted);
    assert!(!reader_progress[0].contract_violated);

    let events = contract_journal.read_causally_ordered().await.unwrap();
    assert!(
        !events.iter().any(|env| matches!(
            &env.event.content,
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal { .. })
        )),
        "diagnostics-only checks must not emit final contract evidence"
    );
}

#[tokio::test]
async fn contract_status_append_failure_keeps_final_emitted_false() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(contract_owner));

    let reader_stage = StageId::new();
    let system_owner = JournalOwner::stage(reader_stage);
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(ControlledJournal::new(
        system_owner,
        Arc::new(|event: &SystemEvent, _call| {
            matches!(&event.event, SystemEventType::ContractStatus { .. })
        }),
    ));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config: ContractConfig::default(),
        system_journal: Some(system_journal.clone()),
        reader_stage: Some(reader_stage),
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    // Avoid contract-chain variability: force legacy fallback path while still
    // emitting a ContractStatus system event.
    subscription.contract_chains = (0..subscription.readers.len()).map(|_| None).collect();
    subscription.contract_policies = (0..subscription.readers.len()).map(|_| None).collect();

    subscription.state.mark_reader_eof(0);

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].advertised_writer_seq = Some(SeqNo(0));
    reader_progress[0].reader_seq = SeqNo(0);

    let _status = subscription.check_contracts(&mut reader_progress).await;

    assert!(!reader_progress[0].final_emitted);
    assert!(!reader_progress[0].contract_violated);

    let contract_events = contract_journal.read_causally_ordered().await.unwrap();
    assert!(
        contract_events.iter().any(|env| matches!(
            &env.event.content,
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal { .. })
        )),
        "expected final event to be persisted even when ContractStatus append fails"
    );
}

#[tokio::test]
async fn progress_contract_heartbeats_are_suppressed_until_data_observed() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(contract_owner));

    let reader_stage = StageId::new();
    let system_owner = JournalOwner::stage(reader_stage);
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(TestJournal::new(system_owner));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config: ContractConfig::default(),
        system_journal: Some(system_journal.clone()),
        reader_stage: Some(reader_stage),
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    // Simulate having observed some flow signals, but no data events.
    //
    // This matches server `startup_mode=manual`, where stages may emit
    // `ConsumptionProgress` signals with `reader_seq=0` before any data flows.
    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].last_event_id = Some(EventId::new());
    reader_progress[0].reader_seq = SeqNo(0);

    let _status = subscription.check_contracts(&mut reader_progress).await;

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(
        !events.iter().any(|env| matches!(
            &env.event.event,
            SystemEventType::ContractResult { .. } | SystemEventType::ContractStatus { .. }
        )),
        "expected progress contract heartbeats to be suppressed before any data is observed"
    );

    // Once data is observed, progress contract heartbeats should be emitted.
    reader_progress[0].reader_seq = SeqNo(1);
    let _status = subscription.check_contracts(&mut reader_progress).await;

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(
        events.iter().any(|env| matches!(
            &env.event.event,
            SystemEventType::ContractResult { contract_name, status, cause, .. }
                if contract_name == TransportContract::NAME
                    && status == ContractResultStatusLabel::Healthy.as_str()
                    && cause.is_none()
        )),
        "expected a healthy TransportContract ContractResult heartbeat once data is observed"
    );
}

#[tokio::test]
async fn progress_emission_uses_receipt_watermark_when_delivery_contract_enabled() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(contract_owner));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: true,
        cycle_guard_config: None,
    });

    let read_event_id = EventId::new();
    let receipted_event_id = EventId::new();
    let mut read_clock = VectorClock::new();
    read_clock.clocks.insert("upstream".to_string(), 3);
    let mut receipted_clock = VectorClock::new();
    receipted_clock.clocks.insert("upstream".to_string(), 1);

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].reader_seq = SeqNo(3);
    reader_progress[0].receipted_seq = SeqNo(1);
    reader_progress[0].last_event_id = Some(read_event_id);
    reader_progress[0].last_vector_clock = Some(read_clock);
    reader_progress[0].last_receipted_event_id = Some(receipted_event_id);
    reader_progress[0].last_receipted_vector_clock = Some(receipted_clock.clone());

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(matches!(status, ContractStatus::ProgressEmitted));
    assert_eq!(reader_progress[0].last_progress_seq, SeqNo(1));

    let events = contract_journal.read_causally_ordered().await.unwrap();
    let progress = events
        .iter()
        .find_map(|env| match &env.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionProgress {
                reader_seq,
                last_event_id,
                vector_clock,
                ..
            }) => Some((*reader_seq, *last_event_id, vector_clock.clone())),
            _ => None,
        })
        .expect("expected ConsumptionProgress event");

    assert_eq!(progress.0, SeqNo(1));
    assert_eq!(progress.1, Some(receipted_event_id));
    assert_eq!(progress.2, Some(receipted_clock));
}

#[tokio::test]
async fn record_delivery_receipt_advances_only_when_receipts_become_contiguous() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(contract_owner));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal,
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: true,
        cycle_guard_config: None,
    });

    let writer_id = WriterId::from(upstream_stage);
    let first = ChainEventFactory::data_event(writer_id, "test.event", json!({"seq": 1}));
    let second = ChainEventFactory::data_event(writer_id, "test.event", json!({"seq": 2}));

    let mut clock_1 = VectorClock::new();
    clock_1.clocks.insert("upstream".to_string(), 1);
    let mut clock_2 = VectorClock::new();
    clock_2.clocks.insert("upstream".to_string(), 2);

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].reader_seq = SeqNo(1);
    reader_progress[0].track_pending_receipt(first.id, first.clone(), clock_1);
    reader_progress[0].reader_seq = SeqNo(2);
    reader_progress[0].track_pending_receipt(second.id, second.clone(), clock_2.clone());

    let second_receipt = ChainEventFactory::delivery_event(
        WriterId::from(contract_stage),
        DeliveryPayload::success("dest", DeliveryMethod::Noop, None),
    )
    .with_causality(CausalityContext::with_parent(second.id));

    assert!(subscription
        .record_delivery_receipt(&second_receipt, &mut reader_progress)
        .is_none());
    assert_eq!(reader_progress[0].receipted_seq, SeqNo(0));

    let first_receipt = ChainEventFactory::delivery_event(
        WriterId::from(contract_stage),
        DeliveryPayload::success("dest", DeliveryMethod::Noop, None),
    )
    .with_causality(CausalityContext::with_parent(first.id));

    let watermark = subscription
        .record_delivery_receipt(&first_receipt, &mut reader_progress)
        .expect("expected contiguous receipts to advance watermark");

    assert_eq!(watermark.0, SeqNo(2));
    assert_eq!(watermark.1, second.id);
    assert_eq!(watermark.2, clock_2);
    assert_eq!(reader_progress[0].receipted_seq, SeqNo(2));
    assert!(reader_progress[0].pending_receipts.is_empty());
    assert!(reader_progress[0].committed_out_of_order.is_empty());
}

#[tokio::test]
async fn stall_append_failure_does_not_set_stalled_since() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(ControlledJournal::new(
        contract_owner,
        Arc::new(|event: &ChainEvent, _call| {
            matches!(
                &event.content,
                ChainEventContent::FlowControl(FlowControlPayload::ReaderStalled { .. })
            )
        }),
    ));

    let config = ContractConfig {
        progress_min_events: Count(100),
        progress_max_interval: DurationMs(10_000),
        stall_threshold: DurationMs(100),
        stall_cooloff: DurationMs(0),
        stall_checks_before_emit: 1,
    };

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal,
        config,
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].last_read_instant =
        Some(Instant::now() - std::time::Duration::from_millis(250));
    reader_progress[0].last_progress_seq = reader_progress[0].reader_seq;

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(
        matches!(status, ContractStatus::Stalled(s) if s == upstream_stage),
        "expected stall status even when append fails"
    );

    assert!(reader_progress[0].stalled_since.is_none());
    assert!(!reader_progress[0].contract_violated);
}

#[tokio::test]
async fn idle_reader_without_any_reads_does_not_emit_stall() {
    tokio::time::pause();

    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(contract_owner));

    let config = ContractConfig {
        progress_min_events: Count(100),
        progress_max_interval: DurationMs(10_000),
        stall_threshold: DurationMs(100),
        stall_cooloff: DurationMs(0),
        stall_checks_before_emit: 1,
    };

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config,
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];

    for _ in 0..4 {
        tokio::time::advance(std::time::Duration::from_millis(150)).await;
        let status = subscription.check_contracts(&mut reader_progress).await;
        assert!(
            matches!(status, ContractStatus::Healthy),
            "idle reader should stay healthy before any reads"
        );
    }

    assert!(reader_progress[0].last_read_instant.is_none());
    assert!(reader_progress[0].stalled_since.is_none());
    assert!(!reader_progress[0].contract_violated);
    assert!(contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract journal")
        .is_empty());
}

#[tokio::test]
async fn multi_reader_progress_isolated_under_partial_append_failure() {
    let upstream_a = StageId::new();
    let upstream_b = StageId::new();

    let journal_a: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(upstream_a)));
    let journal_b: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(upstream_b)));

    let upstreams = [
        (upstream_a, "upstream_a".to_string(), journal_a),
        (upstream_b, "upstream_b".to_string(), journal_b),
    ];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_stage = StageId::new();
    let contract_owner = JournalOwner::stage(contract_stage);
    let contract_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(ControlledJournal::new(
        contract_owner,
        Arc::new(|event: &ChainEvent, _call| match &event.content {
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionProgress {
                reader_index,
                ..
            }) => reader_index.0 == 0,
            _ => false,
        }),
    ));

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal,
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: None,
        control_middleware: Arc::new(NoControlMiddleware),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    let mut reader_progress = [
        ReaderProgress::new(upstream_a),
        ReaderProgress::new(upstream_b),
    ];
    reader_progress[0].reader_seq = SeqNo(1);
    reader_progress[0].last_progress_seq = SeqNo(0);
    reader_progress[1].reader_seq = SeqNo(1);
    reader_progress[1].last_progress_seq = SeqNo(0);

    let status = subscription.check_contracts(&mut reader_progress[..]).await;
    assert!(matches!(status, ContractStatus::ProgressEmitted));

    assert_eq!(reader_progress[0].last_progress_seq, SeqNo(0));
    assert!(reader_progress[0].last_progress_instant.is_none());

    assert_eq!(reader_progress[1].last_progress_seq, SeqNo(1));
    assert!(reader_progress[1].last_progress_instant.is_some());
}

async fn build_upstream_with_seq_divergence(
    control_middleware: Arc<dyn ControlMiddlewareProvider>,
) -> (
    UpstreamSubscription<ChainEvent>,
    Arc<dyn Journal<ChainEvent>>,
    Arc<dyn Journal<SystemEvent>>,
    StageId,
    StageId,
) {
    let upstream_stage = StageId::new();
    let reader_stage = StageId::new();

    let upstream_owner = JournalOwner::stage(upstream_stage);
    let reader_owner = JournalOwner::stage(reader_stage);

    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let contract_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(reader_owner.clone()));
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(TestJournal::new(reader_owner));

    // One data event followed by EOF that advertises more events than read.
    let writer_id = WriterId::Stage(upstream_stage);
    let data_event = ChainEventFactory::data_event(writer_id, "test.event", json!({}));
    upstream_journal.append(data_event, None).await.unwrap();

    let mut eof_event = ChainEventFactory::eof_event(writer_id, true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_id: writer_id_field,
        writer_seq,
        ..
    }) = &mut eof_event.content
    {
        *writer_id_field = Some(writer_id);
        *writer_seq = Some(SeqNo(3));
    }
    upstream_journal.append(eof_event, None).await.unwrap();

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap();

    let contract_config = ContractConfig::default();
    let writer_id_for_contracts = WriterId::from(reader_stage);
    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: writer_id_for_contracts,
        contract_journal: contract_journal.clone(),
        config: contract_config,
        system_journal: Some(system_journal.clone()),
        reader_stage: Some(reader_stage),
        control_middleware,
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    (
        subscription,
        contract_journal,
        system_journal,
        upstream_stage,
        reader_stage,
    )
}

async fn drive_subscription_to_eof(
    subscription: &mut UpstreamSubscription<ChainEvent>,
    reader_progress: &mut [ReaderProgress],
) {
    loop {
        match subscription
            .poll_next_with_state("test_fsm", Some(reader_progress))
            .await
        {
            PollResult::Event(_env) => continue,
            PollResult::NoEvents => break,
            PollResult::Error(e) => {
                panic!("poll_next_with_state returned error: {e:?}");
            }
        }
    }
}

#[tokio::test]
async fn strict_mode_produces_seq_divergence_and_gap_event() {
    let (mut subscription, contract_journal, system_journal, upstream_stage, reader_stage) =
        build_upstream_with_seq_divergence(Arc::new(NoControlMiddleware)).await;

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    drive_subscription_to_eof(&mut subscription, &mut reader_progress).await;

    let status = subscription.check_contracts(&mut reader_progress).await;

    match status {
        ContractStatus::Violated { upstream, cause } => {
            assert_eq!(upstream, upstream_stage);
            match cause {
                EventViolationCause::SeqDivergence { advertised, reader } => {
                    assert_eq!(advertised, Some(SeqNo(3)));
                    assert_eq!(reader, SeqNo(1));
                }
                other => panic!("expected SeqDivergence cause, got {other:?}"),
            }
        }
        other => panic!("expected violated status, got {other:?}"),
    }

    let events = contract_journal.read_causally_ordered().await.unwrap();

    let mut final_found = false;
    let mut gap_found = false;
    let mut violation_found = false;

    for env in &events {
        match &env.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                pass,
                reader_seq,
                advertised_writer_seq,
                failure_reason,
                ..
            }) => {
                final_found = true;
                assert!(!pass);
                assert_eq!(*reader_seq, SeqNo(1));
                assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));
                match failure_reason {
                    Some(EventViolationCause::SeqDivergence { advertised, reader }) => {
                        assert_eq!(*advertised, Some(SeqNo(3)));
                        assert_eq!(*reader, SeqNo(1));
                    }
                    other => panic!("expected SeqDivergence failure_reason, got {other:?}"),
                }
            }
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionGap {
                from_seq,
                to_seq,
                upstream,
            }) => {
                gap_found = true;
                assert_eq!(*from_seq, SeqNo(2));
                assert_eq!(*to_seq, SeqNo(3));
                assert_eq!(*upstream, upstream_stage);
            }
            ChainEventContent::FlowControl(FlowControlPayload::AtLeastOnceViolation {
                upstream,
                reason,
                reader_seq,
                advertised_writer_seq,
            }) => {
                violation_found = true;
                assert_eq!(*upstream, upstream_stage);
                assert_eq!(*reader_seq, SeqNo(1));
                assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));
                match reason {
                    EventViolationCause::SeqDivergence { advertised, reader } => {
                        assert_eq!(*advertised, Some(SeqNo(3)));
                        assert_eq!(*reader, SeqNo(1));
                    }
                    other => panic!(
                        "expected SeqDivergence reason in AtLeastOnceViolation, got {other:?}"
                    ),
                }
            }
            _ => {}
        }
    }

    assert!(final_found, "expected a ConsumptionFinal event");
    assert!(gap_found, "expected a ConsumptionGap event");
    assert!(
        violation_found,
        "expected an AtLeastOnceViolation event for SeqDivergence"
    );

    let system_events = system_journal.read_causally_ordered().await.unwrap();
    let mut status_found = false;
    let mut override_found = false;

    for env in &system_events {
        match &env.event.event {
            SystemEventType::ContractStatus {
                upstream,
                reader,
                pass,
                reason,
                ..
            } => {
                if *pass {
                    // FLOWIP-080r may emit passing contract-status heartbeats during
                    // mid-flight checks. This test asserts that a failure status is
                    // emitted for strict SeqDivergence at EOF.
                    continue;
                }

                status_found = true;
                assert_eq!(*upstream, upstream_stage);
                assert_eq!(*reader, reader_stage);
                match reason {
                    Some(EventViolationCause::SeqDivergence { .. }) => {}
                    other => {
                        panic!("expected SeqDivergence reason in ContractStatus, got {other:?}")
                    }
                }
            }
            SystemEventType::ContractOverrideByPolicy { .. } => {
                override_found = true;
            }
            _ => {}
        }
    }

    assert!(status_found, "expected ContractStatus system event");
    assert!(
        !override_found,
        "did not expect ContractOverrideByPolicy in strict mode"
    );
}

#[tokio::test]
async fn breaker_aware_mode_overrides_seq_divergence_and_emits_override_event() {
    let control_middleware = Arc::new(TestControlMiddlewareProvider::new());
    let (mut subscription, contract_journal, system_journal, upstream_stage, reader_stage) =
        build_upstream_with_seq_divergence(control_middleware.clone()).await;

    // Register breaker-aware contract mode with fallback configured and mark
    // that the breaker has opened at least once. This makes the policy
    // layer eligible to override pure SeqDivergence failures.
    control_middleware.register_stage_mode(
        upstream_stage,
        CircuitBreakerContractMode::BreakerAware,
        true,
    );
    control_middleware.mark_circuit_breaker_opened(&upstream_stage);

    // Rebuild the policy stack so that it includes BreakerAwarePolicy.
    let control_provider: Arc<dyn ControlMiddlewareProvider> = control_middleware.clone();
    subscription.contract_policies = subscription
        .readers
        .iter()
        .map(|(upstream, _name, _reader)| {
            let stack = build_policy_stack_for_upstream(*upstream, &control_provider);
            Some(stack)
        })
        .collect();

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    drive_subscription_to_eof(&mut subscription, &mut reader_progress).await;

    let status = subscription.check_contracts(&mut reader_progress).await;

    // With breaker-aware contracts, the SeqDivergence should be treated as pass.
    match status {
        ContractStatus::ProgressEmitted | ContractStatus::Healthy => {}
        other => panic!("expected non-violated status under BreakerAware, got {other:?}"),
    }

    let events = contract_journal.read_causally_ordered().await.unwrap();

    let mut final_pass_found = false;
    let mut gap_found = false;

    for env in &events {
        match &env.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                pass,
                reader_seq,
                advertised_writer_seq,
                failure_reason,
                ..
            }) => {
                final_pass_found = true;
                assert!(*pass, "expected pass=true in ConsumptionFinal");
                assert_eq!(*reader_seq, SeqNo(1));
                assert_eq!(*advertised_writer_seq, Some(SeqNo(3)));
                assert!(
                    failure_reason.is_none(),
                    "expected no failure_reason when overridden by policy"
                );
            }
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionGap { .. }) => {
                gap_found = true;
            }
            _ => {}
        }
    }

    assert!(
        final_pass_found,
        "expected a ConsumptionFinal event under BreakerAware mode"
    );
    assert!(
        !gap_found,
        "did not expect a ConsumptionGap event when override is applied"
    );

    let system_events = system_journal.read_causally_ordered().await.unwrap();
    let mut status_found = false;
    let mut override_found = false;

    for env in &system_events {
        match &env.event.event {
            SystemEventType::ContractStatus {
                upstream,
                reader,
                pass,
                reason,
                ..
            } => {
                status_found = true;
                assert_eq!(*upstream, upstream_stage);
                assert_eq!(*reader, reader_stage);
                assert!(*pass, "expected pass=true in ContractStatus");
                assert!(
                    reason.is_none(),
                    "expected no reason when contracts are overridden to pass"
                );
            }
            SystemEventType::ContractOverrideByPolicy {
                upstream,
                reader,
                policy,
                ..
            } => {
                override_found = true;
                assert_eq!(*upstream, upstream_stage);
                assert_eq!(*reader, reader_stage);
                assert_eq!(policy, "breaker_aware");
            }
            _ => {}
        }
    }

    assert!(status_found, "expected ContractStatus system event");
    assert!(
        override_found,
        "expected ContractOverrideByPolicy system event in BreakerAware mode"
    );
}

#[tokio::test]
async fn transport_only_skips_observability_events() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));

    let writer_id = WriterId::Stage(upstream_stage);

    // Many real-world stage journals contain large volumes of lifecycle/observability events.
    // Downstream stage subscriptions should not be forced to "process" them as part of normal
    // transport draining; they should be skipped at the subscription layer.
    upstream_journal
        .append(
            ChainEventFactory::stage_running(writer_id, upstream_stage),
            None,
        )
        .await
        .unwrap();
    upstream_journal
        .append(
            ChainEventFactory::stage_running(writer_id, upstream_stage),
            None,
        )
        .await
        .unwrap();

    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.event", json!({"n": 1})),
            None,
        )
        .await
        .unwrap();

    upstream_journal
        .append(
            ChainEventFactory::stage_running(writer_id, upstream_stage),
            None,
        )
        .await
        .unwrap();

    upstream_journal
        .append(ChainEventFactory::eof_event(writer_id, true), None)
        .await
        .unwrap();

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap()
        .transport_only();

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];

    let first = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    match first {
        PollResult::Event(env) => match env.event.content {
            ChainEventContent::Data { .. } => {}
            other => panic!("expected first delivered event to be Data, got {other:?}"),
        },
        other => panic!("expected PollResult::Event, got {other:?}"),
    }

    let second = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    match second {
        PollResult::Event(env) => match env.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. }) => {}
            other => panic!("expected second delivered event to be EOF, got {other:?}"),
        },
        other => panic!("expected PollResult::Event, got {other:?}"),
    }

    let outcome = subscription
        .take_last_eof_outcome()
        .expect("expected subscription to mark authoritative EOF");
    assert!(outcome.is_final);
    assert_eq!(outcome.stage_id, upstream_stage);
    assert_eq!(outcome.reader_index, 0);
    assert_eq!(outcome.eof_count, 1);
    assert_eq!(outcome.total_readers, 1);
}
