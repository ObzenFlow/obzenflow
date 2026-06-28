// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    ContractConfig, ContractStatus, ContractsWiring, FeedIdentity, PollResult, ReaderProgress,
    ReaderSelectionPolicy, SelectedFeedMetadata, SelectedFeedRole, UpstreamSubscription,
};
use crate::control_plane::{ControlPlaneProvider, NoControlPlane};
use async_trait::async_trait;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::journal_event::JournalEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::effect_payload::{
    EffectFactOwner, EffectProvenance, EFFECT_RECORD_EVENT_TYPE,
};
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
use obzenflow_core::{EventId, EventType, StageId, TransportContract, WriterId};
use serde_json::json;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::time::Instant;

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

    async fn read_all_unordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        self.read_causally_ordered().await
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

    async fn read_all_unordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        self.read_causally_ordered().await
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

    async fn read_all_unordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<T>>, JournalError> {
        self.read_causally_ordered().await
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
        control_plane: Arc::new(NoControlPlane),
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
        control_plane: Arc::new(NoControlPlane),
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
        control_plane: Arc::new(NoControlPlane),
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
async fn diagnostics_only_eof_check_does_not_emit_stall() {
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
        system_journal: Some(system_journal.clone()),
        reader_stage: Some(reader_stage),
        control_plane: Arc::new(NoControlPlane),
        include_delivery_contract: true,
        cycle_guard_config: None,
    });

    subscription.state.mark_reader_eof(0);

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].reader_seq = SeqNo(1);
    reader_progress[0].receipted_seq = SeqNo(1);
    reader_progress[0].advertised_writer_seq = Some(SeqNo(1));
    reader_progress[0].last_read_instant =
        Some(Instant::now() - std::time::Duration::from_millis(250));

    let status = subscription
        .check_contracts_diagnostics_only(&mut reader_progress)
        .await;

    assert!(
        matches!(status, ContractStatus::Healthy),
        "EOF readers must not be stall-checked in diagnostics-only mode"
    );
    assert_eq!(reader_progress[0].consecutive_stall_checks, 0);
    assert!(reader_progress[0].stalled_since.is_none());
    assert!(!reader_progress[0].contract_violated);

    assert!(contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract journal")
        .is_empty());
    assert!(system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal")
        .is_empty());
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
        control_plane: Arc::new(NoControlPlane),
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
        control_plane: Arc::new(NoControlPlane),
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
                if contract_name.as_str() == TransportContract::NAME
                    && *status == ContractResultStatusLabel::Healthy
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
        control_plane: Arc::new(NoControlPlane),
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
        control_plane: Arc::new(NoControlPlane),
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
        control_plane: Arc::new(NoControlPlane),
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
async fn stall_cooloff_suppresses_repeat_stalled_emission() {
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
        progress_max_interval: DurationMs(500),
        stall_threshold: DurationMs(100),
        stall_cooloff: DurationMs(1_000),
        stall_checks_before_emit: 1,
    };

    subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(contract_stage),
        contract_journal: contract_journal.clone(),
        config,
        system_journal: None,
        reader_stage: None,
        control_plane: Arc::new(NoControlPlane),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    reader_progress[0].last_read_instant = Some(Instant::now());
    reader_progress[0].last_progress_instant = Some(Instant::now());
    reader_progress[0].last_progress_seq = reader_progress[0].reader_seq;

    // Stall emitted on first check (should_emit_progress is false, stall_threshold exceeded).
    tokio::time::advance(std::time::Duration::from_millis(200)).await;
    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(
        matches!(status, ContractStatus::Stalled(s) if s == upstream_stage),
        "expected initial stalled status"
    );

    // Progress emission clears stalled_since (time_elapsed >= progress_max_interval).
    tokio::time::advance(std::time::Duration::from_millis(400)).await;
    let _ = subscription.check_contracts(&mut reader_progress).await;

    // Still stalled, but within cooloff window, so do not emit ReaderStalled again.
    tokio::time::advance(std::time::Duration::from_millis(50)).await;
    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(
        matches!(status, ContractStatus::Stalled(s) if s == upstream_stage),
        "expected stalled status during cooloff"
    );

    let envelopes = contract_journal
        .read_causally_ordered()
        .await
        .expect("read contract journal");
    let stalled_count = envelopes
        .iter()
        .filter(|e| {
            matches!(
                &e.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::ReaderStalled { .. })
            )
        })
        .count();
    assert_eq!(
        stalled_count, 1,
        "expected ReaderStalled to be emitted once within cooloff"
    );
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
        control_plane: Arc::new(NoControlPlane),
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
        control_plane: Arc::new(NoControlPlane),
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
    control_plane: Arc<dyn ControlPlaneProvider>,
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
        control_plane,
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
        build_upstream_with_seq_divergence(Arc::new(NoControlPlane)).await;

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

    for env in &system_events {
        if let SystemEventType::ContractStatus {
            upstream,
            reader,
            pass,
            reason,
            ..
        } = &env.event.event
        {
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
    }

    assert!(status_found, "expected ContractStatus system event");
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

#[tokio::test]
async fn transport_only_filters_unselected_data_and_reconciles_selected_writer_seq() {
    let upstream_stage = StageId::new();
    let reader_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let reader_owner = JournalOwner::stage(reader_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let contract_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(reader_owner.clone()));
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(TestJournal::new(reader_owner));

    let writer_id = WriterId::Stage(upstream_stage);

    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.ignored.v1", json!({"n": 1})),
            None,
        )
        .await
        .unwrap();
    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.selected.v1", json!({"n": 2})),
            None,
        )
        .await
        .unwrap();

    let mut eof_event = ChainEventFactory::eof_event(writer_id, true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_seq,
        writer_seq_by_event_type,
        ..
    }) = &mut eof_event.content
    {
        *writer_seq = Some(SeqNo(2));
        writer_seq_by_event_type.insert("test.ignored.v1".into(), SeqNo(1));
        writer_seq_by_event_type.insert("test.selected.v1".into(), SeqNo(1));
    }
    upstream_journal.append(eof_event, None).await.unwrap();

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];
    let mut selected_feeds = HashMap::new();
    selected_feeds.insert(
        upstream_stage,
        vec![SelectedFeedMetadata::new(
            EventType::from("test.selected.v1"),
            SelectedFeedRole::Input,
        )],
    );

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap()
        .with_selected_feeds(selected_feeds)
        .with_contracts(ContractsWiring {
            writer_id: WriterId::from(reader_stage),
            contract_journal: contract_journal.clone(),
            config: ContractConfig::default(),
            system_journal: Some(system_journal.clone()),
            reader_stage: Some(reader_stage),
            control_plane: Arc::new(NoControlPlane),
            include_delivery_contract: false,
            cycle_guard_config: None,
        })
        .transport_only();

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];

    let first = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    match first {
        PollResult::Event(env) => match env.event.content {
            ChainEventContent::Data { event_type, .. } => {
                assert_eq!(event_type, "test.selected.v1");
            }
            other => panic!("expected selected Data event, got {other:?}"),
        },
        other => panic!("expected PollResult::Event, got {other:?}"),
    }

    assert_eq!(reader_progress[0].reader_seq, SeqNo(1));
    assert_eq!(
        subscription
            .last_delivered_stage_input_position()
            .expect("selected data should receive a stage input position")
            .0,
        1
    );

    drive_subscription_to_eof(&mut subscription, &mut reader_progress).await;

    assert_eq!(reader_progress[0].reader_seq, SeqNo(1));
    assert_eq!(reader_progress[0].advertised_writer_seq, Some(SeqNo(1)));

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(
        matches!(
            status,
            ContractStatus::ProgressEmitted | ContractStatus::Healthy
        ),
        "selected feed should reconcile successfully, got {status:?}"
    );
    assert!(!reader_progress[0].contract_violated);

    let contract_events = contract_journal.read_causally_ordered().await.unwrap();
    let mut final_found = false;
    for env in &contract_events {
        match &env.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::ConsumptionFinal {
                pass,
                reader_seq,
                advertised_writer_seq,
                failure_reason,
                ..
            }) => {
                final_found = true;
                assert!(*pass);
                assert_eq!(*reader_seq, SeqNo(1));
                assert_eq!(*advertised_writer_seq, Some(SeqNo(1)));
                assert!(failure_reason.is_none());
            }
            ChainEventContent::FlowControl(
                FlowControlPayload::ConsumptionGap { .. }
                | FlowControlPayload::AtLeastOnceViolation { .. },
            ) => panic!("selected feed should not emit gap or violation events"),
            _ => {}
        }
    }
    assert!(final_found, "expected a selected-feed ConsumptionFinal");

    let system_events = system_journal.read_causally_ordered().await.unwrap();
    assert!(system_events.iter().any(|env| matches!(
        &env.event.event,
        SystemEventType::ContractStatus {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            reason,
        } if *upstream == upstream_stage
            && *reader == reader_stage
            && selected_event_type.as_ref().map(|event_type| event_type.as_str())
                == Some("test.selected.v1")
            && feed_role.as_ref().map(|role| role.as_str()) == Some("input")
            && *pass
            && *reader_seq == Some(SeqNo(1))
            && *advertised_writer_seq == Some(SeqNo(1))
            && reason.is_none()
    )));
}

#[tokio::test]
async fn multi_selected_feeds_emit_direct_contract_status_per_feed() {
    let upstream_stage = StageId::new();
    let reader_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let reader_owner = JournalOwner::stage(reader_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let contract_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(reader_owner.clone()));
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(TestJournal::new(reader_owner));

    let writer_id = WriterId::Stage(upstream_stage);
    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.first.v1", json!({"n": 1})),
            None,
        )
        .await
        .unwrap();
    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.second.v1", json!({"n": 2})),
            None,
        )
        .await
        .unwrap();

    let mut eof_event = ChainEventFactory::eof_event(writer_id, true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_seq,
        writer_seq_by_event_type,
        ..
    }) = &mut eof_event.content
    {
        // Aggregate selected count is 2, but the per-feed evidence is deliberately
        // inconsistent: first advertises 2 while second advertises 0.
        *writer_seq = Some(SeqNo(2));
        writer_seq_by_event_type.insert("test.first.v1".into(), SeqNo(2));
        writer_seq_by_event_type.insert("test.second.v1".into(), SeqNo(0));
    }
    upstream_journal.append(eof_event, None).await.unwrap();

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];
    let mut selected_feeds = HashMap::new();
    selected_feeds.insert(
        upstream_stage,
        vec![
            SelectedFeedMetadata::new(EventType::from("test.first.v1"), SelectedFeedRole::Input),
            SelectedFeedMetadata::new(
                EventType::from("test.second.v1"),
                SelectedFeedRole::Reference,
            ),
        ],
    );

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap()
        .with_selected_feeds(selected_feeds)
        .with_contracts(ContractsWiring {
            writer_id: WriterId::from(reader_stage),
            contract_journal: contract_journal.clone(),
            config: ContractConfig::default(),
            system_journal: Some(system_journal.clone()),
            reader_stage: Some(reader_stage),
            control_plane: Arc::new(NoControlPlane),
            include_delivery_contract: false,
            cycle_guard_config: None,
        })
        .transport_only();

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    drive_subscription_to_eof(&mut subscription, &mut reader_progress).await;

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(
        matches!(status, ContractStatus::Violated { upstream, .. } if upstream == upstream_stage),
        "per-feed divergence should fail even when aggregate selected counts reconcile, got {status:?}"
    );

    let system_events = system_journal.read_causally_ordered().await.unwrap();
    let mut first_status = None;
    let mut second_status = None;
    let mut aggregate_status_found = false;

    for env in &system_events {
        if let SystemEventType::ContractStatus {
            upstream,
            reader,
            selected_event_type,
            feed_role,
            pass,
            reader_seq,
            advertised_writer_seq,
            reason,
        } = &env.event.event
        {
            if *upstream != upstream_stage || *reader != reader_stage {
                continue;
            }
            match (
                selected_event_type
                    .as_ref()
                    .map(|event_type| event_type.as_str()),
                feed_role.as_ref().map(|role| role.as_str()),
            ) {
                (Some("test.first.v1"), Some("input")) => {
                    first_status =
                        Some((*pass, *reader_seq, *advertised_writer_seq, reason.clone()));
                }
                (Some("test.second.v1"), Some("reference")) => {
                    second_status =
                        Some((*pass, *reader_seq, *advertised_writer_seq, reason.clone()));
                }
                (None, None) => {
                    aggregate_status_found = true;
                }
                _ => {}
            }
        }
    }

    let first_status = first_status.expect("expected first feed ContractStatus");
    assert!(!first_status.0);
    assert_eq!(first_status.1, Some(SeqNo(1)));
    assert_eq!(first_status.2, Some(SeqNo(2)));
    assert!(matches!(
        first_status.3,
        Some(EventViolationCause::SeqDivergence {
            advertised: Some(SeqNo(2)),
            reader: SeqNo(1),
        })
    ));

    let second_status = second_status.expect("expected second feed ContractStatus");
    assert!(!second_status.0);
    assert_eq!(second_status.1, Some(SeqNo(1)));
    assert_eq!(second_status.2, Some(SeqNo(0)));
    assert!(matches!(
        second_status.3,
        Some(EventViolationCause::SeqDivergence {
            advertised: Some(SeqNo(0)),
            reader: SeqNo(1),
        })
    ));

    assert!(
        !aggregate_status_found,
        "direct feed status should suppress ambiguous aggregate ContractStatus"
    );
}

#[tokio::test]
async fn multi_selected_feeds_emit_midflight_contract_results_per_feed() {
    let upstream_stage = StageId::new();
    let reader_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let reader_owner = JournalOwner::stage(reader_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));
    let contract_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(reader_owner.clone()));
    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(TestJournal::new(reader_owner));

    let writer_id = WriterId::Stage(upstream_stage);
    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.first.v1", json!({"n": 1})),
            None,
        )
        .await
        .unwrap();
    upstream_journal
        .append(
            ChainEventFactory::data_event(writer_id, "test.second.v1", json!({"n": 2})),
            None,
        )
        .await
        .unwrap();

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];
    let mut selected_feeds = HashMap::new();
    selected_feeds.insert(
        upstream_stage,
        vec![
            SelectedFeedMetadata::new(EventType::from("test.first.v1"), SelectedFeedRole::Input),
            SelectedFeedMetadata::new(
                EventType::from("test.second.v1"),
                SelectedFeedRole::Reference,
            ),
        ],
    );

    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap()
        .with_selected_feeds(selected_feeds)
        .with_contracts(ContractsWiring {
            writer_id: WriterId::from(reader_stage),
            contract_journal,
            config: ContractConfig::default(),
            system_journal: Some(system_journal.clone()),
            reader_stage: Some(reader_stage),
            control_plane: Arc::new(NoControlPlane),
            include_delivery_contract: false,
            cycle_guard_config: None,
        })
        .transport_only();

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];

    let first = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    assert!(matches!(
        first,
        PollResult::Event(EventEnvelope {
            event: ChainEvent {
                content: ChainEventContent::Data { ref event_type, .. },
                ..
            },
            ..
        }) if event_type == "test.first.v1"
    ));

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(matches!(
        status,
        ContractStatus::ProgressEmitted | ContractStatus::Healthy
    ));

    let events_after_first = system_journal.read_causally_ordered().await.unwrap();
    let first_feed_results = events_after_first
        .iter()
        .filter(|env| {
            matches!(
                &env.event.event,
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type,
                    feed_role,
                    contract_name,
                    status,
                    reader_seq,
                    advertised_writer_seq,
                    ..
                } if *upstream == upstream_stage
                    && *reader == reader_stage
                    && selected_event_type.as_ref().map(|event_type| event_type.as_str())
                        == Some("test.first.v1")
                    && feed_role.as_ref().map(|role| role.as_str()) == Some("input")
                    && contract_name.as_str() == TransportContract::NAME
                    && *status == ContractResultStatusLabel::Healthy
                    && *reader_seq == Some(SeqNo(1))
                    && advertised_writer_seq.is_none()
            )
        })
        .count();
    let second_feed_results_after_first = events_after_first
        .iter()
        .filter(|env| {
            matches!(
                &env.event.event,
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type,
                    feed_role,
                    ..
                } if *upstream == upstream_stage
                    && *reader == reader_stage
                    && selected_event_type.as_ref().map(|event_type| event_type.as_str())
                        == Some("test.second.v1")
                    && feed_role.as_ref().map(|role| role.as_str()) == Some("reference")
            )
        })
        .count();
    let aggregate_results_after_first = events_after_first
        .iter()
        .filter(|env| {
            matches!(
                &env.event.event,
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type: None,
                    feed_role: None,
                    ..
                } if *upstream == upstream_stage && *reader == reader_stage
            )
        })
        .count();
    assert_eq!(first_feed_results, 1);
    assert_eq!(second_feed_results_after_first, 0);
    assert_eq!(aggregate_results_after_first, 0);

    let second = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    assert!(matches!(
        second,
        PollResult::Event(EventEnvelope {
            event: ChainEvent {
                content: ChainEventContent::Data { ref event_type, .. },
                ..
            },
            ..
        }) if event_type == "test.second.v1"
    ));

    let status = subscription.check_contracts(&mut reader_progress).await;
    assert!(matches!(
        status,
        ContractStatus::ProgressEmitted | ContractStatus::Healthy
    ));

    let events_after_second = system_journal.read_causally_ordered().await.unwrap();
    let first_feed_results_after_second = events_after_second
        .iter()
        .filter(|env| {
            matches!(
                &env.event.event,
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type,
                    feed_role,
                    ..
                } if *upstream == upstream_stage
                    && *reader == reader_stage
                    && selected_event_type.as_ref().map(|event_type| event_type.as_str())
                        == Some("test.first.v1")
                    && feed_role.as_ref().map(|role| role.as_str()) == Some("input")
            )
        })
        .count();
    let second_feed_results = events_after_second
        .iter()
        .filter(|env| {
            matches!(
                &env.event.event,
                SystemEventType::ContractResult {
                    upstream,
                    reader,
                    selected_event_type,
                    feed_role,
                    contract_name,
                    status,
                    reader_seq,
                    advertised_writer_seq,
                    ..
                } if *upstream == upstream_stage
                    && *reader == reader_stage
                    && selected_event_type.as_ref().map(|event_type| event_type.as_str())
                        == Some("test.second.v1")
                    && feed_role.as_ref().map(|role| role.as_str()) == Some("reference")
                    && contract_name.as_str() == TransportContract::NAME
                    && *status == ContractResultStatusLabel::Healthy
                    && *reader_seq == Some(SeqNo(1))
                    && advertised_writer_seq.is_none()
            )
        })
        .count();
    assert_eq!(first_feed_results_after_second, 1);
    assert_eq!(second_feed_results, 1);
}

#[tokio::test]
async fn transport_only_skips_framework_effect_data_without_stage_input_position() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));

    let writer_id = WriterId::Stage(upstream_stage);
    let effect_record = obzenflow_core::event::payloads::effect_payload::EffectRecord {
        cursor: obzenflow_core::event::payloads::effect_payload::EffectCursor::new(
            "recorded-flow",
            "gateway",
            1,
            0,
        ),
        descriptor_hash: "hash".into(),
        descriptor: obzenflow_core::event::payloads::effect_payload::EffectDescriptor::new(
            "test.effect",
            "test",
            1,
            "1",
            "input-hash",
        ),
        outcome: obzenflow_core::event::payloads::effect_payload::EffectOutcomePayload::Succeeded {
            output: json!({"ok": true}),
        },
        origin: None,
    };

    upstream_journal
        .append(
            ChainEventFactory::derived_data_event(
                writer_id,
                &ChainEventFactory::data_event(writer_id, "test.parent", json!({})),
                EFFECT_RECORD_EVENT_TYPE,
                serde_json::to_value(&effect_record).expect("serialize effect record"),
            )
            .with_effect_provenance(EffectProvenance::from_record(
                &effect_record,
                EffectFactOwner::Framework,
            )),
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
            other => {
                panic!("expected framework effect Data to be skipped and domain Data delivered, got {other:?}")
            }
        },
        other => panic!("expected PollResult::Event, got {other:?}"),
    }

    assert_eq!(
        subscription
            .last_delivered_stage_input_position()
            .expect("data event should receive a stage input position")
            .0,
        1
    );

    let second = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    assert!(matches!(second, PollResult::NoEvents));
}

#[tokio::test]
async fn forwarded_eof_with_missing_writer_is_not_terminal() {
    let upstream_stage = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));

    let upstream_writer_id = WriterId::Stage(upstream_stage);

    // Simulate an EOF forwarded into this upstream journal but with a missing payload writer_id.
    // This must not cause the downstream reader to treat the upstream as complete.
    let forwarded_writer_id = WriterId::Stage(StageId::new());

    upstream_journal
        .append(
            ChainEventFactory::data_event(upstream_writer_id, "test.event", json!({"n": 1})),
            None,
        )
        .await
        .unwrap();

    let mut forwarded_eof = ChainEventFactory::eof_event(forwarded_writer_id, true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) =
        &mut forwarded_eof.content
    {
        *writer_id = None;
    }

    upstream_journal.append(forwarded_eof, None).await.unwrap();

    // If the forwarded EOF were treated as terminal, this would never be observed.
    upstream_journal
        .append(
            ChainEventFactory::data_event(upstream_writer_id, "test.event", json!({"n": 2})),
            None,
        )
        .await
        .unwrap();

    // The authoritative EOF for this upstream.
    upstream_journal
        .append(ChainEventFactory::eof_event(upstream_writer_id, true), None)
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

    assert!(
        subscription.take_last_eof_outcome().is_none(),
        "expected forwarded EOF with missing writer_id to be non-terminal"
    );

    let third = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    match third {
        PollResult::Event(env) => match env.event.content {
            ChainEventContent::Data { .. } => {}
            other => panic!("expected third delivered event to be Data, got {other:?}"),
        },
        other => panic!("expected PollResult::Event, got {other:?}"),
    }

    let fourth = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    match fourth {
        PollResult::Event(env) => match env.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. }) => {}
            other => panic!("expected fourth delivered event to be EOF, got {other:?}"),
        },
        other => panic!("expected PollResult::Event, got {other:?}"),
    }

    let outcome = subscription
        .take_last_eof_outcome()
        .expect("expected subscription to mark authoritative EOF");
    assert!(outcome.is_final);
    assert_eq!(outcome.stage_id, upstream_stage);
    assert_eq!(outcome.eof_count, 1);
    assert_eq!(outcome.total_readers, 1);
}

// ---------------------------------------------------------------------------
// FLOWIP-095d: canonical deterministic merge
// ---------------------------------------------------------------------------

/// In-memory journal whose readers observe appends made after reader creation,
/// with explicit vector-clock stamping for causality tests. `TestJournal`
/// snapshots events at reader creation, which cannot express "quiet input
/// later produces a head".
struct SharedTestJournal {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<ChainEvent>>>>,
}

impl SharedTestJournal {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn append_with_clock(&self, event: ChainEvent, vector_clock: VectorClock) {
        let envelope = EventEnvelope {
            journal_writer_id: JournalWriterId::from(self.id),
            vector_clock,
            timestamp: chrono::Utc::now(),
            event,
        };
        self.events.lock().unwrap().push(envelope);
    }
}

struct SharedTestJournalReader {
    events: Arc<Mutex<Vec<EventEnvelope<ChainEvent>>>>,
    pos: usize,
}

#[async_trait]
impl JournalReader<ChainEvent> for SharedTestJournalReader {
    async fn next(
        &mut self,
    ) -> std::result::Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        let guard = self.events.lock().unwrap();
        if self.pos < guard.len() {
            let envelope = guard[self.pos].clone();
            self.pos += 1;
            Ok(Some(envelope))
        } else {
            Ok(None)
        }
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }

    fn is_at_end(&self) -> bool {
        self.pos >= self.events.lock().unwrap().len()
    }
}

#[async_trait]
impl Journal<ChainEvent> for SharedTestJournal {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: ChainEvent,
        _parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> std::result::Result<EventEnvelope<ChainEvent>, JournalError> {
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        self.events.lock().unwrap().push(envelope.clone());
        Ok(envelope)
    }

    async fn read_all_unordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        self.read_causally_ordered().await
    }

    async fn read_causally_ordered(
        &self,
    ) -> std::result::Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(self.events.lock().unwrap().clone())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::EventId,
    ) -> std::result::Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(None)
    }

    async fn reader(
        &self,
    ) -> std::result::Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(SharedTestJournalReader {
            events: self.events.clone(),
            pos: 0,
        }))
    }

    async fn reader_from(
        &self,
        position: u64,
    ) -> std::result::Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(SharedTestJournalReader {
            events: self.events.clone(),
            pos: position as usize,
        }))
    }

    async fn read_last_n(
        &self,
        count: usize,
    ) -> std::result::Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        let guard = self.events.lock().unwrap();
        let len = guard.len();
        let start = len.saturating_sub(count);
        Ok(guard[start..].iter().rev().cloned().collect())
    }
}

fn merge_data(writer: StageId, event_type: &str) -> ChainEvent {
    ChainEventFactory::data_event(WriterId::Stage(writer), event_type, json!({}))
}

fn merge_authored_eof(writer: StageId) -> ChainEvent {
    ChainEventFactory::eof_event(WriterId::Stage(writer), true)
}

fn merge_consumption_progress(writer: StageId, seq: u64) -> ChainEvent {
    ChainEventFactory::consumption_progress_event(
        WriterId::Stage(writer),
        obzenflow_core::event::ConsumptionProgressEventParams {
            reader_seq: SeqNo(seq),
            last_event_id: None,
            vector_clock: None,
            eof_seen: false,
            reader_path: obzenflow_core::event::types::JournalPath("upstream".to_string()),
            reader_index: obzenflow_core::event::types::JournalIndex(0),
            advertised_writer_seq: None,
            advertised_vector_clock: None,
            stalled_since: None,
        },
    )
}

fn merge_clock(entries: &[(&str, u64)]) -> VectorClock {
    let mut clock = VectorClock::new();
    for (writer, seq) in entries {
        for _ in 0..*seq {
            obzenflow_core::event::vector_clock::CausalOrderingService::increment(
                &mut clock, writer,
            );
        }
    }
    clock
}

async fn canonical_pair(
    name_a: &str,
    name_b: &str,
) -> (
    UpstreamSubscription<ChainEvent>,
    (StageId, Arc<SharedTestJournal>),
    (StageId, Arc<SharedTestJournal>),
) {
    let stage_a = StageId::new();
    let stage_b = StageId::new();
    let journal_a = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_a)));
    let journal_b = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_b)));

    let upstreams = [
        (
            stage_a,
            name_a.to_string(),
            journal_a.clone() as Arc<dyn Journal<ChainEvent>>,
        ),
        (
            stage_b,
            name_b.to_string(),
            journal_b.clone() as Arc<dyn Journal<ChainEvent>>,
        ),
    ];

    let subscription = UpstreamSubscription::new_with_names("merge_owner", &upstreams)
        .await
        .unwrap()
        .with_reader_selection(ReaderSelectionPolicy::CanonicalMerge);

    (subscription, (stage_a, journal_a), (stage_b, journal_b))
}

/// `canonical_pair` with the stage-runtime delivery filter applied. The
/// reader-telemetry skip (FLOWIP-095d) is `TransportOnly`-gated, so tests for
/// it must build the subscription the way stage runtimes do.
async fn canonical_pair_transport_only(
    name_a: &str,
    name_b: &str,
) -> (
    UpstreamSubscription<ChainEvent>,
    (StageId, Arc<SharedTestJournal>),
    (StageId, Arc<SharedTestJournal>),
) {
    let (subscription, a, b) = canonical_pair(name_a, name_b).await;
    (subscription.transport_only(), a, b)
}

async fn expect_delivery(
    subscription: &mut UpstreamSubscription<ChainEvent>,
) -> EventEnvelope<ChainEvent> {
    match subscription.poll_next_with_state("test_fsm", None).await {
        PollResult::Event(envelope) => envelope,
        other => panic!(
            "expected PollResult::Event, got {other:?} (delivered={:?}, merge_wait={:?})",
            subscription.delivered_counts(),
            subscription.merge_wait(),
        ),
    }
}

/// Per-reader delivered counts as bare numbers, for readable assertions.
fn delivered(subscription: &UpstreamSubscription<ChainEvent>) -> Vec<u64> {
    subscription
        .delivered_counts()
        .iter()
        .map(|count| count.0)
        .collect()
}

#[tokio::test]
async fn canonical_merge_waits_while_any_input_is_quiet() {
    let (mut subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("upstream_a", "upstream_b").await;

    journal_a.append_with_clock(merge_data(stage_a, "a.event"), VectorClock::new());

    // B is quiet: nothing may deliver, and the wait names B.
    match subscription.poll_next_with_state("test_fsm", None).await {
        PollResult::NoEvents => {}
        other => panic!("expected NoEvents while an input is quiet, got {other:?}"),
    }
    let wait = subscription.merge_wait().expect("merge wait recorded");
    assert_eq!(wait.quiet_inputs, vec![(stage_b, "upstream_b".to_string())]);
    assert_eq!(delivered(&subscription), [0, 0]);

    // B produces a head: the merge decides, and the tiebreak (equal ordinals,
    // stage name) delivers A first.
    journal_b.append_with_clock(merge_data(stage_b, "b.event"), VectorClock::new());
    let first = expect_delivery(&mut subscription).await;
    assert_eq!(first.event.event_type(), "a.event");
    assert_eq!(subscription.last_delivered_upstream_stage(), Some(stage_a));
    assert!(subscription.merge_wait().is_none());

    // The roles flip: A is now the quiet input, so B's held head must wait.
    match subscription.poll_next_with_state("test_fsm", None).await {
        PollResult::NoEvents => {}
        other => panic!("expected NoEvents while A is quiet, got {other:?}"),
    }
    let wait = subscription.merge_wait().expect("merge wait recorded");
    assert_eq!(wait.quiet_inputs, vec![(stage_a, "upstream_a".to_string())]);

    // A seals: B's data (ordinal 1) beats A's EOF (ordinal 2).
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    let second = expect_delivery(&mut subscription).await;
    assert_eq!(second.event.event_type(), "b.event");

    // A's EOF still cannot deliver: B went quiet again. EOFs obey the same
    // wait discipline as data, so exhaustion order stays deterministic.
    match subscription.poll_next_with_state("test_fsm", None).await {
        PollResult::NoEvents => {}
        other => panic!("expected NoEvents while B is quiet, got {other:?}"),
    }
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    let third = expect_delivery(&mut subscription).await;
    assert!(matches!(
        third.event.content,
        ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
    ));
    let fourth = expect_delivery(&mut subscription).await;
    assert!(matches!(
        fourth.event.content,
        ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
    ));
    assert_eq!(delivered(&subscription), [2, 2]);
    assert!(subscription.all_readers_eof());
}

#[tokio::test]
async fn canonical_merge_alternates_by_ordinal_then_stage_key_and_never_waits_on_sealed_inputs() {
    let (mut subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("upstream_a", "upstream_b").await;

    for label in ["a1", "a2", "a3"] {
        journal_a.append_with_clock(merge_data(stage_a, label), VectorClock::new());
    }
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    for label in ["b1", "b2", "b3"] {
        journal_b.append_with_clock(merge_data(stage_b, label), VectorClock::new());
    }
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    // Sealed inputs never wait: eight consecutive deliveries, no NoEvents.
    let mut delivered_from = Vec::new();
    for _ in 0..8 {
        let _ = expect_delivery(&mut subscription).await;
        delivered_from.push(subscription.last_delivered_upstream_stage().unwrap());
    }
    assert_eq!(
        delivered_from,
        vec![stage_a, stage_b, stage_a, stage_b, stage_a, stage_b, stage_a, stage_b],
        "ordinal-balanced tiebreak alternates fairly, stage key breaks ties"
    );
    assert_eq!(delivered(&subscription), [4, 4]);
    assert!(subscription.all_readers_eof());

    match subscription.poll_next_with_state("test_fsm", None).await {
        PollResult::NoEvents => {}
        other => panic!("expected NoEvents after exhaustion, got {other:?}"),
    }
}

#[tokio::test]
async fn canonical_merge_excludes_happened_before_heads() {
    // B sorts first by stage name, so the tiebreak alone would prefer B.
    // Causality must override it: B's head derives from A's second event.
    let (mut subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("z_upstream", "a_upstream").await;

    journal_a.append_with_clock(merge_data(stage_a, "a1"), merge_clock(&[("wa", 1)]));
    journal_a.append_with_clock(merge_data(stage_a, "a2"), merge_clock(&[("wa", 2)]));
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    journal_b.append_with_clock(
        merge_data(stage_b, "b1"),
        merge_clock(&[("wa", 2), ("wb", 1)]),
    );
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    let mut order = Vec::new();
    for _ in 0..3 {
        let envelope = expect_delivery(&mut subscription).await;
        order.push(envelope.event.event_type());
    }
    assert_eq!(
        order,
        vec!["a1".to_string(), "a2".to_string(), "b1".to_string()],
        "causal ancestors deliver before the head that derives from them"
    );
}

#[tokio::test]
async fn canonical_merge_authored_eof_orders_by_tiebreak_and_exhausts_reader() {
    let (mut subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("upstream_a", "upstream_b").await;

    journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    for label in ["b1", "b2", "b3"] {
        journal_b.append_with_clock(merge_data(stage_b, label), VectorClock::new());
    }
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    let mut delivered_from = Vec::new();
    for _ in 0..6 {
        let _ = expect_delivery(&mut subscription).await;
        delivered_from.push(subscription.last_delivered_upstream_stage().unwrap());
    }
    // a1, b1, eofA (ordinal tie at 2, stage key), then B streams unblocked.
    assert_eq!(
        delivered_from,
        vec![stage_a, stage_b, stage_a, stage_b, stage_b, stage_b]
    );
    assert!(subscription.all_readers_eof());

    let outcome = subscription
        .take_last_eof_outcome()
        .expect("final EOF outcome recorded");
    assert!(outcome.is_final);
    assert_eq!(outcome.stage_id, stage_b);
}

#[tokio::test]
async fn canonical_merge_forwarded_control_takes_ordinal_without_exhausting() {
    let (mut subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("upstream_a", "upstream_b").await;

    let foreign_stage = StageId::new();
    journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());
    // Forwarded EOF: authored by another stage, merely present in A's journal.
    journal_a.append_with_clock(merge_authored_eof(foreign_stage), VectorClock::new());
    journal_a.append_with_clock(merge_data(stage_a, "a2"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    journal_b.append_with_clock(merge_data(stage_b, "b1"), VectorClock::new());
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    let mut deliveries = 0;
    loop {
        match subscription.poll_next_with_state("test_fsm", None).await {
            PollResult::Event(_) => deliveries += 1,
            PollResult::NoEvents => break,
            PollResult::Error(e) => panic!("unexpected poll error: {e:?}"),
        }
    }

    // All six rows deliver: the forwarded EOF takes an ordinal and does not
    // exhaust A, whose authored EOF arrives later.
    assert_eq!(deliveries, 6);
    assert_eq!(delivered(&subscription), [4, 2]);
    assert!(subscription.all_readers_eof());
}

#[tokio::test]
async fn canonical_merge_contract_read_accounting_fires_at_delivery_not_at_hold() {
    let (subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("upstream_a", "upstream_b").await;

    let reader_stage = StageId::new();
    let contract_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(reader_stage)));
    let mut subscription = subscription.with_contracts(ContractsWiring {
        writer_id: WriterId::from(reader_stage),
        contract_journal,
        config: ContractConfig::default(),
        system_journal: None,
        reader_stage: Some(reader_stage),
        control_plane: Arc::new(NoControlPlane),
        include_delivery_contract: false,
        cycle_guard_config: None,
    });

    journal_b.append_with_clock(merge_data(stage_b, "b1"), VectorClock::new());

    let mut progress = [ReaderProgress::new(stage_a), ReaderProgress::new(stage_b)];

    // A quiet: B's head is held, the read-side instant stamps, and the
    // delivery-side accounting (reader_seq) must not move.
    match subscription
        .poll_next_with_state("test_fsm", Some(&mut progress[..]))
        .await
    {
        PollResult::NoEvents => {}
        other => panic!("expected NoEvents while A is quiet, got {other:?}"),
    }
    assert_eq!(progress[1].reader_seq, SeqNo(0));
    assert!(
        progress[1].last_read_instant.is_some(),
        "read instant stamps at head acquisition so the held edge stays healthy"
    );

    // A seals: its EOF delivers first (ordinal tie, stage key), then B's data
    // delivers and the delivery-side accounting advances.
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    let first = match subscription
        .poll_next_with_state("test_fsm", Some(&mut progress[..]))
        .await
    {
        PollResult::Event(envelope) => envelope,
        other => panic!("expected delivery, got {other:?}"),
    };
    assert!(matches!(
        first.event.content,
        ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
    ));

    match subscription
        .poll_next_with_state("test_fsm", Some(&mut progress[..]))
        .await
    {
        PollResult::Event(_) => {}
        other => panic!("expected delivery, got {other:?}"),
    }
    assert_eq!(progress[1].reader_seq, SeqNo(1));
}

#[tokio::test]
async fn canonical_merge_filtered_events_take_no_ordinals() {
    let (subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair("upstream_a", "upstream_b").await;

    let mut selected = HashMap::new();
    selected.insert(
        stage_a,
        [EventType::from("sel.event")].into_iter().collect(),
    );
    let mut subscription = subscription
        .with_selected_event_types(selected)
        .transport_only();

    journal_a.append_with_clock(merge_data(stage_a, "sel.event"), VectorClock::new());
    journal_a.append_with_clock(merge_data(stage_a, "other.event"), VectorClock::new());
    journal_a.append_with_clock(merge_data(stage_a, "sel.event"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    journal_b.append_with_clock(merge_data(stage_b, "b.event"), VectorClock::new());
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    let mut deliveries = 0;
    loop {
        match subscription.poll_next_with_state("test_fsm", None).await {
            PollResult::Event(envelope) => {
                assert_ne!(
                    envelope.event.event_type(),
                    "other.event",
                    "unselected events must never deliver"
                );
                deliveries += 1;
            }
            PollResult::NoEvents => break,
            PollResult::Error(e) => panic!("unexpected poll error: {e:?}"),
        }
    }

    assert_eq!(deliveries, 5);
    // The filtered row took no ordinal: A delivered sel, sel, eof.
    assert_eq!(delivered(&subscription), [3, 2]);
}

#[tokio::test]
async fn round_robin_stage_input_positions_skip_control_events() {
    let stage_a = StageId::new();
    let journal_a = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_a)));
    let foreign_stage = StageId::new();

    journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(foreign_stage), VectorClock::new());
    journal_a.append_with_clock(merge_data(stage_a, "a2"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());

    let upstreams = [(
        stage_a,
        "upstream_a".to_string(),
        journal_a as Arc<dyn Journal<ChainEvent>>,
    )];
    let mut subscription = UpstreamSubscription::new_with_names("rr_owner", &upstreams)
        .await
        .unwrap();

    let expected_positions = [Some(1u64), None, Some(2u64), None];
    for expected in expected_positions {
        match subscription.poll_next_with_state("test_fsm", None).await {
            PollResult::Event(_) => {}
            other => panic!("expected delivery, got {other:?}"),
        }
        assert_eq!(
            subscription
                .last_delivered_stage_input_position()
                .map(|position| position.0),
            expected
        );
    }
}

#[tokio::test]
async fn canonical_merge_skips_reader_telemetry_without_taking_ordinals() {
    let (mut subscription, (stage_a, journal_a), (stage_b, journal_b)) =
        canonical_pair_transport_only("upstream_a", "upstream_b").await;

    journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());
    journal_a.append_with_clock(merge_consumption_progress(stage_a, 1), VectorClock::new());
    journal_a.append_with_clock(merge_data(stage_a, "a2"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    journal_b.append_with_clock(merge_data(stage_b, "b1"), VectorClock::new());
    journal_b.append_with_clock(merge_data(stage_b, "b2"), VectorClock::new());
    journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());

    let mut order = Vec::new();
    loop {
        match subscription.poll_next_with_state("test_fsm", None).await {
            PollResult::Event(envelope) => order.push(envelope.event.event_type()),
            PollResult::NoEvents => break,
            PollResult::Error(e) => panic!("unexpected poll error: {e:?}"),
        }
    }

    // The telemetry row is consumed from the journal but never becomes a
    // head: the canonical alternation is undisturbed and both readers end on
    // identical ordinals.
    assert_eq!(order.len(), 6);
    assert_eq!(&order[..4], ["a1", "b1", "a2", "b2"]);
    assert_eq!(delivered(&subscription), [3, 3]);
    assert!(subscription.all_readers_eof());
}

#[tokio::test]
async fn round_robin_transport_only_skips_reader_telemetry() {
    let stage_a = StageId::new();
    let journal_a = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_a)));

    journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());
    journal_a.append_with_clock(merge_consumption_progress(stage_a, 1), VectorClock::new());
    journal_a.append_with_clock(merge_data(stage_a, "a2"), VectorClock::new());
    journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());

    let upstreams = [(
        stage_a,
        "upstream_a".to_string(),
        journal_a as Arc<dyn Journal<ChainEvent>>,
    )];
    let mut subscription = UpstreamSubscription::new_with_names("rr_owner", &upstreams)
        .await
        .unwrap()
        .transport_only();

    let mut order = Vec::new();
    loop {
        match subscription.poll_next_with_state("test_fsm", None).await {
            PollResult::Event(envelope) => order.push(envelope.event.event_type()),
            PollResult::NoEvents => break,
            PollResult::Error(e) => panic!("unexpected poll error: {e:?}"),
        }
    }

    // Both policies share the read-side filter: telemetry never delivers
    // under availability-driven round-robin either, so per-reader ordinal
    // semantics stay uniform across ordered and unordered stages.
    assert_eq!(order.len(), 3);
    assert_eq!(&order[..2], ["a1", "a2"]);
    assert_eq!(delivered(&subscription), [3]);
    assert!(subscription.all_readers_eof());
}

#[tokio::test]
async fn interleaved_telemetry_does_not_perturb_merged_order() {
    // The divergence witness for the FLOWIP-095d telemetry-lane bug: the same
    // logical streams, once with a wall-clock-positioned ConsumptionProgress
    // row interleaved mid-stream (a live run longer than the contract tick)
    // and once without (its replay), must merge identically. Before the
    // read-side telemetry filter, the interleaved row took an ordinal on A
    // and flipped the tiebreak between concurrent heads.
    async fn drain_order(
        subscription: &mut UpstreamSubscription<ChainEvent>,
    ) -> (Vec<String>, Vec<u64>) {
        let mut order = Vec::new();
        loop {
            match subscription.poll_next_with_state("test_fsm", None).await {
                PollResult::Event(envelope) => order.push(envelope.event.event_type()),
                PollResult::NoEvents => break,
                PollResult::Error(e) => panic!("unexpected poll error: {e:?}"),
            }
        }
        let ordinals = delivered(subscription);
        (order, ordinals)
    }

    let (mut clean, (clean_a, clean_journal_a), (clean_b, clean_journal_b)) =
        canonical_pair_transport_only("upstream_a", "upstream_b").await;
    let (mut noisy, (noisy_a, noisy_journal_a), (noisy_b, noisy_journal_b)) =
        canonical_pair_transport_only("upstream_a", "upstream_b").await;

    for (stage_a, journal_a, telemetry) in [
        (clean_a, &clean_journal_a, false),
        (noisy_a, &noisy_journal_a, true),
    ] {
        journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());
        if telemetry {
            journal_a.append_with_clock(merge_consumption_progress(stage_a, 1), VectorClock::new());
        }
        journal_a.append_with_clock(merge_data(stage_a, "a2"), VectorClock::new());
        journal_a.append_with_clock(merge_data(stage_a, "a3"), VectorClock::new());
        journal_a.append_with_clock(merge_authored_eof(stage_a), VectorClock::new());
    }
    for (stage_b, journal_b) in [(clean_b, &clean_journal_b), (noisy_b, &noisy_journal_b)] {
        journal_b.append_with_clock(merge_data(stage_b, "b1"), VectorClock::new());
        journal_b.append_with_clock(merge_data(stage_b, "b2"), VectorClock::new());
        journal_b.append_with_clock(merge_authored_eof(stage_b), VectorClock::new());
    }

    let (clean_order, clean_ordinals) = drain_order(&mut clean).await;
    let (noisy_order, noisy_ordinals) = drain_order(&mut noisy).await;

    assert_eq!(
        clean_order, noisy_order,
        "an interleaved telemetry row must not perturb the merged order"
    );
    assert_eq!(
        clean_ordinals, noisy_ordinals,
        "per-reader delivered ordinals must be a pure function of stream content"
    );
}

#[tokio::test]
#[should_panic(expected = "CanonicalMerge requires reader")]
async fn canonical_merge_rejects_tail_start_readers() {
    let stage_a = StageId::new();
    let journal_a = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_a)));
    journal_a.append_with_clock(merge_data(stage_a, "a1"), VectorClock::new());

    let upstreams = [(
        stage_a,
        "upstream_a".to_string(),
        journal_a as Arc<dyn Journal<ChainEvent>>,
    )];

    // A tail-start reader has a non-zero position and baseline; selecting the
    // canonical merge on it must fail fast (FLOWIP-095d ordinal stability).
    let _ = UpstreamSubscription::new_at_tail("tail_owner", &upstreams, &[1])
        .await
        .unwrap()
        .with_reader_selection(ReaderSelectionPolicy::CanonicalMerge);
}

#[tokio::test]
async fn feed_role_distinguishes_tiebreak_identity() {
    // Two readers can share a stage NAME (the first tiebreak component) while
    // having distinct StageIds; with the same selected event type, only the
    // feed role keeps the tiebreak key total.
    let stage_a = StageId::new();
    let stage_b = StageId::new();
    let journal_a = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_a)));
    let journal_b = Arc::new(SharedTestJournal::new(JournalOwner::stage(stage_b)));

    let upstreams = [
        (
            stage_a,
            "shared_upstream".to_string(),
            journal_a as Arc<dyn Journal<ChainEvent>>,
        ),
        (
            stage_b,
            "shared_upstream".to_string(),
            journal_b as Arc<dyn Journal<ChainEvent>>,
        ),
    ];

    let mut selected_feeds = HashMap::new();
    selected_feeds.insert(
        stage_a,
        vec![SelectedFeedMetadata::new(
            EventType::from("shared.event"),
            SelectedFeedRole::Reference,
        )],
    );
    selected_feeds.insert(
        stage_b,
        vec![SelectedFeedMetadata::new(
            EventType::from("shared.event"),
            SelectedFeedRole::Stream,
        )],
    );

    let subscription = UpstreamSubscription::new_with_names("role_owner", &upstreams)
        .await
        .unwrap()
        .with_selected_feeds(selected_feeds);

    let keys = &subscription.reader_tiebreak_keys;
    assert_eq!(
        keys[0].stage_key, keys[1].stage_key,
        "stage keys deliberately collide"
    );
    assert_ne!(
        keys[0].feed_identity, keys[1].feed_identity,
        "the role qualifier must keep the tiebreak key total"
    );
    assert_eq!(
        keys[0].feed_identity,
        FeedIdentity::from_feeds(&[SelectedFeedMetadata::new(
            EventType::from("shared.event"),
            SelectedFeedRole::Reference,
        )])
    );
    assert_eq!(
        keys[1].feed_identity,
        FeedIdentity::from_feeds(&[SelectedFeedMetadata::new(
            EventType::from("shared.event"),
            SelectedFeedRole::Stream,
        )])
    );
}

/// FLOWIP-095d adjacency: edge identity for a delivered event comes from the
/// reader slot, never from `event.writer_id`. A forwarded row keeps its
/// original author on `writer_id` for causal attribution (an error-marked
/// event passed through an intermediate stage is the in-tree producer), so a
/// consumer deriving its upstream from the event author would misroute
/// backpressure acks and heartbeat attribution. The join supervisors key both
/// on `last_delivered_upstream_stage()`; this test pins the accessor's
/// contract against forwarded rows.
#[tokio::test]
async fn delivered_upstream_identity_ignores_event_writer() {
    let upstream_stage = StageId::new();
    let foreign_author = StageId::new();
    let upstream_owner = JournalOwner::stage(upstream_stage);
    let upstream_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(TestJournal::new(upstream_owner));

    // A forwarded row: it sits in `upstream_stage`'s journal but its writer_id
    // names the original author from further up the topology.
    upstream_journal
        .append(
            ChainEventFactory::data_event(
                WriterId::Stage(foreign_author),
                "test.forwarded",
                json!({"n": 1}),
            ),
            None,
        )
        .await
        .unwrap();

    let upstreams = [(upstream_stage, "upstream".to_string(), upstream_journal)];
    let mut subscription = UpstreamSubscription::new_with_names("test_owner", &upstreams)
        .await
        .unwrap()
        .transport_only();

    let mut reader_progress = [ReaderProgress::new(upstream_stage)];
    let polled = subscription
        .poll_next_with_state("test_fsm", Some(&mut reader_progress[..]))
        .await;
    let envelope = match polled {
        PollResult::Event(envelope) => envelope,
        other => panic!("expected delivered data event, got {other:?}"),
    };
    assert_eq!(
        envelope.event.writer_id,
        WriterId::Stage(foreign_author),
        "premise: the forwarded row preserves its original author"
    );
    assert_eq!(
        subscription.last_delivered_upstream_stage(),
        Some(upstream_stage),
        "edge identity must be the reader slot's stage, not the event author"
    );
}
