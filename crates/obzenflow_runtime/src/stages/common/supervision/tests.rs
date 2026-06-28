// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::backpressure_drain::{drain_one_pending, DrainOutcome};
use super::control_resolution::{
    resolve_control_event, resolve_forward_control_event, ControlAction, ControlResolution,
};
use super::flow_context_factory::make_flow_context;
use crate::backpressure::{BackpressurePlan, BackpressureRegistry, BackpressureWriter};
use crate::feed_plan::{FactVisibility, PayloadTypeDescriptor, StageOutputContract};
use crate::id_conversions::StageIdExt;
use crate::messaging::upstream_subscription::EofOutcome;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::pipeline::config::CycleGuardConfig;
use crate::pipeline::MaxIterations;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::{ProcessingContext, SignalDecision, SignalGate};
use crate::stages::common::cycle_guard::CycleGuard;
use crate::supervised_base::idle_backoff::IdleBackoff;
use async_trait::async_trait;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::{ChainEvent, JournalEvent};
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventEnvelope, SccId, StageId, WriterId};
use obzenflow_topology::{TopologyBuilder, TypeHintInfo};
use serde_json::json;
use std::collections::VecDeque;
use std::num::NonZeroU64;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ulid::Ulid;

#[derive(Debug)]
struct FixedActionStrategy {
    eof: SignalDecision,
    drain: SignalDecision,
    other: SignalDecision,
}

impl SignalGate for FixedActionStrategy {
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        self.eof.clone()
    }

    fn handle_watermark(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        self.other.clone()
    }

    fn handle_checkpoint(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        self.other.clone()
    }

    fn handle_drain(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        self.drain.clone()
    }
}

#[derive(Debug)]
struct PauseEofOnceStrategy;

impl SignalGate for PauseEofOnceStrategy {
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        if ctx.custom_state.contains_key("paused_eof_once") {
            return SignalDecision::Continue;
        }

        ctx.custom_state
            .insert("paused_eof_once".to_string(), "true".to_string());
        SignalDecision::Pause(std::time::Duration::from_millis(1))
    }
}

fn entry_point_config(external: StageId) -> CycleGuardConfig {
    CycleGuardConfig {
        max_iterations: MaxIterations::new(30),
        scc_id: SccId::from_ulid(Ulid::from(0u128)),
        external_upstreams: [external].into_iter().collect(),
        internal_upstreams: std::collections::HashSet::new(),
        is_entry_point: true,
        scc_internal_edges: Vec::new(),
    }
}

#[test]
fn resolve_control_event_entry_point_suppresses_non_terminal_forwarded_eof() {
    let upstream = StageId::new_const(1);
    let forwarded_origin = StageId::new_const(2);

    let eof = ChainEventFactory::eof_event(WriterId::from(forwarded_origin), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = FixedActionStrategy {
        eof: SignalDecision::Continue,
        drain: SignalDecision::Continue,
        other: SignalDecision::Continue,
    };

    let cfg = entry_point_config(upstream);
    let mut guard = CycleGuard::new(MaxIterations::new(30), cfg.scc_id, true, "t");

    let resolution = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        Some(&cfg),
        Some(&mut guard),
        None,
        Some(upstream),
        1,
        true,
    );

    assert_eq!(
        resolution,
        ControlResolution::Ready(ControlAction::Suppress)
    );
}

#[test]
fn resolve_control_event_entry_point_buffers_external_terminal_eof() {
    let upstream = StageId::new_const(1);

    let eof = ChainEventFactory::eof_event(WriterId::from(upstream), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = FixedActionStrategy {
        eof: SignalDecision::Continue,
        drain: SignalDecision::Continue,
        other: SignalDecision::Continue,
    };

    let cfg = entry_point_config(upstream);
    let mut guard = CycleGuard::new(MaxIterations::new(30), cfg.scc_id, true, "t");

    let resolution = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        Some(&cfg),
        Some(&mut guard),
        None,
        Some(upstream),
        1,
        true,
    );

    assert_eq!(
        resolution,
        ControlResolution::Ready(ControlAction::BufferAtEntryPoint { is_drain: false })
    );
}

#[test]
fn resolve_control_event_entry_point_suppresses_internal_terminal_eof() {
    let external = StageId::new_const(1);
    let internal = StageId::new_const(2);

    let eof = ChainEventFactory::eof_event(WriterId::from(internal), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = FixedActionStrategy {
        eof: SignalDecision::Continue,
        drain: SignalDecision::Continue,
        other: SignalDecision::Continue,
    };

    let cfg = entry_point_config(external);
    let mut guard = CycleGuard::new(MaxIterations::new(30), cfg.scc_id, true, "t");

    let resolution = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        Some(&cfg),
        Some(&mut guard),
        None,
        Some(internal),
        1,
        true,
    );

    assert_eq!(
        resolution,
        ControlResolution::Ready(ControlAction::Suppress)
    );
}

#[test]
fn resolve_control_event_drain_respects_stage_policy() {
    let stage = StageId::new_const(1);

    let drain = ChainEventFactory::drain_event(WriterId::from(stage));
    let envelope = EventEnvelope::new(JournalWriterId::new(), drain);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = FixedActionStrategy {
        eof: SignalDecision::Continue,
        drain: SignalDecision::Continue,
        other: SignalDecision::Continue,
    };

    let mut guard = CycleGuard::new(
        MaxIterations::new(30),
        SccId::from_ulid(Ulid::from(0u128)),
        false,
        "t",
    );

    let terminal = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        None,
        Some(&mut guard),
        None,
        Some(stage),
        1,
        true,
    );
    assert_eq!(
        terminal,
        ControlResolution::Ready(ControlAction::ForwardAndDrain)
    );

    let non_terminal = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        None,
        Some(&mut guard),
        None,
        Some(stage),
        1,
        false,
    );
    assert_eq!(
        non_terminal,
        ControlResolution::Ready(ControlAction::Forward)
    );
}

#[test]
fn resolve_forward_control_event_notes_cycle_eof_before_resolving() {
    let upstream = StageId::new_const(1);
    let eof = ChainEventFactory::eof_event(WriterId::from(upstream), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let eof_outcome = EofOutcome {
        stage_id: upstream,
        stage_name: "u".to_string(),
        reader_index: 0,
        eof_count: 1,
        total_readers: 2,
        is_final: false,
    };

    let mut guard = CycleGuard::new(
        MaxIterations::new(30),
        SccId::from_ulid(Ulid::from(0u128)),
        false,
        "t",
    );

    let resolution = resolve_forward_control_event(
        signal,
        &envelope,
        None,
        Some(&mut guard),
        Some(&eof_outcome),
        Some(upstream),
        1,
        true,
    );

    assert_eq!(resolution, ControlAction::ForwardAndDrain);
    assert!(guard.has_seen_all_upstream_eofs(1));
}

#[test]
fn resolve_forward_control_event_does_not_note_non_terminal_forwarded_eof() {
    let upstream = StageId::new_const(1);
    let forwarded_origin = StageId::new_const(2);

    let eof = ChainEventFactory::eof_event(WriterId::from(forwarded_origin), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let mut guard = CycleGuard::new(
        MaxIterations::new(30),
        SccId::from_ulid(Ulid::from(0u128)),
        false,
        "t",
    );

    let resolution = resolve_forward_control_event(
        signal,
        &envelope,
        None,
        Some(&mut guard),
        None,
        Some(upstream),
        1,
        true,
    );

    assert_eq!(resolution, ControlAction::Forward);
    assert!(!guard.has_seen_all_upstream_eofs(1));
}

#[test]
fn resolve_control_event_strategy_skip_prevents_cycle_guard_note() {
    let upstream = StageId::new_const(1);

    let eof = ChainEventFactory::eof_event(WriterId::from(upstream), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = FixedActionStrategy {
        eof: SignalDecision::SuppressSignal,
        drain: SignalDecision::Continue,
        other: SignalDecision::Continue,
    };

    let eof_outcome = EofOutcome {
        stage_id: upstream,
        stage_name: "u".to_string(),
        reader_index: 0,
        eof_count: 1,
        total_readers: 1,
        is_final: true,
    };

    let mut guard = CycleGuard::new(
        MaxIterations::new(30),
        SccId::from_ulid(Ulid::from(0u128)),
        false,
        "t",
    );

    let resolution = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        None,
        Some(&mut guard),
        Some(&eof_outcome),
        Some(upstream),
        1,
        true,
    );

    assert_eq!(resolution, ControlResolution::Ready(ControlAction::Skip));
    assert!(!guard.has_seen_all_upstream_eofs(1));
}

#[test]
fn resolve_control_event_delay_does_not_note_cycle_guard() {
    let upstream = StageId::new_const(1);

    let eof = ChainEventFactory::eof_event(WriterId::from(upstream), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = FixedActionStrategy {
        eof: SignalDecision::Pause(std::time::Duration::from_millis(1)),
        drain: SignalDecision::Continue,
        other: SignalDecision::Continue,
    };

    let mut guard = CycleGuard::new(
        MaxIterations::new(30),
        SccId::from_ulid(Ulid::from(0u128)),
        false,
        "t",
    );

    let resolution = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut crate::stages::common::control_strategies::ProcessingContext::new(),
        None,
        Some(&mut guard),
        None,
        Some(upstream),
        1,
        true,
    );

    assert_eq!(
        resolution,
        ControlResolution::Pause(std::time::Duration::from_millis(1))
    );
    assert!(!guard.has_seen_all_upstream_eofs(1));
}

#[test]
fn resolve_control_event_delay_then_reconsult_notes_cycle_guard_on_second_pass() {
    let upstream = StageId::new_const(1);

    let eof = ChainEventFactory::eof_event(WriterId::from(upstream), true);
    let envelope = EventEnvelope::new(JournalWriterId::new(), eof);
    let signal = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(payload) => payload,
        _ => unreachable!(),
    };

    let strategy = PauseEofOnceStrategy;

    let mut guard = CycleGuard::new(
        MaxIterations::new(30),
        SccId::from_ulid(Ulid::from(0u128)),
        false,
        "t",
    );

    let mut processing_ctx = crate::stages::common::control_strategies::ProcessingContext::new();

    let first_pass = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut processing_ctx,
        None,
        Some(&mut guard),
        None,
        Some(upstream),
        1,
        true,
    );
    assert_eq!(
        first_pass,
        ControlResolution::Pause(std::time::Duration::from_millis(1))
    );
    assert!(
        !guard.has_seen_all_upstream_eofs(1),
        "Delay pass must not note upstream EOF"
    );

    let second_pass = resolve_control_event(
        signal,
        &envelope,
        &strategy,
        &mut processing_ctx,
        None,
        Some(&mut guard),
        None,
        Some(upstream),
        1,
        true,
    );
    assert_eq!(
        second_pass,
        ControlResolution::Ready(ControlAction::ForwardAndDrain)
    );
    assert!(
        guard.has_seen_all_upstream_eofs(1),
        "forward pass must note upstream EOF"
    );
}

#[derive(Debug)]
struct CreditCheckingJournal {
    id: JournalId,
    owner: JournalOwner,
    writer: BackpressureWriter,
    expected_credit_at_append: u64,
    appended: Mutex<Vec<ChainEvent>>,
}

impl CreditCheckingJournal {
    fn new(
        owner: JournalOwner,
        writer: BackpressureWriter,
        expected_credit_at_append: u64,
    ) -> Self {
        Self {
            id: JournalId::new(),
            owner,
            writer,
            expected_credit_at_append,
            appended: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl Journal<ChainEvent> for CreditCheckingJournal {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        Some(&self.owner)
    }

    async fn append(
        &self,
        event: ChainEvent,
        _parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
        let credit = self.writer.min_downstream_credit();
        assert_eq!(credit, self.expected_credit_at_append);

        self.appended
            .lock()
            .expect("CreditCheckingJournal: poisoned lock")
            .push(event.clone());

        Ok(EventEnvelope::new(JournalWriterId::from(self.id), event))
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        self.read_causally_ordered().await
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::event::types::EventId,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::event::types::EventId,
    ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(None)
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(NoopReader::<ChainEvent>::default()))
    }

    async fn reader_from(
        &self,
        _position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        Ok(Box::new(NoopReader::<ChainEvent>::default()))
    }

    async fn read_last_n(
        &self,
        _count: usize,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        Ok(Vec::new())
    }
}

struct NoopReader<T: JournalEvent> {
    _marker: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> Default for NoopReader<T> {
    fn default() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: JournalEvent> JournalReader<T> for NoopReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    fn position(&self) -> u64 {
        0
    }
}

#[derive(Debug)]
struct NoopJournal<T: JournalEvent> {
    id: JournalId,
    owner: JournalOwner,
    _marker: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> NoopJournal<T> {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner,
            _marker: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for NoopJournal<T> {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        Some(&self.owner)
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        Ok(EventEnvelope::new(JournalWriterId::from(self.id), event))
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.read_causally_ordered().await
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::event::types::EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::event::types::EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(NoopReader::<T>::default()))
    }

    async fn reader_from(&self, _position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(NoopReader::<T>::default()))
    }

    async fn read_last_n(&self, _count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }
}

fn make_writer_with_window(window: NonZeroU64) -> (StageId, BackpressureWriter) {
    let mut builder = TopologyBuilder::new();
    let s_top = builder.add_stage(Some("s".to_string()));
    let _d_top = builder.add_stage(Some("d".to_string()));
    let topology = builder.build_unchecked().expect("topology");

    let s = StageId::from_topology_id(s_top);
    let d = StageId::from_topology_id(_d_top);

    let plan = BackpressurePlan::disabled().with_stage_window(s, window);
    let registry = BackpressureRegistry::new(&topology, &plan);

    let writer = registry.writer(s);
    let _reader = registry.reader(s, d);

    (s, writer)
}

fn output_contract_for_event_type(event_type: &str) -> StageOutputContract {
    StageOutputContract {
        outputs: vec![PayloadTypeDescriptor {
            type_hint: TypeHintInfo::exact("test::Declared"),
            event_type: Some(event_type.to_string()),
            schema_version: Some(1),
            visibility: FactVisibility::Unrouted,
        }],
    }
}

#[tokio::test]
async fn drain_one_pending_reserves_before_journal_append_and_records_output_for_data() {
    let (stage_id, writer) = make_writer_with_window(NonZeroU64::new(1).expect("window"));

    let flow_context = make_flow_context(
        "flow",
        "flow_id",
        "stage",
        stage_id,
        obzenflow_core::event::context::StageType::Transform,
    );

    let data_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(CreditCheckingJournal::new(
        JournalOwner::stage(stage_id),
        writer.clone(),
        /* expected_credit_at_append */ 0,
    ));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));

    let instrumentation = Arc::new(StageInstrumentation::new());
    let mut pulse = BackpressureActivityPulse::new();
    let mut backoff = IdleBackoff::exponential_with_cap(Duration::ZERO, Duration::ZERO);
    let mut pending_outputs = VecDeque::new();
    let output_contract = output_contract_for_event_type("x");

    let event = ChainEventFactory::data_event(WriterId::from(stage_id), "x", json!({"n": 1}));

    let outcome = drain_one_pending(
        crate::stages::common::supervision::backpressure_drain::PendingOutput {
            event,
            scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        },
        &flow_context,
        stage_id,
        None,
        &data_journal,
        &system_journal,
        None,
        &instrumentation,
        &writer,
        &mut pulse,
        &mut backoff,
        Some(&output_contract),
        None,
        &mut pending_outputs,
    )
    .await
    .expect("drain_one_pending");

    assert_eq!(outcome, DrainOutcome::Committed { was_data: true });
    assert_eq!(
        instrumentation
            .events_emitted_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn drain_one_pending_accepts_semantic_event_for_versioned_output_contract() {
    let (stage_id, writer) = make_writer_with_window(NonZeroU64::new(1).expect("window"));

    let flow_context = make_flow_context(
        "flow",
        "flow_id",
        "stage",
        stage_id,
        obzenflow_core::event::context::StageType::Transform,
    );

    let data_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(CreditCheckingJournal::new(
        JournalOwner::stage(stage_id),
        writer.clone(),
        /* expected_credit_at_append */ 0,
    ));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));

    let instrumentation = Arc::new(StageInstrumentation::new());
    let mut pulse = BackpressureActivityPulse::new();
    let mut backoff = IdleBackoff::exponential_with_cap(Duration::ZERO, Duration::ZERO);
    let mut pending_outputs = VecDeque::new();
    let output_contract = output_contract_for_event_type("semantic.test.v1");

    let event =
        ChainEventFactory::data_event(WriterId::from(stage_id), "semantic.test", json!({"n": 1}));

    let outcome = drain_one_pending(
        crate::stages::common::supervision::backpressure_drain::PendingOutput {
            event,
            scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        },
        &flow_context,
        stage_id,
        None,
        &data_journal,
        &system_journal,
        None,
        &instrumentation,
        &writer,
        &mut pulse,
        &mut backoff,
        Some(&output_contract),
        None,
        &mut pending_outputs,
    )
    .await
    .expect("semantic event should satisfy versioned contract");

    assert_eq!(outcome, DrainOutcome::Committed { was_data: true });
    assert_eq!(
        instrumentation
            .events_emitted_total
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[tokio::test]
async fn drain_one_pending_rejects_undeclared_data_output() {
    let (stage_id, writer) = make_writer_with_window(NonZeroU64::new(1).expect("window"));

    let flow_context = make_flow_context(
        "flow",
        "flow_id",
        "stage",
        stage_id,
        obzenflow_core::event::context::StageType::Transform,
    );

    let data_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));

    let instrumentation = Arc::new(StageInstrumentation::new());
    let mut pulse = BackpressureActivityPulse::new();
    let mut backoff = IdleBackoff::exponential_with_cap(Duration::ZERO, Duration::ZERO);
    let mut pending_outputs = VecDeque::new();
    let output_contract = output_contract_for_event_type("declared.v1");

    let event =
        ChainEventFactory::data_event(WriterId::from(stage_id), "undeclared.v1", json!({"n": 1}));

    let err = drain_one_pending(
        crate::stages::common::supervision::backpressure_drain::PendingOutput {
            event,
            scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        },
        &flow_context,
        stage_id,
        None,
        &data_journal,
        &system_journal,
        None,
        &instrumentation,
        &writer,
        &mut pulse,
        &mut backoff,
        Some(&output_contract),
        None,
        &mut pending_outputs,
    )
    .await
    .expect_err("undeclared output must fail closed");

    assert!(
        err.to_string().contains("undeclared.v1"),
        "error should identify undeclared event type: {err}"
    );
    assert_eq!(writer.min_downstream_credit(), 1);
    assert_eq!(
        instrumentation
            .events_emitted_total
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn drain_one_pending_does_not_reserve_for_non_data() {
    let (stage_id, writer) = make_writer_with_window(NonZeroU64::new(1).expect("window"));

    let flow_context = make_flow_context(
        "flow",
        "flow_id",
        "stage",
        stage_id,
        obzenflow_core::event::context::StageType::Transform,
    );

    let data_journal: Arc<dyn Journal<ChainEvent>> = Arc::new(CreditCheckingJournal::new(
        JournalOwner::stage(stage_id),
        writer.clone(),
        /* expected_credit_at_append */ 1,
    ));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));

    let instrumentation = Arc::new(StageInstrumentation::new());
    let mut pulse = BackpressureActivityPulse::new();
    let mut backoff = IdleBackoff::exponential_with_cap(Duration::ZERO, Duration::ZERO);
    let mut pending_outputs = VecDeque::new();

    let event = ChainEventFactory::drain_event(WriterId::from(stage_id));

    let outcome = drain_one_pending(
        crate::stages::common::supervision::backpressure_drain::PendingOutput {
            event,
            scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        },
        &flow_context,
        stage_id,
        None,
        &data_journal,
        &system_journal,
        None,
        &instrumentation,
        &writer,
        &mut pulse,
        &mut backoff,
        None,
        None,
        &mut pending_outputs,
    )
    .await
    .expect("drain_one_pending");

    assert_eq!(outcome, DrainOutcome::Committed { was_data: false });
    assert_eq!(
        instrumentation
            .events_emitted_total
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test]
async fn drain_one_pending_requeues_and_returns_backed_off_when_reserve_fails() {
    if BackpressureWriter::is_bypass_enabled() {
        return;
    }

    let (stage_id, writer) = make_writer_with_window(NonZeroU64::new(1).expect("window"));
    writer.reserve(1).expect("reserve").commit(1);
    assert_eq!(writer.min_downstream_credit(), 0);

    let flow_context = make_flow_context(
        "flow",
        "flow_id",
        "stage",
        stage_id,
        obzenflow_core::event::context::StageType::Transform,
    );

    let data_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(NoopJournal::new(JournalOwner::stage(stage_id)));

    let instrumentation = Arc::new(StageInstrumentation::new());
    let mut pulse = BackpressureActivityPulse::new();
    let mut backoff = IdleBackoff::exponential_with_cap(Duration::ZERO, Duration::ZERO);
    let mut pending_outputs = VecDeque::new();

    let event = ChainEventFactory::data_event(WriterId::from(stage_id), "x", json!({"n": 1}));
    let id = event.id;

    let outcome = drain_one_pending(
        crate::stages::common::supervision::backpressure_drain::PendingOutput {
            event,
            scope: obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        },
        &flow_context,
        stage_id,
        None,
        &data_journal,
        &system_journal,
        None,
        &instrumentation,
        &writer,
        &mut pulse,
        &mut backoff,
        None,
        None,
        &mut pending_outputs,
    )
    .await
    .expect("drain_one_pending");

    assert_eq!(outcome, DrainOutcome::BackedOff);
    assert_eq!(pending_outputs.len(), 1);
    assert_eq!(pending_outputs.front().expect("front").event.id, id);
}
