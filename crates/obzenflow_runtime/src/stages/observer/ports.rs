// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned, observe-only stage interception ports.
//!
//! The runtime constructs every context and lends only immutable execution
//! views. Ordinary observers return no value and receive no framework writer,
//! control boundary, continuation, executor, resolver, or settlement handle.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::{ChainEvent, EventEnvelope, FlowId, StageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Reference,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinCanonicalMergeMetadata {
    selected_feed: Option<String>,
    reader_index: Option<usize>,
}

impl JoinCanonicalMergeMetadata {
    pub(crate) fn new(selected_feed: Option<String>, reader_index: Option<usize>) -> Self {
        Self {
            selected_feed,
            reader_index,
        }
    }

    pub fn selected_feed(&self) -> Option<&str> {
        self.selected_feed.as_deref()
    }

    pub fn reader_index(&self) -> Option<usize> {
        self.reader_index
    }
}

#[derive(Debug, Clone)]
pub struct JoinDeliverySnapshot {
    side: JoinSide,
    delivered_source_stage_id: StageId,
    delivered_stage_input_position: u64,
    input_envelope: EventEnvelope<ChainEvent>,
    reference_high_water: VectorClock,
    canonical_merge: Option<JoinCanonicalMergeMetadata>,
}

impl JoinDeliverySnapshot {
    pub(crate) fn new(
        side: JoinSide,
        delivered_source_stage_id: StageId,
        delivered_stage_input_position: u64,
        input_envelope: EventEnvelope<ChainEvent>,
        reference_high_water: VectorClock,
        canonical_merge: Option<JoinCanonicalMergeMetadata>,
    ) -> Self {
        Self {
            side,
            delivered_source_stage_id,
            delivered_stage_input_position,
            input_envelope,
            reference_high_water,
            canonical_merge,
        }
    }

    pub fn side(&self) -> JoinSide {
        self.side
    }

    pub fn delivered_source_stage_id(&self) -> StageId {
        self.delivered_source_stage_id
    }

    pub fn delivered_stage_input_position(&self) -> u64 {
        self.delivered_stage_input_position
    }

    pub fn input(&self) -> &ChainEvent {
        &self.input_envelope.event
    }

    pub fn reference_high_water(&self) -> &VectorClock {
        &self.reference_high_water
    }

    pub fn canonical_merge(&self) -> Option<&JoinCanonicalMergeMetadata> {
        self.canonical_merge.as_ref()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSignalKind {
    Eof,
    Drain,
    OtherControl,
}

#[derive(Debug, Clone)]
pub struct JoinSignalSnapshot {
    side: Option<JoinSide>,
    signal: JoinSignalKind,
}

impl JoinSignalSnapshot {
    pub(crate) fn new(side: Option<JoinSide>, signal: JoinSignalKind) -> Self {
        Self { side, signal }
    }

    pub fn side(&self) -> Option<JoinSide> {
        self.side
    }

    pub fn signal(&self) -> JoinSignalKind {
        self.signal
    }
}

#[derive(Debug, Clone, Copy)]
pub enum JoinObserverOccurrence<'a> {
    Delivery(&'a JoinDeliverySnapshot),
    Signal(&'a JoinSignalSnapshot),
}

pub struct HandlerObserverContext<'a> {
    flow_context: &'a FlowContext,
    input: &'a ChainEvent,
    stage_input_position: Option<u64>,
}

impl<'a> HandlerObserverContext<'a> {
    pub(crate) fn new(
        flow_context: &'a FlowContext,
        input: &'a ChainEvent,
        stage_input_position: Option<u64>,
    ) -> Self {
        Self {
            flow_context,
            input,
            stage_input_position,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_context.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.flow_context.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.flow_context.stage_name
    }

    pub fn stage_type(&self) -> StageType {
        self.flow_context.stage_type
    }

    pub fn input(&self) -> &ChainEvent {
        self.input
    }

    pub fn stage_input_position(&self) -> Option<u64> {
        self.stage_input_position
    }
}

pub struct StatefulObserverContext<'a> {
    flow_context: &'a FlowContext,
    input: Option<&'a ChainEvent>,
    stage_input_position: Option<u64>,
}

impl<'a> StatefulObserverContext<'a> {
    pub(crate) fn new(
        flow_context: &'a FlowContext,
        input: Option<&'a ChainEvent>,
        stage_input_position: Option<u64>,
    ) -> Self {
        Self {
            flow_context,
            input,
            stage_input_position,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_context.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.flow_context.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.flow_context.stage_name
    }

    pub fn stage_type(&self) -> StageType {
        self.flow_context.stage_type
    }

    pub fn input(&self) -> Option<&ChainEvent> {
        self.input
    }

    pub fn stage_input_position(&self) -> Option<u64> {
        self.stage_input_position
    }
}

pub struct JoinObserverContext<'a> {
    flow_context: &'a FlowContext,
    occurrence: JoinObserverOccurrence<'a>,
}

impl<'a> JoinObserverContext<'a> {
    pub(crate) fn new(
        flow_context: &'a FlowContext,
        occurrence: JoinObserverOccurrence<'a>,
    ) -> Self {
        Self {
            flow_context,
            occurrence,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_context.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.flow_context.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.flow_context.stage_name
    }

    pub fn stage_type(&self) -> StageType {
        self.flow_context.stage_type
    }

    pub fn occurrence(&self) -> JoinObserverOccurrence<'_> {
        self.occurrence
    }

    pub fn input(&self) -> Option<&ChainEvent> {
        match self.occurrence {
            JoinObserverOccurrence::Delivery(delivery) => Some(delivery.input()),
            JoinObserverOccurrence::Signal(_) => None,
        }
    }

    pub fn stage_input_position(&self) -> Option<u64> {
        match self.occurrence {
            JoinObserverOccurrence::Delivery(delivery) => {
                Some(delivery.delivered_stage_input_position())
            }
            JoinObserverOccurrence::Signal(_) => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePollObserverOutcome {
    Batch { events: usize },
    Eof,
    Error { kind: ErrorKind },
    Rejected { policy: Option<String> },
}

pub struct SourcePollObserverContext<'a> {
    flow_context: &'a FlowContext,
    outcome: SourcePollObserverOutcome,
}

impl<'a> SourcePollObserverContext<'a> {
    pub(crate) fn new(flow_context: &'a FlowContext, outcome: SourcePollObserverOutcome) -> Self {
        Self {
            flow_context,
            outcome,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_context.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.flow_context.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.flow_context.stage_name
    }

    pub fn stage_type(&self) -> StageType {
        self.flow_context.stage_type
    }

    pub fn outcome(&self) -> &SourcePollObserverOutcome {
        &self.outcome
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectObserverOutcome {
    Succeeded,
    Failed,
}

pub struct EffectObserverContext<'a> {
    flow_id: FlowId,
    stage_id: StageId,
    stage_name: &'a str,
    effect_type: &'a str,
    outcome: EffectObserverOutcome,
}

impl<'a> EffectObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        stage_id: StageId,
        stage_name: &'a str,
        effect_type: &'a str,
        outcome: EffectObserverOutcome,
    ) -> Self {
        Self {
            flow_id,
            stage_id,
            stage_name,
            effect_type,
            outcome,
        }
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.stage_id
    }

    pub fn stage_name(&self) -> &str {
        self.stage_name
    }

    pub fn effect_type(&self) -> &str {
        self.effect_type
    }

    pub fn outcome(&self) -> EffectObserverOutcome {
        self.outcome
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkDeliveryObserverOutcome {
    Attempted { result: SinkDeliveryAttemptResult },
    Rejected { policy: Option<String> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkDeliveryAttemptResult {
    ReportedSuccess,
    ReportedPartial {
        successful_count: u64,
        failed_count: u64,
    },
    ReportedBuffered,
    ReportedFailure {
        final_attempt: bool,
    },
    HandlerError {
        kind: ErrorKind,
    },
    HandlerPanicked,
}

pub struct SinkDeliveryObserverContext<'a> {
    flow_context: &'a FlowContext,
    input: &'a ChainEvent,
    stage_input_position: Option<u64>,
    outcome: SinkDeliveryObserverOutcome,
}

impl<'a> SinkDeliveryObserverContext<'a> {
    pub(crate) fn new(
        flow_context: &'a FlowContext,
        input: &'a ChainEvent,
        stage_input_position: Option<u64>,
        outcome: SinkDeliveryObserverOutcome,
    ) -> Self {
        Self {
            flow_context,
            input,
            stage_input_position,
            outcome,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_context.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.flow_context.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.flow_context.stage_name
    }

    pub fn stage_type(&self) -> StageType {
        self.flow_context.stage_type
    }

    pub fn input(&self) -> &ChainEvent {
        self.input
    }

    pub fn stage_input_position(&self) -> Option<u64> {
        self.stage_input_position
    }

    pub fn outcome(&self) -> &SinkDeliveryObserverOutcome {
        &self.outcome
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageLifecyclePhase {
    Running,
    Completed,
    Failed,
}

pub struct StageLifecycleObserverContext<'a> {
    flow_context: &'a FlowContext,
    phase: StageLifecyclePhase,
}

impl<'a> StageLifecycleObserverContext<'a> {
    pub(crate) fn new(flow_context: &'a FlowContext, phase: StageLifecyclePhase) -> Self {
        Self {
            flow_context,
            phase,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_context.flow_id
    }

    pub fn stage_id(&self) -> StageId {
        self.flow_context.stage_id
    }

    pub fn stage_name(&self) -> &str {
        &self.flow_context.stage_name
    }

    pub fn stage_type(&self) -> StageType {
        self.flow_context.stage_type
    }

    pub fn phase(&self) -> StageLifecyclePhase {
        self.phase
    }
}

pub trait HandlerObserver: Send + Sync {
    fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) {}

    fn after_handle(&self, _ctx: &HandlerObserverContext<'_>, _outputs: &[ChainEvent]) {}
}

pub trait StatefulObserver: Send + Sync {
    fn before_state_accumulate(&self, _ctx: &StatefulObserverContext<'_>) {}

    fn after_state_accumulate(&self, _ctx: &StatefulObserverContext<'_>) {}

    fn after_state_emit(&self, _ctx: &StatefulObserverContext<'_>, _outputs: &[ChainEvent]) {}
}

pub trait JoinObserver: Send + Sync {
    fn before_join_input(&self, _ctx: &JoinObserverContext<'_>) {}

    fn after_join_output(&self, _ctx: &JoinObserverContext<'_>, _outputs: &[ChainEvent]) {}
}

pub trait SourcePollObserver: Send + Sync {
    fn after_source_poll(&self, _ctx: &SourcePollObserverContext<'_>, _outputs: &[ChainEvent]) {}
}

pub trait EffectObserver: Send + Sync {
    fn after_effect(&self, _ctx: &EffectObserverContext<'_>) {}
}

pub trait SinkDeliveryObserver: Send + Sync {
    fn after_sink_delivery(&self, _ctx: &SinkDeliveryObserverContext<'_>) {}
}

pub trait StageLifecycleObserver: Send + Sync {
    fn on_stage_lifecycle(&self, _ctx: &StageLifecycleObserverContext<'_>) {}
}
