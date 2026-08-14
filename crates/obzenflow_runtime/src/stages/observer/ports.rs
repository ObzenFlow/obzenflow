// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned, observe-only stage interception ports.
//!
//! The runtime constructs every context and lends only immutable execution
//! views. Ordinary observers return no value and receive no framework writer,
//! control boundary, continuation, executor, resolver, or settlement handle.

use crate::messaging::upstream_subscription::StageInputPosition;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::{ChainEvent, EventEnvelope, FlowId, StageId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Reference,
    Stream,
}

#[derive(Debug, Clone)]
pub struct JoinDeliverySnapshot {
    side: JoinSide,
    delivered_source_stage_id: StageId,
    delivered_stage_input_position: StageInputPosition,
    input_envelope: EventEnvelope<ChainEvent>,
    reference_high_water: VectorClock,
}

impl JoinDeliverySnapshot {
    pub(crate) fn new(
        side: JoinSide,
        delivered_source_stage_id: StageId,
        delivered_stage_input_position: StageInputPosition,
        input_envelope: EventEnvelope<ChainEvent>,
        reference_high_water: VectorClock,
    ) -> Self {
        Self {
            side,
            delivered_source_stage_id,
            delivered_stage_input_position,
            input_envelope,
            reference_high_water,
        }
    }

    pub fn side(&self) -> JoinSide {
        self.side
    }

    pub fn delivered_source_stage_id(&self) -> StageId {
        self.delivered_source_stage_id
    }

    pub fn delivered_stage_input_position(&self) -> StageInputPosition {
        self.delivered_stage_input_position
    }

    pub fn input(&self) -> &ChainEvent {
        &self.input_envelope.event
    }

    pub fn reference_high_water(&self) -> &VectorClock {
        &self.reference_high_water
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
    flow_id: FlowId,
    flow_context: &'a FlowContext,
    input: &'a ChainEvent,
    stage_input_position: StageInputPosition,
}

impl<'a> HandlerObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        flow_context: &'a FlowContext,
        input: &'a ChainEvent,
        stage_input_position: StageInputPosition,
    ) -> Self {
        Self {
            flow_id,
            flow_context,
            input,
            stage_input_position,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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

    pub fn stage_input_position(&self) -> StageInputPosition {
        self.stage_input_position
    }
}

pub struct StatefulObserverContext<'a> {
    flow_id: FlowId,
    flow_context: &'a FlowContext,
    input: Option<&'a ChainEvent>,
    stage_input_position: Option<StageInputPosition>,
}

impl<'a> StatefulObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        flow_context: &'a FlowContext,
        input: Option<&'a ChainEvent>,
        stage_input_position: Option<StageInputPosition>,
    ) -> Self {
        Self {
            flow_id,
            flow_context,
            input,
            stage_input_position,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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

    pub fn stage_input_position(&self) -> Option<StageInputPosition> {
        self.stage_input_position
    }
}

pub struct JoinObserverContext<'a> {
    flow_id: FlowId,
    flow_context: &'a FlowContext,
    occurrence: JoinObserverOccurrence<'a>,
}

impl<'a> JoinObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        flow_context: &'a FlowContext,
        occurrence: JoinObserverOccurrence<'a>,
    ) -> Self {
        Self {
            flow_id,
            flow_context,
            occurrence,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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

    pub fn stage_input_position(&self) -> Option<StageInputPosition> {
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
    flow_id: FlowId,
    flow_context: &'a FlowContext,
    outcome: SourcePollObserverOutcome,
}

impl<'a> SourcePollObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        flow_context: &'a FlowContext,
        outcome: SourcePollObserverOutcome,
    ) -> Self {
        Self {
            flow_id,
            flow_context,
            outcome,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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
    flow_id: FlowId,
    flow_context: &'a FlowContext,
    input: &'a ChainEvent,
    stage_input_position: StageInputPosition,
    outcome: SinkDeliveryObserverOutcome,
}

impl<'a> SinkDeliveryObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        flow_context: &'a FlowContext,
        input: &'a ChainEvent,
        stage_input_position: StageInputPosition,
        outcome: SinkDeliveryObserverOutcome,
    ) -> Self {
        Self {
            flow_id,
            flow_context,
            input,
            stage_input_position,
            outcome,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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

    pub fn stage_input_position(&self) -> StageInputPosition {
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
    flow_id: FlowId,
    flow_context: &'a FlowContext,
    phase: StageLifecyclePhase,
}

impl<'a> StageLifecycleObserverContext<'a> {
    pub(crate) fn new(
        flow_id: FlowId,
        flow_context: &'a FlowContext,
        phase: StageLifecyclePhase,
    ) -> Self {
        Self {
            flow_id,
            flow_context,
            phase,
        }
    }

    pub fn flow_name(&self) -> &str {
        &self.flow_context.flow_name
    }

    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{ChainEventFactory, JournalWriterId};
    use obzenflow_core::WriterId;

    fn require_flow_id(_: FlowId) {}
    fn require_position(_: StageInputPosition) {}
    fn require_optional_position(_: Option<StageInputPosition>) {}

    #[test]
    fn public_context_coordinates_preserve_nominal_types_and_honest_absence() {
        let flow_id = FlowId::new();
        let stage_id = StageId::new();
        let flow_context = FlowContext {
            flow_name: "flow".to_string(),
            // Proves contexts do not parse or trust the serialised event field.
            flow_id: "legacy-serialised-id".to_string(),
            stage_name: "stage".to_string(),
            stage_id,
            stage_type: StageType::Transform,
        };
        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.input",
            serde_json::json!({}),
        );
        let position = StageInputPosition(7);

        let handler = HandlerObserverContext::new(flow_id, &flow_context, &event, position);
        require_flow_id(handler.flow_id());
        require_position(handler.stage_input_position());
        assert_eq!(handler.flow_id(), flow_id);
        assert_eq!(handler.stage_input_position(), position);

        let stateful = StatefulObserverContext::new(flow_id, &flow_context, None, None);
        require_flow_id(stateful.flow_id());
        require_optional_position(stateful.stage_input_position());
        assert_eq!(stateful.stage_input_position(), None);

        let delivery = JoinDeliverySnapshot::new(
            JoinSide::Stream,
            StageId::new(),
            position,
            EventEnvelope::new(JournalWriterId::new(), event.clone()),
            VectorClock::new(),
        );
        require_position(delivery.delivered_stage_input_position());
        let join_delivery = JoinObserverContext::new(
            flow_id,
            &flow_context,
            JoinObserverOccurrence::Delivery(&delivery),
        );
        require_flow_id(join_delivery.flow_id());
        require_optional_position(join_delivery.stage_input_position());
        assert_eq!(join_delivery.stage_input_position(), Some(position));

        let signal = JoinSignalSnapshot::new(None, JoinSignalKind::Drain);
        let join_signal = JoinObserverContext::new(
            flow_id,
            &flow_context,
            JoinObserverOccurrence::Signal(&signal),
        );
        assert_eq!(join_signal.stage_input_position(), None);

        let source = SourcePollObserverContext::new(
            flow_id,
            &flow_context,
            SourcePollObserverOutcome::Batch { events: 1 },
        );
        require_flow_id(source.flow_id());

        let effect = EffectObserverContext::new(
            flow_id,
            stage_id,
            "stage",
            "test.effect",
            EffectObserverOutcome::Succeeded,
        );
        require_flow_id(effect.flow_id());

        let sink = SinkDeliveryObserverContext::new(
            flow_id,
            &flow_context,
            &event,
            position,
            SinkDeliveryObserverOutcome::Attempted {
                result: SinkDeliveryAttemptResult::ReportedSuccess,
            },
        );
        require_flow_id(sink.flow_id());
        require_position(sink.stage_input_position());

        let lifecycle = StageLifecycleObserverContext::new(
            flow_id,
            &flow_context,
            StageLifecyclePhase::Running,
        );
        require_flow_id(lifecycle.flow_id());
    }
}
