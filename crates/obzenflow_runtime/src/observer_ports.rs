// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned observer middleware ports.
//!
//! These traits are intentionally observe-only. Ordinary hooks can return
//! diagnostics, but they cannot return control decisions. The output-commit hook
//! may fail only as a commit/invariant failure.
//!
//! The ports live here instead of `obzenflow_core` because they describe
//! runtime stage and boundary observation surfaces. Core provides the domain
//! primitives used by those surfaces, but it must not own runtime middleware
//! contracts.

use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::{ChainEvent, EventEnvelope, StageId};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObserverDeterminism {
    Deterministic,
    LiveOnly,
}

impl ObserverDeterminism {
    pub fn should_run(self, scope: MiddlewareExecutionScope) -> bool {
        !matches!(self, Self::LiveOnly) || !scope.is_deterministic_replay()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ObserverReport {
    pub diagnostics: Vec<ChainEvent>,
}

impl ObserverReport {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_diagnostic(mut self, diagnostic: ChainEvent) -> Self {
        self.diagnostics.push(diagnostic);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.diagnostics.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ObserverCommitError {
    message: String,
}

impl ObserverCommitError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ObserverCommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

impl std::error::Error for ObserverCommitError {}

pub type ObserverCommitResult = Result<ObserverReport, ObserverCommitError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Reference,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinCanonicalMergeMetadata {
    pub selected_feed: Option<String>,
    pub reader_index: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct JoinDeliverySnapshot {
    pub side: JoinSide,
    pub delivered_source_stage_id: StageId,
    pub delivered_stage_input_position: u64,
    pub input_envelope: EventEnvelope<ChainEvent>,
    pub reference_high_water: VectorClock,
    pub canonical_merge: Option<JoinCanonicalMergeMetadata>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSignalKind {
    Eof,
    Drain,
    OtherControl,
}

#[derive(Debug, Clone)]
pub struct JoinSignalSnapshot {
    pub side: Option<JoinSide>,
    pub signal: JoinSignalKind,
}

pub struct HandlerObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_context: &'a FlowContext,
    pub scope: MiddlewareExecutionScope,
    pub input: &'a ChainEvent,
    pub stage_input_position: Option<u64>,
}

pub struct StatefulObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_context: &'a FlowContext,
    pub scope: MiddlewareExecutionScope,
    pub input: Option<&'a ChainEvent>,
    pub stage_input_position: Option<u64>,
}

pub struct JoinObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_context: &'a FlowContext,
    pub scope: MiddlewareExecutionScope,
    pub input: Option<&'a ChainEvent>,
    pub delivery: Option<&'a JoinDeliverySnapshot>,
    pub signal: Option<&'a JoinSignalSnapshot>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourcePollObserverOutcome {
    Batch { events: usize },
    Eof,
    Error { message: String },
    Rejected { reason: String },
}

pub struct SourcePollObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_context: &'a FlowContext,
    pub scope: MiddlewareExecutionScope,
    pub poll_duration: Duration,
    pub outcome: SourcePollObserverOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectObserverOutcome {
    Succeeded,
    Failed { message: String },
    SuppressedByReplay,
}

pub struct EffectObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_context: Option<&'a FlowContext>,
    pub scope: MiddlewareExecutionScope,
    pub effect_type: &'a str,
    pub outcome: EffectObserverOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SinkDeliveryObserverOutcome {
    Delivered,
    Failed { message: String },
    Rejected { reason: String },
}

pub struct SinkDeliveryObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub scope: MiddlewareExecutionScope,
    pub input: &'a ChainEvent,
    pub stage_input_position: Option<u64>,
    pub outcome: SinkDeliveryObserverOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressObserverOutcome {
    Accepted,
    Refused { reason: String },
}

pub struct IngressObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub outcome: IngressObserverOutcome,
}

pub struct OutputCommitObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_context: &'a FlowContext,
    pub scope: MiddlewareExecutionScope,
    pub parent: Option<&'a ChainEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageLifecyclePhase {
    Running,
    Draining,
    Completed,
    Failed,
}

pub struct StageLifecycleObserverContext<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub scope: MiddlewareExecutionScope,
    pub phase: StageLifecyclePhase,
}

pub trait HandlerObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn before_handle(&self, _ctx: &HandlerObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }

    fn after_handle(
        &self,
        _ctx: &HandlerObserverContext<'_>,
        _outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        ObserverReport::empty()
    }
}

pub trait StatefulObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn before_state_accumulate(&self, _ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }

    fn after_state_accumulate(&self, _ctx: &StatefulObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }

    fn after_state_emit(
        &self,
        _ctx: &StatefulObserverContext<'_>,
        _outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        ObserverReport::empty()
    }
}

pub trait JoinObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn before_join_input(&self, _ctx: &JoinObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }

    fn after_join_output(
        &self,
        _ctx: &JoinObserverContext<'_>,
        _outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        ObserverReport::empty()
    }
}

pub trait SourcePollObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn after_source_poll(
        &self,
        _ctx: &SourcePollObserverContext<'_>,
        _outputs: &mut [ChainEvent],
    ) -> ObserverReport {
        ObserverReport::empty()
    }
}

pub trait EffectObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn after_effect(&self, _ctx: &EffectObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }
}

pub trait SinkDeliveryObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn after_sink_delivery(&self, _ctx: &SinkDeliveryObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }
}

/// Reserved cross-cutting surface. Ingress (listener) observation is owned by
/// the infra hosted-web ingress path (FLOWIP-115d), not the stage runtime, so
/// there is no stage-runtime dispatcher for this port yet. It is kept defined so
/// the carrier and infra path can adopt it without re-reserving the surface.
pub trait IngressObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn after_ingress(&self, _ctx: &IngressObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }
}

pub trait OutputCommitObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn before_output_commit(
        &self,
        _ctx: &OutputCommitObserverContext<'_>,
        _event: &mut ChainEvent,
    ) -> ObserverCommitResult {
        Ok(ObserverReport::empty())
    }
}

pub trait StageLifecycleObserver: Send + Sync {
    fn label(&self) -> &'static str;

    fn determinism(&self) -> ObserverDeterminism {
        ObserverDeterminism::Deterministic
    }

    fn on_stage_lifecycle(&self, _ctx: &StageLifecycleObserverContext<'_>) -> ObserverReport {
        ObserverReport::empty()
    }
}

/// Composed, per-surface observer ports for one stage.
///
/// Each field is a single neutral port. The adapter folds the resolved observer
/// list for a surface into one composed port (mirroring how control policies
/// fold into one `Per*PolicyBoundary`), so the runtime calls one port per
/// surface and never iterates an observer list or evaluates observer
/// determinism itself. The composed port owns iteration, the determinism gate,
/// and report merging.
#[derive(Clone, Default)]
pub struct StageObserverBundle {
    pub handler: Option<Arc<dyn HandlerObserver>>,
    pub stateful: Option<Arc<dyn StatefulObserver>>,
    pub join: Option<Arc<dyn JoinObserver>>,
    pub source_poll: Option<Arc<dyn SourcePollObserver>>,
    pub effect: Option<Arc<dyn EffectObserver>>,
    pub sink_delivery: Option<Arc<dyn SinkDeliveryObserver>>,
    pub ingress: Option<Arc<dyn IngressObserver>>,
    pub output_commit: Option<Arc<dyn OutputCommitObserver>>,
    pub stage_lifecycle: Option<Arc<dyn StageLifecycleObserver>>,
}

impl StageObserverBundle {
    pub fn is_empty(&self) -> bool {
        self.handler.is_none()
            && self.stateful.is_none()
            && self.join.is_none()
            && self.source_poll.is_none()
            && self.effect.is_none()
            && self.sink_delivery.is_none()
            && self.ingress.is_none()
            && self.output_commit.is_none()
            && self.stage_lifecycle.is_none()
    }
}

impl fmt::Debug for StageObserverBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StageObserverBundle")
            .field("has_observers", &!self.is_empty())
            .finish()
    }
}
