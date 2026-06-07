// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage-authored output commit chokepoint (FLOWIP-120b, Step 1).
//!
//! `OutputCommitter` is the single place a stage-authored event is written to
//! the stage data journal. Two append paths exist today: the supervisor
//! pending-output drain (`backpressure_drain`) and the effects-layer effect and
//! capture record append (`effects::append_effect_record`, also reached by the
//! transactional `EffectCommitHandle`). Step 1 routes both through this one type
//! so the commit core (wide-event enrichment, per-type instrumentation, journal
//! append, heartbeat tracking, and the middleware mirror) lives in one place
//! instead of drifting between two appenders.
//!
//! The caller still owns the decisions this committer does not yet absorb:
//!
//! * Backpressure credit and requeue stay in the drain. They move to input
//!   admission plus routed-output debt in Step 2, at which point the effects
//!   layer gains the handles to build a fully-featured committer.
//! * The pending-output drain still calls validation before credit reservation,
//!   preserving the fail-before-backpressure-ordering rule. The validation rule
//!   itself now lives here so every committer-based authoring path has one
//!   contract check.
//! * Deterministic output identity is not assigned here yet. Handler outputs
//!   already carry a deterministic id from `deterministic_typed_output_event`;
//!   effect records still carry their inherited derived-event id. Unifying both
//!   under one per-input `output_ordinal` counter is Step 4. It cannot be done
//!   here because an effect record keyed by `effect_ordinal = 0` would collide
//!   with a handler output keyed by `output_ordinal = 0`; the shared counter
//!   that separates those namespaces is the Step 4 deliverable.
//!
//! Behaviour is selected by which handles are present and by [`CommitOptions`].
//! The drain supplies every handle; the effects layer supplies only the journal,
//! which reproduces today's bare, credit-free, unenriched effect-record append.

use std::sync::Arc;

use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::{EventEnvelope, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::ChainEvent;

use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal;

/// A boxed, thread-safe error from a commit attempt. Each caller maps this onto
/// its own error type: the drain prefixes it with `Failed to write pending
/// output`, and the effects layer wraps it in `EffectError::Journal`.
pub(crate) type CommitError = Box<dyn std::error::Error + Send + Sync>;

/// Per-commit behaviour that is not implied by which handles are present.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct CommitOptions {
    /// Count this event in per-type producer instrumentation through
    /// `record_output_event`. Only the drain's data branch sets this. The
    /// drain's non-data branch and the unrouted effect-record path leave it
    /// `false`, matching their behaviour before Step 1.
    pub count_output: bool,
    /// Validate `Data` event types against the stage output contract before
    /// committing. The pending-output drain enables this and runs the same
    /// validation before credit reservation. Framework-owned effect/capture
    /// compatibility facts leave it disabled until typed domain outcome facts
    /// replace that compatibility path.
    pub validate_output_contract: bool,
}

/// The single stage-authored-output commit path (FLOWIP-120b, Step 1).
///
/// Holds borrowed handles for the duration of one commit. An absent handle
/// (`None`) skips the corresponding step, which is how the effects layer
/// reproduces its current bare append with only a journal handle.
pub(crate) struct OutputCommitter<'a> {
    /// The stage data journal every stage-authored event is appended to.
    pub data_journal: &'a Arc<dyn Journal<ChainEvent>>,
    /// Wide-event flow context stamped on the committed event. Absent on the
    /// effects-layer path, which does not enrich effect records today.
    pub flow_context: Option<&'a FlowContext>,
    /// System journal for mirroring middleware lifecycle rows. Absent on the
    /// effects-layer path.
    pub system_journal: Option<&'a Arc<dyn Journal<SystemEvent>>>,
    /// Stage instrumentation for per-type producer counting and the
    /// runtime-context snapshot. Absent on the effects-layer path.
    pub instrumentation: Option<&'a Arc<StageInstrumentation>>,
    /// Heartbeat state, updated with the last committed output id. Absent on the
    /// effects-layer path.
    pub heartbeat_state: Option<&'a Arc<HeartbeatState>>,
    /// Runtime output contract for stage-authored domain `Data` facts. Absent
    /// on reserved framework append paths and legacy callers.
    pub output_contract: Option<&'a StageOutputContract>,
}

impl OutputCommitter<'_> {
    /// Commit a fully-constructed event to the data journal.
    ///
    /// The event must already carry its content, identity, and any
    /// effect-provenance or error status its author set. This applies the shared
    /// commit core in the same order the drain used before Step 1: flow-context
    /// enrichment, optional per-type counting, runtime-context enrichment,
    /// journal append, heartbeat tracking, and the middleware mirror. Each step
    /// is gated on the relevant handle, so an effects-layer committer holding
    /// only a journal handle performs a bare append, exactly as
    /// `append_effect_record` did before Step 1.
    pub(crate) async fn commit_prebuilt(
        &self,
        event: ChainEvent,
        parent: Option<&EventEnvelope<ChainEvent>>,
        options: CommitOptions,
    ) -> Result<EventEnvelope<ChainEvent>, CommitError> {
        self.validate_prebuilt(&event, options)?;

        let mut event = event;

        if let Some(flow_context) = self.flow_context {
            event = event.with_flow_context(flow_context.clone());
        }

        if let Some(instrumentation) = self.instrumentation {
            if options.count_output && event.is_data() {
                instrumentation.record_output_event(&event);
            }
            event = event.with_runtime_context(instrumentation.snapshot_with_control());
        }

        let written = self
            .data_journal
            .append(event, parent)
            .await
            .map_err(|e| -> CommitError { e.to_string().into() })?;

        if let Some(heartbeat) = self.heartbeat_state {
            heartbeat.record_last_output(written.event.id);
        }

        if let Some(system_journal) = self.system_journal {
            mirror_middleware_event_to_system_journal(&written, system_journal).await;
        }

        Ok(written)
    }

    /// Validate a prebuilt event before a caller performs any external gating
    /// such as backpressure reservation.
    pub(crate) fn validate_prebuilt(
        &self,
        event: &ChainEvent,
        options: CommitOptions,
    ) -> Result<(), CommitError> {
        if !options.validate_output_contract || !event.is_data() {
            return Ok(());
        }

        let Some(output_contract) = self.output_contract else {
            return Ok(());
        };
        if output_contract.is_empty() {
            return Ok(());
        }

        let event_type = event.event_type();
        if output_contract.contains_event_type(&event_type) {
            return Ok(());
        }

        Err(format!(
            "Data output event type `{event_type}` is not declared in the stage output contract"
        )
        .into())
    }
}
