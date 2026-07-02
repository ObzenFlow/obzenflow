// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n catch-up flip, shared by the consumer supervisors (transform,
//! stateful, join, sink).
//!
//! Two triggers re-evaluate the caught-up frontier: a delivered catch-up
//! watermark (the subscription has already F8/F11-validated it and advanced
//! the delivering reader's generation) and an authored EOF that completes the
//! frontier vacuously (F17: the finite prefix outlives the infinite one, so
//! no watermark follows). The marker is consumed here: never forwarded
//! downstream (each stage authors its own), never handed to user handlers.
//! Strategy-agnostic: a bounded replay of a resumed archive re-authors
//! downstream markers, which is what preserves closure.

use crate::execution::RuntimeExecution;
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, ReaderGeneration, StageId, StageKey, WriterId};
use std::sync::Arc;

/// What the flip evaluation decided.
pub(crate) enum CatchUpDisposition {
    /// Consumed (flipped or not yet flippable); the supervisor continues.
    Consumed,
    /// Fail-closed validation or authoring failed; the supervisor takes its
    /// error transition with this message.
    Failed(String),
}

/// The consuming stage's identity and write path for the authored marker.
pub(crate) struct CatchUpStage<'a> {
    pub stage_id: StageId,
    pub stage_name: &'a str,
    pub flow_name: &'a str,
    pub flow_id: &'a str,
    pub stage_type: StageType,
    pub writer_id: Option<WriterId>,
    pub data_journal: &'a Arc<dyn Journal<ChainEvent>>,
    pub instrumentation: &'a StageInstrumentation,
}

/// Run the catch-up flip for `target` (FLOWIP-120n): frontier check, F15
/// validation, marker authoring, boundary recording.
///
/// Called from both triggers: the watermark arm (with the announced
/// generation) and the authored-EOF arm (with the max reader generation).
/// `all_inputs_caught_up` is the flip predicate: every reader of every input
/// subscription crossed to `target` or is EOF-exhausted (both sides for a
/// join). `delivered_data_count` is the stage's merged delivered-data total
/// (summed across sides for a join), compared fail-closed against the
/// recorded high water at the flip (F15). `author_marker` is false for sinks:
/// terminal stages author nothing, a forwarded marker would violate F8 for
/// readers of the sink journal. `flip_latch` makes the flip idempotent per
/// generation: a second trigger observing an already-crossed frontier must
/// not re-author the marker or re-record the boundary.
pub(crate) async fn maybe_flip_caught_up(
    target: ReaderGeneration,
    all_inputs_caught_up: bool,
    delivered_data_count: u64,
    stage: CatchUpStage<'_>,
    author_marker: bool,
    runtime_execution: &RuntimeExecution,
    flip_latch: &mut Option<ReaderGeneration>,
) -> CatchUpDisposition {
    // Already flipped at (or past) this generation.
    if flip_latch.is_some_and(|flipped| flipped >= target) {
        return CatchUpDisposition::Consumed;
    }

    // Non-flip: other inputs are still recorded; the delivering reader's
    // generation already advanced inside the subscription.
    if !all_inputs_caught_up {
        return CatchUpDisposition::Consumed;
    }

    // F15 fail-closed validation: at the flip the full recorded prefix must
    // have been re-delivered. The check runs at the flip because only then is
    // the merged delivered total comparable to the stage's recorded maximum.
    if let Some(control) = runtime_execution.resume_control() {
        if let Some(max) = control.recorded_delivered_high_water(stage.stage_id) {
            if delivered_data_count < max {
                return CatchUpDisposition::Failed(format!(
                    "stage '{}' crossed its catch-up boundary after {delivered_data_count} \
                     delivered data events but the archive records {max}; torn or short \
                     re-delivery (FLOWIP-120n F15)",
                    stage.stage_name
                ));
            }
        }
    }

    // Author this stage's own marker into its own data journal, mirroring
    // the authored-EOF write path: fresh event, own writer_id. The append is
    // FIFO-behind any backpressure-deferred outputs, which the loop flushes
    // before dispatching the next delivery (F9).
    if author_marker {
        let Some(writer_id) = stage.writer_id else {
            return CatchUpDisposition::Failed(format!(
                "stage '{}' has no writer ID to author its catch-up watermark (FLOWIP-120n)",
                stage.stage_name
            ));
        };
        let mut marker = ChainEventFactory::catch_up_complete_event(
            writer_id,
            target,
            StageKey::from(stage.stage_name.to_owned()),
        );
        marker.flow_context = make_flow_context(
            stage.flow_name,
            stage.flow_id,
            stage.stage_name,
            stage.stage_id,
            stage.stage_type,
        );
        stage.instrumentation.record_emitted(&marker);
        if let Err(e) = stage.data_journal.append(marker, None).await {
            return CatchUpDisposition::Failed(format!(
                "stage '{}' failed to author its catch-up watermark: {e}",
                stage.stage_name
            ));
        }
        tracing::info!(
            stage_name = %stage.stage_name,
            generation = target.0,
            "stage crossed its catch-up boundary and authored its own watermark"
        );
    }

    if let Some(control) = runtime_execution.resume_control() {
        control.record_generation_boundary(stage.stage_id, target);
    }

    *flip_latch = Some(target);
    CatchUpDisposition::Consumed
}

/// Re-run the flip when an authored EOF is the delivery that completes the
/// caught-up frontier (F17): no watermark follows an EOF-exhausted reader, so
/// the EOF arm re-evaluates against the max delivered generation. `None` when
/// nothing failed (flipped or not); the caller continues normal EOF
/// processing. `Some(message)` is the fail-closed error transition.
pub(crate) async fn flip_on_authored_eof(
    subscription: &UpstreamSubscription<ChainEvent>,
    stage: CatchUpStage<'_>,
    author_marker: bool,
    runtime_execution: &RuntimeExecution,
    flip_latch: &mut Option<ReaderGeneration>,
) -> Option<String> {
    let target = subscription.max_reader_generation();
    // No watermark delivered yet on any input: nothing to cross.
    if target == ReaderGeneration(0) {
        return None;
    }
    match maybe_flip_caught_up(
        target,
        subscription.all_readers_caught_up(target),
        subscription.delivered_data_count(),
        stage,
        author_marker,
        runtime_execution,
        flip_latch,
    )
    .await
    {
        CatchUpDisposition::Consumed => None,
        CatchUpDisposition::Failed(message) => Some(message),
    }
}
