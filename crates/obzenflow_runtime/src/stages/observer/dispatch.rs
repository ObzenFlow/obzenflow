// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime observer dispatch helpers.
//!
//! Each helper invokes the one composed observer port for its surface (the
//! adapter folds the resolved observer list and the determinism gate into that
//! port) and appends any returned diagnostics. The runtime never iterates an
//! observer list or evaluates observer determinism here; it owns only the
//! journal append.

use std::sync::Arc;

use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope};
use obzenflow_core::event::{EventEnvelope, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};

use super::{
    DiagnosticProvenance, EffectObserverContext, EffectObserverOutcome, HandlerObserverContext,
    JoinObserverContext, ObserverCommitResult, ObserverReport, OutputCommitObserverContext,
    SinkDeliveryObserverContext, SinkDeliveryObserverOutcome, SourcePollObserverContext,
    StageLifecycleObserverContext, StageLifecyclePhase, StageObserverBundle,
    StatefulObserverContext,
};

use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::supervision::output_committer::{
    append_observer_diagnostics, reject_invalid_observer_diagnostics,
};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_before_handler_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: Option<u64>,
    lineage: LineagePolicy,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: &EventEnvelope<ChainEvent>,
) {
    let Some(observer) = observers.handler.as_ref() else {
        return;
    };
    let ctx = HandlerObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        input,
        stage_input_position,
    };
    let report = observer.before_handle(&ctx);
    append_observer_diagnostics(
        report,
        Some(flow_context),
        Some(instrumentation),
        data_journal,
        DiagnosticProvenance::Derived { parent, lineage },
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_after_handler_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: Option<u64>,
    lineage: LineagePolicy,
    outputs: &[ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: &EventEnvelope<ChainEvent>,
) {
    let Some(observer) = observers.handler.as_ref() else {
        return;
    };
    let ctx = HandlerObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        input,
        stage_input_position,
    };
    let report = observer.after_handle(&ctx, outputs);
    append_observer_diagnostics(
        report,
        Some(flow_context),
        Some(instrumentation),
        data_journal,
        DiagnosticProvenance::Derived { parent, lineage },
    )
    .await;
}

pub(crate) async fn run_stateful_after_emit_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    lineage: LineagePolicy,
    outputs: &[ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) {
    let Some(observer) = observers.stateful.as_ref() else {
        return;
    };
    let report = observer.after_state_emit(ctx, outputs);
    if ctx.input.is_some() && parent.is_none() {
        reject_invalid_observer_diagnostics(report, ctx.flow_context, Some(instrumentation));
        return;
    }
    append_observer_diagnostics(
        report,
        Some(ctx.flow_context),
        Some(instrumentation),
        data_journal,
        parent.map_or(DiagnosticProvenance::Root, |parent| {
            DiagnosticProvenance::Derived { parent, lineage }
        }),
    )
    .await;
}

pub(crate) async fn run_stateful_before_accumulate_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    lineage: LineagePolicy,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) {
    let Some(observer) = observers.stateful.as_ref() else {
        return;
    };
    let report = observer.before_state_accumulate(ctx);
    if ctx.input.is_some() && parent.is_none() {
        reject_invalid_observer_diagnostics(report, ctx.flow_context, Some(instrumentation));
        return;
    }
    append_observer_diagnostics(
        report,
        Some(ctx.flow_context),
        Some(instrumentation),
        data_journal,
        parent.map_or(DiagnosticProvenance::Root, |parent| {
            DiagnosticProvenance::Derived { parent, lineage }
        }),
    )
    .await;
}

pub(crate) async fn run_stateful_after_accumulate_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    lineage: LineagePolicy,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) {
    let Some(observer) = observers.stateful.as_ref() else {
        return;
    };
    let report = observer.after_state_accumulate(ctx);
    if ctx.input.is_some() && parent.is_none() {
        reject_invalid_observer_diagnostics(report, ctx.flow_context, Some(instrumentation));
        return;
    }
    append_observer_diagnostics(
        report,
        Some(ctx.flow_context),
        Some(instrumentation),
        data_journal,
        parent.map_or(DiagnosticProvenance::Root, |parent| {
            DiagnosticProvenance::Derived { parent, lineage }
        }),
    )
    .await;
}

pub(crate) async fn run_source_poll_observers(
    observers: &StageObserverBundle,
    ctx: &SourcePollObserverContext<'_>,
    outputs: &[ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
) {
    let Some(observer) = observers.source_poll.as_ref() else {
        return;
    };
    let report = observer.after_source_poll(ctx, outputs);
    append_observer_diagnostics(
        report,
        Some(ctx.flow_context),
        Some(instrumentation),
        data_journal,
        DiagnosticProvenance::Root,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_sink_delivery_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: Option<u64>,
    outcome: SinkDeliveryObserverOutcome,
    lineage: LineagePolicy,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: &EventEnvelope<ChainEvent>,
) {
    let Some(observer) = observers.sink_delivery.as_ref() else {
        return;
    };
    let ctx = SinkDeliveryObserverContext {
        stage_id,
        stage_name,
        scope,
        input,
        stage_input_position,
        outcome,
    };
    let report = observer.after_sink_delivery(&ctx);
    append_observer_diagnostics(
        report,
        Some(flow_context),
        Some(instrumentation),
        data_journal,
        DiagnosticProvenance::Derived { parent, lineage },
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_effect_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: Option<&FlowContext>,
    scope: MiddlewareExecutionScope,
    effect_type: &str,
    outcome: EffectObserverOutcome,
    lineage: LineagePolicy,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: Option<&Arc<StageInstrumentation>>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) {
    let Some(observer) = observers.effect.as_ref() else {
        return;
    };
    let ctx = EffectObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        effect_type,
        outcome,
    };
    let report = observer.after_effect(&ctx);
    let provenance = parent.map_or(DiagnosticProvenance::Root, |parent| {
        DiagnosticProvenance::Derived { parent, lineage }
    });
    append_observer_diagnostics(
        report,
        flow_context,
        instrumentation,
        data_journal,
        provenance,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_stage_lifecycle_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    phase: StageLifecyclePhase,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
) {
    let Some(observer) = observers.stage_lifecycle.as_ref() else {
        return;
    };
    let ctx = StageLifecycleObserverContext {
        stage_id,
        stage_name,
        scope,
        phase,
    };
    let report = observer.on_stage_lifecycle(&ctx);
    append_observer_diagnostics(
        report,
        Some(flow_context),
        Some(instrumentation),
        data_journal,
        DiagnosticProvenance::Root,
    )
    .await;
}

pub(crate) async fn run_join_after_output_observers(
    observers: &StageObserverBundle,
    ctx: &JoinObserverContext<'_>,
    lineage: LineagePolicy,
    outputs: &[ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) {
    let Some(observer) = observers.join.as_ref() else {
        return;
    };
    let report = observer.after_join_output(ctx, outputs);
    if ctx.input.is_some() && parent.is_none() {
        reject_invalid_observer_diagnostics(report, ctx.flow_context, Some(instrumentation));
        return;
    }
    append_observer_diagnostics(
        report,
        Some(ctx.flow_context),
        Some(instrumentation),
        data_journal,
        parent.map_or(DiagnosticProvenance::Root, |parent| {
            DiagnosticProvenance::Derived { parent, lineage }
        }),
    )
    .await;
}

pub(crate) async fn run_join_before_input_observers(
    observers: &StageObserverBundle,
    ctx: &JoinObserverContext<'_>,
    lineage: LineagePolicy,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) {
    let Some(observer) = observers.join.as_ref() else {
        return;
    };
    let report = observer.before_join_input(ctx);
    if ctx.input.is_some() && parent.is_none() {
        reject_invalid_observer_diagnostics(report, ctx.flow_context, Some(instrumentation));
        return;
    }
    append_observer_diagnostics(
        report,
        Some(ctx.flow_context),
        Some(instrumentation),
        data_journal,
        parent.map_or(DiagnosticProvenance::Root, |parent| {
            DiagnosticProvenance::Derived { parent, lineage }
        }),
    )
    .await;
}

pub(crate) fn run_output_commit_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    parent: Option<&ChainEvent>,
    event: &mut ChainEvent,
) -> ObserverCommitResult {
    let Some(observer) = observers.output_commit.as_ref() else {
        return Ok(ObserverReport::empty());
    };
    let ctx = OutputCommitObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        parent,
    };
    observer.before_output_commit(&ctx, event)
}

#[allow(dead_code)]
pub(crate) async fn append_framework_mirror_allowed(
    event: ChainEvent,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<SystemEvent>>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<EventEnvelope<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let written = data_journal
        .append(event, parent)
        .await
        .map_err(|e| e.to_string())?;
    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
        &written,
        system_journal,
    )
    .await;
    Ok(written)
}
