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

use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope};
use obzenflow_core::event::{EventEnvelope, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId};

use crate::{
    EffectObserverContext, EffectObserverOutcome, HandlerObserverContext, JoinObserverContext,
    ObserverCommitResult, ObserverReport, OutputCommitObserverContext, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserverContext, StageLifecycleObserverContext,
    StageLifecyclePhase, StageObserverBundle, StatefulObserverContext,
};

use crate::metrics::instrumentation::StageInstrumentation;

pub(crate) async fn append_observer_diagnostics(
    report: ObserverReport,
    flow_context: &FlowContext,
    instrumentation: Option<&Arc<StageInstrumentation>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for diagnostic in report.diagnostics {
        let mut diagnostic = diagnostic.with_flow_context(flow_context.clone());
        if let Some(instrumentation) = instrumentation {
            diagnostic = diagnostic.with_runtime_context(instrumentation.snapshot_with_control());
        }
        data_journal
            .append(diagnostic, parent)
            .await
            .map_err(|e| format!("Failed to append observer diagnostic: {e}"))?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_before_handler_observers(
    observers: &StageObserverBundle,
    stage_id: StageId,
    stage_name: &str,
    flow_context: &FlowContext,
    scope: MiddlewareExecutionScope,
    input: &ChainEvent,
    stage_input_position: Option<u64>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: &EventEnvelope<ChainEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.handler.as_ref() else {
        return Ok(());
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
        flow_context,
        Some(instrumentation),
        data_journal,
        Some(parent),
    )
    .await
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
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: &EventEnvelope<ChainEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.handler.as_ref() else {
        return Ok(());
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
        flow_context,
        Some(instrumentation),
        data_journal,
        Some(parent),
    )
    .await
}

pub(crate) async fn run_stateful_after_emit_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.stateful.as_ref() else {
        return Ok(());
    };
    let report = observer.after_state_emit(ctx, outputs);
    append_observer_diagnostics(
        report,
        ctx.flow_context,
        Some(instrumentation),
        data_journal,
        parent,
    )
    .await
}

pub(crate) async fn run_stateful_before_accumulate_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.stateful.as_ref() else {
        return Ok(());
    };
    let report = observer.before_state_accumulate(ctx);
    append_observer_diagnostics(
        report,
        ctx.flow_context,
        Some(instrumentation),
        data_journal,
        parent,
    )
    .await
}

pub(crate) async fn run_stateful_after_accumulate_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.stateful.as_ref() else {
        return Ok(());
    };
    let report = observer.after_state_accumulate(ctx);
    append_observer_diagnostics(
        report,
        ctx.flow_context,
        Some(instrumentation),
        data_journal,
        parent,
    )
    .await
}

pub(crate) async fn run_source_poll_observers(
    observers: &StageObserverBundle,
    ctx: &SourcePollObserverContext<'_>,
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.source_poll.as_ref() else {
        return Ok(());
    };
    let report = observer.after_source_poll(ctx, outputs);
    append_observer_diagnostics(
        report,
        ctx.flow_context,
        Some(instrumentation),
        data_journal,
        None,
    )
    .await
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
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: &EventEnvelope<ChainEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.sink_delivery.as_ref() else {
        return Ok(());
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
        flow_context,
        Some(instrumentation),
        data_journal,
        Some(parent),
    )
    .await
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
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: Option<&Arc<StageInstrumentation>>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.effect.as_ref() else {
        return Ok(());
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
    if let Some(flow_context) = flow_context {
        append_observer_diagnostics(report, flow_context, instrumentation, data_journal, parent)
            .await
    } else if !report.is_empty() {
        tracing::warn!(
            stage_name,
            effect_type,
            "dropping effect observer diagnostics because no flow context is available"
        );
        Ok(())
    } else {
        Ok(())
    }
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
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.stage_lifecycle.as_ref() else {
        return Ok(());
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
        flow_context,
        Some(instrumentation),
        data_journal,
        None,
    )
    .await
}

pub(crate) async fn run_join_after_output_observers(
    observers: &StageObserverBundle,
    ctx: &JoinObserverContext<'_>,
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.join.as_ref() else {
        return Ok(());
    };
    let report = observer.after_join_output(ctx, outputs);
    append_observer_diagnostics(
        report,
        ctx.flow_context,
        Some(instrumentation),
        data_journal,
        parent,
    )
    .await
}

pub(crate) async fn run_join_before_input_observers(
    observers: &StageObserverBundle,
    ctx: &JoinObserverContext<'_>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(observer) = observers.join.as_ref() else {
        return Ok(());
    };
    let report = observer.before_join_input(ctx);
    append_observer_diagnostics(
        report,
        ctx.flow_context,
        Some(instrumentation),
        data_journal,
        parent,
    )
    .await
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
