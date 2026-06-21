// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime observer dispatch helpers.

use std::sync::Arc;

use obzenflow_core::event::context::{FlowContext, MiddlewareExecutionScope};
use obzenflow_core::event::{EventEnvelope, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::{
    ChainEvent, EffectObserverContext, EffectObserverOutcome, HandlerObserverContext,
    JoinObserverContext, ObserverCommitResult, ObserverReport, OutputCommitObserverContext,
    SinkDeliveryObserverContext, SinkDeliveryObserverOutcome, SourcePollObserverContext, StageId,
    StageLifecycleObserverContext, StageLifecyclePhase, StageObserverBundle,
    StatefulObserverContext,
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
    if observers.handler.is_empty() {
        return Ok(());
    }
    let ctx = HandlerObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        input,
        stage_input_position,
    };
    for observer in &observers.handler {
        if observer.determinism().should_run(scope) {
            append_observer_diagnostics(
                observer.before_handle(&ctx),
                flow_context,
                Some(instrumentation),
                data_journal,
                Some(parent),
            )
            .await?;
        }
    }
    Ok(())
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
    if observers.handler.is_empty() {
        return Ok(());
    }
    let ctx = HandlerObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        input,
        stage_input_position,
    };
    for observer in &observers.handler {
        if observer.determinism().should_run(scope) {
            append_observer_diagnostics(
                observer.after_handle(&ctx, outputs),
                flow_context,
                Some(instrumentation),
                data_journal,
                Some(parent),
            )
            .await?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_stateful_after_emit_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for observer in &observers.stateful {
        if observer.determinism().should_run(ctx.scope) {
            append_observer_diagnostics(
                observer.after_state_emit(ctx, outputs),
                ctx.flow_context,
                Some(instrumentation),
                data_journal,
                parent,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn run_stateful_before_accumulate_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for observer in &observers.stateful {
        if observer.determinism().should_run(ctx.scope) {
            append_observer_diagnostics(
                observer.before_state_accumulate(ctx),
                ctx.flow_context,
                Some(instrumentation),
                data_journal,
                parent,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn run_stateful_after_accumulate_observers(
    observers: &StageObserverBundle,
    ctx: &StatefulObserverContext<'_>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for observer in &observers.stateful {
        if observer.determinism().should_run(ctx.scope) {
            append_observer_diagnostics(
                observer.after_state_accumulate(ctx),
                ctx.flow_context,
                Some(instrumentation),
                data_journal,
                parent,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn run_source_poll_observers(
    observers: &StageObserverBundle,
    ctx: &SourcePollObserverContext<'_>,
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for observer in &observers.source_poll {
        if observer.determinism().should_run(ctx.scope) {
            append_observer_diagnostics(
                observer.after_source_poll(ctx, outputs),
                ctx.flow_context,
                Some(instrumentation),
                data_journal,
                None,
            )
            .await?;
        }
    }
    Ok(())
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
    if observers.sink_delivery.is_empty() {
        return Ok(());
    }
    let ctx = SinkDeliveryObserverContext {
        stage_id,
        stage_name,
        scope,
        input,
        stage_input_position,
        outcome,
    };
    for observer in &observers.sink_delivery {
        if observer.determinism().should_run(scope) {
            append_observer_diagnostics(
                observer.after_sink_delivery(&ctx),
                flow_context,
                Some(instrumentation),
                data_journal,
                Some(parent),
            )
            .await?;
        }
    }
    Ok(())
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
    if observers.effect.is_empty() {
        return Ok(());
    }
    let ctx = EffectObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        effect_type,
        outcome,
    };
    for observer in &observers.effect {
        if observer.determinism().should_run(scope) {
            let report = observer.after_effect(&ctx);
            if let Some(flow_context) = flow_context {
                append_observer_diagnostics(
                    report,
                    flow_context,
                    instrumentation,
                    data_journal,
                    parent,
                )
                .await?;
            } else if !report.is_empty() {
                tracing::warn!(
                    stage_name,
                    effect_type,
                    "dropping effect observer diagnostics because no flow context is available"
                );
            }
        }
    }
    Ok(())
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
    if observers.stage_lifecycle.is_empty() {
        return Ok(());
    }
    let ctx = StageLifecycleObserverContext {
        stage_id,
        stage_name,
        phase,
    };
    for observer in &observers.stage_lifecycle {
        if observer.determinism().should_run(scope) {
            append_observer_diagnostics(
                observer.on_stage_lifecycle(&ctx),
                flow_context,
                Some(instrumentation),
                data_journal,
                None,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn run_join_after_output_observers(
    observers: &StageObserverBundle,
    ctx: &JoinObserverContext<'_>,
    outputs: &mut [ChainEvent],
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for observer in &observers.join {
        if observer.determinism().should_run(ctx.scope) {
            append_observer_diagnostics(
                observer.after_join_output(ctx, outputs),
                ctx.flow_context,
                Some(instrumentation),
                data_journal,
                parent,
            )
            .await?;
        }
    }
    Ok(())
}

pub(crate) async fn run_join_before_input_observers(
    observers: &StageObserverBundle,
    ctx: &JoinObserverContext<'_>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for observer in &observers.join {
        if observer.determinism().should_run(ctx.scope) {
            append_observer_diagnostics(
                observer.before_join_input(ctx),
                ctx.flow_context,
                Some(instrumentation),
                data_journal,
                parent,
            )
            .await?;
        }
    }
    Ok(())
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
    let ctx = OutputCommitObserverContext {
        stage_id,
        stage_name,
        flow_context,
        scope,
        parent,
    };
    let mut merged = ObserverReport::empty();
    for observer in &observers.output_commit {
        if observer.determinism().should_run(scope) {
            let report = observer.before_output_commit(&ctx, event)?;
            merged.diagnostics.extend(report.diagnostics);
        }
    }
    Ok(merged)
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
