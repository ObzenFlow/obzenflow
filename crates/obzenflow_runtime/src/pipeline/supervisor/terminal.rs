// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::types::ViolationCause;

pub(super) async fn dispatch_drained(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // Terminal: write pipeline_completed (natural completion) or pipeline_cancelled
    // (intentional stop), then terminate. We also keep the lighter-weight
    // "drained" marker in write_completion_event().

    // Best-effort reconciliation with tail system events to ensure we have
    // the latest wide lifecycle snapshots before computing the rollup.
    if let Err(e) = supervisor.reconcile_stage_metrics_from_tail(context).await {
        tracing::warn!(
            pipeline = %supervisor.name,
            error = %e,
            "Failed to reconcile stage lifecycle metrics from tail before terminal event"
        );
    }

    // Compute flow duration (best-effort).
    let duration_ms = context
        .flow_start_time
        .map(|start| start.elapsed().as_millis() as u64)
        .unwrap_or(0);

    // Compute flow-level lifecycle metrics from per-stage snapshots.
    let metrics = crate::pipeline::fsm::compute_flow_lifecycle_metrics(context);

    let system_event_factory =
        obzenflow_core::event::system_event::SystemEventFactory::new(supervisor.system_id);

    if context.stop_intent.requested {
        let reason = context.stop_intent.reason_label();
        let cancelled = system_event_factory.pipeline_cancelled(
            reason.clone(),
            duration_ms,
            Some(metrics.clone()),
            Some(ViolationCause::Other(reason.clone())),
        );

        if let Err(e) = supervisor.system_journal.append(cancelled, None).await {
            tracing::error!(
                pipeline = %supervisor.name,
                journal_error = %e,
                "Failed to write pipeline cancelled event"
            );
        } else {
            tracing::info!(
                pipeline = %supervisor.name,
                reason = %reason,
                "Pipeline cancelled event written"
            );
        }
    } else if context.flow_start_time.is_some() {
        let completed = system_event_factory.pipeline_completed(duration_ms, metrics);

        if let Err(e) = supervisor.system_journal.append(completed, None).await {
            tracing::error!(
                pipeline = %supervisor.name,
                journal_error = %e,
                "Failed to write pipeline completed event"
            );
        } else {
            tracing::info!(
                pipeline = %supervisor.name,
                "Pipeline completed event written (success path)"
            );
        }
    }

    tracing::info!("Pipeline drained, terminating");
    Ok(EventLoopDirective::Terminate)
}

pub(super) async fn dispatch_failed(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
    reason: &str,
    failure_cause: &Option<ViolationCause>,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // Terminal failure: write flow_failed with duration + best-effort rollup metrics.
    // This snapshot is derived from per-stage lifecycle snapshots
    // (`stage_lifecycle_metrics`) via `compute_flow_lifecycle_metrics`, after a
    // best-effort reconciliation of wide lifecycle events from the system
    // journal tail to capture any completions written after we stopped polling.
    if let Err(e) = supervisor.reconcile_stage_metrics_from_tail(context).await {
        tracing::warn!(
            pipeline = %supervisor.name,
            error = %e,
            "Failed to reconcile stage lifecycle metrics from tail before failure"
        );
    }

    let duration_ms = context
        .flow_start_time
        .map(|start| start.elapsed().as_millis() as u64)
        .unwrap_or(0);

    let metrics = Some(crate::pipeline::fsm::compute_flow_lifecycle_metrics(
        context,
    ));

    let system_event_factory =
        obzenflow_core::event::system_event::SystemEventFactory::new(supervisor.system_id);

    if context.stop_intent.requested {
        let reason_label = context
            .stop_intent
            .reason
            .clone()
            .unwrap_or_else(|| reason.to_string());
        let cancelled = system_event_factory.pipeline_cancelled(
            reason_label.clone(),
            duration_ms,
            metrics,
            Some(ViolationCause::Other(reason_label.clone())),
        );

        if let Err(e) = supervisor.system_journal.append(cancelled, None).await {
            tracing::error!(
                pipeline = %supervisor.name,
                journal_error = %e,
                "Failed to write pipeline cancelled event"
            );
        } else {
            tracing::info!(
                pipeline = %supervisor.name,
                reason = %reason_label,
                "Pipeline cancelled event written"
            );
        }

        tracing::info!("Pipeline cancelled: {}", reason_label);
    } else {
        let failed = system_event_factory.pipeline_failed(
            reason.to_string(),
            duration_ms,
            metrics,
            failure_cause.clone(),
        );

        if let Err(e) = supervisor.system_journal.append(failed, None).await {
            tracing::error!(
                pipeline = %supervisor.name,
                journal_error = %e,
                "Failed to write pipeline failed event"
            );
        } else {
            tracing::error!(
                pipeline = %supervisor.name,
                error = %reason,
                "Pipeline failed event written (failure path)"
            );
        }

        // Terminal state.
        tracing::error!("Pipeline failed: {}", reason);
    }
    Ok(EventLoopDirective::Terminate)
}
