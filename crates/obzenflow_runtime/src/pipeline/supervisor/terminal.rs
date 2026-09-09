// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, FlowStopMode, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::pipeline::termination::{ExecutionFailure, ExecutionOutcome};
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::types::{DurationMs, ViolationCause};

pub(super) async fn dispatch_drained(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    let infinite = context.topology.stages().any(|stage| {
        matches!(
            stage.stage_type,
            obzenflow_topology::StageType::InfiniteSource
        )
    });
    let outcome = if let Some(failure) = &context.termination.failure {
        ExecutionOutcome::Failed(failure.clone())
    } else if context.stop_intent.requested
        && (context.flow_start_time.is_none()
            || infinite
            || matches!(context.stop_intent.mode, Some(FlowStopMode::Cancel)))
    {
        ExecutionOutcome::Cancelled {
            reason: context.stop_intent.reason_label(),
        }
    } else if context.flow_start_time.is_some() {
        // A successful graceful drain of finite-only sources completes admitted
        // work. This does not promise exhaustion of unread source input.
        ExecutionOutcome::Completed
    } else {
        ExecutionOutcome::NotStarted
    };
    publish_terminal(supervisor, context, outcome).await
}

pub(super) async fn dispatch_failed(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
    reason: &str,
    failure_cause: &Option<ViolationCause>,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // Failed is also the historical FSM teardown state for cancellation. An
    // accepted execution failure takes precedence over any cleanup stop intent.
    let outcome = if let Some(failure) = &context.termination.failure {
        ExecutionOutcome::Failed(failure.clone())
    } else if context.stop_intent.requested {
        ExecutionOutcome::Cancelled {
            reason: context.stop_intent.reason_label(),
        }
    } else {
        ExecutionOutcome::Failed(ExecutionFailure {
            reason: reason.to_string(),
            cause: failure_cause.clone(),
        })
    };
    publish_terminal(supervisor, context, outcome).await
}

async fn publish_terminal(
    supervisor: &mut PipelineSupervisor,
    context: &mut PipelineContext,
    outcome: ExecutionOutcome,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    if matches!(outcome, ExecutionOutcome::NotStarted) {
        context.termination.retain(outcome, None)?;
        return Ok(EventLoopDirective::Terminate);
    }
    if let Err(error) = supervisor.reconcile_stage_metrics_from_tail(context).await {
        tracing::warn!(pipeline = %supervisor.name, %error,
            "Failed to reconcile stage lifecycle metrics before terminal publication");
    }
    let duration = DurationMs(
        context
            .flow_start_time
            .map(|start| start.elapsed().as_millis() as u64)
            .unwrap_or(0),
    );
    let metrics = crate::pipeline::fsm::compute_flow_lifecycle_metrics(context);
    let factory =
        obzenflow_core::event::system_event::SystemEventFactory::new(supervisor.system_id);
    let event = match &outcome {
        ExecutionOutcome::Completed => factory.pipeline_completed(duration, metrics),
        ExecutionOutcome::Cancelled { reason } => factory.pipeline_cancelled(
            reason.clone(),
            duration,
            Some(metrics),
            Some(ViolationCause::Other(reason.clone())),
        ),
        ExecutionOutcome::Failed(failure) => factory.pipeline_failed(
            failure.reason.clone(),
            duration,
            Some(metrics),
            failure.cause.clone(),
        ),
        ExecutionOutcome::NotStarted => {
            return Err(std::io::Error::other(
                "Pre-execution teardown cannot publish an execution terminal fact",
            )
            .into())
        }
    };
    let event_id = event.id;
    supervisor.system_journal.append(event, None).await.inspect_err(|error| {
        tracing::error!(pipeline = %supervisor.name, %error, "Failed to publish pipeline terminal outcome");
    })?;
    // No await between acknowledged append and retaining its outcome. State
    // notifications and task completion alone never establish publication.
    context.termination.retain(outcome, Some(event_id))?;
    match &context
        .termination
        .published
        .get()
        .expect("retained above")
        .outcome
    {
        ExecutionOutcome::Failed(failure) => tracing::error!(
            pipeline = %supervisor.name, %event_id, reason = %failure.reason,
            "Pipeline failed event written"
        ),
        outcome => tracing::info!(
            pipeline = %supervisor.name, %event_id, ?outcome,
            "Pipeline terminal outcome published"
        ),
    }
    Ok(EventLoopDirective::Terminate)
}
