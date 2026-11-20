// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::TransformResources;
use crate::effects::EffectInvocationContext;
use crate::messaging::{upstream_subscription::StageInputPosition, DeliveredRecord};
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::supervision::error_routing::{error_row_in_scope, processing_failed};
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::observer::dispatch::{
    run_after_handler_observers, run_before_handler_observers,
};
use obzenflow_core::event::{provenance::FlowContext, ChainPayload};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{StageId, WriterId};
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::time::Instant;

type DispatchError = Box<dyn std::error::Error + Send + Sync>;

struct Input {
    parent: DeliveredRecord<ChainPayload>,
    position: StageInputPosition,
    generation: Option<obzenflow_core::ReaderGeneration>,
}

pub(crate) struct SinkBatch {
    size: usize,
    max_wait: Duration,
    opened: Option<Instant>,
    inputs: Vec<Input>,
    group: Option<Box<dyn crate::effects::OpenSinkBatch>>,
}

impl SinkBatch {
    pub(crate) fn new(size: usize, max_wait: Duration) -> Self {
        Self {
            size,
            max_wait,
            opened: None,
            inputs: Vec::new(),
            group: None,
        }
    }

    pub(super) fn idle_wait(&self) -> Duration {
        self.opened.map_or(Duration::ZERO, |instant| {
            self.max_wait.saturating_sub(instant.elapsed())
        })
    }

    pub(super) fn expired(&self) -> bool {
        !self.max_wait.is_zero()
            && self
                .opened
                .is_some_and(|instant| instant.elapsed() >= self.max_wait)
    }
}

pub(super) async fn enqueue<H: UnifiedTransformHandler>(
    ctx: &mut TransformResources<H>,
    flow: &FlowContext,
    parent: DeliveredRecord<ChainPayload>,
    position: StageInputPosition,
    upstream: Option<StageId>,
    generation: Option<obzenflow_core::ReaderGeneration>,
) -> Result<(), DispatchError> {
    if ctx.sink_batch.as_ref().is_some_and(SinkBatch::expired) {
        flush(ctx, flow).await?;
    }
    let scope = ctx
        .runtime_execution
        .dispatch_scope(ctx.stage_id, Some(position), generation);
    run_before_handler_observers(
        &ctx.observers,
        ctx.flow_id,
        flow,
        scope,
        parent.view(),
        position,
    );
    let batch = ctx.sink_batch.as_mut().expect("configured sink group");
    batch.opened.get_or_insert_with(Instant::now);
    if batch.group.is_none() {
        batch.group = ctx
            .handler
            .as_ref()
            .and_then(|handler| handler.open_sink_batch());
    }
    let stage = ctx
        .stage_constants
        .clone()
        .ok_or("sink group has no effect context")?;
    let input = EffectInvocationContext {
        stage,
        input_seq: position,
        parent: parent.clone(),
        backpressure_writer: ctx.backpressure_writer.clone(),
        emit_enabled: true,
        effect_boundary: None,
    };
    let admission = batch
        .group
        .as_mut()
        .ok_or("sink group has no handler")?
        .admit(input)
        .await;
    let probe = match admission {
        Ok(close) => close,
        Err(error) => {
            if let Some(fatal) = error.as_fatal() {
                record_stage_fatal(
                    fatal,
                    StageFatalCommit {
                        error_journal: &ctx.error_journal,
                        writer_id: ctx.stage_id.into(),
                        stage_id: ctx.stage_id,
                        stage_key: &ctx.stage_name,
                        input_position: Some(position),
                        parent: Some(&parent),
                        lineage: ctx.lineage_policy,
                    },
                )
                .await?;
            }
            return Err(error.into());
        }
    };
    batch.inputs.push(Input {
        parent,
        position,
        generation,
    });
    if let Some(reader) = upstream.and_then(|id| ctx.backpressure_readers.get(&id)) {
        reader.ack_consumed(1);
    }
    if probe || batch.inputs.len() >= batch.size || batch.expired() {
        flush(ctx, flow).await?;
    }
    Ok(())
}

pub(super) async fn flush<H: UnifiedTransformHandler>(
    ctx: &mut TransformResources<H>,
    flow: &FlowContext,
) -> Result<(), DispatchError> {
    let Some(batch) = &mut ctx.sink_batch else {
        return Ok(());
    };
    if batch.inputs.is_empty() {
        return Ok(());
    }
    let inputs = std::mem::take(&mut batch.inputs);
    batch.opened = None;
    let group = batch.group.take().ok_or("sink group has no handler")?;
    ctx.instrumentation
        .events_processed_total
        .fetch_add(inputs.len() as u64, Ordering::Relaxed);
    let results = crate::metrics::instrumentation::process_with_instrumentation_no_count(
        &ctx.instrumentation,
        || group.finish(),
    )
    .await?;
    if results.len() != inputs.len() {
        return Err("sink group failed to account for every input".into());
    }
    for (input, result) in inputs.into_iter().zip(results) {
        let parent = &input.parent;
        let scope = ctx.runtime_execution.dispatch_scope(
            ctx.stage_id,
            Some(input.position),
            input.generation,
        );
        let mut outputs = Vec::new();
        if let Err(error) = result {
            if let Some(fatal) = error.as_fatal() {
                record_stage_fatal(
                    fatal,
                    StageFatalCommit {
                        error_journal: &ctx.error_journal,
                        writer_id: WriterId::from(ctx.stage_id),
                        stage_id: ctx.stage_id,
                        stage_key: &ctx.stage_name,
                        input_position: Some(input.position),
                        parent: Some(parent),
                        lineage: ctx.lineage_policy,
                    },
                )
                .await?;
                return Err(error.into());
            }
            ctx.instrumentation.record_error(error.kind());
            outputs.push(processing_failed(
                WriterId::from(ctx.stage_id),
                flow,
                parent.view(),
                &error,
                ctx.lineage_policy,
            ));
        }
        run_after_handler_observers(
            &ctx.observers,
            ctx.flow_id,
            flow,
            scope,
            parent.view(),
            input.position,
            &outputs,
        );
        for event in outputs {
            crate::supervised_base::publication::append(
                &ctx.error_journal,
                error_row_in_scope(event, scope),
                AppendOptions::from_record(Some(parent))?,
            )
            .await?;
        }
        if let Some(heartbeat) = &ctx.heartbeat {
            heartbeat.state.record_last_consumed(parent.view().id);
        }
    }
    Ok(())
}
