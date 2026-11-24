// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::query::{SourceInput, SourceInputRecord};

impl EffectsCore {
    pub(crate) fn source_registry(&self) -> &EffectPortRegistry {
        &self.ctx.stage.effect_ports
    }

    pub(crate) fn query_context(&self) -> crate::queries::QueryContext {
        crate::queries::QueryContext {
            stage_key: self.ctx.stage.stage_key.clone(),
            number: self.ctx.input_seq.0,
            recorder: self
                .ctx
                .stage
                .instrumentation
                .as_ref()
                .map(|i| i.observation_recorder()),
        }
    }

    pub(crate) async fn source_input<F, Fut>(
        &mut self,
        accepts: impl Fn(&SourceInput) -> bool,
        acquire: F,
    ) -> Result<SourceInput, EffectError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<SourceInput, EffectError>>,
    {
        let cursor = self.next_cursor();
        let plan = self.take_action(RecordedAction::SourceInput(cursor.clone()))?;
        let recorded = self
            .ctx
            .stage
            .effect_history
            .as_ref()
            .and_then(|h| h.source_inputs.get(&cursor))
            .cloned();
        if let Some(mut event) = recorded {
            let ChainPayload::Execution(ExecutionPayload::SourceInput(record)) = &event.payload
            else {
                return Err(EffectError::ReplayArchive("invalid source input".into()));
            };
            if !accepts(&record.input) || matches!(plan, ActionPlan::Live) {
                return Err(EffectError::ReplayArchive(format!(
                    "source request differs at {cursor:?}"
                )));
            }
            let input = record.input.clone();
            self.reauthor_recorded_crossing(ActionRow::SourceInput(cursor))
                .await?;
            self.mark_lineage(&mut event, RowLineage::Reproduced(None));
            self.commit_framework_event(event).await?;
            return Ok(input);
        }
        if matches!(plan, ActionPlan::Reproduce)
            || self.ctx.stage.runtime_execution.archive_for_io().is_some()
                && self.ctx.stage.runtime_execution.resume_control().is_none()
        {
            return Err(EffectError::ReplayArchive(format!(
                "missing source input at {cursor:?}"
            )));
        }
        self.cross_before_live_action().await?;
        let input = acquire().await?;
        let attempt = cursor
            .effect_ordinal
            .get()
            .checked_add(1)
            .ok_or_else(|| EffectError::Execution("source attempts exhausted".into()))?;
        let record = SourceInputRecord {
            recorded_flow_id: cursor.recorded_flow_id.to_string(),
            stage_key: self.ctx.stage.stage_key.clone(),
            number: self.ctx.input_seq.0,
            attempt,
            input: input.clone(),
        };
        let mut event = ChainEventFactory::execution_event(
            self.ctx.stage.writer_id,
            ExecutionPayload::SourceInput(Box::new(record)),
        );
        self.mark_lineage(&mut event, RowLineage::Live);
        self.commit_framework_event(event).await?;
        Ok(input)
    }
}
