// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::source::MemberRecorded;

impl EffectsCore {
    pub(crate) async fn record_member<F, Fut>(
        &mut self,
        acquire: F,
    ) -> Result<serde_json::Value, EffectError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<serde_json::Value, EffectError>>,
    {
        let cursor = self.next_cursor();
        let plan = self.take_action(RecordedAction::Member(cursor.clone()))?;
        let recorded = self
            .ctx
            .stage
            .effect_history
            .as_ref()
            .and_then(|history| history.members.get(&cursor))
            .cloned();
        if let Some(mut event) = recorded {
            let ChainPayload::Execution(ExecutionPayload::MemberRecorded(record)) = &event.payload
            else {
                return Err(EffectError::ReplayArchive(
                    "the member record is not valid".into(),
                ));
            };
            if matches!(plan, ActionPlan::Live) {
                return Err(EffectError::ReplayArchive(format!(
                    "the member at {cursor:?} does not match the recording"
                )));
            }
            let member = record.member.clone();
            self.reauthor_recorded_crossing(ActionRow::Member(cursor))
                .await?;
            self.mark_lineage(&mut event, RowLineage::Reproduced(None));
            self.answer = Some(self.commit_framework_event(event).await?);
            return Ok(member);
        }
        if matches!(plan, ActionPlan::Reproduce)
            || self.ctx.stage.runtime_execution.archive_for_io().is_some()
                && self.ctx.stage.runtime_execution.resume_control().is_none()
        {
            return Err(EffectError::ReplayArchive(format!(
                "the archive has no member at {cursor:?}"
            )));
        }
        self.cross_before_live_action().await?;
        let member = acquire().await?;
        let record = MemberRecorded {
            recorded_flow_id: cursor.recorded_flow_id.to_string(),
            stage_key: self.ctx.stage.stage_key.clone(),
            number: self.ctx.input_seq.0,
            member: member.clone(),
        };
        let mut event = ChainEventFactory::execution_event(
            self.ctx.stage.writer_id,
            ExecutionPayload::MemberRecorded(Box::new(record)),
        );
        self.mark_lineage(&mut event, RowLineage::Live);
        self.answer = Some(self.commit_framework_event(event).await?);
        Ok(member)
    }
}
