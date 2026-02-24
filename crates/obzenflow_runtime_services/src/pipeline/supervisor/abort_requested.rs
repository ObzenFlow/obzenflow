// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineEvent, PipelineSupervisor};
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::types::ViolationCause;
use obzenflow_core::StageId;

pub(super) async fn dispatch_abort_requested(
    _supervisor: &mut PipelineSupervisor,
    reason: &ViolationCause,
    upstream: &Option<StageId>,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    let msg = format!("Pipeline abort requested: {reason:?} (upstream={upstream:?})");
    Ok(EventLoopDirective::Transition(PipelineEvent::Error {
        message: msg,
    }))
}
