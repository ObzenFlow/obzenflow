// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_source_completed(
    _supervisor: &mut PipelineSupervisor,
    _context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // Source has completed: initiate Jonestown protocol.
    tracing::info!("Source completed - beginning pipeline drain");
    Ok(EventLoopDirective::Transition(PipelineEvent::BeginDrain))
}
