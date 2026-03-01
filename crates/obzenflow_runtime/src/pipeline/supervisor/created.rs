// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{BoxError, PipelineContext, PipelineEvent, PipelineSupervisor};
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_created(
    _supervisor: &mut PipelineSupervisor,
    _context: &mut PipelineContext,
) -> Result<EventLoopDirective<PipelineEvent>, BoxError> {
    // In Created state, we wait for external trigger to materialise. This is
    // typically driven by FlowHandle via external event injection.
    tracing::info!("✅ Pipeline state in Created");
    Ok(EventLoopDirective::Continue)
}
