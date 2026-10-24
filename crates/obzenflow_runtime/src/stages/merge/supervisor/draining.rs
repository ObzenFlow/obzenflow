// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{common, BoxError};
use crate::stages::merge::fsm::{MergeEvent, MergeResources};
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_draining(
    ctx: &mut MergeResources,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }
    if common::flush_pending_outputs(ctx).await?.backed_off {
        return Ok(EventLoopDirective::Continue);
    }
    Ok(EventLoopDirective::Transition(MergeEvent::DrainComplete))
}
