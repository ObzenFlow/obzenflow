// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{common, BoxError};
use crate::stages::merge::fsm::{MergeEvent, MergeResources, MergeState};
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_last_phase(
    state: &MergeState,
    ctx: &mut MergeResources,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    common::align_held_selection(ctx);
    let mut input = std::mem::take(&mut ctx.held);
    let directive = common::dispatch_reading(state, &mut input, ctx).await;
    ctx.held = input;
    directive
}
