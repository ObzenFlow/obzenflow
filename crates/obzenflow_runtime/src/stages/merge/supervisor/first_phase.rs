// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{common, BoxError};
use crate::stages::merge::fsm::{MergeEvent, MergeResources, MergeState};
use crate::supervised_base::EventLoopDirective;

pub(super) async fn dispatch_first_phase(
    state: &MergeState,
    ctx: &mut MergeResources,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    common::align_held_selection(ctx);
    renew_held_edges(ctx);
    let mut input = std::mem::take(&mut ctx.first_phase);
    let directive = common::dispatch_reading(state, &mut input, ctx).await;
    ctx.first_phase = input;
    directive
}

fn renew_held_edges(ctx: &MergeResources) {
    for producer in &ctx.held_producers {
        if let Some(reader) = ctx.backpressure_readers.get(producer) {
            reader.renew_hold();
        }
    }
}
