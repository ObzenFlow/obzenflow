// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, Input, ZeroSlotNamedEffect};

fn main() {
    let other = support::other_zero_slot_binding();
    let _ = obzenflow_dsl::effectful_transform!(
        Input ->{ ZeroSlotNamedEffect via other } First => support::AllowsZeroSlotNamedEffect,
        observers: []
    );
}
