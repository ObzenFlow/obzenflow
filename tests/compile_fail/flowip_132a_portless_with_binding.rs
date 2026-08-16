// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstEffect, Input};

fn main() {
    let binding = support::zero_slot_binding();
    let _ = obzenflow_dsl::effectful_transform!(
        Input ->{ FirstEffect via binding } First => support::AllowsFirstEffect,
        observers: []
    );
}
