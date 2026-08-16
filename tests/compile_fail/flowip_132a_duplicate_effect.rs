// SPDX-License-Identifier: MIT OR Apache-2.0

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstEffect, Input};

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        Input ->{ FirstEffect, FirstEffect } First => support::AllowsFirstEffect,
        observers: []
    );
}
