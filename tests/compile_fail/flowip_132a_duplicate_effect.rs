// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "support/typed_effectful.rs"]
mod support;
#[allow(unused_imports)] use support::{First, FirstEffect, Input};

fn main() {
    let _ = obzenflow_dsl::effectful_transform!(
        Input ->{ FirstEffect, FirstEffect } First => support::AllowsFirstEffect,
        observers: []
    );
}
