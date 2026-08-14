// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::observer::{HandlerObserver, HandlerObserverContext};

struct ReturningObserver;

impl HandlerObserver for ReturningObserver {
    fn after_handle(
        &self,
        _ctx: &HandlerObserverContext<'_>,
        outputs: &[ChainEvent],
    ) -> ChainEvent {
        outputs[0].clone()
    }
}

fn main() {}
