// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::observer::{HandlerObserver, HandlerObserverContext};

struct PublishingObserver;

impl HandlerObserver for PublishingObserver {
    fn after_handle(&self, ctx: &HandlerObserverContext<'_>, outputs: &[ChainEvent]) {
        ctx.publish(outputs[0].clone());
    }
}

fn main() {}
