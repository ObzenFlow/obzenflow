// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_runtime::stages::observer::{
    HandlerObserver, HandlerObserverContext, ObserverReport,
};

struct MutatingObserver;

impl HandlerObserver for MutatingObserver {
    fn label(&self) -> &'static str {
        "mutating"
    }

    fn after_handle(
        &self,
        _ctx: &HandlerObserverContext<'_>,
        outputs: &[ChainEvent],
    ) -> ObserverReport {
        outputs[0].writer_id = WriterId::from(StageId::new());
        ObserverReport::empty()
    }
}

fn main() {}
