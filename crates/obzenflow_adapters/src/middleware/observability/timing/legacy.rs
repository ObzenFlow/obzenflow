// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::TimingMiddleware;
use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase,
};
use obzenflow_core::event::chain_event::ChainEvent;

impl Middleware for TimingMiddleware {
    fn label(&self) -> &'static str {
        "timing"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        let _ = ctx;

        self.remember_start(event);

        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        ErrorAction::Propagate
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        let _ = (event, ctx);
    }
}
