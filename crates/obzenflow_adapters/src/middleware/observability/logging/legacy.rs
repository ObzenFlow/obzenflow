// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::LoggingMiddleware;
use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase,
};
use obzenflow_core::event::chain_event::ChainEvent;

impl Middleware for LoggingMiddleware {
    fn label(&self) -> &'static str {
        "logging"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
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
}
