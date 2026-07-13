// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::middleware::MiddlewareContext;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use std::time::Duration;

/// Sink-shaped policy context. It owns the observability outbox returned by the
/// boundary report and never crosses into the runtime supervisor.
pub struct SinkPolicyCtx {
    middleware_ctx: MiddlewareContext,
    attempt_failure: Option<(ErrorKind, Option<Duration>)>,
}

impl Default for SinkPolicyCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkPolicyCtx {
    pub fn new() -> Self {
        Self {
            middleware_ctx: MiddlewareContext::with_scope(
                MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            ),
            attempt_failure: None,
        }
    }

    pub fn write_control_event(&mut self, event: ChainEvent) {
        self.middleware_ctx.write_control_event(event);
    }

    pub fn take_control_events(&mut self) -> Vec<ChainEvent> {
        self.middleware_ctx.take_control_events()
    }

    pub(crate) fn middleware_context_mut(&mut self) -> &mut MiddlewareContext {
        &mut self.middleware_ctx
    }

    pub(crate) fn middleware_context(&self) -> &MiddlewareContext {
        &self.middleware_ctx
    }

    pub(crate) fn set_attempt_failure(&mut self, kind: ErrorKind, retry_after: Option<Duration>) {
        self.attempt_failure = Some((kind, retry_after));
    }

    pub(crate) fn attempt_failure(&self) -> Option<&(ErrorKind, Option<Duration>)> {
        self.attempt_failure.as_ref()
    }
}
