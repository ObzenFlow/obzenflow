// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::middleware::MiddlewareContext;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope, WriterId};
use std::time::{SystemTime, UNIX_EPOCH};

/// Source-shaped policy context. It owns the observability outbox returned by
/// the boundary report and never crosses into the runtime supervisor.
pub struct SourcePolicyCtx {
    writer_id: WriterId,
    synthetic_event: Option<ChainEvent>,
    middleware_ctx: MiddlewareContext,
    retry_physical_call: bool,
}

impl SourcePolicyCtx {
    pub fn new(writer_id: WriterId) -> Self {
        Self {
            writer_id,
            synthetic_event: None,
            middleware_ctx: MiddlewareContext::with_scope(
                MiddlewareExecutionScope::LiveSourceBoundary,
            ),
            retry_physical_call: false,
        }
    }

    pub(crate) fn new_retry_physical_call(writer_id: WriterId) -> Self {
        let mut ctx = Self::new(writer_id);
        ctx.retry_physical_call = true;
        ctx
    }

    pub(crate) fn retry_physical_call(&self) -> bool {
        self.retry_physical_call
    }

    pub fn writer_id(&self) -> WriterId {
        self.writer_id
    }

    pub fn synthetic_event_clone(&mut self) -> ChainEvent {
        self.synthetic_event().clone()
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

    fn synthetic_event(&mut self) -> &ChainEvent {
        if self.synthetic_event.is_none() {
            self.synthetic_event = Some(ChainEventFactory::data_event(
                self.writer_id,
                "system.source.next",
                serde_json::json!({
                    "source_type": "boundary",
                    "timestamp_ms": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                }),
            ));
        }
        self.synthetic_event
            .as_ref()
            .expect("synthetic event must be initialized")
    }
}
