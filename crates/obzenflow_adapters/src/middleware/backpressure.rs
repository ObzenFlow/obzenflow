// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::middleware::control::ControlMiddlewareAggregator;
use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::context::StageType;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use std::num::NonZeroU64;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct BackpressureMiddlewareFactory {
    window: NonZeroU64,
}

impl BackpressureMiddlewareFactory {
    pub fn new(window: NonZeroU64) -> Self {
        Self { window }
    }
}

impl MiddlewareFactory for BackpressureMiddlewareFactory {
    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> Box<dyn Middleware> {
        Box::new(BackpressureMiddleware)
    }

    fn name(&self) -> &str {
        "backpressure"
    }

    fn supported_stage_types(&self) -> &[StageType] {
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ]
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "window": self.window.get(),
        }))
    }
}

#[derive(Debug)]
struct BackpressureMiddleware;

impl Middleware for BackpressureMiddleware {
    fn middleware_name(&self) -> &'static str {
        "backpressure"
    }

    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        MiddlewareAction::Continue
    }
}

pub fn backpressure(window: u64) -> Box<dyn MiddlewareFactory> {
    Box::new(BackpressureMiddlewareFactory::new(
        NonZeroU64::new(window).expect("window must be > 0"),
    ))
}
