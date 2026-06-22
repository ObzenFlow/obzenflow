// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::TimingMiddleware;
use crate::middleware::{Middleware, MiddlewareContext};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::time::MetricsDuration;
use obzenflow_core::{StageId, WriterId};
use serde_json::json;
use std::thread;

#[test]
fn test_timing_middleware_adds_processing_time() {
    let middleware = TimingMiddleware::new("test_stage");

    let event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"data": "test"}),
    );

    middleware.remember_start(&event);
    thread::sleep(MetricsDuration::from_millis(10).to_std());

    let mut result_event = event.clone();
    middleware.stamp_outputs(Some(&event), std::slice::from_mut(&mut result_event));

    assert!(result_event.processing_info.processing_time.as_nanos() >= 9_000_000);
}

#[test]
fn test_timing_middleware_handles_missing_start_time() {
    let middleware = TimingMiddleware::new("test_stage");
    let ctx = MiddlewareContext::live_handler();

    let mut event = ChainEventFactory::data_event(
        WriterId::from(StageId::new()),
        "test.event",
        json!({"data": "test"}),
    );

    middleware.pre_write(&mut event, &ctx);

    assert_eq!(event.processing_info.processing_time, MetricsDuration::ZERO);
}
