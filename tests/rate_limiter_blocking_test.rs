#![cfg(any())] // Disabled: superseded by focused rate limiter metrics tests

//! Integration test for blocking rate limiter
//!
//! Verifies that the rate limiter properly blocks and processes all events
//! without data loss, as specified in FLOWIP-050e.

use obzenflow_test_utils::integration::{FlowTestBuilder, TestUtils};
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_rate_limiter_no_event_loss() {
    let test_utils = TestUtils::new();

    // Create flow with rate-limited transform
    let mut builder = FlowTestBuilder::new(&test_utils, "rate_limit_test")
        .with_source("generator", |config| {
            config.with_type("generator").with_config(json!({
                "count": 100,
                "event_type": "test.event"
            }))
        })
        .with_transform("rate_limited", |config| {
            config.with_type("passthrough").with_middleware(vec![
                obzenflow_adapters::middleware::common::rate_limit(10.0),
            ])
        })
        .with_sink("counter", |config| {
            config.with_type("counter").with_config(json!({
                "expected": 100
            }))
        });

    builder.connect("generator", "rate_limited");
    builder.connect("rate_limited", "counter");

    let flow = builder.build().await.expect("Failed to build flow");

    // Start timing
    let start = Instant::now();

    // Run the flow
    flow.run().await.expect("Failed to run flow");

    let elapsed = start.elapsed();

    // Verify all events were processed
    let events = test_utils.get_stage_events(&flow.flow_id, "counter").await;
    assert_eq!(events.len(), 100, "Should process all 100 events");

    // Verify it took approximately the right amount of time
    // 100 events at 10/second should take ~10 seconds
    assert!(
        elapsed >= Duration::from_secs(9),
        "Should take at least 9 seconds"
    );
    assert!(
        elapsed <= Duration::from_secs(12),
        "Should take no more than 12 seconds"
    );
}

#[tokio::test]
async fn test_rate_limiter_with_burst() {
    let test_utils = TestUtils::new();

    // Create flow with burst capacity
    let mut builder = FlowTestBuilder::new(&test_utils, "rate_limit_burst_test")
        .with_source("generator", |config| {
            config.with_type("generator").with_config(json!({
                "count": 50,
                "event_type": "test.event"
            }))
        })
        .with_transform("rate_limited", |config| {
            config.with_type("passthrough").with_middleware(vec![
                obzenflow_adapters::middleware::common::rate_limit_with_burst(10.0, 20.0),
            ])
        })
        .with_sink("counter", |config| {
            config.with_type("counter").with_config(json!({
                "expected": 50
            }))
        });

    builder.connect("generator", "rate_limited");
    builder.connect("rate_limited", "counter");

    let flow = builder.build().await.expect("Failed to build flow");

    // Start timing
    let start = Instant::now();

    // Run the flow
    flow.run().await.expect("Failed to run flow");

    let elapsed = start.elapsed();

    // Verify all events were processed
    let events = test_utils.get_stage_events(&flow.flow_id, "counter").await;
    assert_eq!(events.len(), 50, "Should process all 50 events");

    // With burst of 20 and rate of 10/sec:
    // - First 20 events: immediate (burst)
    // - Remaining 30 events: 3 seconds at 10/sec
    // Total should be ~3 seconds
    assert!(
        elapsed >= Duration::from_secs(2),
        "Should take at least 2 seconds"
    );
    assert!(
        elapsed <= Duration::from_secs(5),
        "Should take no more than 5 seconds"
    );
}

#[tokio::test]
async fn test_rate_limiter_control_events_not_blocked() {
    let test_utils = TestUtils::new();

    // Create flow where control events should pass through immediately
    let mut builder = FlowTestBuilder::new(&test_utils, "rate_limit_control_test")
        .with_source("generator", |config| {
            config.with_type("generator").with_config(json!({
                "count": 5,
                "event_type": "test.event",
                "finite": true  // Will emit EOF
            }))
        })
        .with_transform("rate_limited", |config| {
            config.with_type("passthrough").with_middleware(vec![
                obzenflow_adapters::middleware::common::rate_limit(1.0), // Very slow rate
            ])
        })
        .with_sink("counter", |config| {
            config.with_type("counter").with_config(json!({
                "expected": 5
            }))
        });

    builder.connect("generator", "rate_limited");
    builder.connect("rate_limited", "counter");

    let flow = builder.build().await.expect("Failed to build flow");

    // Start timing
    let start = Instant::now();

    // Run the flow
    flow.run().await.expect("Failed to run flow");

    let elapsed = start.elapsed();

    // With 5 events at 1/second, if EOF was blocked it would take 5+ seconds
    // But EOF should pass through immediately after the last event
    assert!(
        elapsed <= Duration::from_secs(6),
        "EOF should not be blocked by rate limiter"
    );

    // Verify we got the EOF
    let events = test_utils.get_stage_events(&flow.flow_id, "counter").await;
    let eof_events: Vec<_> = events.iter().filter(|e| e.is_control()).collect();
    assert!(!eof_events.is_empty(), "Should have received EOF event");
}
