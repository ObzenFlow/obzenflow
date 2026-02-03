// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed Infinite Source Demo - FLOWIP-081d
//!
//! Demonstrates typed infinite sources without `WriterId` and `ChainEvent` boilerplate:
//! - `InfiniteSourceTyped`: `new` (sync), `fallible` (sync fallible)
//! - `AsyncInfiniteSourceTyped`: `new` (async), `fallible` (async fallible)
//! - `AsyncInfiniteSourceTyped::from_receiver`: channel-backed async infinite source
//!
//! The demo runs briefly, then requests a graceful stop so the flow terminates deterministically.
//!
//! Run with: `cargo run -p obzenflow --example typed_infinite_source_demo`

use anyhow::{ensure, Result};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::{event::chain_event::ChainEvent, TypedPayload};
use obzenflow_dsl_infra::{async_infinite_source, flow, infinite_source, sink};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use obzenflow_runtime_services::stages::source::{AsyncInfiniteSourceTyped, InfiniteSourceTyped};
use obzenflow_runtime_services::stages::SourceError;
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DemoEvent {
    source: String,
    index: usize,
}

impl TypedPayload for DemoEvent {
    const EVENT_TYPE: &'static str = "demo.typed_infinite_source_event";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Default)]
struct Counts {
    total_events: usize,
    demo_data_events: usize,
    demo_events_by_source: HashMap<String, usize>,
    eof_events: usize,
    error_events: usize,
    other_event_types: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
struct CountingSink {
    counts: Arc<Mutex<Counts>>,
    expected_event_type: String,
}

impl CountingSink {
    fn new(counts: Arc<Mutex<Counts>>) -> Self {
        Self {
            counts,
            expected_event_type: DemoEvent::versioned_event_type(),
        }
    }
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let mut counts = self
            .counts
            .lock()
            .expect("CountingSink counts lock poisoned");

        counts.total_events += 1;

        if event.is_eof() {
            counts.eof_events += 1;
        }

        if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
            counts.error_events += 1;
        }

        let event_type = event.event_type();
        *counts
            .other_event_types
            .entry(event_type.clone())
            .or_insert(0) += 1;

        if event.is_data() && event_type == self.expected_event_type {
            counts.demo_data_events += 1;
            if let Some(payload) = DemoEvent::from_event(&event) {
                *counts
                    .demo_events_by_source
                    .entry(payload.source)
                    .or_insert(0) += 1;
            }
        }

        Ok(DeliveryPayload::success("noop", DeliveryMethod::Noop, None))
    }
}

fn expected_demo_counts(per_source: usize) -> HashMap<String, usize> {
    HashMap::from([
        ("sync_from_fn".to_string(), per_source),
        ("sync_from_fallible_fn".to_string(), per_source),
        ("async_from_async_fn".to_string(), per_source),
        ("async_from_fallible_async_fn".to_string(), per_source),
        ("async_from_receiver".to_string(), per_source),
    ])
}

#[tokio::main]
async fn main() -> Result<()> {
    let journal_path = std::path::PathBuf::from("target/typed_infinite_source_demo_journal");
    let counts = Arc::new(Mutex::new(Counts::default()));

    let per_source = 3usize;

    // Pre-fill an async channel source; keep the sender alive so the receiver doesn't close.
    let (tx, rx) = tokio::sync::mpsc::channel::<DemoEvent>(16);
    for index in 0..per_source {
        tx.send(DemoEvent {
            source: "async_from_receiver".to_string(),
            index,
        })
        .await
        .expect("send to async channel");
    }

    let flow_handle = {
        let counts_for_sink = counts.clone();
        flow! {
            name: "typed_infinite_source_demo",
            journals: disk_journals(journal_path.clone()),
            middleware: [],

            stages: {
                sync_src = infinite_source!("sync_from_fn" => InfiniteSourceTyped::new(move |index| {
                    if index >= per_source {
                        Vec::new()
                    } else {
                        vec![DemoEvent { source: "sync_from_fn".to_string(), index }]
                    }
                }));

                sync_fallible_src = infinite_source!("sync_from_fallible_fn" => {
                    let did_error = Arc::new(AtomicBool::new(false));
                    InfiniteSourceTyped::fallible(move |index| {
                        if index >= per_source {
                            return Ok(Vec::new());
                        }

                        // Error exactly once on the first attempt at index=1.
                        if index == 1 && !did_error.swap(true, Ordering::SeqCst) {
                            return Err(SourceError::Transport(
                                "simulated transport failure (sync)".to_string(),
                            ));
                        }

                        Ok(vec![DemoEvent { source: "sync_from_fallible_fn".to_string(), index }])
                    })
                });

                async_src = async_infinite_source!("async_from_async_fn" =>
                    AsyncInfiniteSourceTyped::new({
                        move |index| async move {
                            if index >= per_source {
                                Vec::new()
                            } else {
                                tokio::time::sleep(Duration::from_millis(1)).await;
                                vec![DemoEvent { source: "async_from_async_fn".to_string(), index }]
                            }
                        }
                    })
                );

                async_fallible_src = async_infinite_source!("async_from_fallible_async_fn" => {
                    let did_error = Arc::new(AtomicBool::new(false));
                    AsyncInfiniteSourceTyped::fallible(move |index| {
                        let did_error = did_error.clone();
                        async move {
                            if index >= per_source {
                                return Ok(Vec::new());
                            }

                            // Error exactly once on the first attempt at index=1.
                            if index == 1 && !did_error.swap(true, Ordering::SeqCst) {
                                return Err(SourceError::Timeout(
                                    "simulated timeout (async)".to_string(),
                                ));
                            }

                            tokio::time::sleep(Duration::from_millis(1)).await;
                            Ok(vec![DemoEvent { source: "async_from_fallible_async_fn".to_string(), index }])
                        }
                    })
                });

                async_rx_src = async_infinite_source!("async_from_receiver" =>
                    AsyncInfiniteSourceTyped::from_receiver(rx)
                );

                snk = sink!("counting_sink" => CountingSink::new(counts_for_sink.clone()));
            },

            topology: {
                sync_src |> snk;
                sync_fallible_src |> snk;
                async_src |> snk;
                async_fallible_src |> snk;
                async_rx_src |> snk;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?
    };

    flow_handle
        .start()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to start flow: {e:?}"))?;

    // Let sources emit their bounded demo events before requesting a stop.
    tokio::time::sleep(Duration::from_millis(250)).await;

    flow_handle
        .stop_graceful(Duration::from_secs(5))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to request graceful stop: {e:?}"))?;

    flow_handle
        .wait_for_completion()
        .await
        .map_err(|e| anyhow::anyhow!("Flow did not complete: {e:?}"))?;

    drop(tx);

    let counts = counts.lock().expect("Counts lock poisoned");
    let expected = expected_demo_counts(per_source);

    ensure!(
        counts.demo_events_by_source == expected,
        "unexpected demo event counts by source: expected {:?}, got {:?}",
        expected,
        counts.demo_events_by_source
    );
    ensure!(
        counts.demo_data_events == expected.values().sum::<usize>(),
        "expected {} demo data events, got {}",
        expected.values().sum::<usize>(),
        counts.demo_data_events
    );
    println!("\nTyped infinite source demo summary:");
    println!("  total events observed: {}", counts.total_events);
    println!("  demo data events: {}", counts.demo_data_events);
    println!("  error events: {}", counts.error_events);
    println!("  eof events: {}", counts.eof_events);

    println!("\nDemo event counts by constructor:");
    for (name, count) in counts.demo_events_by_source.iter() {
        println!("  {name}: {count}");
    }

    Ok(())
}
