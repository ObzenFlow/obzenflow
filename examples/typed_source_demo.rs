//! Typed Source Demo - FLOWIP-081
//!
//! Demonstrates typed finite sources without `WriterId` and `ChainEvent` boilerplate:
//! - `FiniteSourceTyped`: `new` (from iterator), `from_item_fn`, `from_fn` (batch)
//! - Fallible sync: `FiniteSourceTyped::from_fallible_item_fn`
//! - `AsyncFiniteSourceTyped`: `new` (from async producer)
//! - Fallible async: `AsyncFiniteSourceTyped::from_fallible_async_item_fn` (internal boxing)
//!
//! Run with: `cargo run -p obzenflow --example typed_source_demo`

use anyhow::{ensure, Result};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::{event::chain_event::ChainEvent, TypedPayload};
use obzenflow_dsl_infra::{async_source, flow, sink, source};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use obzenflow_runtime_services::stages::source::{AsyncFiniteSourceTyped, FiniteSourceTyped};
use obzenflow_runtime_services::stages::SourceError;
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
    const EVENT_TYPE: &'static str = "demo.typed_source_event";
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
        *counts.other_event_types.entry(event_type.clone()).or_insert(0) += 1;

        if event.is_data() && event_type == self.expected_event_type {
            counts.demo_data_events += 1;
            if let Some(payload) = DemoEvent::from_event(&event) {
                *counts
                    .demo_events_by_source
                    .entry(payload.source)
                    .or_insert(0) += 1;
            }
        }

        Ok(DeliveryPayload::success(
            "noop",
            DeliveryMethod::Noop,
            None,
        ))
    }
}

fn expected_demo_counts() -> HashMap<String, usize> {
    HashMap::from([
        ("from_iter".to_string(), 3),
        ("from_item_fn".to_string(), 3),
        ("from_producer_batch".to_string(), 5),
        ("from_fallible_item_fn".to_string(), 3),
        ("from_async_fn".to_string(), 3),
        ("from_fallible_async_item_fn".to_string(), 3),
    ])
}

#[tokio::main]
async fn main() -> Result<()> {
    let journal_path = std::path::PathBuf::from("target/typed_source_demo_journal");
    let counts = Arc::new(Mutex::new(Counts::default()));
    let counts_for_sink = counts.clone();

    FlowApplication::run(flow! {
        name: "typed_source_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],

        stages: {
            iter_src = source!("from_iter" => {
                let items: Vec<DemoEvent> = (0..3)
                    .map(|i| DemoEvent { source: "from_iter".to_string(), index: i })
                    .collect();
                FiniteSourceTyped::new(items)
            });

            item_src = source!("from_item_fn" => FiniteSourceTyped::from_item_fn(|index| {
                if index >= 3 {
                    return None;
                }
                Some(DemoEvent {
                    source: "from_item_fn".to_string(),
                    index,
                })
            }));

            batch_src = source!("from_producer_batch" => FiniteSourceTyped::from_producer({
                let total = 5usize;
                move |index| {
                    if index >= total {
                        return None;
                    }

                    let remaining = total - index;
                    let batch_size = remaining.min(2);
                    let mut batch = Vec::with_capacity(batch_size);
                    for offset in 0..batch_size {
                        batch.push(DemoEvent {
                            source: "from_producer_batch".to_string(),
                            index: index + offset,
                        });
                    }
                    Some(batch)
                }
            }));

            fallible_src = source!("from_fallible_item_fn" => {
                let total = 3usize;
                let did_error = Arc::new(AtomicBool::new(false));

                FiniteSourceTyped::from_fallible_item_fn(move |index| {
                    if index >= total {
                        return Ok(None);
                    }

                    // Error exactly once on the first attempt at index=1.
                    if index == 1 && !did_error.swap(true, Ordering::SeqCst) {
                        return Err(SourceError::Transport(
                            "simulated transport failure (sync)".to_string(),
                        ));
                    }

                    Ok(Some(DemoEvent {
                        source: "from_fallible_item_fn".to_string(),
                        index,
                    }))
                })
            });

            async_src = async_source!("from_async_fn" => AsyncFiniteSourceTyped::new({
                let total = 3usize;
                move |index| async move {
                    if index >= total {
                        None
                    } else {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                        Some(vec![DemoEvent {
                            source: "from_async_fn".to_string(),
                            index,
                        }])
                    }
                }
            }));

            fallible_async_src = async_source!("from_fallible_async_item_fn" => {
                let total = 3usize;
                let did_error = Arc::new(AtomicBool::new(false));

                AsyncFiniteSourceTyped::from_fallible_async_item_fn(move |index| {
                    let did_error = did_error.clone();
                    async move {
                        if index >= total {
                            return Ok(None);
                        }

                        // Error exactly once on the first attempt at index=1.
                        if index == 1 && !did_error.swap(true, Ordering::SeqCst) {
                            return Err(SourceError::Timeout(
                                "simulated timeout (async)".to_string(),
                            ));
                        }

                        tokio::time::sleep(Duration::from_millis(1)).await;
                        Ok(Some(DemoEvent {
                            source: "from_fallible_async_item_fn".to_string(),
                            index,
                        }))
                    }
                })
            });

            snk = sink!("counting_sink" => CountingSink::new(counts_for_sink.clone()));
        },

        topology: {
            iter_src |> snk;
            item_src |> snk;
            batch_src |> snk;
            fallible_src |> snk;
            async_src |> snk;
            fallible_async_src |> snk;
        }
    })
    .await
    ?;

    let counts = counts.lock().expect("Counts lock poisoned");
    let expected = expected_demo_counts();
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

    println!("\nTyped source demo summary:");
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
