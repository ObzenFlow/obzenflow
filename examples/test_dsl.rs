//! Minimal test for DSL macro expansion

use obzenflow_dsl_infra::prelude::*;
use obzenflow_dsl_infra::flow;
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use serde_json::json;
use std::sync::Arc;
use anyhow::Result;
use tempfile;

struct TestSource {
    writer_id: WriterId,
    count: usize,
}

impl TestSource {
    fn new() -> Self {
        Self {
            writer_id: WriterId::new(),
            count: 0,
        }
    }
}

impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count < 3 {
            self.count += 1;
            Some(ChainEvent::new(
                EventId::new(),
                self.writer_id.clone(),
                "test",
                json!({ "count": self.count })
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.count >= 3
    }
}

struct TestTransform;

impl TransformHandler for TestTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["transformed"] = json!(true);
        vec![event]
    }
}

struct TestSink {
    events: std::sync::Arc<std::sync::Mutex<Vec<ChainEvent>>>,
}

impl TestSink {
    fn new() -> Self {
        Self {
            events: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }
}

impl SinkHandler for TestSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow_dsl_infra=debug,test_dsl=debug")
        .init();

    println!("Testing DSL macro expansion...");

    // Create a temporary journal
    let temp_dir = tempfile::tempdir()?;
    let journal_path = temp_dir.path().to_path_buf();
    let journal = Arc::new(DiskJournal::new(journal_path, "test_dsl").await?);

    let sink = TestSink::new();
    let events = sink.events.clone();

    // Create a simple pipeline: source -> transform -> sink
    let handle = flow! {
        journal: journal,
        middleware: [],
        ("source" => TestSource::new(), [])
        |> ("transform" => TestTransform, [])
        |> ("sink" => sink, [])
    }.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Run the pipeline
    handle.run().await?;

    // Check results
    let collected = events.lock().unwrap();
    println!("Collected {} events", collected.len());
    for event in collected.iter() {
        println!("Event: {:?}", event);
    }

    Ok(())
}