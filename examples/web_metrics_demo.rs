//! Demo showing how to expose metrics and topology via HTTP using FlowApplication
//! 
//! This follows FLOWIP-057e user-centric design with Spring Boot-style framework.
//! 
//! Run without server:
//!   cargo run --example web_metrics_demo
//! 
//! Run WITH visualization server:
//!   cargo run --example web_metrics_demo --features obzenflow_infra/warp-server -- --server
//!   
//! Run with custom port:
//!   cargo run --example web_metrics_demo --features obzenflow_infra/warp-server -- --server --server-port 8080

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::application::FlowApplication;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;

/// Simple source that generates events
#[derive(Clone, Debug)]
struct TestSource {
    count: usize,
    writer_id: WriterId,
    total_events: usize,
}

impl TestSource {
    fn new(total_events: usize) -> Self {
        Self {
            count: 0,
            writer_id: WriterId::from(StageId::new()),
            total_events,
        }
    }
}

impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= self.total_events {
            return None;
        }

        self.count += 1;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            "test.event",
            json!({
                "id": self.count,
                "timestamp": "2024-01-01T00:00:00Z",
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= self.total_events
    }
}

/// Simple transform that adds processing time
#[derive(Clone, Debug)]
struct TestTransform;

#[async_trait]
impl TransformHandler for TestTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Simulate some processing
        std::thread::sleep(std::time::Duration::from_millis(10));
        
        let mut payload = event.payload();
        payload["processed"] = json!(true);
        
        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            event.event_type(),
            payload,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Simple sink
#[derive(Clone, Debug)]
struct TestSink {
    count: usize,
}

impl TestSink {
    fn new() -> Self {
        Self { count: 0 }
    }
}

#[async_trait]
impl SinkHandler for TestSink {
    async fn consume(&mut self, _event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        self.count += 1;
        
        Ok(DeliveryPayload::success(
            "test_sink",
            DeliveryMethod::Custom("InMemory".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // The FlowApplication framework handles EVERYTHING:
    // - CLI parsing (--server, --server-port)
    // - Logging initialization
    // - Server lifecycle management
    // - Graceful shutdown
    
    // Just call FlowApplication::run() with your flow - that's it!
    FlowApplication::run(async {
        flow! {
            name: "web_metrics_demo",
            journals: disk_journals(std::path::PathBuf::from("target/web_metrics_demo_journal")),
            middleware: [],
            
            stages: {
                src = source!("event_source" => TestSource::new(100));
                trans = transform!("processor" => TestTransform);
                snk = sink!("event_sink" => TestSink::new());
            },
            
            topology: {
                src |> trans;
                trans |> snk;
            }
        }
        .await
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application error: {}", e))?;
    
    Ok(())
}