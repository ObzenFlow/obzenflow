//! Demo showing how to expose metrics via HTTP using the new web infrastructure
//! 
//! This follows FLOWIP-057a web infrastructure pattern - simple one-line setup!
//! Run with: cargo run --example web_metrics_demo --features "warp-server"

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::web::start_metrics_server;
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
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,web_metrics_demo=info")
        .init();

    println!("🚀 Web Metrics Demo - FLOWIP-057a Implementation");
    println!("================================================\n");

    // Create flow with metrics
    let flow_handle = flow! {
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
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("▶️  Running flow...\n");
    
    // Run the flow and get metrics exporter
    let metrics_exporter = flow_handle.run_with_metrics().await?;
    
    println!("✅ Flow completed!");
    
    // Start web server with ONE LINE if metrics are available! 🎉
    if let Some(metrics) = metrics_exporter {
        start_metrics_server(metrics, 9090).await?;
        
        println!("\n🌐 Metrics server started!");
        println!("   📊 Metrics: http://localhost:9090/metrics");
        println!("   ❤️  Health: http://localhost:9090/health");
        println!("   ✅ Ready:  http://localhost:9090/ready\n");
        
        println!("📈 You can now view the final metrics via HTTP!");
        println!("   curl http://localhost:9090/metrics");
        
        // Keep the server running
        println!("\n⏸️  Press Ctrl+C to exit...");
        tokio::signal::ctrl_c().await?;
        println!("\n👋 Shutting down...");
    } else {
        println!("\n⚠️  No metrics exporter configured");
    }
    
    Ok(())
}