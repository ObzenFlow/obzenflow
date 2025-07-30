//! Demo showing natural fan-in through multiple subscriptions
//! 
//! This example demonstrates how a single stage can subscribe to
//! multiple upstream journals and round-robin between them.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use serde_json::json;
use std::sync::{Arc, Mutex};

/// Source that generates events with a specific type
#[derive(Clone, Debug)]
struct TypedSource {
    event_type: String,
    count: usize,
    max_events: usize,
    writer_id: WriterId,
}

impl TypedSource {
    fn new(event_type: &str, max_events: usize) -> Self {
        Self {
            event_type: event_type.to_string(),
            count: 0,
            max_events,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TypedSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= self.max_events {
            return None;
        }

        self.count += 1;

        Some(ChainEventFactory::data_event(
            self.writer_id.clone(),
            &format!("{}.event", self.event_type),
            json!({
                "source": self.event_type.clone(),
                "id": self.count,
                "event_num": self.count,
                "total": self.max_events,
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= self.max_events
    }
}

/// Aggregator that merges events from multiple sources
#[derive(Clone, Debug)]
struct EventAggregator {
    events_by_source: Arc<Mutex<std::collections::HashMap<String, usize>>>,
}

impl EventAggregator {
    fn new() -> Self {
        Self {
            events_by_source: Arc::new(Mutex::new(std::collections::HashMap::new())),
        }
    }
    
    fn get_stats(&self) -> Vec<(String, usize)> {
        let map = self.events_by_source.lock().unwrap();
        let mut stats: Vec<_> = map.iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        stats.sort_by(|a, b| a.0.cmp(&b.0));
        stats
    }
}

#[async_trait]
impl TransformHandler for EventAggregator {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let payload = event.payload();
        let source = payload["source"].as_str().unwrap_or("unknown");
        
        // Track which source this came from
        {
            let mut map = self.events_by_source.lock().unwrap();
            *map.entry(source.to_string()).or_insert(0) += 1;
        }
        
        // Enrich with aggregation info
        let stats = self.get_stats();
        let total: usize = stats.iter().map(|(_, count)| count).sum();
        
        let mut enriched = payload.clone();
        enriched["total_events"] = json!(total);
        enriched["source_stats"] = json!(stats);
        
        vec![ChainEventFactory::derived_data_event(
            event.writer_id.clone(),
            &event,
            "aggregated.event",
            enriched,
        )]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Sink that prints aggregated stats
#[derive(Clone, Debug)]
struct StatsSink;

#[async_trait]
impl SinkHandler for StatsSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        let payload = event.payload();
        println!("📊 Event #{} from '{}' - Total aggregated: {} - Stats: {}", 
            payload["id"],
            payload["source"],
            payload["total_events"],
            payload["source_stats"]);
        
        Ok(DeliveryPayload::success(
            "stats_sink",
            DeliveryMethod::Custom("Console".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter for nice summaries
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,fan_in_demo=debug")
        .with_target(true)
        .with_thread_ids(true)
        .init();

    println!("🌊 Fan-In Demo - Multiple Upstream Subscriptions");
    println!("===============================================\n");

    let journal_path = std::path::PathBuf::from("target/fan_in_demo_journal");
    println!("📁 Using DiskJournal at: {}", journal_path.display());

    println!("\n📊 Creating flow with natural fan-in...\n");

    // Create flow demonstrating fan-in
    let flow_handle = flow! {
        name: "fan_in_demo",
        journals: disk_journals(journal_path.clone()),
        middleware: [],  // No global middleware

        stages: {
            // Multiple sources with different characteristics
            kafka = source!("kafka_source" => TypedSource::new("kafka", 5));
            http = source!("http_source" => TypedSource::new("http", 3));
            file = source!("file_source" => TypedSource::new("file", 7));
            
            // Single aggregator that subscribes to all sources
            aggregator = transform!("aggregator" => EventAggregator::new());
            
            // Sink for results
            snk = sink!("stats_sink" => StatsSink);
        },

        topology: {
            // Fan-in: aggregator subscribes to all three journals!
            kafka |> aggregator;    // aggregator.readers[0]
            http |> aggregator;     // aggregator.readers[1]  
            file |> aggregator;     // aggregator.readers[2]
            
            aggregator |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("▶️  Running flow...\n");
    println!("📨 Kafka source: 5 events @ 50ms each");
    println!("🌐 HTTP source: 3 events @ 100ms each");
    println!("📁 File source: 7 events @ 20ms each\n");

    // Run the flow to completion and get the metrics exporter
    let metrics_exporter = flow_handle.run_with_metrics().await?;

    println!("\n✅ Flow completed!");

    // Get the metrics summary after completion
    if let Some(exporter) = metrics_exporter {
        let summary = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        println!("{}", summary);
    } else {
        println!("No metrics exporter configured");
    }
    
    println!("\n💡 Key Insights:");
    println!("  - UpstreamSubscription handles multiple readers");
    println!("  - Round-robin ensures fairness between sources");
    println!("  - Each reader maintains independent position");
    println!("  - Sources can produce at different rates");
    println!("  - No special 'merge' primitive needed!");
    
    println!("\n🔍 How it works:");
    println!("  1. Aggregator creates Vec<(StageId, JournalReader)>");
    println!("  2. recv() round-robins through readers");
    println!("  3. Each reader tracks its own position");
    println!("  4. Natural flow control per source");

    Ok(())
}