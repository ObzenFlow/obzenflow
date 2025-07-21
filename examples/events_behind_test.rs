//! Test to verify "events behind" metric propagation with rate-limited sink
//! This creates backpressure by rate-limiting the sink while source/transform run full speed
//! Run with: cargo run --example events_behind_test

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_core::{
    event::{chain_event::ChainEvent, event_id::EventId},
    journal::writer_id::WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Fast source that generates events quickly
#[derive(Clone, Debug)]
struct FastSource {
    count: usize,
    writer_id: WriterId,
    total_events: usize,
}

impl FastSource {
    fn new(total_events: usize) -> Self {
        Self {
            count: 0,
            writer_id: WriterId::new(),
            total_events,
        }
    }
}

impl FiniteSourceHandler for FastSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.count >= self.total_events {
            return None;
        }

        self.count += 1;

        Some(ChainEvent::new(
            EventId::new(),
            self.writer_id.clone(),
            "test.event",
            json!({
                "sequence": self.count,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            }),
        ))
    }

    fn is_complete(&self) -> bool {
        self.count >= self.total_events
    }
}

/// Pass-through transform that runs at full speed
#[derive(Clone, Debug)]
struct PassThroughTransform;

#[async_trait]
impl TransformHandler for PassThroughTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Just pass through - no delay
        vec![event]
    }

    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Simple sink that counts events
#[derive(Clone, Debug)]
struct CountingSink {
    count: Arc<Mutex<usize>>,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            count: Arc::new(Mutex::new(0)),
        }
    }
    
    async fn get_count(&self) -> usize {
        *self.count.lock().await
    }
}

impl SinkHandler for CountingSink {
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        let count = self.count.clone();
        tokio::spawn(async move {
            let mut c = count.lock().await;
            *c += 1;
            tracing::info!(
                "Sink consumed event {} (total: {})", 
                event.payload["sequence"], 
                *c
            );
        });
        Ok(())
    }
}


#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,events_behind_test=info")
        .with_target(true)
        .init();

    println!("🔍 Events Behind Test - Rate-Limited Sink");
    println!("=========================================\n");

    // Create journal path
    let journal_path = std::path::PathBuf::from("target/events_behind_test_journal");
    let journal_path_display = journal_path.display().to_string();
    println!("📁 Using DiskJournal at: {}", journal_path_display);

    // Clean up previous run
    if std::path::Path::new("target/events_behind_test_journal").exists() {
        std::fs::remove_dir_all("target/events_behind_test_journal")?;
    }

    println!("📊 Creating flow with rate-limited sink...\n");

    let sink_handler = CountingSink::new();
    let sink_clone = sink_handler.clone();

    // Create flow
    let flow_handle = flow! {
        name: "events_behind_test",
        journals: disk_journals(journal_path.clone()),
        middleware: [],  // No global middleware

        stages: {
            // Fast source - generates 100 events quickly
            src = source!("fast_source" => FastSource::new(100));

            // Transform runs at full speed
            trans = transform!("fast_transform" => PassThroughTransform);

            // Rate-limited sink - only 2 events per second
            snk = sink!("rate_limited_sink" => sink_handler, [
                rate_limit(2.0)  // 2 events per second
            ]);
        },

        topology: {
            src |> trans;
            trans |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {}", e))?;

    println!("▶️  Running flow...");
    println!("📈 Source will generate 100 events quickly");
    println!("🐌 Sink is rate-limited to 2 events/second");
    println!("📊 This should create growing 'events_behind' in sink\n");

    // Run the flow
    let start_time = std::time::Instant::now();
    flow_handle.run().await?;
    let elapsed = start_time.elapsed();

    println!("\n✅ Flow completed in {:.2}s", elapsed.as_secs_f64());
    
    // Get final sink count
    let final_count = sink_clone.get_count().await;
    println!("📊 Sink processed {} events total", final_count);
    println!("📈 Expected time at 2 events/sec: ~50 seconds");

    // Print instructions for analyzing journal
    println!("\n📝 To analyze 'events_behind' progression:");
    println!("   1. Read events from journal at: {}", journal_path_display);
    println!("   2. Look at event.runtime_context.events_behind field");
    println!("   3. Sink events should show increasing events_behind values");
    println!("   4. Transform events should show minimal events_behind");
    println!("   5. Source events should show 0 events_behind");

    Ok(())
}