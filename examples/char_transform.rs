//! Character-transform demo (v2):
//!   • Outputs each sentence on its own line.
//!   • Pipeline = Source → CapStage → DigitWordStage → Sink
//! Run with:  `cargo run --example char_transform`

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::disk_journals;
use obzenflow_core::event::WriterId;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use obzenflow_core::id::StageId;
use obzenflow_core::time::MetricsDuration;
// Monitoring taxonomies are no longer needed with FLOWIP-056-666
// Metrics are automatically collected by MetricsAggregator from the event journal
use serde_json::json;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;

/// Source that emits one `Char` event per character from sample sentences
#[derive(Debug, Clone)]
struct TextCharSource {
    sentences: Vec<String>,
    chars: Vec<char>,
    start_idx: Vec<usize>,
    current_index: usize,
    announced: usize,
    writer_id: WriterId,
}

impl TextCharSource {
    fn new() -> Self {
        let sentences = vec![
            "Hello, FlowState!".to_string(),
            "Rust makes systems programming fun.".to_string(),
            "Numbers 1 2 3 should become words.".to_string(),
            "How about 42 or 564?".to_string(),
        ];

        // flatten sentences with '\n' delimiter
        let mut chars     = Vec::<char>::new();
        let mut start_idx = Vec::<usize>::new();

        for (i, s) in sentences.iter().enumerate() {
            start_idx.push(chars.len());
            chars.extend(s.chars());
            if i + 1 < sentences.len() {          // newline between sentences, not after last
                chars.push('\n');
            }
        }

        Self {
            sentences,
            chars,
            start_idx,
            current_index: 0,
            announced: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TextCharSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.current_index < self.chars.len() {
            let idx = self.current_index;
            self.current_index += 1;
            
            // Announce sentence start (console progress)
            if self.announced < self.start_idx.len() && idx == self.start_idx[self.announced] {
                println!("📝  Processing sentence: {}", self.sentences[self.announced]);
                self.announced += 1;
            }
            
            let ch = self.chars[idx];
            Some(ChainEventFactory::data_event(
                self.writer_id.clone(),
                "Char",
                json!({ "ch": ch.to_string() })
            ))
        } else {
            None
        }
    }

    fn is_complete(&self) -> bool {
        self.current_index >= self.chars.len()
    }
}

/// First Stage – Capitalize letters
#[derive(Debug, Clone)]
struct CapStage;

impl CapStage {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for CapStage {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type() == "Char" {
            let payload = event.payload();
            if let Some(ch) = payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let out = if ch.is_ascii_alphabetic() { ch.to_ascii_uppercase() } else { ch };
                return vec![ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    "Char",
                    json!({ "ch": out.to_string() })
                )];
            }
        }
        vec![]
    }
    
    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Second Stage -- Digit → Word
fn digit_word(d: char) -> &'static str {
    match d {
        '0' => "zero",  '1' => "one",   '2' => "two",  '3' => "three", '4' => "four",
        '5' => "five",  '6' => "six",   '7' => "seven",'8' => "eight", '9' => "nine",
        _   => "",
    }
}

#[derive(Debug, Clone)]
struct DigitWordStage;

impl DigitWordStage {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl TransformHandler for DigitWordStage {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type() == "Char" {
            let payload = event.payload();
            if let Some(ch) = payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let frag = if ch.is_ascii_digit() {
                    digit_word(ch).to_string()
                } else {
                    ch.to_string()
                };
                return vec![ChainEventFactory::derived_data_event(
                    event.writer_id.clone(),
                    &event,
                    "OutFragment",
                    json!({ "frag": frag })
                )];
            }
        }
        vec![]
    }
    
    async fn drain(&mut self) -> obzenflow_core::Result<()> {
        Ok(())
    }
}

/// Sink – collects output fragments into a string buffer
#[derive(Debug, Clone)]
struct TextCollectorSink {
    buf: Arc<std::sync::Mutex<String>>,
}

impl TextCollectorSink {
    fn new() -> Self {
        Self {
            buf: Arc::new(std::sync::Mutex::new(String::new())),
        }
    }
}

#[async_trait]
impl SinkHandler for TextCollectorSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        let mut bytes_processed = 0;
        
        if event.event_type() == "OutFragment" {
            let payload = event.payload();
            if let Some(frag) = payload["frag"].as_str() {
                let mut b = self.buf.lock().unwrap();
                b.push_str(frag);
                bytes_processed = frag.len() as u64;
            }
        }
        
        Ok(DeliveryPayload::success(
            "memory_buffer",
            DeliveryMethod::Custom("InMemoryCollector".to_string()),
            Some(bytes_processed),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter for nice summaries
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");
    
    // Initialize tracing for better error messages
    tracing_subscriber::fmt()
        .with_env_filter("obzenflow=debug,char_transform=debug")
        .with_file(true)
        .with_line_number(true)
        .with_thread_ids(true)
        .init();

    println!("🚀 FlowState RS - Character Transform Demo (newline & 2-stage)");
    println!("============================================================\n");

    // Create sink that collects output in memory
    let sink = TextCollectorSink::new();
    let final_buffer = sink.buf.clone();

    println!("⏳ Initializing pipeline...");

    // Create a journal for the flow
    let journal_path = std::path::PathBuf::from("target/char-transform-logs");
    std::fs::create_dir_all(&journal_path)?;

    // Create the flow using the new flow! macro
    let handle = flow! {
        name: "char_transform",
        journals: disk_journals(journal_path.clone()),
        middleware: [],
        
        stages: {
            src = source!("source" => TextCharSource::new());
            cap = transform!("cap" => CapStage::new());
            digit = transform!("digit" => DigitWordStage::new());
            out = sink!("sink" => sink);
        },
        
        topology: {
            src |> cap;
            cap |> digit;
            digit |> out;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow with DSL: {:?}", e))?;
    
    println!("📌 Pipeline created, starting execution...");
    
    // Run the flow to completion and get the metrics exporter
    let metrics_exporter = handle.run_with_metrics().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;

    println!("\n✅ Pipeline completed!\n");

    let result = final_buffer.lock().unwrap().clone();
    println!("🔍 Final text:\n{}", result);

    // Get the metrics summary after completion
    if let Some(exporter) = metrics_exporter {
        let summary = exporter
            .render_metrics()
            .map_err(|e| anyhow::anyhow!("Failed to render metrics: {}", e))?;
        println!("\n📊 Pipeline Metrics:\n{}", summary);
    } else {
        println!("\nNo metrics exporter configured");
    }

    // Cleanup
    println!("\nJournal written to: target/char-transform-logs/char_transform.log");
    Ok(())
}