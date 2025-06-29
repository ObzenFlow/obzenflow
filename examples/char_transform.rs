//! Character-transform demo (v2):
//!   • Outputs each sentence on its own line.
//!   • Pipeline = Source → CapStage → DigitWordStage → Sink
//! Run with:  `cargo run --example char_transform`

use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::monitoring::taxonomies::{
    golden_signals::GoldenSignals,
    red::RED,
    use_taxonomy::USE,
    saafe::SAAFE,
};
use serde_json::json;
use std::sync::Arc;
use anyhow::Result;
use async_trait::async_trait;

/// Source that emits one `Char` event per character from sample sentences
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
            writer_id: WriterId::new(),
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
            Some(ChainEvent::new(
                EventId::new(),
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
struct CapStage;

impl CapStage {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for CapStage {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Char" {
            if let Some(ch) = event.payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let out = if ch.is_ascii_alphabetic() { ch.to_ascii_uppercase() } else { ch };
                event.payload["ch"] = json!(out.to_string());
                return vec![event];
            }
        }
        vec![]
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

struct DigitWordStage;

impl DigitWordStage {
    fn new() -> Self {
        Self
    }
}

impl TransformHandler for DigitWordStage {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Char" {
            if let Some(ch) = event.payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let frag = if ch.is_ascii_digit() {
                    digit_word(ch).to_string()
                } else {
                    ch.to_string()
                };
                return vec![ChainEvent::new(
                    EventId::new(),
                    event.writer_id.clone(),
                    "OutFragment",
                    json!({ "frag": frag })
                )];
            }
        }
        vec![]
    }
}

/// Sink – collects output fragments into a string buffer
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
    fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
        if event.event_type == "OutFragment" {
            if let Some(frag) = event.payload["frag"].as_str() {
                let mut b = self.buf.lock().unwrap();
                b.push_str(frag);
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
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
    let journal = Arc::new(DiskJournal::new(journal_path, "char_transform").await?);

    // Create the flow using the new flow! macro
    let handle = flow! {
        journal: journal,
        middleware: [GoldenSignals::monitoring()],
        
        stages: {
            src = source!("source" => TextCharSource::new(), [RED::monitoring()]);
            cap = transform!("cap" => CapStage::new(), [USE::monitoring()]);
            digit = transform!("digit" => DigitWordStage::new(), [USE::monitoring()]);
            out = sink!("sink" => sink, [SAAFE::monitoring()]);
        },
        
        topology: {
            src |> cap;
            cap |> digit;
            digit |> out;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow with DSL: {:?}", e))?;
    
    println!("📌 Pipeline created, starting execution...");
    
    // Start the pipeline
    handle.run().await
        .map_err(|e| anyhow::anyhow!("Failed to run pipeline: {:?}", e))?;

    println!("\n✅ Pipeline completed!\n");

    let result = final_buffer.lock().unwrap().clone();
    println!("🔍 Final text:\n{}", result);

    // Cleanup
    println!("\nJournal written to: target/char-transform-logs/char_transform.log");
    Ok(())
}