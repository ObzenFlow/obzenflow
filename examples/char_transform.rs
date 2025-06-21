//! Character-transform demo (v2):
//!   • Outputs each sentence on its own line.
//!   • Pipeline = Source → CapStage → DigitWordStage → Sink
//! Run with:  `cargo run --example char_transform`

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

// Source -- Emits one `Char` event per character from sample sentences.
struct TextCharSource {
    sentences: Vec<String>,
    chars: Vec<char>,
    start_idx: Vec<usize>,
    announced: AtomicU64,
    emitted:   AtomicU64,
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
            announced: AtomicU64::new(0),
            emitted:   AtomicU64::new(0),
        }
    }
}

impl EventHandler for TextCharSource {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let idx = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
        if idx >= self.chars.len() { 
            return vec![]; // No more characters to emit
        }

        // Announce sentence start (console progress)
        let next = self.announced.load(Ordering::Relaxed) as usize;
        if next < self.start_idx.len() && idx == self.start_idx[next] {
            println!("📝  Processing sentence: {}", self.sentences[next]);
            self.announced.fetch_add(1, Ordering::Relaxed);
        }

        let ch = self.chars[idx];
        vec![ChainEvent::new("Char", json!({ "ch": ch.to_string() }))]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

// First Stage – Capitalize letters
struct CapStage;

impl CapStage { fn new() -> Self { Self } }

impl EventHandler for CapStage {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Char" {
            if let Some(ch) = event.payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let out = if ch.is_ascii_alphabetic() { ch.to_ascii_uppercase() } else { ch };
                return vec![ChainEvent::new("Char", json!({ "ch": out.to_string() }))];
            }
        }
        vec![]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

// Second Stage -- Digit → Word
fn digit_word(d: char) -> &'static str {
    match d {
        '0' => "zero",  '1' => "one",   '2' => "two",  '3' => "three", '4' => "four",
        '5' => "five",  '6' => "six",   '7' => "seven",'8' => "eight", '9' => "nine",
        _   => "",
    }
}

struct DigitWordStage;

impl DigitWordStage { fn new() -> Self { Self } }

impl EventHandler for DigitWordStage {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Char" {
            if let Some(ch) = event.payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let frag = if ch.is_ascii_digit() {
                    digit_word(ch).to_string()
                } else {
                    ch.to_string()
                };
                return vec![ChainEvent::new("OutFragment", json!({ "frag": frag }))];
            }
        }
        vec![]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

// Sink – collects output fragments into a string buffer
struct TextCollectorSink {
    buf: Arc<Mutex<String>>,
}

impl TextCollectorSink {
    fn new() -> (Self, Arc<Mutex<String>>) {
        let buf = Arc::new(Mutex::new(String::new()));
        (
            Self { buf: buf.clone() },
            buf,
        )
    }
}

impl EventHandler for TextCollectorSink {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "OutFragment" {
            if let Some(frag) = event.payload["frag"].as_str() {
                let mut b = self.buf.lock().unwrap();
                b.push_str(frag);
            }
        }
        vec![] // Sinks consume events
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

// Example goes brrr!
#[tokio::main]
async fn main() -> Result<()> {
    // Enable debug logging to see what's happening
    tracing_subscriber::fmt()
        .with_env_filter("flowstate_rs=debug")
        .init();

    println!("🚀 FlowState RS - Character Transform Demo (newline & 2-stage)");
    println!("============================================================\n");

    // Create sink that collects output in memory
    let (sink, final_buffer) = TextCollectorSink::new();

    println!("⏳ Initializing pipeline...");

    let handle = flow! {
        name: "char_transform",
        flow_taxonomy: GoldenSignals,
        ("source"   => TextCharSource::new(), [RED::monitoring()])
        |> ("cap"   => CapStage::new(),       [USE::monitoring()])
        |> ("digit" => DigitWordStage::new(), [USE::monitoring()])
        |> ("sink"  => sink,                 [SAAFE::monitoring()])
    }?;
    
    println!("📌 Flow handle created, pipeline should be running...");
    
    // TODO: Remove this sleep once FLOWIP-026 is implemented
    // Currently needed because transient sources can't signal completion
    println!("⏳ Waiting for pipeline to process all characters...");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    
    println!("📌 Now calling shutdown...");

    // Gracefully shut down the flow (waits for all events to drain)
    handle.shutdown().await?;

    println!("\n✅ Pipeline completed!\n");

    let result = final_buffer.lock().unwrap().clone();
    println!("🔍 Final text:\n{}", result);

    Ok(()) // The grooviest!
}
