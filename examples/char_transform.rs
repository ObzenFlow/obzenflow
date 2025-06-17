//! Character-transform demo (v2):
//!   • Outputs each sentence on its own line.
//!   • Pipeline = Source → CapStage → DigitWordStage → Sink
//! Run with:  `cargo run --example char_transform`

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

// ────────────────────────────────────────────────────────────────
// SOURCE
// Emits one `Char` event per character from sample sentences.
struct TextCharSource {
    sentences: Vec<String>,
    chars: Vec<char>,
    start_idx: Vec<usize>,
    announced: AtomicU64,
    emitted:   AtomicU64,
    metrics: <RED as Taxonomy>::Metrics,
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
            metrics:   RED::create_metrics("TextCharSource"),
        }
    }
}

impl Step for TextCharSource {
    type Taxonomy = RED;

    fn taxonomy(&self) -> &Self::Taxonomy { &RED }
    fn metrics (&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Source }

    fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        let idx = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
        if idx >= self.chars.len() { return vec![]; }

        // Announce sentence start (console progress)
        let next = self.announced.load(Ordering::Relaxed) as usize;
        if next < self.start_idx.len() && idx == self.start_idx[next] {
            println!("📝  Processing sentence: {}", self.sentences[next]);
            self.announced.fetch_add(1, Ordering::Relaxed);
        }

        let ch = self.chars[idx];
        vec![ChainEvent::new("Char", json!({ "ch": ch.to_string() }))]
    }
}

// ────────────────────────────────────────────────────────────────
// STAGE 1 – Capitalize letters
struct CapStage { metrics: <USE as Taxonomy>::Metrics }

impl CapStage { fn new() -> Self { Self { metrics: USE::create_metrics("CapStage") } } }

impl Step for CapStage {
    type Taxonomy = USE;
    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics (&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Stage }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "Char" {
            if let Some(ch) = event.payload["ch"].as_str().and_then(|s| s.chars().next()) {
                let out = if ch.is_ascii_alphabetic() { ch.to_ascii_uppercase() } else { ch };
                return vec![ChainEvent::new("Char", json!({ "ch": out.to_string() }))];
            }
        }
        vec![]
    }
}

// ────────────────────────────────────────────────────────────────
// STAGE 2 – Digit → word
fn digit_word(d: char) -> &'static str {
    match d {
        '0' => "zero",  '1' => "one",   '2' => "two",  '3' => "three", '4' => "four",
        '5' => "five",  '6' => "six",   '7' => "seven",'8' => "eight", '9' => "nine",
        _   => "",
    }
}

struct DigitWordStage { metrics: <USE as Taxonomy>::Metrics }

impl DigitWordStage { fn new() -> Self { Self { metrics: USE::create_metrics("DigitWordStage") } } }

impl Step for DigitWordStage {
    type Taxonomy = USE;
    fn taxonomy(&self) -> &Self::Taxonomy { &USE }
    fn metrics (&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Stage }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
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
}

// ────────────────────────────────────────────────────────────────
// SINK – collects output fragments into a string buffer
struct TextCollectorSink {
    buf: Arc<Mutex<String>>,
    metrics: <SAAFE as Taxonomy>::Metrics,
}

impl TextCollectorSink {
    fn new() -> (Self, Arc<Mutex<String>>) {
        let buf = Arc::new(Mutex::new(String::new()));
        (
            Self { buf: buf.clone(), metrics: SAAFE::create_metrics("TextCollectorSink") },
            buf,
        )
    }
}

impl Step for TextCollectorSink {
    type Taxonomy = SAAFE;
    fn taxonomy(&self) -> &Self::Taxonomy { &SAAFE }
    fn metrics (&self) -> &<Self::Taxonomy as Taxonomy>::Metrics { &self.metrics }
    fn step_type(&self) -> StepType { StepType::Sink }

    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if event.event_type == "OutFragment" {
            if let Some(frag) = event.payload["frag"].as_str() {
                let mut b = self.buf.lock().unwrap();
                b.push_str(frag);
            }
        }
        vec![]
    }
}

// ────────────────────────────────────────────────────────────────
// MAIN
#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 FlowState RS - Character Transform Demo (newline & 2-stage)");
    println!("============================================================\n");

    // Create sink that collects output in memory
    let (sink, final_buffer) = TextCollectorSink::new();

    println!("⏳ Initializing pipeline...");

    let handle = flow! {
        name: "char_transform",
        flow_taxonomy: GoldenSignals,
        ("source"   => TextCharSource::new(), RED)
        |> ("cap"   => CapStage::new(),       USE)
        |> ("digit" => DigitWordStage::new(), USE)
        |> ("sink"  => sink,                 SAAFE)
    }?;

    // let the source wind down
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    handle.shutdown().await?;

    println!("\n✅ Pipeline completed!\n");

    let result = final_buffer.lock().unwrap().clone();
    println!("🔍 Final text:\n{}", result);

    Ok(())
}
