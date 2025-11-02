//! Character-transform demo
//!   • Outputs each sentence on its own line.
//!   • Pipeline = Source → CapStage → DigitWordStage → Sink
//! Run with:  `cargo run -p obzenflow --example char_transform`

use obzenflow_dsl_infra::{flow, source, transform, stateful, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, StatefulHandler, SinkHandler
};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_core::event::WriterId;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod};
use obzenflow_core::id::StageId;
use serde_json::json;
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

/// State for text accumulation
#[derive(Clone, Debug, Default)]
struct TextCollectorState {
    buffer: String,
}

/// Stateful stage that accumulates text fragments and emits final result
#[derive(Debug, Clone)]
struct TextCollector {
    writer_id: WriterId,
}

impl TextCollector {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl StatefulHandler for TextCollector {
    type State = TextCollectorState;

    fn process(&self, state: &Self::State, event: ChainEvent) -> (Self::State, Vec<ChainEvent>) {
        let mut new_state = state.clone();

        if event.event_type() == "OutFragment" {
            let payload = event.payload();
            if let Some(frag) = payload["frag"].as_str() {
                new_state.buffer.push_str(frag);
            }
        }

        // Accumulate only - emit on drain
        (new_state, vec![])
    }

    fn initial_state(&self) -> Self::State {
        TextCollectorState::default()
    }

    async fn drain(
        &mut self,
        state: &Self::State,
    ) -> Result<Vec<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
        // Emit final collected text
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "FinalText",
            json!({
                "text": state.buffer,
            }),
        )])
    }
}

/// Sink that prints the final collected text
#[derive(Clone, Debug)]
struct FinalTextSink;

impl FinalTextSink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for FinalTextSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        if event.event_type() == "FinalText" {
            let payload = event.payload();
            if let Some(text) = payload["text"].as_str() {
                println!("\n🔍 Transformed output:\n");
                println!("{}", text);
                println!();
            }
        }

        Ok(DeliveryPayload::success(
            "stdout",
            DeliveryMethod::Custom("Print".to_string()),
            Some(1),
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Set environment to use console exporter for nice summaries
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    println!("🚀 FlowState RS - Character Transform Demo");
    println!("===========================================\n");
    println!("Demonstrating:");
    println!("  • Multi-stage transformation pipeline");
    println!("  • Character-level processing");
    println!("  • StatefulHandler for text accumulation");
    println!("  • Capitalize letters + convert digits to words\n");

    // Use FlowApplication to handle everything
    FlowApplication::run(async {
        flow! {
            name: "char_transform",
            journals: disk_journals(std::path::PathBuf::from("target/char-transform-logs")),
            middleware: [],

            stages: {
                src = source!("source" => TextCharSource::new());
                cap = transform!("cap" => CapStage::new());
                digit = transform!("digit" => DigitWordStage::new());
                collector = stateful!("collector" => TextCollector::new());
                printer = sink!("printer" => FinalTextSink::new());
            },

            topology: {
                src |> cap;
                cap |> digit;
                digit |> collector;
                collector |> printer;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Pipeline completed!");
    println!("📝 Check the journal for complete event history: target/char-transform-logs/");

    Ok(())
}