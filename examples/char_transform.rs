//! Character Transform Demo
//!
//! Pipeline: Source → Transformer → Sink
//!   • Source: Emits individual characters from sample sentences
//!   • Transformer: Stateful handler that capitalizes letters and converts digits to words
//!   • Sink: Prints the final transformed text
//!
//! This demonstrates:
//!   - Character-level processing pipeline
//!   - Clean use of StatefulHandler for accumulation and transformation
//!   - Processing multiple sentences with proper formatting
//!   - Drain-based final output
//!
//! Run with: `cargo run -p obzenflow --example char_transform`

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryPayload, DeliveryMethod},
    WriterId,
    id::StageId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, StatefulHandler,
};
use serde_json::json;

/// Source that emits individual characters from sample sentences
#[derive(Clone, Debug)]
struct TextCharSource {
    sentences: Vec<String>,
    current_sentence: usize,
    current_char: usize,
    writer_id: WriterId,
}

impl TextCharSource {
    fn new() -> Self {
        Self {
            sentences: vec![
                "hello 2024 world!".to_string(),
                "42 is the answer.".to_string(),
                "rust 1 python 0.".to_string(),
            ],
            current_sentence: 0,
            current_char: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TextCharSource {
    fn next(&mut self) -> Option<ChainEvent> {
        // Check if we've processed all sentences
        if self.current_sentence >= self.sentences.len() {
            return None;
        }

        let sentence = &self.sentences[self.current_sentence];
        let chars: Vec<char> = sentence.chars().collect();

        if self.current_char < chars.len() {
            let ch = chars[self.current_char];
            self.current_char += 1;

            // Emit character event
            Some(ChainEventFactory::data_event(
                self.writer_id.clone(),
                "char",
                json!({
                    "value": ch.to_string(),
                    "sentence_idx": self.current_sentence,
                    "char_idx": self.current_char - 1,
                }),
            ))
        } else {
            // Move to next sentence
            self.current_sentence += 1;
            self.current_char = 0;

            // Emit newline between sentences
            if self.current_sentence < self.sentences.len() {
                Some(ChainEventFactory::data_event(
                    self.writer_id.clone(),
                    "char",
                    json!({
                        "value": "\n",
                        "sentence_idx": self.current_sentence - 1,
                        "char_idx": chars.len(),
                    }),
                ))
            } else {
                // Recursively call to handle end or get next character
                self.next()
            }
        }
    }

    fn is_complete(&self) -> bool {
        self.current_sentence >= self.sentences.len()
    }
}

/// State for the text transformer
#[derive(Clone, Debug, Default)]
struct TextTransformerState {
    transformed_text: String,
    sentence_count: usize,
}

/// Stateful handler that transforms characters as they accumulate
#[derive(Debug, Clone)]
struct TextTransformer {
    writer_id: WriterId,
}

impl TextTransformer {
    fn new() -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
        }
    }

    fn transform_char(&self, ch: char) -> String {
        if ch.is_ascii_digit() {
            // Convert digit to word
            match ch {
                '0' => "zero",
                '1' => "one",
                '2' => "two",
                '3' => "three",
                '4' => "four",
                '5' => "five",
                '6' => "six",
                '7' => "seven",
                '8' => "eight",
                '9' => "nine",
                _ => "",
            }.to_string()
        } else if ch.is_ascii_lowercase() {
            // Capitalize letters
            ch.to_uppercase().to_string()
        } else {
            // Keep everything else as-is (including newlines, punctuation, spaces)
            ch.to_string()
        }
    }
}

#[async_trait]
impl StatefulHandler for TextTransformer {
    type State = TextTransformerState;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if event.event_type() == "char" {
            let payload = event.payload();
            if let Some(ch_str) = payload["value"].as_str() {
                if let Some(ch) = ch_str.chars().next() {
                    // Track sentence count for stats
                    if ch == '\n' {
                        state.sentence_count += 1;
                    }

                    // Transform and accumulate the character
                    let transformed = self.transform_char(ch);
                    state.transformed_text.push_str(&transformed);
                }
            }
        }
    }

    fn initial_state(&self) -> Self::State {
        TextTransformerState::default()
    }

    fn create_events(&self, state: &Self::State) -> Vec<ChainEvent> {
        // Count the last sentence if it doesn't end with newline
        let final_sentence_count = if state.transformed_text.ends_with('\n') {
            state.sentence_count
        } else {
            state.sentence_count + 1
        };

        // Create the transformed text event
        vec![ChainEventFactory::data_event(
            self.writer_id.clone(),
            "transformed_text",
            json!({
                "text": state.transformed_text.clone(),
                "sentences": final_sentence_count,
                "characters": state.transformed_text.len(),
            }),
        )]
    }

    // Note: We don't override should_emit, emit, or drain
    // The defaults handle everything perfectly:
    // - should_emit returns false (only emit on EOF)
    // - emit calls create_events
    // - drain calls create_events
}

/// Sink that prints the final transformed text
#[derive(Clone, Debug)]
struct OutputSink;

impl OutputSink {
    fn new() -> Self {
        Self
    }
}

#[async_trait]
impl SinkHandler for OutputSink {
    async fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<DeliveryPayload> {
        if event.event_type() == "transformed_text" {
            let payload = event.payload();
            if let Some(text) = payload["text"].as_str() {
                println!("\n🔍 Transformed output:\n");
                println!("{}", text);

                if let (Some(sentences), Some(chars)) =
                    (payload["sentences"].as_u64(), payload["characters"].as_u64()) {
                    println!("\n📊 Stats: {} sentences, {} characters total", sentences, chars);
                }
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

    println!("🚀 Character Transform Demo");
    println!("===========================\n");
    println!("Demonstrating:");
    println!("  • Character-level processing with StatefulHandler");
    println!("  • Capitalize letters + convert digits to words");
    println!("  • Clean accumulation and transformation pattern\n");
    println!("Input sentences:");
    println!("  1. \"hello 2024 world!\"");
    println!("  2. \"42 is the answer.\"");
    println!("  3. \"rust 1 python 0.\"\n");

    // Use FlowApplication to handle everything
    FlowApplication::run(async {
        flow! {
            name: "char_transform",
            journals: disk_journals(std::path::PathBuf::from("target/char-transform-logs")),
            middleware: [],

            stages: {
                src = source!("source" => TextCharSource::new());
                transformer = stateful!("transformer" => TextTransformer::new());
                output = sink!("output" => OutputSink::new());
            },

            topology: {
                src |> transformer;
                transformer |> output;
            }
        }
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
    })
    .await
    .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Pipeline completed!");
    println!("\n💡 This demo shows clean use of StatefulHandler:");
    println!("   - Only implement accumulate(), initial_state(), and create_events()");
    println!("   - Framework handles should_emit, emit, and drain with sensible defaults");
    println!("   - Could easily use .with_emission(EveryN::new(10)) to emit every 10 chars!");

    Ok(())
}