//! Character Transform Demo - Using FLOWIP-080h, FLOWIP-080j & FLOWIP-082a
//!
//! Pipeline: Source → Transformer → Sink
//!   • Source: Emits individual characters from sample sentences
//!   • Transformer: Uses ReduceTyped for type-safe character accumulation
//!   • Sink: Prints the final transformed text
//!
//! This demonstrates:
//!   - Character-level processing pipeline
//!   - FLOWIP-080h: MapTyped for type-safe character transformations
//!   - FLOWIP-080j: ReduceTyped for type-safe accumulation
//!   - FLOWIP-082a: TypedPayload for strongly-typed events
//!   - Zero ChainEvent manipulation in business logic
//!   - Processing multiple sentences with proper formatting
//!
//! Run with: `cargo run -p obzenflow --example char_transform`

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
// FLOWIP-080h: Typed transform helpers
use obzenflow_runtime_services::stages::transform::MapTyped;
// FLOWIP-080j: Typed stateful accumulators
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::ReduceTyped;
use serde::{Deserialize, Serialize};

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
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError> {
        // Check if we've processed all sentences
        if self.current_sentence >= self.sentences.len() {
            return Ok(None);
        }

        let sentence = &self.sentences[self.current_sentence];
        let chars: Vec<char> = sentence.chars().collect();

        if self.current_char < chars.len() {
            let ch = chars[self.current_char];
            self.current_char += 1;

            // ✨ FLOWIP-082a: Emit typed character event
            let char_event = CharEvent {
                value: ch.to_string(),
                sentence_idx: self.current_sentence,
                char_idx: self.current_char - 1,
            };

            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                CharEvent::EVENT_TYPE,
                serde_json::to_value(&char_event).unwrap(),
            )]))
        } else {
            // Move to next sentence
            self.current_sentence += 1;
            self.current_char = 0;

            // Emit newline between sentences
            if self.current_sentence < self.sentences.len() {
                // ✨ FLOWIP-082a: Emit typed newline event
                let newline_event = CharEvent {
                    value: "\n".to_string(),
                    sentence_idx: self.current_sentence - 1,
                    char_idx: chars.len(),
                };

                Ok(Some(vec![ChainEventFactory::data_event(
                    self.writer_id.clone(),
                    CharEvent::EVENT_TYPE,
                    serde_json::to_value(&newline_event).unwrap(),
                )]))
            } else {
                // Recursively call to handle end or get next character
                self.next()
            }
        }
    }
}

// FLOWIP-080h & FLOWIP-082a: Domain types for type-safe transformations
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CharEvent {
    value: String,
    sentence_idx: usize,
    char_idx: usize,
}

impl TypedPayload for CharEvent {
    const EVENT_TYPE: &'static str = "char";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransformedChar {
    value: String,
    is_newline: bool,
}

impl TypedPayload for TransformedChar {
    const EVENT_TYPE: &'static str = "transformed_char";
    const SCHEMA_VERSION: u32 = 1;
}

// FLOWIP-080j: Accumulated state for the reducer
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TextAccumulator {
    transformed_text: String,
    sentence_count: usize,
    total_chars: usize,
    last_was_punctuation: bool, // Track consecutive punctuation to avoid double-counting
}

impl Default for TextAccumulator {
    fn default() -> Self {
        Self {
            transformed_text: String::new(),
            sentence_count: 0,
            total_chars: 0,
            last_was_punctuation: false,
        }
    }
}

impl TypedPayload for TextAccumulator {
    const EVENT_TYPE: &'static str = "text_accumulator";
    const SCHEMA_VERSION: u32 = 1;
}

// Pure function: Transform a character (no ChainEvent!)
fn transform_char(ch: char) -> String {
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
        }
        .to_string()
    } else if ch.is_ascii_lowercase() {
        // Capitalize letters
        ch.to_uppercase().to_string()
    } else {
        // Keep everything else as-is (including newlines, punctuation, spaces)
        ch.to_string()
    }
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
        // ✨ FLOWIP-082a: Check event type using constant (from ReduceTyped)
        if event.event_type() == TextAccumulator::EVENT_TYPE {
            let payload = event.payload();
            if let Some(result) = payload["result"].as_object() {
                if let Some(text) = result["transformed_text"].as_str() {
                    println!("\n🔍 Transformed output:\n");
                    println!("{}", text);

                    if let (Some(sentences), Some(chars)) = (
                        result["sentence_count"].as_u64(),
                        result["total_chars"].as_u64(),
                    ) {
                        println!(
                            "\n📊 Stats: {} sentences, {} characters total",
                            sentences, chars
                        );
                    }
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
    FlowApplication::builder()
        .with_console_subscriber()
        .with_log_level(LogLevel::Info)
        .run_async(async {
            flow! {
                name: "char_transform",
                journals: disk_journals(std::path::PathBuf::from("target/char-transform-logs")),
                middleware: [],

                stages: {
                    src = source!("source" => TextCharSource::new());

                    // FLOWIP-080h: MapTyped for type-safe character transformation
                    mapper = transform!("char_mapper" =>
                        MapTyped::new(|char_event: CharEvent| {
                            let ch = char_event.value.chars().next().unwrap_or(' ');
                            TransformedChar {
                                value: transform_char(ch),
                                is_newline: ch == '\n',
                            }
                        })
                    );

                    // FLOWIP-080j: ReduceTyped for type-safe accumulation
                    reducer = stateful!("text_accumulator" =>
                        ReduceTyped::new(
                            TextAccumulator {
                                transformed_text: String::new(),
                                total_chars: 0,
                                sentence_count: 0,
                                last_was_punctuation: false,  // Track consecutive punctuation
                            },
                            |acc: &mut TextAccumulator, transformed: &TransformedChar| {  // CORRECTED: state first
                                acc.transformed_text.push_str(&transformed.value);
                                acc.total_chars += transformed.value.len();

                                // Count sentences: only increment on first punctuation mark after content
                                let is_sentence_end = transformed.value == "." || transformed.value == "!" || transformed.value == "?";
                                let is_content = transformed.value.chars().any(|c| c.is_alphanumeric());

                                if is_sentence_end && !acc.last_was_punctuation {
                                    acc.sentence_count += 1;
                                }

                                // Only reset punctuation flag when we see actual content (not whitespace)
                                if is_content {
                                    acc.last_was_punctuation = false;
                                } else if is_sentence_end {
                                    acc.last_was_punctuation = true;
                                }
                            }
                        ).emit_on_eof()  // Only emit when pipeline completes
                    );

                    output = sink!("output" => OutputSink::new());
                },

                topology: {
                    src |> mapper;
                    mapper |> reducer;
                    reducer |> output;
                }
            }
            .await
            .map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))
        })
        .await
        .map_err(|e| anyhow::anyhow!("Application failed: {:?}", e))?;

    println!("\n✅ Pipeline completed!");
    println!("\n💡 Key Improvements (FLOWIP-080h, 080j & 082a):");
    println!("   FLOWIP-082a TypedPayload:");
    println!("   • CharEvent::EVENT_TYPE instead of \"char\"");
    println!("   • SCHEMA_VERSION for evolution tracking");
    println!("   • Strongly-typed event structs");
    println!("\n   FLOWIP-080h MapTyped:");
    println!("   • Type-safe: CharEvent → TransformedChar");
    println!("   • No ChainEvent manipulation");
    println!("   • Pure function: transform_char(ch)");
    println!("\n   FLOWIP-080j ReduceTyped:");
    println!("   • Type-safe: TransformedChar + TextAccumulator");
    println!("   • Declarative: .emit_on_eof()");
    println!("   • Zero boilerplate compared to custom StatefulHandler!");

    Ok(())
}
