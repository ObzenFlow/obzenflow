//! Character Transform Demo - Using FLOWIP-080h, FLOWIP-080j, FLOWIP-081, FLOWIP-081c & FLOWIP-082a
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
use obzenflow_core::TypedPayload;
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::source::FiniteSourceTyped;
// FLOWIP-080h: Typed transform helpers
use obzenflow_runtime_services::stages::transform::MapTyped;
// FLOWIP-080j: Typed stateful accumulators
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::ReduceTyped;
use serde::{Deserialize, Serialize};

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

fn build_char_events(sentences: &[&str]) -> Vec<CharEvent> {
    let mut char_events = Vec::new();
    for (sentence_idx, sentence) in sentences.iter().enumerate() {
        let mut char_idx = 0usize;
        for ch in sentence.chars() {
            char_events.push(CharEvent {
                value: ch.to_string(),
                sentence_idx,
                char_idx,
            });
            char_idx += 1;
        }

        if sentence_idx + 1 < sentences.len() {
            char_events.push(CharEvent {
                value: "\n".to_string(),
                sentence_idx,
                char_idx,
            });
        }
    }
    char_events
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

    let sentences = ["hello 2024 world!", "42 is the answer.", "rust 1 python 0."];
    let char_events = build_char_events(&sentences);

    FlowApplication::builder()
        .with_console_subscriber()
        .with_log_level(LogLevel::Info)
        .run_async(flow! {
            name: "char_transform",
            journals: disk_journals(std::path::PathBuf::from("target/char-transform-logs")),
            middleware: [],

            stages: {
                // FLOWIP-081: Typed finite sources (no WriterId/ChainEvent boilerplate)
                src = source!("source" => FiniteSourceTyped::new(char_events));

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
                            last_was_punctuation: false,
                        },
                        |acc: &mut TextAccumulator, transformed: &TransformedChar| {
                            acc.transformed_text.push_str(&transformed.value);
                            acc.total_chars += transformed.value.len();

                            let is_sentence_end = transformed.value == "."
                                || transformed.value == "!"
                                || transformed.value == "?";
                            let is_content = transformed.value.chars().any(|c| c.is_alphanumeric());

                            if is_sentence_end && !acc.last_was_punctuation {
                                acc.sentence_count += 1;
                            }

                            if is_content {
                                acc.last_was_punctuation = false;
                            } else if is_sentence_end {
                                acc.last_was_punctuation = true;
                            }
                        }
                    ).emit_on_eof()
                );

                output = sink!("output" => |final_state: TextAccumulator| {
                    println!("\n🔍 Transformed output:\n");
                    println!("{}", final_state.transformed_text);

                    println!(
                        "\n📊 Stats: {} sentences, {} characters total",
                        final_state.sentence_count,
                        final_state.total_chars
                    );
                });
            },

            topology: {
                src |> mapper;
                mapper |> reducer;
                reducer |> output;
            }
        })
        .await?;

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
