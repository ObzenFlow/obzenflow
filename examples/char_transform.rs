// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Character Transform Demo
//!
//! A small end-to-end flow that:
//! - emits characters from a few input lines
//! - transforms each character
//! - rebuilds the final text in a stateful stage
//! - prints the final result with `ConsoleSink`
//!
//! Run with: `cargo run -p obzenflow --example char_transform`

use anyhow::Result;
use obzenflow::sinks::ConsoleSink;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::source::FiniteSourceTyped;
use obzenflow_runtime::stages::stateful::strategies::accumulators::ReduceTyped;
use obzenflow_runtime::stages::transform::MapTyped;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const INPUT_LINES: [&str; 3] = ["hello 2024 world!", "42 is the answer.", "rust 1 python 0."];

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CharInput {
    character: char,
}

impl TypedPayload for CharInput {
    const EVENT_TYPE: &'static str = "char.input";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TextChunk {
    text: String,
}

impl TypedPayload for TextChunk {
    const EVENT_TYPE: &'static str = "text.chunk";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct TransformedText {
    text: String,
    sentence_count: usize,
    character_count: usize,
    last_was_sentence_end: bool,
}

impl TypedPayload for TransformedText {
    const EVENT_TYPE: &'static str = "text.output";
    const SCHEMA_VERSION: u32 = 1;
}

fn transform_char(ch: char) -> String {
    if ch.is_ascii_digit() {
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
        ch.to_uppercase().to_string()
    } else {
        ch.to_string()
    }
}

fn build_char_inputs(lines: &[&str]) -> Vec<CharInput> {
    let mut char_inputs = Vec::new();
    for (line_index, line) in lines.iter().enumerate() {
        for ch in line.chars() {
            char_inputs.push(CharInput { character: ch });
        }

        if line_index + 1 < lines.len() {
            char_inputs.push(CharInput { character: '\n' });
        }
    }

    char_inputs
}

fn format_output(output: &TransformedText) -> String {
    format!(
        "Input\n{}\n\nOutput\n{}\n\n{} sentences, {} characters",
        INPUT_LINES.join("\n"),
        output.text,
        output.sentence_count,
        output.character_count,
    )
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "warn");
    }
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    let char_inputs = build_char_inputs(&INPUT_LINES);

    FlowApplication::run(flow! {
        name: "char_transform",
        journals: disk_journals(PathBuf::from("target/char-transform-logs")),
        middleware: [],

        stages: {
            characters = source!(<CharInput> "characters" => FiniteSourceTyped::new(char_inputs));

            transform_text = transform!(<CharInput, TextChunk> "transform_text" => MapTyped::new(|input: CharInput| {
                TextChunk {
                    text: transform_char(input.character),
                }
            }));

            collect_text = stateful!(<TextChunk, TransformedText> "collect_text" => ReduceTyped::new(
                TransformedText::default(),
                |text: &mut TransformedText, chunk: &TextChunk| {
                    text.text.push_str(&chunk.text);
                    text.character_count += chunk.text.chars().count();

                    let ends_sentence = matches!(chunk.text.as_str(), "." | "!" | "?");
                    if ends_sentence && !text.last_was_sentence_end {
                        text.sentence_count += 1;
                    }
                    text.last_was_sentence_end = ends_sentence;
                }
            ).emit_on_eof());

            output = sink!(<TransformedText> "output" => ConsoleSink::<TransformedText>::new(format_output));
        },

        topology: {
            characters |> transform_text;
            transform_text |> collect_text;
            collect_text |> output;
        }
    })
    .await?;

    Ok(())
}
