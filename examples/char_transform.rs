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
use obzenflow::typed::{sinks, sources, stateful as typed_stateful, transforms};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
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

fn main() -> Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "warn");
    }
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    let char_inputs = build_char_inputs(&INPUT_LINES);

    FlowApplication::builder().run_blocking(flow! {
        name: "char_transform",
        journals: disk_journals(PathBuf::from("target/char-transform-logs")),
        middleware: [],

        stages: {
            characters = source!(CharInput => sources::finite(char_inputs));

            transform_text = transform!(
                CharInput -> TextChunk => transforms::map(|input: CharInput| TextChunk {
                    text: transform_char(input.character),
                })
            );

            collect_text = stateful!(
                TextChunk -> TransformedText => typed_stateful::reduce(
                    TransformedText::default(),
                    |acc, chunk: &TextChunk| {
                        acc.text.push_str(&chunk.text);
                        acc.character_count += chunk.text.chars().count();

                        let ends_sentence = matches!(chunk.text.as_str(), "." | "!" | "?");
                        if ends_sentence && !acc.last_was_sentence_end {
                            acc.sentence_count += 1;
                        }
                        acc.last_was_sentence_end = ends_sentence;
                    }
                ).emit_on_eof()
            );

            output = sink!(TransformedText => sinks::console(format_output));
        },

        topology: {
            characters |> transform_text;
            transform_text |> collect_text;
            collect_text |> output;
        }
    })?;

    Ok(())
}
