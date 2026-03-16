// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Character Transform Skeleton
//!
//! The same pipeline shape as `char_transform`, but with every handler
//! replaced by `placeholder!()`. This compiles and can be opened in
//! the UI. Placeholder handlers are lifecycle-aware and will not panic,
//! but the flow produces no output.
//!
//! The point: you can go from an event-storming diagram to a
//! compilable flow skeleton without writing any business logic.
//! Define your types, wire the topology, and fill in behaviour later.
//!
//! Run with:
//!
//! ```sh
//! cargo run -p obzenflow --example char_transform_skeleton
//! ```
//!
//! To view the topology in the UI, start the server in manual mode so
//! the pipeline does not run until you press Play:
//!
//! ```sh
//! cargo run -p obzenflow --features obzenflow_infra/warp-server \
//!     --example char_transform_skeleton -- --server --startup-mode manual
//! ```
//!
//! When you press Play, the placeholder finite source returns EOF
//! immediately, and the pipeline drains and exits quickly.

use anyhow::Result;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

// ── Event types ─────────────────────────────────────────────────────────────
// These are the only things you need to decide up front. Each type represents
// a distinct event in the dataflow, matching what you would draw on a board
// during event storming.

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

// ── Flow ────────────────────────────────────────────────────────────────────
// Types and topology only. Every handler is placeholder!().

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "warn");
    }
    std::env::set_var("OBZENFLOW_METRICS_EXPORTER", "console");

    FlowApplication::run(flow! {
        name: "char_transform_skeleton",
        journals: disk_journals(PathBuf::from("target/char-transform-skeleton-logs")),
        middleware: [],

        stages: {
            characters = source!(CharInput => placeholder!());
            transform_text = transform!(CharInput -> TextChunk => placeholder!());
            collect_text = stateful!(TextChunk -> TransformedText => placeholder!());
            output = sink!(TransformedText => placeholder!());
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
