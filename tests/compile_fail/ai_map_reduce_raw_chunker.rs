// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ChainEvent;
use obzenflow_dsl::dsl::composites::ai_map_reduce::generated_map_reduce;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::TransformHandler;

#[path = "support/ai_surface.rs"]
mod support;
use support::*;

#[derive(Clone, Debug)]
struct RawChunker;

#[async_trait]
impl TransformHandler for RawChunker {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

fn main() {
    let _ = generated_map_reduce::<Seed, Item, Partial, Output, _, _>(
        "raw-chunker",
        (RawChunker, MapRole, FinaliseRole),
        (contract(), contract()),
        (Vec::new(), Vec::new()),
    );
}
