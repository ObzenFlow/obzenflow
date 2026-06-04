// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The tutorial source: a small, scripted, deterministic stream of payment
//! commands.
//!
//! Determinism is the point. Because the sequence is fixed, you can line up
//! logs, journal events, and metrics, and a replay reproduces exactly the same
//! inputs. The high-volume `payment_gateway_highvolume` example swaps this for a
//! semi-reliable feed that glitches on its own schedule.

use super::domain::PaymentCommand;
use super::fixtures;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_runtime::stages::common::handlers::source::traits::SourceError;
use obzenflow_runtime::stages::common::handlers::FiniteSourceHandler;
use serde_json::json;

/// Finite source that replays a scripted sequence of payment commands.
///
/// This models an upstream order system sending us payment work.
#[derive(Clone, Debug)]
pub struct PaymentCommandSource {
    commands: Vec<PaymentCommand>,
    current_index: usize,
    writer_id: WriterId,
}

impl PaymentCommandSource {
    pub fn new() -> Self {
        Self {
            commands: fixtures::scripted_commands(),
            current_index: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for PaymentCommandSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.current_index >= self.commands.len() {
            return Ok(None);
        }

        let cmd = &self.commands[self.current_index];
        self.current_index += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            PaymentCommand::EVENT_TYPE,
            json!(cmd),
        )]))
    }
}
