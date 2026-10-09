// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Current public event-construction API; fixture payloads use schema version 1.
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, WriterId};

pub fn data_event(writer: WriterId, event_type: &str, payload: serde_json::Value) -> ChainEvent {
    ChainEventFactory::data_event(writer, event_type, std::num::NonZeroU32::MIN, payload)
}
