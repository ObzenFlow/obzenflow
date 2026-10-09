// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Outer fixture adapter for reference 854c04bab261477ad4bec813369244e84f2ffb20.
//! That public API predates explicit payload schema versions. The validator
//! installs this module only in its copied benchmark crate, never the framework.
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, WriterId};

pub fn data_event(writer: WriterId, event_type: &str, payload: serde_json::Value) -> ChainEvent {
    ChainEventFactory::data_event(writer, event_type, payload)
}
