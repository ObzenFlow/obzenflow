// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event-specific payloads

pub mod chain_payload;
pub mod composite_data_payload;
pub mod correlation_payload;
pub mod delivery_payload;
pub mod effect_payload;
pub mod execution_payload;
pub mod flow_control_payload;
pub mod sink_operation_payload;
pub mod stage_fatal_payload;
pub mod supervisor_descriptor;

pub mod journal_payload;
pub mod system_payload;

pub use chain_payload::ChainPayload;
pub use journal_payload::JournalPayload;
pub use system_payload::SystemPayload;
