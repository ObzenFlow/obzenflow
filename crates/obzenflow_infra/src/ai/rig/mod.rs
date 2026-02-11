// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rig.rs-backed AI clients.
//!
//! This module implements the `obzenflow_core::ai` port traits using `rig` as the
//! provider substrate, per FLOWIP-086r.

mod chat_client;
mod embedding_client;
mod error_mapping;
mod preflight;

pub use chat_client::RigChatClient;
pub use embedding_client::RigEmbeddingClient;
