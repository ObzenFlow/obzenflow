// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rig.rs-backed chat clients.
//!
//! Embeddings use the native provider-wire client exported from [`crate::ai`].

mod chat_client;
mod error_mapping;
mod preflight;

pub use chat_client::RigChatClient;
