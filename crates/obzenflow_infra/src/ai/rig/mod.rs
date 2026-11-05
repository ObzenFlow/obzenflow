// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rig.rs-backed chat clients.
//!
//! Embeddings use the native provider-wire client exported from [`crate::ai`].

mod chat_model;
mod error_mapping;
mod preflight;
mod transport;

pub use chat_model::RigChatModel;
