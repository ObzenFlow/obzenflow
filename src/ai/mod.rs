// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI facade for ObzenFlow (FLOWIP-086d).
//!
//! This module re-exports the provider-agnostic AI transforms from the adapters
//! layer. When the `ai-rig` feature is enabled, it also provides a fluent
//! builder API (FLOWIP-086d-part-2) that constructs Rig-backed clients and
//! wires them into transforms without exposing infra types in user code.

pub use obzenflow_adapters::ai::{ChatTransform, EmbeddingTransform};

#[cfg(feature = "ai-rig")]
mod rig_builder;

#[cfg(feature = "ai-rig")]
pub use rig_builder::{
    ChatRequestTemplate, ChatTransformBuilder, ChatTransformExt, EmbeddingTransformBuilder,
    EmbeddingTransformExt,
};
