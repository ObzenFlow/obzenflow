// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Event schema support (FLOWIP-082a)
//!
//! This module provides type-safe event handling through the TypedPayload trait,
//! enabling compile-time event type checking.

mod typed_payload;

pub use typed_payload::TypedPayload;
