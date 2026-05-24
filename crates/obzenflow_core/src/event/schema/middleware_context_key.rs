// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed per-pass middleware context keys.

/// Marker trait for typed middleware context values stored inside `MiddlewareContext`.
///
/// Keys are used only for typed lookup; `LABEL` is diagnostics-only.
pub trait MiddlewareContextKey: 'static {
    type Value: Send + Sync + 'static;

    /// Human-readable diagnostics only. Not a lookup key.
    const LABEL: &'static str;
}

