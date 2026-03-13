// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI-specific middleware adapters.
//!
//! These are framework-owned helpers used by typed AI composites (e.g. map-reduce)
//! to safely interoperate with stage-scoped subscriptions.

pub mod map_reduce;
