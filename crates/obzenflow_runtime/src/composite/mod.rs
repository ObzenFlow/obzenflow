// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pure composite lifecycle projection (FLOWIP-128a B1).
//!
//! Composites are graph abstractions, not executable runtime nodes. Their
//! lifecycle is therefore a Moore-style view reconstructed from the topology's
//! member mapping and the ordered `StageLifecycle` input tape. This module owns
//! that semantic fold and performs no I/O, journalling, scheduling, or runtime
//! signalling.

mod projection;

pub use projection::{
    CompositeDefinition, CompositeLifecycleProjection, CompositeProjectionError, CompositeStatus,
};
