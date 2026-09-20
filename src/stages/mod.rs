// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage constructors and the contracts for implementing application handlers.
//!
//! Each family keeps its ready-made handlers beside its custom authoring traits.
//! Cross-cutting policies live in [`crate::middleware`], external operations in
//! [`crate::effects`], and application hosting in [`crate::application`].

pub mod joins;
pub mod sinks;
pub mod sources;
pub mod stateful;
pub mod transforms;
