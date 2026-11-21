// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Clients used by sink systems to write to their destinations.

#[cfg(feature = "postgres")]
pub mod postgres;
