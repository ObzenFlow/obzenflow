// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Clients that sources and systems hold to reach outside services.

pub mod http;
#[cfg(feature = "postgres")]
pub mod postgres;
