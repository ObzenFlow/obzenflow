// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful handler components

pub mod traits;
pub mod wrapper;

pub use traits::StatefulHandler;
pub use wrapper::{StatefulHandlerExt, StatefulHandlerWithEmission};
