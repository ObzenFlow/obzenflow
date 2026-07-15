// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Effect-boundary behaviour tests, grouped by concern. Shared fixtures live
//! in [`support`] and stay private to this tree.

mod concurrency;
mod onion;
mod recovery;
mod support;
mod timing;
