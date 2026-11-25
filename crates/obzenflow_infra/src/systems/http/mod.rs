// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The framework's HTTP API system, which reads JSON from an HTTP API.

mod api;

pub use api::{HttpApi, JsonEndpoint};
