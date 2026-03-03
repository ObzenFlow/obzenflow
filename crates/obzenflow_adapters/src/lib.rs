// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composable middleware, concrete source/sink adapters, and monitoring
//! exporters for ObzenFlow pipelines. Middleware factories (rate limiting,
//! circuit breakers) are applied at the flow or stage level through the
//! `flow!` macro. Sources and sinks provide the I/O boundary for ingesting
//! and emitting events.
//!
#![doc = include_str!("../README.md")]

pub mod ai;
pub mod middleware;
pub mod monitoring;
pub mod sinks;
pub mod sources;
