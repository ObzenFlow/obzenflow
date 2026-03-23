// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Outermost infrastructure layer for ObzenFlow. Provides durable journal
//! backends (disk-based, in-memory), an optional Warp web server for metrics
//! and topology endpoints, and the [`application::FlowApplication`] runner
//! that turns a `flow! { ... }` definition into a managed process with CLI
//! parsing, observability, and graceful shutdown. Most capabilities are gated
//! behind feature flags (see `Cargo.toml`).
//!
#![doc = include_str!("../README.md")]

pub mod ai;
pub mod application;
pub mod env;
pub mod http_client;
pub mod journal;
pub mod monitoring_backend;
pub mod web;

#[cfg(test)]
pub(crate) mod test_support;
