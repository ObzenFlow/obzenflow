// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Ready-to-use source adapters for ingesting data into a flow.
//!
//! Sources are the entry points of every pipeline. This module re-exports the
//! built-in source implementations from [`obzenflow_adapters::sources`] so that
//! most applications only need `obzenflow` in their dependency list.
//!
//! ## CSV sources
//!
//! [`CsvSource`] (via [`CsvSourceBuilder`]) reads rows from CSV files on disk.
//! Each row is emitted as a [`CsvRow`] event.
//!
//! ## HTTP pull sources
//!
//! [`HttpPullSource`] performs a single HTTP request and decodes the response
//! body using a [`PullDecoder`]. [`HttpPollSource`] wraps the same logic in a
//! polling loop controlled by [`HttpPollConfig`].
//!
//! Both require the `http-pull` feature flag
//! (`obzenflow_infra/reqwest-client`).

/// CSV file source and its row type.
pub use obzenflow_adapters::sources::{CsvRow, CsvSource, CsvSourceBuilder};

/// HTTP pull and poll sources, decoders, and configuration types.
pub use obzenflow_adapters::sources::{
    simple_poll, CursorlessPullDecoder, DecodeError, DecodeResult, FnPullDecoder, HttpPollConfig,
    HttpPollSource, HttpPullConfig, HttpPullSource, HttpResponse, PullDecoder,
};

/// HTTP primitives re-exported from `obzenflow_core` for building request specs.
pub use obzenflow_core::http_client::{HeaderMap, RequestSpec, Url};
