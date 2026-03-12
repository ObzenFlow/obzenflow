// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform helper types for common patterns
//!
//! This module provides pre-built `TransformHandler` implementations for common
//! transformation patterns, reducing boilerplate and improving readability.
//!
//! # Helper Types
//!
//! - [`Filter`] - Predicate-based event filtering (pass/drop)
//! - [`Map`] - Event transformation (always succeeds, 1-to-1)
//! - [`TryMap`] - Fallible Map using `TryFrom`/`Into` traits (can fail)
//! - [`TryMapWith`] - Fallible Map using closures (can fail)
//!
//! # Composition
//!
//! Map is the primitive for 1-to-1 transformations. TryMap and TryMapWith are
//! fallible variants that mark failed transformations as errors.
//!
//! # Usage
//!
//! ```ignore
//! use obzenflow_runtime::stages::transform::strategies::{Filter, Map, TryMapWith};
//!
//! // Filter events by predicate
//! let error_filter = Filter::new(|event| {
//!     event.payload()["level"].as_str() == Some("error")
//! });
//!
//! // Map events (always succeeds)
//! let enricher = Map::new(|event| {
//!     let mut payload = event.payload();
//!     payload["processed"] = json!(true);
//!     ChainEventFactory::data_event(event.writer_id.clone(), event.event_type(), payload)
//! });
//!
//! // TryMap with validation (can fail - marks as error)
//! let validator = TryMapWith::new(|event| {
//!     let amount = event.payload()["amount"].as_f64()
//!         .ok_or("Missing amount")?;
//!     if amount < 0.0 { return Err("Negative amount".to_string()); }
//!     Ok(event)
//! });
//! ```
//!
//! All helpers implement `TransformHandler` and work seamlessly with the
//! existing `transform!` macro from the DSL.

pub mod async_map;
pub mod async_try_map_with;
pub mod filter;
pub mod filter_map;
pub mod map;
pub mod try_map;
pub mod try_map_with;

pub use async_map::{AsyncMap, AsyncMapTyped};
pub use async_try_map_with::{AsyncTryMapWith, AsyncTryMapWithTyped};
pub use filter::{Filter, FilterTyped};
pub use filter_map::{FilterMap, FilterMapTyped};
pub use map::{Map, MapTyped};
pub use try_map::TryMap;
pub use try_map_with::{ErrorStrategy, TryMapWith, TryMapWithTyped};
