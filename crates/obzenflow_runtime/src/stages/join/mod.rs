// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join stage implementation
//!
//! Joins combine events from two upstream sources (reference and stream) to produce
//! enriched output events.
//!
//! By default (`JoinReferenceMode::FiniteEof`), joins follow a hydrate-then-enrich model:
//! reference loads to EOF before stream processing begins. In `JoinReferenceMode::Live`,
//! the join processes stream events continuously while the reference side can keep
//! receiving updates.
//!
//! # Key Features
//! - Reference-first convention (no left/right confusion)
//! - Optional live reference updates (`JoinReferenceMode::Live`)
//! - Per-source EOF handling
//! - 3 join strategies: InnerJoin, LeftJoin, StrictJoin
//! - Type-safe key extraction via closures
//! - In-memory HashMap catalogs (<1GB per side)
//!
//! # Architecture
//! Joins are implemented as a dedicated stage type with custom FSM, not built on
//! top of StatefulHandler or TransformHandler, because joins have fundamentally
//! different semantics (immediate 1:1 emission, per-source EOF, distinct upstream roles).

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod strategies;
pub mod supervisor;

// Public API - only expose builder, handle, and essential types
pub use crate::stages::common::handlers::JoinHandler;
pub use builder::JoinBuilder;
pub use config::{JoinConfig, JoinReferenceMode};
pub use fsm::{JoinEvent, JoinState};
pub use handle::JoinHandle;

// Re-export join strategies
pub use strategies::{
    InnerJoin, InnerJoinBuilder, LeftJoin, LeftJoinBuilder, StrictJoin, StrictJoinBuilder,
    TypedJoinState,
};

// Note: JoinSupervisor is NOT exported! It's an implementation detail.
