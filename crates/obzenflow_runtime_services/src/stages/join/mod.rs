//! Join stage implementation
//!
//! Joins combine events from two upstream sources (reference and stream) to produce
//! enriched output events. The reference-first convention means reference data always
//! loads before stream processing begins.
//!
//! # Key Features
//! - Reference-first convention (no left/right confusion)
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
pub use config::JoinConfig;
pub use fsm::{JoinEvent, JoinState};
pub use handle::JoinHandle;

// Re-export join strategies
pub use strategies::{
    InnerJoin, InnerJoinBuilder, LeftJoin, LeftJoinBuilder, StrictJoin, StrictJoinBuilder,
    TypedJoinState,
};

// Note: JoinSupervisor is NOT exported! It's an implementation detail.
