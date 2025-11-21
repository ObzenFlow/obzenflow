//! Join strategy implementations
//!
//! Three strategies for handling stream events during enrichment:
//! - InnerJoin: Drop unmatched stream events
//! - LeftJoin: Pass through unmatched stream events
//! - StrictJoin: Trigger jonestown protocol on unmatched events

pub mod common;
pub mod inner_join;
pub mod left_join;
pub mod strict_join;

// Re-export key types
pub use common::{JoinStrategy, JoinWithStrategy, TypedJoinState};
pub use inner_join::{InnerJoin, InnerJoinBuilder};
pub use left_join::{LeftJoin, LeftJoinBuilder};
pub use strict_join::{StrictJoin, StrictJoinBuilder};
