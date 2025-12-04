//! DSL module for ObzenFlow
//!
//! This module contains the flow! macro and related DSL components
//! that provide the high-level API for building ObzenFlow pipelines.

mod dsl;
pub mod stage_descriptor;
mod stage_macros;
pub mod error;

#[cfg(test)]
mod tests;

// Re-export all public items
#[allow(unused_imports)]
pub use dsl::*;
#[allow(unused_imports)]
pub use stage_macros::*;
pub use error::FlowBuildError;
