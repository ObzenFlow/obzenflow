//! DSL module for ObzenFlow
//!
//! This module contains the flow! macro and related DSL components
//! that provide the high-level API for building ObzenFlow pipelines.

mod dsl;
pub mod error;
mod flow_definition;
pub mod stage_descriptor;
mod stage_macros;

#[cfg(test)]
mod tests;

// Re-export all public items
#[allow(unused_imports)]
pub use dsl::*;
pub use error::FlowBuildError;
pub use flow_definition::FlowDefinition;
#[allow(unused_imports)]
pub use stage_macros::*;
