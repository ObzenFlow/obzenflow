//! Source stage implementations
//!
//! Sources are divided into two types:
//! - Finite: Sources that eventually complete (files, bounded collections)
//! - Infinite: Sources that run indefinitely (Kafka, WebSocket, etc)

pub mod finite;
pub mod infinite;
pub mod strategies;
pub mod typed;

pub use typed::{
    AsyncFiniteSourceTyped, FallibleAsyncFiniteSourceTyped, FallibleFiniteSourceTyped,
    FiniteSourceTyped,
};
