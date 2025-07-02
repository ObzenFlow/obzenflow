//! Sink handler components

pub mod traits;
pub mod context;
pub mod r#impl;

pub use traits::SinkHandler;
pub use context::SinkContext;
pub use r#impl::SinkWrapper;