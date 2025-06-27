//! StageHandler trait and implementations for all handler types

pub mod stage_handler_trait;
pub mod source_handler_impl;
pub mod transform_handler_impl;
pub mod sink_handler_impl;

pub use stage_handler_trait::StageHandler;
pub use source_handler_impl::{FiniteSourceWrapper, InfiniteSourceWrapper};
pub use transform_handler_impl::TransformWrapper;
pub use sink_handler_impl::SinkWrapper;