//! Source handler components

pub mod traits;

pub use traits::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler, SourceError,
};
