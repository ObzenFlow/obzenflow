//! Core metric primitives - atomic operations only
//!
//! These are the foundational building blocks that all metrics are built upon.
//! They provide only atomic operations and can evolve without breaking upper layers.

mod counter;
mod gauge;
mod histogram;
mod time_unit;

pub use counter::Counter;
pub use gauge::Gauge;
pub use histogram::Histogram;
pub use time_unit::TimeUnit;