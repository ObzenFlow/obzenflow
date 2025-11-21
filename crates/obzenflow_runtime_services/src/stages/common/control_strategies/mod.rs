//! Control event handling strategies for supervisors
//!
//! This module implements the Strategy pattern to keep supervisor event loops clean
//! while allowing middleware to configure control event behavior.

// Core types and traits
mod core;
pub use core::{ControlEventAction, ControlEventStrategy, ProcessingContext};

// Concrete strategy implementations
mod strategies;
pub use strategies::{
    BackoffStrategy, CompositeStrategy, JonestownStrategy, RetryStrategy, WindowingStrategy,
};
