// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control event handling strategies for supervisors
//!
//! This module implements the Strategy pattern to keep supervisor event loops clean
//! while allowing middleware to configure control event behavior.

// Core types and traits
mod core;
pub use core::{ControlEventAction, ControlEventStrategy, ProcessingContext};

// Control signal dispatch helper
pub(crate) mod dispatch;

// Concrete strategy implementations
mod strategies;
pub use strategies::{BackoffStrategy, CompositeStrategy, JonestownStrategy, WindowingStrategy};
