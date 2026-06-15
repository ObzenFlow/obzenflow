// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control event handling strategies for supervisors
//!
//! This module implements the Strategy pattern to keep supervisor event loops clean
//! while allowing middleware to configure control event behavior.

// Core types and traits
mod core;
pub use core::{ProcessingContext, SignalDecision, SignalGate};

// Control signal dispatch helper
pub(crate) mod dispatch;

// Concrete strategy implementations
mod strategies;
pub use strategies::{BackoffStrategy, CompositeStrategy, JonestownSignalStrategy};

// Runtime control-strategy hooks (FLOWIP-115c): the admission and observation
// hooks plus the shared `Pause` vocabulary the binding slices consume.
mod hooks;
pub use hooks::{
    AdmissionDecision, AdmissionGate, AdmissionPosition, AttemptObserver, AttemptOutcome,
    CreditWaker, PostAdmitDecision, WakeOn,
};
