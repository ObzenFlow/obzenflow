// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::control_plane::{
    CircuitBreakerState, CircuitBreakerStateSnapshot, CircuitBreakerStateView,
};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

/// Circuit breaker states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum CircuitState {
    /// Normal operation - requests pass through.
    Closed = 0,
    /// Circuit is open - requests are rejected.
    Open = 1,
    /// Testing if the circuit can be closed - limited requests allowed.
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// FLOWIP-115b: typed read-only projection of the breaker's authoritative state.
///
/// Wraps the private state cell and the probe generation. This is the single
/// state authority behind [`CircuitBreakerStateView`]; source completion,
/// metrics, topology, and exporters read it instead of the raw atomic.
#[derive(Debug)]
pub(crate) struct CircuitBreakerStateViewImpl {
    pub(super) state: Arc<AtomicU8>,
    pub(super) generation: Arc<AtomicU64>,
}

impl CircuitBreakerStateView for CircuitBreakerStateViewImpl {
    fn snapshot(&self) -> CircuitBreakerStateSnapshot {
        CircuitBreakerStateSnapshot {
            state: CircuitBreakerState::from_u8(self.state.load(Ordering::SeqCst)),
            generation: self.generation.load(Ordering::SeqCst),
        }
    }
}
