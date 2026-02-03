// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for finite source stages

use crate::stages::common::handlers::FiniteSourceHandler;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{FiniteSourceEvent, FiniteSourceState};

/// Type alias for the finite source handle
pub type FiniteSourceHandle<H = Box<dyn FiniteSourceHandler>> =
    StandardHandle<FiniteSourceEvent<H>, FiniteSourceState<H>>;

/// Extension trait for finite source-specific convenience methods
pub trait FiniteSourceHandleExt<H> {
    /// Initialize the source
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Mark source as ready (transitions to WaitingForGun)
    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Start the source (fire the gun!)
    fn start(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Begin draining the source
    fn begin_drain(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Check if the source is in a terminal state
    fn is_terminal(&self) -> bool;
}

impl<H: Send + Sync + 'static> FiniteSourceHandleExt<H> for FiniteSourceHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(FiniteSourceEvent::<H>::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(FiniteSourceEvent::<H>::Ready).await
    }

    async fn start(&self) -> Result<(), HandleError> {
        self.send_event(FiniteSourceEvent::<H>::Start).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(FiniteSourceEvent::<H>::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            FiniteSourceState::<H>::Drained | FiniteSourceState::<H>::Failed(_)
        )
    }
}
