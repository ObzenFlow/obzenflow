// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for infinite source stages

use crate::stages::common::handlers::InfiniteSourceHandler;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{InfiniteSourceEvent, InfiniteSourceState};

/// Type alias for the infinite source handle
pub type InfiniteSourceHandle<H = Box<dyn InfiniteSourceHandler>> =
    StandardHandle<InfiniteSourceEvent<H>, InfiniteSourceState<H>>;

/// Extension trait for infinite source-specific convenience methods
pub trait InfiniteSourceHandleExt<H> {
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

impl<H: Send + Sync + 'static> InfiniteSourceHandleExt<H> for InfiniteSourceHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(InfiniteSourceEvent::<H>::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(InfiniteSourceEvent::<H>::Ready).await
    }

    async fn start(&self) -> Result<(), HandleError> {
        self.send_event(InfiniteSourceEvent::<H>::Start).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(InfiniteSourceEvent::<H>::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            InfiniteSourceState::<H>::Drained | InfiniteSourceState::<H>::Failed(_)
        )
    }
}
