// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for transform stages

use crate::stages::common::handlers::TransformHandler;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{TransformEvent, TransformState};

/// Type alias for the transform handle
pub type TransformHandle<H = Box<dyn TransformHandler>> =
    StandardHandle<TransformEvent<H>, TransformState<H>>;

/// Extension trait for transform-specific convenience methods
pub trait TransformHandleExt<H> {
    /// Initialize the transform
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Mark transform as ready (starts processing immediately)
    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Begin draining the transform
    fn begin_drain(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Check if the transform is in a terminal state
    fn is_terminal(&self) -> bool;
}

impl<H: TransformHandler + Send + Sync + 'static> TransformHandleExt<H> for TransformHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(TransformEvent::<H>::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(TransformEvent::<H>::Ready).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(TransformEvent::<H>::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            TransformState::<H>::Drained | TransformState::<H>::Failed(_)
        )
    }
}
