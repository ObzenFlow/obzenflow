// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for stateful stages

use crate::stages::common::handlers::StatefulHandler;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{StatefulEvent, StatefulState};

/// Type alias for the stateful stage handle
///
/// Note: Unlike TransformHandle, we don't provide a default `Box<dyn>` type
/// because StatefulHandler has an associated type (State) which cannot be
/// used in a trait object without specifying it.
pub type StatefulHandle<H> = StandardHandle<StatefulEvent<H>, StatefulState<H>>;

/// Extension trait for stateful-specific convenience methods
pub trait StatefulHandleExt<H> {
    /// Initialize the stateful stage
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Mark stateful stage as ready (starts processing immediately)
    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Begin draining the stateful stage
    fn begin_drain(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Check if the stateful stage is in a terminal state
    fn is_terminal(&self) -> bool;
}

impl<H: StatefulHandler + Send + Sync + 'static> StatefulHandleExt<H> for StatefulHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(StatefulEvent::<H>::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(StatefulEvent::<H>::Ready).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(StatefulEvent::<H>::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            StatefulState::<H>::Drained | StatefulState::<H>::Failed(_)
        )
    }
}
