// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for merge stages

use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{MergeEvent, MergeState};

/// Type alias for the merge handle
pub type MergeHandle = StandardHandle<MergeEvent, MergeState>;

/// Extension trait for merge-specific convenience methods
pub trait MergeHandleExt {
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    fn begin_drain(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    fn is_terminal(&self) -> bool;
}

impl MergeHandleExt for MergeHandle {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(MergeEvent::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(MergeEvent::Ready).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(MergeEvent::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            MergeState::Drained | MergeState::Failed(_) | MergeState::Cancelled(_)
        )
    }
}
