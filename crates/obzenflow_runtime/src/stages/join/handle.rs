// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for join stages

use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{JoinEvent, JoinState};

/// Type alias for the join handle
pub type JoinHandle = StandardHandle<JoinEvent, JoinState>;

/// Extension trait for join-specific convenience methods
pub trait JoinHandleExt {
    /// Initialize the join
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Mark join as ready to start processing
    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Check if the join is in a terminal state
    fn is_terminal(&self) -> bool;

    /// Check if the join is in hydrating state
    fn is_hydrating(&self) -> bool;

    /// Check if the join is in enriching state
    fn is_enriching(&self) -> bool;
}

impl JoinHandleExt for JoinHandle {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(JoinEvent::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(JoinEvent::Ready).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            JoinState::Drained | JoinState::Failed(_)
        )
    }

    fn is_hydrating(&self) -> bool {
        matches!(self.current_state(), JoinState::Hydrating(_))
    }

    fn is_enriching(&self) -> bool {
        matches!(self.current_state(), JoinState::Enriching(_))
    }
}
