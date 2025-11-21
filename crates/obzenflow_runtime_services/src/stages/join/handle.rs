//! Handle for join stages

use crate::stages::common::handlers::JoinHandler;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};
use obzenflow_core::StageId;

use super::fsm::{JoinEvent, JoinState};

/// Type alias for the join handle
pub type JoinHandle<H = Box<dyn JoinHandler>> = StandardHandle<JoinEvent<H>, JoinState<H>>;

/// Extension trait for join-specific convenience methods
pub trait JoinHandleExt<H> {
    /// Initialize the join
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Mark join as ready to start processing
    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Notify that a specific upstream reached EOF
    fn source_eof(
        &self,
        source_id: StageId,
    ) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Notify that reference upstream is complete
    fn reference_complete(
        &self,
    ) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Begin draining the join
    fn begin_drain(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Check if the join is in a terminal state
    fn is_terminal(&self) -> bool;

    /// Check if the join is in hydrating state
    fn is_hydrating(&self) -> bool;

    /// Check if the join is in enriching state
    fn is_enriching(&self) -> bool;
}

impl<H: JoinHandler + Send + Sync + 'static> JoinHandleExt<H> for JoinHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(JoinEvent::<H>::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(JoinEvent::<H>::Ready).await
    }

    async fn source_eof(&self, _source_id: StageId) -> Result<(), HandleError> {
        self.send_event(JoinEvent::<H>::ReceivedEOF).await
    }

    async fn reference_complete(&self) -> Result<(), HandleError> {
        self.send_event(JoinEvent::<H>::ReferenceComplete).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(JoinEvent::<H>::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            JoinState::<H>::Drained | JoinState::<H>::Failed(_)
        )
    }

    fn is_hydrating(&self) -> bool {
        matches!(self.current_state(), JoinState::<H>::Hydrating { .. })
    }

    fn is_enriching(&self) -> bool {
        matches!(self.current_state(), JoinState::<H>::Enriching)
    }
}
