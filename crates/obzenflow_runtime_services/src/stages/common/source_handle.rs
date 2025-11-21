//! Source handle enum for pipeline coordination
//!
//! This enum allows the pipeline to store both finite and infinite sources
//! in a single collection and call source-specific methods like `ready()`.

use crate::stages::source::finite::fsm::FiniteSourceEvent;
use crate::stages::source::finite::handle::FiniteSourceHandle;
use crate::stages::source::infinite::fsm::InfiniteSourceEvent;
use crate::stages::source::infinite::handle::InfiniteSourceHandle;
use crate::supervised_base::builder::SupervisorHandle;
use crate::supervised_base::HandleError;

/// Enum to hold either finite or infinite source handles
pub enum SourceHandle {
    Finite(FiniteSourceHandle),
    Infinite(InfiniteSourceHandle),
}

impl SourceHandle {
    /// Call ready() on the underlying source (transitions to WaitingForGun)
    pub async fn ready(&self) -> Result<(), HandleError> {
        match self {
            Self::Finite(h) => h.send_event(FiniteSourceEvent::Ready).await,
            Self::Infinite(h) => h.send_event(InfiniteSourceEvent::Ready).await,
        }
    }
}
