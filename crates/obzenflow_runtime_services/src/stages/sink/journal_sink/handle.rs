// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handle for journal sink stages

use crate::stages::common::handlers::SinkHandler;
use crate::supervised_base::{HandleError, StandardHandle, SupervisorHandle};

use super::fsm::{JournalSinkEvent, JournalSinkState};

/// Type alias for the journal sink handle
pub type JournalSinkHandle<H = Box<dyn SinkHandler>> =
    StandardHandle<JournalSinkEvent<H>, JournalSinkState<H>>;

/// Extension trait for journal sink-specific convenience methods
pub trait JournalSinkHandleExt<H> {
    /// Initialize the sink
    fn initialize(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Mark sink as ready to consume events
    fn ready(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Received EOF from upstream - begin flush
    fn received_eof(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Begin flushing buffered data
    fn begin_flush(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Flush completed successfully
    fn flush_complete(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Begin draining the sink
    fn begin_drain(&self) -> impl std::future::Future<Output = Result<(), HandleError>> + Send;

    /// Check if the sink is in a terminal state
    fn is_terminal(&self) -> bool;

    /// Check if the sink is currently flushing
    fn is_flushing(&self) -> bool;
}

impl<H: SinkHandler + Send + Sync + 'static> JournalSinkHandleExt<H> for JournalSinkHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(JournalSinkEvent::<H>::Initialize).await
    }

    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(JournalSinkEvent::<H>::Ready).await
    }

    async fn received_eof(&self) -> Result<(), HandleError> {
        self.send_event(JournalSinkEvent::<H>::ReceivedEOF).await
    }

    async fn begin_flush(&self) -> Result<(), HandleError> {
        self.send_event(JournalSinkEvent::<H>::BeginFlush).await
    }

    async fn flush_complete(&self) -> Result<(), HandleError> {
        self.send_event(JournalSinkEvent::<H>::FlushComplete).await
    }

    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(JournalSinkEvent::<H>::BeginDrain).await
    }

    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            JournalSinkState::<H>::Drained | JournalSinkState::<H>::Failed(_)
        )
    }

    fn is_flushing(&self) -> bool {
        matches!(self.current_state(), JournalSinkState::<H>::Flushing)
    }
}
