//! Handle for error sink stages

use crate::supervised_base::{StandardHandle, HandleError, SupervisorHandle};
use crate::stages::common::handlers::ErrorSinkHandler;

use super::fsm::{ErrorSinkEvent, ErrorSinkState};

/// Type alias for the error sink handle
pub type ErrorSinkHandle<H = Box<dyn ErrorSinkHandler>> = StandardHandle<ErrorSinkEvent<H>, ErrorSinkState<H>>;

/// Extension trait for error sink-specific convenience methods
pub trait ErrorSinkHandleExt<H> {
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

impl<H: ErrorSinkHandler + Send + Sync + 'static> ErrorSinkHandleExt<H> for ErrorSinkHandle<H> {
    async fn initialize(&self) -> Result<(), HandleError> {
        self.send_event(ErrorSinkEvent::<H>::Initialize).await
    }
    
    async fn ready(&self) -> Result<(), HandleError> {
        self.send_event(ErrorSinkEvent::<H>::Ready).await
    }
    
    async fn received_eof(&self) -> Result<(), HandleError> {
        self.send_event(ErrorSinkEvent::<H>::ReceivedEOF).await
    }
    
    async fn begin_flush(&self) -> Result<(), HandleError> {
        self.send_event(ErrorSinkEvent::<H>::BeginFlush).await
    }
    
    async fn flush_complete(&self) -> Result<(), HandleError> {
        self.send_event(ErrorSinkEvent::<H>::FlushComplete).await
    }
    
    async fn begin_drain(&self) -> Result<(), HandleError> {
        self.send_event(ErrorSinkEvent::<H>::BeginDrain).await
    }
    
    fn is_terminal(&self) -> bool {
        matches!(
            self.current_state(),
            ErrorSinkState::<H>::Drained | ErrorSinkState::<H>::Failed(_)
        )
    }
    
    fn is_flushing(&self) -> bool {
        matches!(
            self.current_state(),
            ErrorSinkState::<H>::Flushing
        )
    }
}