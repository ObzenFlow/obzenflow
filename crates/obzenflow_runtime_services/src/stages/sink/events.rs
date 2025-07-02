//! Events that trigger state transitions for sink stages

use serde::{Deserialize, Serialize};
use obzenflow_fsm::EventVariant;

/// Events that can trigger sink state transitions
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SinkEvent {
    /// Initialize the sink - open connections, create output files, etc.
    Initialize,
    
    /// Ready to consume events
    Ready,
    
    /// Received EOF from all upstream stages
    ReceivedEOF,
    
    /// Begin flush operation - write any buffered data
    /// UNIQUE TO SINKS: Ensures no data loss
    BeginFlush,
    
    /// Flush operation completed successfully
    FlushComplete,
    
    /// Begin graceful shutdown (after flush)
    BeginDrain,
    
    /// Unrecoverable error occurred
    Error(String),
}

// ============================================================================
// FSM Framework Integration - see source_events.rs for explanation
// ============================================================================

impl EventVariant for SinkEvent {
    fn variant_name(&self) -> &str {
        match self {
            SinkEvent::Initialize => "Initialize",
            SinkEvent::Ready => "Ready",
            SinkEvent::ReceivedEOF => "ReceivedEOF",
            SinkEvent::BeginFlush => "BeginFlush",      // Sink-specific!
            SinkEvent::FlushComplete => "FlushComplete", // Sink-specific!
            SinkEvent::BeginDrain => "BeginDrain",
            SinkEvent::Error(_) => "Error",
        }
    }
}