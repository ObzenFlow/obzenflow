//! Stage type classification for pipeline coordination
//!
//! This is the canonical StageType definition used throughout the system.
//! It provides detailed classification to support proper pipeline coordination,
//! lifecycle management, and safety validation.

use serde::{Deserialize, Serialize};

/// Stage type classification for pipeline coordination
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StageType {
    /// Source that will eventually complete (e.g., file reader, bounded collection)
    FiniteSource,
    
    /// Source that runs indefinitely (e.g., network listener, message queue consumer)
    InfiniteSource,
    
    /// Transforms input events to output events
    Transform,
    
    /// Terminal stage that consumes events (e.g., database writer, API caller)
    Sink,
    
    /// Stage that maintains internal state across events
    Stateful,
}

impl StageType {
    /// Check if this is a source that needs explicit start
    pub fn is_source(&self) -> bool {
        matches!(self, StageType::FiniteSource | StageType::InfiniteSource)
    }
    
    /// Check if this is a finite source
    pub fn is_finite_source(&self) -> bool {
        matches!(self, StageType::FiniteSource)
    }
    
    /// Check if this is an infinite source
    pub fn is_infinite_source(&self) -> bool {
        matches!(self, StageType::InfiniteSource)
    }
    
    /// Check if this stage generates events
    pub fn generates_events(&self) -> bool {
        self.is_source()
    }
    
    /// Check if this stage consumes events
    pub fn consumes_events(&self) -> bool {
        !self.is_source()
    }
    
    /// Check if this is a terminal stage
    pub fn is_terminal(&self) -> bool {
        matches!(self, StageType::Sink)
    }
    
    /// Get a human-readable name for the stage type
    pub fn as_str(&self) -> &'static str {
        match self {
            StageType::FiniteSource => "finite_source",
            StageType::InfiniteSource => "infinite_source",
            StageType::Transform => "transform",
            StageType::Sink => "sink",
            StageType::Stateful => "stateful",
        }
    }
}

impl std::fmt::Display for StageType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Simplified stage type for cases where source distinction doesn't matter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimpleStageType {
    Source,
    Transform,
    Sink,
}

impl StageType {
    /// Get simplified stage type (collapses FiniteSource/InfiniteSource to Source)
    pub fn simple(&self) -> SimpleStageType {
        match self {
            StageType::FiniteSource | StageType::InfiniteSource => SimpleStageType::Source,
            StageType::Transform | StageType::Stateful => SimpleStageType::Transform,
            StageType::Sink => SimpleStageType::Sink,
        }
    }
}

impl From<SimpleStageType> for StageType {
    fn from(simple: SimpleStageType) -> Self {
        match simple {
            SimpleStageType::Source => StageType::InfiniteSource, // Default to infinite
            SimpleStageType::Transform => StageType::Transform,
            SimpleStageType::Sink => StageType::Sink,
        }
    }
}