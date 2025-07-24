//! Stage shapes define connectivity
//! 
//! Inspired by Akka Streams - everything is a Stage with a Shape

use serde::{Deserialize, Serialize};
use obzenflow_core::event::context::StageType;

/// Port identifier for connecting stages
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PortId(pub String);

impl PortId {
    pub fn new() -> Self {
        Self(ulid::Ulid::new().to_string())
    }
}

/// Shape defines the connectivity of a stage
#[derive(Debug, Clone)]
pub enum Shape {
    /// No inlets, one outlet (generates events)
    Source { out: PortId },
    
    /// One inlet, one outlet (transforms events)
    Flow { in_port: PortId, out: PortId },
    
    /// One inlet, no outlets (consumes events)
    Sink { in_port: PortId },
    
    /// One inlet, multiple outlets (splits stream)
    Broadcast { in_port: PortId, outs: Vec<PortId> },
    
    /// Multiple inlets, one outlet (merges streams)
    Merge { ins: Vec<PortId>, out: PortId },
}

impl Shape {
    /// Get all inlet ports
    pub fn inlets(&self) -> Vec<&PortId> {
        match self {
            Shape::Source { .. } => vec![],
            Shape::Flow { in_port, .. } => vec![in_port],
            Shape::Sink { in_port } => vec![in_port],
            Shape::Broadcast { in_port, .. } => vec![in_port],
            Shape::Merge { ins, .. } => ins.iter().collect(),
        }
    }
    
    /// Get all outlet ports
    pub fn outlets(&self) -> Vec<&PortId> {
        match self {
            Shape::Source { out } => vec![out],
            Shape::Flow { out, .. } => vec![out],
            Shape::Sink { .. } => vec![],
            Shape::Broadcast { outs, .. } => outs.iter().collect(),
            Shape::Merge { out, .. } => vec![out],
        }
    }
    
    /// Simple classification for backwards compatibility
    pub fn stage_type(&self) -> StageType {
        use obzenflow_core::event::context::SimpleStageType;
        
        let simple = match self {
            Shape::Source { .. } => SimpleStageType::Source,
            Shape::Sink { .. } => SimpleStageType::Sink,
            _ => SimpleStageType::Transform,
        };
        
        simple.into()
    }
}

