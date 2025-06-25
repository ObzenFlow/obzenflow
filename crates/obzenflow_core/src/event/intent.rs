//! Event intent types
//!
//! Supports the CHAIN maturity model by making event intent explicit.

use serde::{Deserialize, Serialize};

/// The intent behind an event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Intent {
    /// A command to perform an action
    Command { 
        action: String, 
        target: String 
    },
    
    /// A query for information
    Query { 
        question: String 
    },
    
    /// A fact that occurred
    Event { 
        fact: String 
    },
    
    /// A document or content
    Document { 
        content: String 
    },
}

impl Intent {
    /// Check if this is a command intent
    pub fn is_command(&self) -> bool {
        matches!(self, Intent::Command { .. })
    }
    
    /// Check if this is a query intent
    pub fn is_query(&self) -> bool {
        matches!(self, Intent::Query { .. })
    }
    
    /// Check if this is an event intent
    pub fn is_event(&self) -> bool {
        matches!(self, Intent::Event { .. })
    }
    
    /// Check if this is a document intent
    pub fn is_document(&self) -> bool {
        matches!(self, Intent::Document { .. })
    }
}