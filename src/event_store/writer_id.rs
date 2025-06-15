//! Writer identification with strong typing
//! 
//! Part of FLOWIP-019: Strong Stage Identification

use crate::topology::StageId;
use std::fmt;
use serde::{Serialize, Deserialize, Serializer, Deserializer};
use serde::de::Error as DeError;

/// Strongly typed writer identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WriterId {
    stage_id: StageId,
    worker_index: Option<u32>,
}

impl WriterId {
    /// Create a writer ID for a single-worker stage
    pub fn new(stage_id: StageId) -> Self {
        Self {
            stage_id,
            worker_index: None,
        }
    }
    
    /// Create a writer ID for a multi-worker stage
    pub fn with_worker(stage_id: StageId, worker_index: u32) -> Self {
        Self {
            stage_id,
            worker_index: Some(worker_index),
        }
    }
    
    /// Get the stage ID
    pub fn stage_id(&self) -> StageId {
        self.stage_id
    }
    
    /// Get the worker index if this is a multi-worker stage
    pub fn worker_index(&self) -> Option<u32> {
        self.worker_index
    }
    
    /// Convert to string for backward compatibility
    /// Format: "stage_N" or "stage_N_worker_M"
    pub fn to_string(&self) -> String {
        match self.worker_index {
            Some(idx) => format!("stage_{}_worker_{}", self.stage_id.as_u32(), idx),
            None => format!("stage_{}", self.stage_id.as_u32()),
        }
    }
    
    /// Parse from string (temporary for migration)
    pub fn from_string(s: &str) -> Option<Self> {
        if let Some(worker_pos) = s.find("_worker_") {
            // Format: stage_N_worker_M
            let stage_part = &s[..worker_pos];
            let worker_part = &s[worker_pos + 8..]; // Skip "_worker_"
            
            if let Some(stage_num) = stage_part.strip_prefix("stage_") {
                if let (Ok(stage_id), Ok(worker_idx)) = (stage_num.parse::<u32>(), worker_part.parse::<u32>()) {
                    return Some(Self {
                        stage_id: StageId::from_u32(stage_id),
                        worker_index: Some(worker_idx),
                    });
                }
            }
        } else if let Some(stage_num) = s.strip_prefix("stage_") {
            // Format: stage_N
            if let Ok(stage_id) = stage_num.parse::<u32>() {
                return Some(Self {
                    stage_id: StageId::from_u32(stage_id),
                    worker_index: None,
                });
            }
        }
        
        None
    }
}

impl fmt::Display for WriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Serialize for WriterId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for WriterId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        WriterId::from_string(&s)
            .ok_or_else(|| DeError::custom(format!("Invalid WriterId format: {}", s)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_writer_id_single_worker() {
        let stage_id = StageId::from_u32(42);
        let writer_id = WriterId::new(stage_id);
        
        assert_eq!(writer_id.stage_id(), stage_id);
        assert_eq!(writer_id.worker_index(), None);
        assert_eq!(writer_id.to_string(), "stage_42");
    }
    
    #[test]
    fn test_writer_id_multi_worker() {
        let stage_id = StageId::from_u32(42);
        let writer_id = WriterId::with_worker(stage_id, 3);
        
        assert_eq!(writer_id.stage_id(), stage_id);
        assert_eq!(writer_id.worker_index(), Some(3));
        assert_eq!(writer_id.to_string(), "stage_42_worker_3");
    }
    
    #[test]
    fn test_writer_id_parsing() {
        // Single worker
        let parsed = WriterId::from_string("stage_42").unwrap();
        assert_eq!(parsed.stage_id().as_u32(), 42);
        assert_eq!(parsed.worker_index(), None);
        
        // Multi worker
        let parsed = WriterId::from_string("stage_42_worker_3").unwrap();
        assert_eq!(parsed.stage_id().as_u32(), 42);
        assert_eq!(parsed.worker_index(), Some(3));
        
        // Invalid formats
        assert!(WriterId::from_string("invalid").is_none());
        assert!(WriterId::from_string("stage_").is_none());
        assert!(WriterId::from_string("stage_abc").is_none());
    }
}