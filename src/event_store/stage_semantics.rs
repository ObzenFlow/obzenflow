//! Stage semantics for different processing patterns
//! 
//! Defines how stages behave and can be parallelized.
//! This information guides routing and consistency decisions.

use serde::{Serialize, Deserialize};
use std::time::Duration;

/// Describes the processing semantics of a stage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StageSemantics {
    /// Pure function - no internal state, freely parallelizable
    /// Examples: JSON parsing, field extraction, simple transformations
    Stateless,
    
    /// Maintains state but processes each event independently
    /// Examples: Caching enrichment, rate limiting, deduplication
    StatefulIndependent {
        /// How to partition state across workers
        partition_strategy: PartitionStrategy,
    },
    
    /// Accumulates/windows multiple events into outputs
    /// Examples: Aggregations, joins, windowing operations
    StatefulAccumulator {
        window_size: Duration,
        emit_strategy: EmitStrategy,
        /// Key extractor for grouping
        group_by: Option<String>, // JSON path for grouping key
    },
    
    /// Maintains ordered sequence processing
    /// Examples: State machines, session tracking
    StatefulSequential {
        /// Key for sequence tracking
        sequence_key: String,
    },
}

/// Strategy for partitioning work across multiple workers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Hash-based partitioning on event key
    KeyHash { key_path: String },
    /// Round-robin distribution
    RoundRobin,
    /// Affinity-based (e.g., GPU memory locality)
    Affinity { affinity_fn: String },
    /// Route based on worker utilization (requires USE taxonomy)
    UtilizationAware,
    /// Route based on saturation levels (requires USE/Golden Signals)
    SaturationAware { threshold: f64 },
    /// Composite routing using multiple metrics
    MetricAware { 
        weights: std::collections::HashMap<String, f64>, // metric_name -> weight
    },
}

/// When to emit accumulated results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EmitStrategy {
    /// Emit when time window closes
    OnWindowClose,
    /// Emit on every update
    OnUpdate,
    /// Emit periodically
    Periodic { interval: Duration },
    /// Emit when accumulator reaches threshold
    OnCount { threshold: usize },
    /// Emit on size threshold (bytes)
    OnSize { bytes: usize },
    /// Composite: emit on whichever comes first
    OnFirst { conditions: Vec<EmitStrategy> },
    /// Dynamic: emit based on downstream pressure (future)
    Dynamic { initial: Box<EmitStrategy> },
}

impl Default for StageSemantics {
    fn default() -> Self {
        StageSemantics::Stateless
    }
}

impl StageSemantics {
    /// Check if this stage type requires state partitioning
    pub fn requires_partitioning(&self) -> bool {
        matches!(
            self,
            StageSemantics::StatefulIndependent { .. } |
            StageSemantics::StatefulAccumulator { .. } |
            StageSemantics::StatefulSequential { .. }
        )
    }
    
    /// Check if this stage type can be freely parallelized
    pub fn is_freely_parallelizable(&self) -> bool {
        matches!(self, StageSemantics::Stateless)
    }
    
    /// Get the partition key for an event (if applicable)
    pub fn get_partition_key(&self, event: &serde_json::Value) -> Option<String> {
        match self {
            StageSemantics::StatefulIndependent { partition_strategy } => {
                match partition_strategy {
                    PartitionStrategy::KeyHash { key_path } => {
                        extract_json_path(event, key_path)
                    }
                    _ => None,
                }
            }
            StageSemantics::StatefulAccumulator { group_by, .. } => {
                group_by.as_ref().and_then(|path| extract_json_path(event, path))
            }
            StageSemantics::StatefulSequential { sequence_key } => {
                extract_json_path(event, sequence_key)
            }
            _ => None,
        }
    }
}

/// Extract a value from JSON using a simple path notation
/// e.g., "user.id" extracts from {"user": {"id": "123"}}
fn extract_json_path(json: &serde_json::Value, path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = json;
    
    for part in parts {
        match current.get(part) {
            Some(value) => current = value,
            None => return None,
        }
    }
    
    match current {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    
    #[test]
    fn test_stage_semantics_classification() {
        let stateless = StageSemantics::Stateless;
        assert!(stateless.is_freely_parallelizable());
        assert!(!stateless.requires_partitioning());
        
        let stateful = StageSemantics::StatefulIndependent {
            partition_strategy: PartitionStrategy::RoundRobin,
        };
        assert!(!stateful.is_freely_parallelizable());
        assert!(stateful.requires_partitioning());
    }
    
    #[test]
    fn test_partition_key_extraction() {
        let event = json!({
            "user": {
                "id": "user123",
                "name": "Alice"
            },
            "action": "purchase"
        });
        
        let semantics = StageSemantics::StatefulIndependent {
            partition_strategy: PartitionStrategy::KeyHash {
                key_path: "user.id".to_string(),
            },
        };
        
        assert_eq!(
            semantics.get_partition_key(&event),
            Some("user123".to_string())
        );
    }
    
    #[test]
    fn test_json_path_extraction() {
        let json = json!({
            "level1": {
                "level2": {
                    "value": "found"
                }
            },
            "simple": 42,
            "bool": true
        });
        
        assert_eq!(
            extract_json_path(&json, "level1.level2.value"),
            Some("found".to_string())
        );
        assert_eq!(
            extract_json_path(&json, "simple"),
            Some("42".to_string())
        );
        assert_eq!(
            extract_json_path(&json, "bool"),
            Some("true".to_string())
        );
        assert_eq!(
            extract_json_path(&json, "missing.path"),
            None
        );
    }
}