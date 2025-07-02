//! Stage execution semantics
//! 
//! Defines how stages behave during execution and how the runtime
//! should coordinate them. This is a runtime concern, NOT a storage concern!
//!
//! NOTE: This is currently DEAD CODE - not wired up to the runtime.
//! Will be integrated as part of FLOWIP-080 implementation.

use serde::{Serialize, Deserialize};
use std::time::Duration;

/// Describes the execution semantics of a stage
/// 
/// This guides the runtime on how to schedule and coordinate stages
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

/// Strategy for partitioning work across multiple stage instances
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitionStrategy {
    /// Hash-based partitioning on event key
    KeyHash { key_path: String },
    /// Round-robin distribution
    RoundRobin,
    /// Affinity-based (e.g., GPU memory locality)
    Affinity { affinity_fn: String },
    /// Single instance only (no partitioning)
    SingleInstance,
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
    /// Emit on size threshold
    OnSize { bytes: usize },
    // TODO(FLOWIP-080): Add OnEOF variant for emit-on-completion semantics
    // OnEOF,  // Emit only when stage receives EOF (dangerous with infinite sources!)
}

impl Default for StageSemantics {
    fn default() -> Self {
        StageSemantics::Stateless
    }
}

impl StageSemantics {
    /// Check if this stage can be parallelized across multiple instances
    pub fn supports_parallelism(&self) -> bool {
        match self {
            StageSemantics::Stateless => true,
            StageSemantics::StatefulIndependent { partition_strategy } => {
                !matches!(partition_strategy, PartitionStrategy::SingleInstance)
            }
            StageSemantics::StatefulAccumulator { .. } => false, // Usually single instance
            StageSemantics::StatefulSequential { .. } => false,  // Must be single instance
        }
    }
    
    /// Get the recommended number of parallel instances
    pub fn recommended_parallelism(&self) -> usize {
        match self {
            StageSemantics::Stateless => num_cpus::get(),
            StageSemantics::StatefulIndependent { .. } => num_cpus::get() / 2,
            _ => 1,
        }
    }
}