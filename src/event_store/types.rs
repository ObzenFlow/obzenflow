//! Common types for the event store module

use crate::chain_event::ChainEvent;
use crate::event_store::{VectorClock, WriterId};
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};
use std::path::PathBuf;

/// Event envelope with vector clock for causal ordering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Writer that created this event
    #[serde(serialize_with = "serialize_writer_id", deserialize_with = "deserialize_writer_id")]
    pub writer_id: WriterId,
    /// Vector clock for causal ordering
    pub vector_clock: VectorClock,
    /// Timestamp for wall-clock time
    pub timestamp: DateTime<Utc>,
    /// The actual event
    pub event: ChainEvent,
}

// Custom serialization to maintain compatibility
fn serialize_writer_id<S>(writer_id: &WriterId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&writer_id.to_string())
}

fn deserialize_writer_id<'de, D>(deserializer: D) -> Result<WriterId, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    WriterId::from_string(&s)
        .ok_or_else(|| serde::de::Error::custom(format!("Invalid writer ID format: {}", s)))
}

/// Isolation mode for testing vs production
#[derive(Debug, Clone)]
pub enum IsolationMode {
    /// Production: Shared persistent storage
    Shared,
    /// Testing: Isolated temporary storage with automatic cleanup
    Isolated,
    /// Development: Named persistent storage for debugging
    Named(String),
}

/// Configuration for EventStore
#[derive(Debug, Clone)]
pub struct EventStoreConfig {
    /// Path to store event logs
    pub path: PathBuf,
    /// Maximum size of each writer segment file
    pub max_segment_size: u64,
}

impl Default for EventStoreConfig {
    fn default() -> Self {
        Self {
            path: PathBuf::from("./event_store"),
            max_segment_size: 10 * 1024 * 1024, // 10MB default
        }
    }
}

/// Retention policy for event storage
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_size_mb: Option<u64>,
    pub max_age_days: Option<u64>,
    pub auto_cleanup: bool,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_size_mb: None,
            max_age_days: None,
            auto_cleanup: false,
        }
    }
}