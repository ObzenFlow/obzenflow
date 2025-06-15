// src/stages/sinks/mod.rs
//! Sink stages process events and may produce side effects
//! 
//! With EventStore, sinks are just Steps that may:
//! - Write to vector databases
//! - Send to external APIs
//! - Export to traditional databases
//! - Produce no output events (terminal stages)
//! 
//! ## Adding a Custom Sink
//! 
//! With EventStore, sinks just implement the Step trait:
//! 
//! ```rust
//! use flowstate_rs::prelude::*;
//! 
//! struct MyCustomSink {
//!     metrics: <RED as Taxonomy>::Metrics,
//! }
//! 
//! impl MyCustomSink {
//!     fn write_to_database(&self, event: &ChainEvent) {
//!         // Example database write logic
//!         println!("Writing event: {:?}", event.event_type);
//!     }
//! }
//! 
//! impl Step for MyCustomSink {
//!     type Taxonomy = RED;
//!     
//!     fn taxonomy(&self) -> &Self::Taxonomy {
//!         &RED
//!     }
//!     
//!     fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
//!         &self.metrics
//!     }
//!     
//!     fn step_type(&self) -> StepType {
//!         StepType::Sink
//!     }
//!     
//!     fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
//!         // Process event (side effects)
//!         self.write_to_database(&event);
//!         vec![] // Sinks typically return empty vec
//!     }
//! }
//! ```
//! 
//! For specific sink types, implement the specialized traits:
//! - `VectorSink` for vector database storage
//! - `DatabaseSink` for relational database storage
//! - `ApiSink` for external API integration

use crate::step::{ChainEvent, Result};
use async_trait::async_trait;

// Submodules
pub mod http;

// Re-exports  
pub use http::*;


/// Specialized trait for vector database sinks
/// 
/// Vector sinks store embeddings and enable semantic search.
/// They typically store both the original content and its vector representation.
#[async_trait]
pub trait VectorSink: Send + Sync {
    /// Store an event with its vector embedding
    async fn store_with_embedding(&self, event: &ChainEvent, embedding: &[f32]) -> Result<()>;
    
    /// Generate embedding for event content (if not provided)
    async fn generate_embedding(&self, content: &str) -> Result<Vec<f32>>;
    
    /// Store event by automatically generating embedding
    async fn store_event(&self, event: &ChainEvent) -> Result<()> {
        let content = event.payload.get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("");
            
        if content.is_empty() {
            return Err("Event has no content to embed".into());
        }
        
        let embedding = self.generate_embedding(content).await?;
        self.store_with_embedding(event, &embedding).await
    }
    
    /// Optional: Get vector sink configuration
    fn vector_config(&self) -> VectorSinkConfig {
        VectorSinkConfig::default()
    }
}

/// Configuration for vector sinks
#[derive(Debug, Clone)]
pub struct VectorSinkConfig {
    /// Dimension of the embedding vectors
    pub embedding_dimension: usize,
    /// Collection/index name in the vector database
    pub collection_name: String,
    /// Whether to store full event payload or just content
    pub store_full_payload: bool,
}

impl Default for VectorSinkConfig {
    fn default() -> Self {
        Self {
            embedding_dimension: 384, // Common dimension for sentence transformers
            collection_name: "events".to_string(),
            store_full_payload: true,
        }
    }
}

/// Specialized trait for traditional database sinks
/// 
/// Database sinks store structured event data in relational or document databases.
#[async_trait]
pub trait DatabaseSink: Send + Sync {
    /// Store event in structured format
    async fn store_structured(&self, event: &ChainEvent) -> Result<()>;
    
    /// Optional: Create necessary tables/collections if they don't exist
    async fn ensure_schema(&self) -> Result<()> {
        Ok(())
    }
    
    /// Optional: Get database sink configuration
    fn database_config(&self) -> DatabaseSinkConfig {
        DatabaseSinkConfig::default()
    }
}

/// Configuration for database sinks
#[derive(Debug, Clone)]
pub struct DatabaseSinkConfig {
    /// Table/collection name for storing events
    pub table_name: String,
    /// Whether to auto-create schema if missing
    pub auto_create_schema: bool,
    /// Connection pool size
    pub connection_pool_size: u32,
}

impl Default for DatabaseSinkConfig {
    fn default() -> Self {
        Self {
            table_name: "events".to_string(),
            auto_create_schema: true,
            connection_pool_size: 10,
        }
    }
}

/// Specialized trait for external API sinks
/// 
/// API sinks send event data to external services via HTTP/REST APIs.
#[async_trait]
pub trait ApiSink: Send + Sync {
    /// Send event to external API
    async fn send_to_api(&self, event: &ChainEvent) -> Result<()>;
    
    /// Optional: Transform event before sending
    fn transform_for_api(&self, event: &ChainEvent) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(event)?)
    }
    
    /// Optional: Get API sink configuration
    fn api_config(&self) -> ApiSinkConfig {
        ApiSinkConfig::default()
    }
}

/// Configuration for API sinks
#[derive(Debug, Clone)]
pub struct ApiSinkConfig {
    /// Base URL for the API
    pub base_url: String,
    /// Authentication method
    pub auth_method: String,
    /// Request timeout in seconds
    pub timeout_seconds: u64,
    /// Number of retry attempts
    pub max_retries: u32,
}

impl Default for ApiSinkConfig {
    fn default() -> Self {
        Self {
            base_url: "https://api.example.com".to_string(),
            auth_method: "none".to_string(),
            timeout_seconds: 30,
            max_retries: 3,
        }
    }
}

// Note: Each concrete sink type implements Sink manually to avoid trait conflicts
// The specialized traits (VectorSink, DatabaseSink, ApiSink) provide domain-specific APIs
// while Sink provides the common interface for the pipeline system.