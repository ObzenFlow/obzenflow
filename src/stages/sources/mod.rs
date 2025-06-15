// src/stages/sources/mod.rs
//! Source stages generate events from external data or create new events
//! 
//! Sources are the entry points for data into the pipeline. They can:
//! - Generate events from external APIs (Fetchers)
//! - Create synthetic events for testing
//! - Read from databases, files, etc.
//! 
//! ## Adding a Custom Source
//! 
//! With EventStore, sources just implement the Step trait:
//! 
//! ```rust
//! use flowstate_rs::prelude::*;
//! use serde_json::json;
//! 
//! struct MyCustomSource {
//!     metrics: <RED as Taxonomy>::Metrics,
//! }
//! 
//! impl Step for MyCustomSource {
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
//!         StepType::Source
//!     }
//!     
//!     fn handle(&self, _: ChainEvent) -> Vec<ChainEvent> {
//!         // Generate new events
//!         vec![ChainEvent::new("Generated", json!({"data": "value"}))]  
//!     }
//! }
//! ```
//! 
//! For external data fetching, implement `Fetcher` which provides additional utilities:
//! 
//! ```rust
//! use flowstate_rs::stages::sources::Fetcher;
//! use flowstate_rs::stages::FetchedItem;
//! use flowstate_rs::step::Result;
//! use async_trait::async_trait;
//! 
//! struct MyApiFetcher;
//! 
//! #[async_trait]
//! impl Fetcher for MyApiFetcher {
//!     async fn fetch(&self) -> Result<Vec<FetchedItem>> {
//!         // Built-in HTTP client, rate limiting, retry logic available
//!         Ok(vec![])
//!     }
//!     
//!     fn source_id(&self) -> &str { "my_api" }
//! }
//! ```

use crate::step::{ChainEvent, Result};
use async_trait::async_trait;
use std::collections::HashMap;
use chrono::{DateTime, Utc};

// Submodules
pub mod http;

// Re-exports
pub use http::*;


/// Specialized trait for fetching data from external APIs/feeds
/// 
/// Fetchers are a specific type of Source that pull data from external systems.
/// They provide additional structure for the fetched data and common utilities.
#[async_trait]
pub trait Fetcher: Send + Sync {
    /// Fetch items from the external source
    async fn fetch(&self) -> Result<Vec<FetchedItem>>;
    
    /// Get a unique identifier for this fetcher (used for logging/metrics)
    fn source_id(&self) -> &str;
    
    /// Optional: Get the base URL or endpoint being fetched from
    fn endpoint(&self) -> Option<&str> {
        None
    }
    
    /// Optional: Get rate limiting information
    fn rate_limit_info(&self) -> Option<RateLimitInfo> {
        None
    }
    
    /// Optional: Validate configuration before first fetch
    async fn validate_config(&self) -> Result<()> {
        Ok(())
    }
}

/// Represents an item fetched from an external source
/// 
/// This provides a common structure for all fetched data, regardless of source type.
/// The metadata field allows source-specific data to be preserved.
#[derive(Debug, Clone)]
pub struct FetchedItem {
    /// Unique identifier for this item from the source
    pub id: String,
    /// Human-readable title or headline
    pub title: String,
    /// Main content or description
    pub content: String,
    /// Optional URL where the full item can be accessed
    pub url: Option<String>,
    /// Source-specific metadata (tags, scores, etc.)
    pub metadata: HashMap<String, serde_json::Value>,
    /// When this item was fetched
    pub fetched_at: DateTime<Utc>,
    /// Optional: When this item was originally published/created
    pub published_at: Option<DateTime<Utc>>,
}

impl FetchedItem {
    pub fn new(id: String, title: String, content: String) -> Self {
        Self {
            id,
            title,
            content,
            url: None,
            metadata: HashMap::new(),
            fetched_at: Utc::now(),
            published_at: None,
        }
    }

    pub fn with_url(mut self, url: String) -> Self {
        self.url = Some(url);
        self
    }

    pub fn with_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        self.metadata.insert(key, value);
        self
    }

    pub fn with_published_at(mut self, published_at: DateTime<Utc>) -> Self {
        self.published_at = Some(published_at);
        self
    }

    /// Convert to ChainEvent for pipeline processing
    pub fn to_event(&self, event_type: &str) -> ChainEvent {
        let mut payload = serde_json::json!({
            "id": self.id,
            "title": self.title,
            "content": self.content,
            "fetched_at": self.fetched_at.to_rfc3339(),
            "source": event_type
        });

        if let Some(url) = &self.url {
            payload["url"] = serde_json::json!(url);
        }

        if let Some(published_at) = &self.published_at {
            payload["published_at"] = serde_json::json!(published_at.to_rfc3339());
        }

        for (key, value) in &self.metadata {
            payload[key] = value.clone();
        }

        ChainEvent::new(event_type, payload)
    }
}

/// Rate limiting information for fetchers
#[derive(Debug, Clone)]
pub struct RateLimitInfo {
    /// Maximum requests per time window
    pub max_requests: u32,
    /// Time window duration in seconds
    pub window_seconds: u32,
    /// Whether rate limiting is strictly enforced
    pub strict: bool,
}

impl RateLimitInfo {
    pub fn new(max_requests: u32, window_seconds: u32) -> Self {
        Self {
            max_requests,
            window_seconds,
            strict: true,
        }
    }
    
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
}

