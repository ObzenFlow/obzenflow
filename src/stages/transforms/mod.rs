// src/stages/transforms/mod.rs
//! Transform stages process events (1:1 or 1:N transformations)
//! 
//! Transforms are the core processing stages that take input events and produce
//! output events. They can:
//! - Split content (Chunkers)
//! - Retrieve additional context (RAG)  
//! - Generate new content (Generators)
//! - Filter, aggregate, or enrich events
//! 
//! ## Adding a Custom Transform
//! 
//! Implement the `Transform` trait for any processing logic:
//! 
//! ```rust
//! use flowstate_rs::stages::transforms::Transform;
//! use flowstate_rs::step::ChainEvent;
//! 
//! struct MyProcessor;
//! 
//! impl Transform for MyProcessor {
//!     fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
//!         // Your transformation logic
//!         vec![event]
//!     }
//! }
//! ```
//! 
//! For specific transform types, implement the specialized traits:
//! - `Chunker` for content splitting
//! - `Rag` for context retrieval  
//! - `Generator` for content creation

use crate::step::ChainEvent;
use std::collections::HashMap;

// Submodules
pub mod llm;

// Re-exports  
pub use llm::*;

/// Base trait for all transform stages
/// 
/// Transforms take input events and produce zero or more output events.
/// They are the core processing stages in the pipeline.
pub trait Transform: Send + Sync {
    /// Transform an input event into zero or more output events
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent>;
    
    /// Optional: Get a human-readable identifier for this transform
    fn transform_id(&self) -> &str {
        "unknown_transform"
    }
    
    /// Optional: Validate the transform configuration
    fn validate_config(&self) -> crate::step::Result<()> {
        Ok(())
    }
}

/// Specialized trait for chunking content into smaller pieces
/// 
/// Chunkers take textual content and split it into manageable chunks
/// for downstream processing (like embedding generation or analysis).
pub trait Chunker: Send + Sync {
    /// Split content into chunks
    fn chunk(&self, content: &str) -> Vec<ChunkedItem>;
    
    /// Optional: Get chunker configuration info
    fn chunk_config(&self) -> ChunkConfig {
        ChunkConfig::default()
    }
}

/// Represents a chunk of content created by a Chunker
#[derive(Debug, Clone)]
pub struct ChunkedItem {
    /// The text content of this chunk
    pub content: String,
    /// Index of this chunk in the original content
    pub index: usize,
    /// Total number of chunks created from the source
    pub total_chunks: usize,
    /// Character offset where this chunk starts in the original text
    pub start_offset: usize,
    /// Character offset where this chunk ends in the original text  
    pub end_offset: usize,
    /// Optional metadata about this chunk
    pub metadata: HashMap<String, serde_json::Value>,
}

impl ChunkedItem {
    pub fn new(content: String, index: usize, total_chunks: usize, start_offset: usize, end_offset: usize) -> Self {
        Self {
            content,
            index,
            total_chunks,
            start_offset,
            end_offset,
            metadata: HashMap::new(),
        }
    }
    
    pub fn with_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Configuration for chunking behavior
#[derive(Debug, Clone)]
pub struct ChunkConfig {
    /// Maximum characters per chunk
    pub max_chunk_size: usize,
    /// Overlap between adjacent chunks
    pub overlap_size: usize,
    /// Whether to preserve word boundaries
    pub preserve_words: bool,
    /// Whether to preserve sentence boundaries
    pub preserve_sentences: bool,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            max_chunk_size: 1000,
            overlap_size: 100,
            preserve_words: true,
            preserve_sentences: true,
        }
    }
}

/// Specialized trait for Retrieval-Augmented Generation (RAG)
/// 
/// RAG stages retrieve relevant context for input events,
/// typically from vector databases or knowledge bases.
pub trait Rag: Send + Sync {
    /// Retrieve relevant context for the input content
    fn retrieve_context(&self, query: &str) -> crate::step::Result<RagContext>;
    
    /// Optional: Get RAG configuration info  
    fn rag_config(&self) -> RagConfig {
        RagConfig::default()
    }
}

/// Context retrieved by a RAG stage
#[derive(Debug, Clone)]
pub struct RagContext {
    /// The retrieved context text
    pub context: String,
    /// Relevance score (0.0 to 1.0)
    pub relevance_score: f64,
    /// Sources of the context
    pub sources: Vec<String>,
    /// Additional metadata about the retrieval
    pub metadata: HashMap<String, serde_json::Value>,
}

impl RagContext {
    pub fn new(context: String, relevance_score: f64) -> Self {
        Self {
            context,
            relevance_score,
            sources: Vec::new(),
            metadata: HashMap::new(),
        }
    }
    
    pub fn with_sources(mut self, sources: Vec<String>) -> Self {
        self.sources = sources;
        self
    }
    
    pub fn with_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Configuration for RAG behavior
#[derive(Debug, Clone)]
pub struct RagConfig {
    /// Maximum number of context documents to retrieve
    pub max_contexts: usize,
    /// Minimum relevance score to include context
    pub min_relevance: f64,
    /// Maximum total context length
    pub max_context_length: usize,
}

impl Default for RagConfig {
    fn default() -> Self {
        Self {
            max_contexts: 5,
            min_relevance: 0.5,
            max_context_length: 2000,
        }
    }
}

/// Specialized trait for content generation
/// 
/// Generators create new content based on input events and optional context.
/// Examples include script generation, summarization, translation, etc.
pub trait Generator: Send + Sync {
    /// Generate content based on input
    fn generate(&self, input: &str, context: Option<&RagContext>) -> crate::step::Result<GeneratedContent>;
    
    /// Optional: Get generator configuration info
    fn generator_config(&self) -> GeneratorConfig {
        GeneratorConfig::default()
    }
}

/// Content generated by a Generator stage
#[derive(Debug, Clone)]
pub struct GeneratedContent {
    /// The generated content
    pub content: String,
    /// Type/format of the generated content
    pub content_type: String,
    /// Quality/confidence score (0.0 to 1.0)
    pub quality_score: f64,
    /// Additional metadata about the generation
    pub metadata: HashMap<String, serde_json::Value>,
}

impl GeneratedContent {
    pub fn new(content: String, content_type: String) -> Self {
        Self {
            content,
            content_type,
            quality_score: 1.0,
            metadata: HashMap::new(),
        }
    }
    
    pub fn with_quality_score(mut self, score: f64) -> Self {
        self.quality_score = score;
        self
    }
    
    pub fn with_metadata(mut self, key: String, value: serde_json::Value) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

/// Configuration for content generation
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Maximum length of generated content
    pub max_length: usize,
    /// Creativity/randomness parameter (0.0 to 1.0)
    pub temperature: f64,
    /// Whether to include context in generation
    pub use_context: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            max_length: 2000,
            temperature: 0.7,
            use_context: true,
        }
    }
}

// Automatic implementations of Transform for specialized traits
impl<T: Chunker> Transform for T {
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Extract content from event and chunk it
        let content = event.payload.get("content")
            .and_then(|v| v.as_str())
            .unwrap_or("");
            
        if content.is_empty() {
            return vec![];
        }
        
        let chunks = self.chunk(content);
        let mut result_events = Vec::new();
        
        for chunk in chunks {
            let chunk_event = ChainEvent::new("ChunkIndexed", serde_json::json!({
                "chunk_text": chunk.content,
                "chunk_index": chunk.index,
                "total_chunks": chunk.total_chunks,
                "start_offset": chunk.start_offset,
                "end_offset": chunk.end_offset,
                "original_event_id": event.ulid,
                "original_content_length": content.len(),
                "chunk_metadata": chunk.metadata
            }));
            result_events.push(chunk_event);
        }
        
        result_events
    }
    
    fn transform_id(&self) -> &str {
        "chunker"
    }
}