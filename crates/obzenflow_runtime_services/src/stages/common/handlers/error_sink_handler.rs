//! Error sink handler trait for FLOWIP-082e
//!
//! Error sinks implement special handling for error events including:
//! - Deduplication by correlation_id/origin_id  
//! - Hop budget enforcement
//! - Rate limiting
//! - Error event aggregation

use async_trait::async_trait;
use obzenflow_core::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
use std::time::Duration;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Trait for error sink implementations
#[async_trait]
pub trait ErrorSinkHandler: Send + Sync + 'static {
    /// Process an error event
    /// 
    /// Implementations should:
    /// 1. Check hop budget (drop if error_hops_remaining == 0)
    /// 2. Deduplicate by correlation_id || origin_id
    /// 3. Apply rate limiting if configured
    /// 4. Return delivery payload for the processed error
    async fn process_error(
        &mut self,
        event: ChainEvent,
    ) -> Result<Option<DeliveryPayload>, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Flush any buffered error events
    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(None)
    }
    
    /// Drain the handler on shutdown
    async fn drain(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }
}

/// Default error sink handler with deduplication and hop budget
#[derive(Clone, Debug)]
pub struct DefaultErrorSinkHandler {
    /// Deduplication cache: key -> last seen timestamp
    dedup_cache: Arc<RwLock<HashMap<String, std::time::Instant>>>,
    /// Deduplication window
    dedup_window: Duration,
    /// Rate limiter (if configured)
    rate_limit: Option<f64>,
    /// Last rate limit check
    last_rate_check: std::time::Instant,
    /// Events processed in current window
    events_in_window: usize,
}

impl DefaultErrorSinkHandler {
    /// Create a new default error sink handler
    pub fn new(dedup_window_secs: u64, rate_limit: Option<f64>) -> Self {
        Self {
            dedup_cache: Arc::new(RwLock::new(HashMap::new())),
            dedup_window: Duration::from_secs(dedup_window_secs),
            rate_limit,
            last_rate_check: std::time::Instant::now(),
            events_in_window: 0,
        }
    }
    
    /// Generate deduplication key from event
    fn dedup_key(event: &ChainEvent) -> String {
        // Use correlation_id if available, otherwise use event ID itself
        if let Some(corr_id) = &event.correlation_id {
            corr_id.to_string()
        } else {
            event.id.to_string()
        }
    }
    
    /// Check if event should be rate limited
    fn check_rate_limit(&mut self) -> bool {
        if let Some(limit) = self.rate_limit {
            let now = std::time::Instant::now();
            let elapsed = now.duration_since(self.last_rate_check);
            
            // Reset window every second
            if elapsed >= Duration::from_secs(1) {
                self.last_rate_check = now;
                self.events_in_window = 0;
            }
            
            self.events_in_window += 1;
            self.events_in_window as f64 > limit
        } else {
            false
        }
    }
}

#[async_trait]
impl ErrorSinkHandler for DefaultErrorSinkHandler {
    async fn process_error(
        &mut self,
        event: ChainEvent,
    ) -> Result<Option<DeliveryPayload>, Box<dyn std::error::Error + Send + Sync>> {
        // 1. Check hop budget
        let remaining_hops = event.processing_info.error_hops_remaining.unwrap_or(3);
        if remaining_hops == 0 {
            tracing::warn!(
                event_id = %event.id,
                correlation_id = ?event.correlation_id,
                "Dropping error event: hop budget exhausted"
            );
            return Ok(None);
        }
        
        // 2. Check rate limit
        if self.check_rate_limit() {
            tracing::warn!(
                event_id = %event.id,
                "Dropping error event: rate limit exceeded"
            );
            return Ok(None);
        }
        
        // 3. Deduplication check
        let dedup_key = Self::dedup_key(&event);
        let mut cache = self.dedup_cache.write().await;
        let now = std::time::Instant::now();
        
        // Clean expired entries
        cache.retain(|_, timestamp| now.duration_since(*timestamp) < self.dedup_window);
        
        // Check if we've seen this event recently
        if cache.contains_key(&dedup_key) {
            tracing::debug!(
                event_id = %event.id,
                dedup_key = %dedup_key,
                "Dropping duplicate error event"
            );
            return Ok(None);
        }
        
        // Add to cache
        cache.insert(dedup_key.clone(), now);
        drop(cache);
        
        // 4. Create delivery payload for the error event
        let error_summary = format!(
            "Error event processed: {} (hops remaining: {})",
            event.id,
            remaining_hops - 1
        );
        
        let mut payload = DeliveryPayload::success(
            "error_sink",
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
            None,  // bytes_processed
        );
        
        // Add error event details to middleware context
        payload.middleware_context = Some(serde_json::json!({
            "event_id": event.id.to_string(),
            "correlation_id": event.correlation_id,
            "error_hops_remaining": remaining_hops - 1,
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "error_info": error_summary,
        }));
        
        Ok(Some(payload))
    }
}