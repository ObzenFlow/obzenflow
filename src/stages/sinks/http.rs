//! Generic HTTP sink for sending events to any HTTP endpoint
//! 
//! This provides a configurable HTTP sink that can send events to any API
//! without needing domain-specific implementations.

use super::{ApiSink, ApiSinkConfig};
use crate::chain_event::ChainEvent;
use crate::step::{Result, StepType};
use crate::stages::Sink;
use async_trait::async_trait;
use std::collections::HashMap;
use std::time::Duration;
use serde_json::Value;

/// Generic HTTP sink that can send events to any endpoint
/// 
/// Configure this for your specific API rather than writing custom sink code.
/// Supports different payload formatters and authentication methods.
pub struct HttpSink {
    endpoint: String,
    method: HttpMethod,
    headers: HashMap<String, String>,
    timeout: Duration,
    formatter: Box<dyn PayloadFormatter>,
    retry_config: RetryConfig,
}

impl HttpSink {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            method: HttpMethod::POST,
            headers: HashMap::new(),
            timeout: Duration::from_secs(30),
            formatter: Box::new(JsonFormatter::new()),
            retry_config: RetryConfig::default(),
        }
    }
    
    pub fn with_method(mut self, method: HttpMethod) -> Self {
        self.method = method;
        self
    }
    
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
    
    pub fn with_auth_bearer(self, token: impl Into<String>) -> Self {
        self.with_header("Authorization", format!("Bearer {}", token.into()))
    }
    
    pub fn with_auth_header(self, header: impl Into<String>, value: impl Into<String>) -> Self {
        self.with_header(header, value)
    }
    
    pub fn with_content_type(self, content_type: impl Into<String>) -> Self {
        self.with_header("Content-Type", content_type)
    }
    
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
    
    pub fn with_formatter(mut self, formatter: Box<dyn PayloadFormatter>) -> Self {
        self.formatter = formatter;
        self
    }
    
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }
}

impl Sink for HttpSink {
    fn step_type(&self) -> StepType {
        StepType::Sink
    }
    
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // TODO: Implement actual HTTP client
        // This would:
        // 1. Format event using the configured formatter
        // 2. Make HTTP request with reqwest or similar
        // 3. Apply headers, timeout, retry logic
        // 4. Handle response and errors appropriately
        
        match self.formatter.format_event(&event) {
            Ok(payload) => {
                println!("HttpSink: Would send {} request to {} with payload: {}", 
                         self.method, self.endpoint, payload);
            }
            Err(e) => {
                eprintln!("HttpSink: Failed to format event: {}", e);
            }
        }
        
        // Sinks typically don't emit events
        vec![]
    }
}

#[async_trait]
impl ApiSink for HttpSink {
    async fn send_to_api(&self, event: &ChainEvent) -> Result<()> {
        let payload = self.formatter.format_event(event)?;
        println!("HttpSink: Would send {} request to {} with payload: {}", 
                 self.method, self.endpoint, payload);
        Ok(())
    }
    
    fn transform_for_api(&self, event: &ChainEvent) -> Result<Value> {
        let payload_str = self.formatter.format_event(event)?;
        Ok(serde_json::from_str(&payload_str)?)
    }
    
    fn api_config(&self) -> ApiSinkConfig {
        ApiSinkConfig {
            base_url: self.endpoint.clone(),
            auth_method: "custom".to_string(),
            timeout_seconds: self.timeout.as_secs(),
            max_retries: self.retry_config.max_attempts,
        }
    }
}

/// HTTP methods supported by the sink
#[derive(Debug, Clone)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    PATCH,
    DELETE,
}

impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpMethod::GET => write!(f, "GET"),
            HttpMethod::POST => write!(f, "POST"),
            HttpMethod::PUT => write!(f, "PUT"),
            HttpMethod::PATCH => write!(f, "PATCH"),
            HttpMethod::DELETE => write!(f, "DELETE"),
        }
    }
}

/// Retry configuration for HTTP requests
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            backoff_multiplier: 2.0,
        }
    }
}

/// Trait for formatting events as HTTP payloads
pub trait PayloadFormatter: Send + Sync {
    /// Format a single event as a string payload
    fn format_event(&self, event: &ChainEvent) -> Result<String>;
    
    /// Format multiple events as a batch payload (optional)
    /// Return None if batch formatting is not supported
    fn format_batch(&self, events: &[ChainEvent]) -> Result<Option<String>> {
        // Default: no batch support
        let _ = events;
        Ok(None)
    }
}

/// JSON payload formatter
pub struct JsonFormatter {
    include_metadata: bool,
    custom_fields: HashMap<String, String>,
}

impl JsonFormatter {
    pub fn new() -> Self {
        Self {
            include_metadata: true,
            custom_fields: HashMap::new(),
        }
    }
    
    pub fn with_metadata(mut self, include: bool) -> Self {
        self.include_metadata = include;
        self
    }
    
    pub fn with_custom_field(mut self, field: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom_fields.insert(field.into(), value.into());
        self
    }
}

impl Default for JsonFormatter {
    fn default() -> Self {
        Self::new()
    }
}

impl PayloadFormatter for JsonFormatter {
    fn format_event(&self, event: &ChainEvent) -> Result<String> {
        let mut payload = event.payload.clone();
        
        // Add metadata if requested
        if self.include_metadata {
            payload["ulid"] = Value::String(event.ulid.to_string());
            payload["event_type"] = serde_json::to_value(&event.event_type).unwrap();
            payload["event_time_ms"] = Value::Number(serde_json::Number::from(event.processing_info.event_time_ms));
        }
        
        // Add custom fields
        for (key, value) in &self.custom_fields {
            payload[key] = Value::String(value.clone());
        }
        
        Ok(serde_json::to_string(&payload)?)
    }
    
    fn format_batch(&self, events: &[ChainEvent]) -> Result<Option<String>> {
        let formatted_events: Result<Vec<Value>> = events
            .iter()
            .map(|event| {
                let event_str = self.format_event(event)?;
                Ok(serde_json::from_str(&event_str)?)
            })
            .collect();
        
        let batch = serde_json::json!({
            "events": formatted_events?,
            "batch_size": events.len(),
            "batch_timestamp": chrono::Utc::now().to_rfc3339()
        });
        
        Ok(Some(serde_json::to_string(&batch)?))
    }
}

/// Form-encoded payload formatter
pub struct FormFormatter {
    field_mappings: HashMap<String, String>,
}

impl FormFormatter {
    pub fn new() -> Self {
        Self {
            field_mappings: HashMap::new(),
        }
    }
    
    pub fn with_mapping(mut self, form_field: impl Into<String>, event_field: impl Into<String>) -> Self {
        self.field_mappings.insert(form_field.into(), event_field.into());
        self
    }
}

impl PayloadFormatter for FormFormatter {
    fn format_event(&self, event: &ChainEvent) -> Result<String> {
        let mut params = Vec::new();
        
        for (form_field, event_field) in &self.field_mappings {
            if let Some(value) = event.payload.get(event_field) {
                let value_str = match value {
                    Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                // TODO: Add urlencoding dependency for proper form encoding
                params.push(format!("{}={}", form_field, value_str));
            }
        }
        
        Ok(params.join("&"))
    }
}