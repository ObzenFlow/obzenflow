// Event types for FLOWIP-007 enhanced schema
// These types support the rich ChainEvent schema with CHAIN maturity model support

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use ulid::Ulid;

// Re-export WriterId from event_store since it already exists
pub use crate::event_store::WriterId;

// ===== Time-related type aliases for clarity =====

/// Milliseconds since Unix epoch (for event timestamps)
pub type TimestampMs = u64;

/// Duration in milliseconds (for processing times)
pub type DurationMs = u64;

// ===== Strong Types for Application Layer =====

/// Strong typing for flow identification
pub type FlowId = String;

/// Helper functions for FlowId
pub fn new_flow_id(flow_name: &str) -> FlowId {
    format!("{}_{}", flow_name, Ulid::new())
}

/// Strong typing for stage names
pub type StageName = String;

/// Helper function to convert stage name to display name for UI
pub fn stage_name_to_display(name: &str) -> String {
    name.replace('_', " ").split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Strong typing for event types
pub type EventType = String;

/// Strong typing for correlation IDs (traces related events through the pipeline)
pub type CorrelationId = String;

/// Strong typing for event IDs (unique identifier for each event)
pub type EventId = String;

/// Helper function to create a new correlation ID
pub fn new_correlation_id() -> CorrelationId {
    Ulid::new().to_string()
}

// ===== Core Enums =====

/// Stage type classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageType {
    Source,
    Transform, 
    Sink,
    FanOut,
    FanIn,
}

impl StageType {
    /// Check if this stage type is a flow entry point
    pub fn is_source(&self) -> bool {
        matches!(self, StageType::Source)
    }
    
    /// Check if this stage type is a flow exit point
    pub fn is_sink(&self) -> bool {
        matches!(self, StageType::Sink)
    }
    
    /// Check if this stage type handles flow boundaries
    pub fn is_boundary(&self) -> bool {
        self.is_source() || self.is_sink()
    }
}

/// Flow boundary types for monitoring
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BoundaryType {
    FlowEntry,  // Source stage - flow entry point
    FlowExit,   // Sink stage - flow exit point
}

/// Processing outcome - what actually happened (past tense facts only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProcessingOutcome {
    /// Successfully processed
    Succeeded {
        /// Quality indicator (0.0-1.0) for SLI computation
        /// 1.0 = perfect quality, 0.0 = barely acceptable
        quality_score: Option<f64>,
        /// Met service level expectations
        sla_compliant: bool,
    },
    
    /// Failed to process
    Failed { 
        error_type: String,
        retryable: bool,
        /// How this failure impacts SLIs
        sli_impact: SLIImpact,
    },
    
    /// Skipped processing (intentionally not processed)
    Skipped { 
        skip_reason: SkipReason,
        /// Whether this skip affects SLI calculations
        affects_sli: bool,
    },
}

impl ProcessingOutcome {
    /// Create a perfect success outcome (quality=1.0, SLA compliant)
    pub fn perfect() -> Self {
        ProcessingOutcome::Succeeded {
            quality_score: Some(1.0),
            sla_compliant: true,
        }
    }
    
    /// Create a basic success outcome (no quality score, SLA compliant)
    pub fn success() -> Self {
        ProcessingOutcome::Succeeded {
            quality_score: None,
            sla_compliant: true,
        }
    }
    
    /// Create a degraded success outcome (quality < 1.0, may not be SLA compliant)
    pub fn degraded(quality: f64) -> Self {
        ProcessingOutcome::Succeeded {
            quality_score: Some(quality),
            sla_compliant: quality >= 0.9, // Auto-determine SLA compliance
        }
    }
}

/// SLI impact classification for failures
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SLIImpact {
    AffectsAvailability,  // Counts against error budget
    AffectsLatency,       // Degrades latency percentiles
    NoImpact,             // Client error, not our fault
}

/// Structured reasons for skipping processing
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkipReason {
    Duplicate,           // Already processed this event
    FilteredOut,         // Didn't match stage filters
    CircuitBreakerOpen,  // Stage temporarily disabled
    RateLimited,         // Exceeded rate limits
    InvalidInput,        // Input validation failed
    PolicyDenied,        // Policy engine rejected
}

// ===== Core Structs =====

/// Causality relationships between events (application-level references)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CausalityInfo {
    /// Parent events that directly caused this one
    /// Single parent for most cases, multiple for fan-in scenarios
    pub parent_ids: Vec<Ulid>,
}

/// Flow and stage identification
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowContext {
    /// Human-readable flow name (e.g., "crypto_analysis")
    pub flow_name: String,
    
    /// Unique flow instance ID (e.g., "crypto_analysis_01HKGX7M...")
    pub flow_id: FlowId,
    
    /// Stage name within the flow (e.g., "price_analyzer")
    pub stage_name: StageName,
    
    /// Stage type classification
    pub stage_type: StageType,
}

/// Processing metadata for monitoring and debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessingInfo {
    /// Which stage processed this event (for monitoring aggregation)
    /// Always the stage name without worker suffix, even for parallel workers
    /// Example: "price_analyzer" (not "price_analyzer_worker_2")
    pub processed_by: StageName,
    
    /// Processing duration in milliseconds
    pub processing_time_ms: DurationMs,
    
    /// Monitoring taxonomy used for this stage (stored as name since Taxonomy is a trait)
    pub taxonomy: Option<String>,
    
    /// When this event occurred (milliseconds since Unix epoch)
    /// This is event time, not processing time
    pub event_time_ms: TimestampMs,
    
    /// Stage position in flow (for visualization)
    pub stage_position: Option<u32>,
    
    /// Processing outcome (for SLI tracking)
    pub outcome: ProcessingOutcome,
    
    /// ESSENTIAL for FLOWIP-005 boundary tracking
    pub is_boundary_event: Option<BoundaryType>,
    
    /// Correlation ID for tracing related events through the pipeline
    pub correlation_id: Option<CorrelationId>,
}

// ===== Intent Model (CHAIN I1-I4) =====

/// Comprehensive Intent model based on security industry patterns
/// Supports progressive enhancement from I1 to I4 maturity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Intent {
    // === Core Intent (Level I1) ===
    /// Human-readable purpose (required for I1)
    pub purpose: String,
    
    /// Flow execution context (critical for understanding intent)
    pub flow_mode: FlowMode,
    
    /// Processing guarantees (from stream processing patterns)
    pub processing_guarantee: ProcessingGuarantee,
    
    // === Structured Goals (Level I2) ===
    /// Machine-readable goals with validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub goals: Option<serde_json::Value>,
    
    // === Validation & Outcomes (Level I3) ===
    /// Expected outcomes for validation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation: Option<serde_json::Value>,
    
    // === Advanced Features (Level I4) ===
    /// Policy-based controls
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policies: Option<serde_json::Value>,
}

/// Flow execution modes - critical context for intent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowMode {
    /// One-time batch processing (e.g., CSV import)
    BatchOneOff { 
        source: SourceType,
        completion_criteria: CompletionCriteria,
    },
    /// Continuous stream processing (e.g., live data feed)
    ContinuousStream { 
        source: SourceType,
        termination_condition: Option<TerminationCondition>,
    },
    /// Scheduled batch jobs (e.g., nightly reports)
    ScheduledBatch { 
        schedule: Schedule,
        source: SourceType,
    },
}

/// Source types that inform processing intent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SourceType {
    CsvFile { path: String },
    HttpEndpoint { url: String, method: HttpMethod },
    WebSocket { endpoint: String },
    Kafka { topic: String, consumer_group: String },
    Database { connection_string: String, query: String },
    FileWatch { path: String, pattern: String },
}

/// Processing guarantees (from Kafka/Flink patterns)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProcessingGuarantee {
    AtLeastOnce,    // May process duplicates
    ExactlyOnce,    // No duplicates, no losses
    BestEffort,     // No guarantees
}

/// Completion criteria for batch processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CompletionCriteria {
    AllRowsProcessed,
    TimeLimit { seconds: u64 },
    RowLimit { max_rows: usize },
    ErrorThreshold { max_errors: usize },
}

/// Termination conditions for continuous streams
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TerminationCondition {
    Never,  // Run forever
    After { duration_seconds: u64 },
    AtTime { unix_timestamp: u64 },
    OnSignal { signal_name: String },
}

/// Schedule for batch jobs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Schedule {
    Daily { hour: u8, minute: u8 },
    Hourly { minute: u8 },
    Every { seconds: u64 },
    Cron { expression: String },
}

/// HTTP methods for endpoints
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HttpMethod {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
}

// ===== EventExtensions for Future CHAIN Levels =====

/// Extensions for future CHAIN maturity levels
/// All fields are optional to maintain backward compatibility
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventExtensions {
    // === History H3-H4 ===
    /// Hash of previous event for chain integrity
    #[serde(skip_serializing_if = "Option::is_none")]
    pub previous_hash: Option<EventHash>,
    
    /// Reference to snapshot for time-travel queries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_ref: Option<SnapshotReference>,
    
    // === Agency A2-A4 ===
    /// Enhanced identity information
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity: Option<IdentityInfo>,
    
    /// Policy enforcement decision
    #[serde(skip_serializing_if = "Option::is_none")]
    pub policy_decision: Option<PolicyDecision>,
    
    // === Intent I2-I4 ===
    /// Validation of intent against outcomes
    #[serde(skip_serializing_if = "Option::is_none")]
    pub intent_validation_result: Option<IntentValidationResult>,
    
    // === Monitoring Support ===
    /// Optional hints for monitoring systems
    #[serde(skip_serializing_if = "Option::is_none")]
    pub monitoring_hints: Option<MonitoringHints>,
}

impl EventExtensions {
    /// Check if all extension fields are empty
    pub fn is_empty(&self) -> bool {
        self.previous_hash.is_none() &&
        self.snapshot_ref.is_none() &&
        self.identity.is_none() &&
        self.policy_decision.is_none() &&
        self.intent_validation_result.is_none() &&
        self.monitoring_hints.is_none()
    }
}

/// Optional monitoring hints for visualization and alerting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringHints {
    /// Current queue depth (for visualization)
    pub queue_depth: Option<u32>,
    
    /// Stage metrics snapshot
    pub metrics_snapshot: Option<HashMap<String, f64>>,
    
    /// Important dimensions for queries
    pub key_dimensions: Vec<String>,
}

/// Strong typing for event hashes
pub type EventHash = String;

/// Strong typing for policy IDs
pub type PolicyId = String;

/// Strong typing for identity providers
pub type IdentityProvider = String;

/// Supporting types for extensions (defined as project matures)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotReference {
    pub snapshot_id: Ulid,
    pub timestamp: TimestampMs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub provider: IdentityProvider,
    pub subject: String,  // This can stay string as it's external data
    pub verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyDecision {
    pub policy_id: PolicyId,
    pub decision: PolicyDecisionType,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecisionType {
    Allow,
    Deny,
    Defer,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntentValidationResult {
    pub validated_at: TimestampMs,
    pub outcome_matched: bool,
    pub divergence_reason: Option<String>,
}


