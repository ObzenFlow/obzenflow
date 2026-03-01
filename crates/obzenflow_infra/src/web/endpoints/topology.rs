// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Topology HTTP endpoint for flow visualization
//!
//! Provides `/api/topology` endpoint that returns the flow structure
//! for visualization in UI tools.

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response, WebError};
use obzenflow_core::StageId;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_topology::EdgeKind;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

type StageMetadataMap = std::collections::HashMap<StageId, StageMetadata>;
type MiddlewareStacks = Arc<std::collections::HashMap<StageId, MiddlewareStackConfig>>;
type ContractAttachments = Arc<std::collections::HashMap<(StageId, StageId), Vec<String>>>;
type JoinMetadataMap =
    Arc<std::collections::HashMap<StageId, obzenflow_runtime::pipeline::JoinMetadata>>;

/// JSON representation of flow topology for the API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTopologyResponse {
    pub flow_name: String,
    /// Topology API schema version (for clients to handle evolution)
    pub version: String,
    pub stages: Vec<StageApiInfo>,
    pub edges: Vec<EdgeApiInfo>,
}

/// Stage information for API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageApiInfo {
    pub stage_id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub stage_type: String,
    pub status: String,
    /// Semantic stage type from topology (finite_source, join, sink, ...)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_type: Option<String>,
    /// Connection role derived from StageType (producer/processor/consumer)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Whether this stage participates in a cycle (SCC) in the topology graph.
    ///
    /// Derived from `obzenflow_topology::Topology::is_in_cycle`, which uses
    /// Tarjan's algorithm during topology construction and caches membership
    /// in a HashSet for O(1) lookup.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_cycle_member: Option<bool>,
    /// Middleware observability info for this stage (FLOWIP-059).
    ///
    /// Currently a placeholder; populated once middleware introspection is wired.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub middleware: Option<MiddlewareApiInfo>,

    /// Join-specific metadata (only present when semantic_type == "join")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_metadata: Option<JoinMetadataApiInfo>,
}

/// Join-specific metadata for a stage (FLOWIP-082a).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinMetadataApiInfo {
    /// Stage IDs (base32 ULID strings) whose outputs are catalog/reference inputs.
    pub catalog_sources: Vec<String>,
    /// Stage IDs (base32 ULID strings) whose outputs are stream inputs.
    pub stream_sources: Vec<String>,
}

/// Middleware information for a stage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareApiInfo {
    /// Ordered list of middleware names in the stack
    pub stack: Vec<String>,
    /// Circuit breaker configuration (static)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub circuit_breaker: Option<CircuitBreakerApiInfo>,
    /// Rate limiter configuration (static)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limiter: Option<RateLimiterApiInfo>,
    /// Retry configuration (static)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryApiInfo>,
}

/// Circuit breaker configuration (structural only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerApiInfo {
    /// Number of failures before opening
    pub threshold: usize,
    /// Cooldown period before half-open (milliseconds)
    pub cooldown_ms: u64,
    /// Policy when circuit is open
    pub open_policy: OpenPolicyType,
    /// Whether a fallback handler is configured
    pub has_fallback: bool,
}

/// Circuit breaker open policy type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenPolicyType {
    EmitFallback,
    FailFast,
    Skip,
}

/// Rate limiter configuration (structural only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimiterApiInfo {
    /// Maximum tokens per second
    pub tokens_per_sec: f64,
    /// Burst capacity (max tokens in bucket)
    pub burst_capacity: f64,
}

/// Retry policy configuration (structural only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryApiInfo {
    /// Maximum retry attempts (None = infinite)
    pub max_attempts: Option<usize>,
    /// Backoff strategy
    pub backoff: BackoffType,
    /// Base delay for backoff (milliseconds)
    pub base_delay_ms: Option<u64>,
}

/// Retry backoff strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffType {
    Fixed,
    Exponential,
    None,
}

/// Edge information for API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeApiInfo {
    pub from: String,
    pub to: String,
    /// Edge operator semantics (`|>` vs `<|`)
    pub kind: String,
    /// Legacy field for edge throughput; always None.
    /// All runtime metrics are exported via /metrics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_per_sec: Option<f64>,
    /// Structural contract information attached to this edge
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contracts: Option<Vec<ContractApiInfo>>,
}

/// Contract configuration for an edge (structural only)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractApiInfo {
    /// Contract name (e.g. "TransportContract", "SourceContract")
    pub name: String,
    /// Optional serialized configuration for the contract
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<serde_json::Value>,
}

/// Re-export MiddlewareStackConfig from runtime_services for the web layer
pub use obzenflow_runtime::pipeline::MiddlewareStackConfig;

/// HTTP endpoint that serves flow topology
pub struct TopologyHttpEndpoint {
    topology: Arc<obzenflow_topology::Topology>,
    stages_metadata: Arc<StageMetadataMap>,
    flow_name: String,
    /// Structural middleware config per stage (FLOWIP-059)
    middleware_stacks: Option<MiddlewareStacks>,
    contract_attachments: Option<ContractAttachments>,
    /// Join metadata per stage (FLOWIP-082a)
    join_metadata: Option<JoinMetadataMap>,
}

/// Additional metadata about stages (type and status)
#[derive(Debug, Clone)]
pub struct StageMetadata {
    pub stage_type: StageType,
    pub status: StageStatus,
}

/// Stage types for visualization
/// Note: Broadcast and Merge are implemented as Transform handlers in runtime,
/// but tracked separately here for proper UI visualization
#[derive(Debug, Clone)]
pub enum StageType {
    Source,    // Simplified - UI doesn't need to distinguish finite/infinite
    Transform, // Regular transform operations
    Sink,
    Broadcast, // One-to-many distribution (implemented as Transform)
    Merge,     // Many-to-one aggregation (implemented as Transform)
}

impl StageType {
    fn as_str(&self) -> &str {
        match self {
            StageType::Source => "source",
            StageType::Transform => "transform",
            StageType::Sink => "sink",
            StageType::Broadcast => "broadcast",
            StageType::Merge => "merge",
        }
    }
}

#[derive(Debug, Clone)]
pub enum StageStatus {
    Running,
    Completed,
    Failed,
    Pending,
}

impl StageStatus {
    fn as_str(&self) -> &str {
        match self {
            StageStatus::Running => "running",
            StageStatus::Completed => "completed",
            StageStatus::Failed => "failed",
            StageStatus::Pending => "pending",
        }
    }
}

/// Flags for optional sections controlled via query params
#[derive(Debug, Default, Clone, Copy)]
struct IncludeFlags {
    middleware: bool,
    contracts: bool,
}

fn parse_include_flags(_request: &Request) -> IncludeFlags {
    // FLOWIP-059 spec change:
    // The topology endpoint now includes all structural observability
    // sections (middleware + contracts) by default. The `include`
    // query parameter is accepted for backwards compatibility but
    // no longer gates payload sections.
    //
    // We still parse the parameter so existing clients that send it
    // don't break, but we treat the absence/presence as equivalent.
    IncludeFlags {
        middleware: true,
        contracts: true,
    }
}

impl TopologyHttpEndpoint {
    /// Create a new topology endpoint
    pub fn new(
        topology: Arc<obzenflow_topology::Topology>,
        stages_metadata: Arc<StageMetadataMap>,
        flow_name: String,
        middleware_stacks: Option<MiddlewareStacks>,
        contract_attachments: Option<ContractAttachments>,
        join_metadata: Option<JoinMetadataMap>,
    ) -> Self {
        Self {
            topology,
            stages_metadata,
            flow_name,
            middleware_stacks,
            contract_attachments,
            join_metadata,
        }
    }

    /// Convert internal topology to API response format
    fn build_response(&self, include: IncludeFlags) -> FlowTopologyResponse {
        let _ = (include.middleware, include.contracts);
        let stages: Vec<StageApiInfo> = self
            .topology
            .stages()
            .map(|stage_info| {
                // Convert topology StageId to core StageId for HashMap lookup
                let core_stage_id = StageId::from_ulid(stage_info.id.ulid());
                let metadata = self.stages_metadata.get(&core_stage_id);
                let middleware =
                    self.middleware_stacks
                        .as_ref()
                        .and_then(|stacks| stacks.get(&core_stage_id))
                        .map(|config| {
                            // Deserialize circuit breaker config from JSON snapshot
                            let circuit_breaker = config.circuit_breaker.as_ref().and_then(|v| {
                                serde_json::from_value::<CircuitBreakerApiInfo>(v.clone()).ok()
                            });

                            // Deserialize rate limiter config from JSON snapshot
                            let rate_limiter = config.rate_limiter.as_ref().and_then(|v| {
                                serde_json::from_value::<RateLimiterApiInfo>(v.clone()).ok()
                            });

                            // Deserialize retry config from JSON snapshot
                            let retry = config.retry.as_ref().and_then(|v| {
                                serde_json::from_value::<RetryApiInfo>(v.clone()).ok()
                            });

                            MiddlewareApiInfo {
                                stack: config.stack.clone(),
                                circuit_breaker,
                                rate_limiter,
                                retry,
                            }
                        });

                let semantic_type_str = stage_info.stage_type.as_str().to_string();
                let join_metadata = if semantic_type_str == "join" {
                    self.join_metadata
                        .as_ref()
                        .and_then(|map| map.get(&core_stage_id))
                        .map(|meta| JoinMetadataApiInfo {
                            // Use topology StageId formatting so these
                            // strings match the stage_id / edge ids.
                            catalog_sources: meta
                                .catalog_source_ids
                                .iter()
                                .map(|id| id.to_topology_id().to_string())
                                .collect(),
                            stream_sources: meta
                                .stream_source_ids
                                .iter()
                                .map(|id| id.to_topology_id().to_string())
                                .collect(),
                        })
                } else {
                    None
                };

                StageApiInfo {
                    stage_id: stage_info.id.to_string(),
                    name: stage_info.name.clone(),
                    stage_type: metadata
                        .map(|m| m.stage_type.as_str().to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    status: metadata
                        .map(|m| m.status.as_str().to_string())
                        .unwrap_or_else(|| "pending".to_string()),
                    semantic_type: Some(semantic_type_str),
                    role: Some(stage_info.stage_type.role().to_string()),
                    is_cycle_member: Some(self.topology.is_in_cycle(stage_info.id)),
                    middleware,
                    join_metadata,
                }
            })
            .collect();

        let edges: Vec<EdgeApiInfo> = self
            .topology
            .edges()
            .iter()
            .map(|edge| {
                // Convert topology IDs to core StageIds for lookup
                let from_core = StageId::from_ulid(edge.from.ulid());
                let to_core = StageId::from_ulid(edge.to.ulid());

                let contracts = self
                    .contract_attachments
                    .as_ref()
                    .and_then(|map| map.get(&(from_core, to_core)))
                    .map(|names| {
                        names
                            .iter()
                            .cloned()
                            .map(|name| ContractApiInfo { name, config: None })
                            .collect::<Vec<_>>()
                    });

                EdgeApiInfo {
                    from: edge.from.to_string(),
                    to: edge.to.to_string(),
                    // FLOWIP-059: Expose semantic kind labels instead of DSL operators.
                    // API clients should see "forward" / "backward" to reconstruct EdgeKind.
                    kind: match edge.kind {
                        EdgeKind::Forward => "forward".to_string(),
                        EdgeKind::Backward => "backward".to_string(),
                    },
                    // Topology endpoint remains structural; throughput is handled
                    // by the metrics pipeline and remains unset here.
                    events_per_sec: None,
                    contracts,
                }
            })
            .collect();

        FlowTopologyResponse {
            flow_name: self.flow_name.clone(),
            // FLOWIP-082a: Topology API schema version 0.3 includes join_metadata.
            version: "0.3".to_string(),
            stages,
            edges,
        }
    }
}

#[async_trait]
impl HttpEndpoint for TopologyHttpEndpoint {
    fn path(&self) -> &str {
        "/api/topology"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, request: Request) -> Result<Response, WebError> {
        let include = parse_include_flags(&request);
        let topology_response = self.build_response(include);

        let json_body = serde_json::to_string(&topology_response).map_err(|e| {
            WebError::RequestHandlingFailed {
                message: format!("Failed to serialize topology: {e}"),
                source: None,
            }
        })?;

        let mut response = Response::ok();
        response
            .headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        response
            .headers
            .insert("Cache-Control".to_string(), "max-age=60".to_string());
        response.body = json_body.into_bytes();

        Ok(response)
    }
}
