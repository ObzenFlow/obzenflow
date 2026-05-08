// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Topology HTTP endpoint for flow visualization
//!
//! Provides `/api/topology` endpoint that returns the flow structure
//! for visualization in UI tools.

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use obzenflow_core::StageId;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::typing::{StageTypingInfo, TypeHintInfo};
use obzenflow_topology::{DirectedEdge, EdgeKind, StageType as TopologyStageType};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

type StageMetadataMap = std::collections::HashMap<StageId, StageMetadata>;
type MiddlewareStacks = Arc<std::collections::HashMap<StageId, MiddlewareStackConfig>>;
type ContractAttachments = Arc<std::collections::HashMap<(StageId, StageId), Vec<String>>>;
type JoinMetadataMap =
    Arc<std::collections::HashMap<StageId, obzenflow_runtime::pipeline::JoinMetadata>>;
type StageTypingMap = Arc<std::collections::HashMap<StageId, StageTypingInfo>>;
type StageSubgraphMembershipMap = Arc<
    std::collections::HashMap<
        StageId,
        obzenflow_core::topology::subgraphs::StageSubgraphMembership,
    >,
>;
type SubgraphRegistry = Arc<Vec<obzenflow_core::topology::subgraphs::TopologySubgraphInfo>>;

/// JSON representation of flow topology for the API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTopologyResponse {
    pub flow_name: String,
    /// Topology API schema version (for clients to handle evolution)
    pub version: String,
    pub stages: Vec<StageApiInfo>,
    pub edges: Vec<EdgeApiInfo>,
    pub subgraphs: Vec<SubgraphApiInfo>,
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

    /// Authoring-time stage typing contract (FLOWIP-114b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typing: Option<StageTypingApiInfo>,

    /// Logical subgraph membership for this stage (FLOWIP-086z-part-2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subgraph: Option<StageSubgraphApiInfo>,
}

/// Stage typing contract exposed by the topology API (FLOWIP-114b).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageTypingApiInfo {
    pub input_type: TypeHintApiInfo,
    pub output_type: TypeHintApiInfo,
    pub boundary_in_type: TypeHintApiInfo,
    pub boundary_out_type: TypeHintApiInfo,
    pub reference_type: TypeHintApiInfo,
    pub stream_type: TypeHintApiInfo,
    pub is_placeholder: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder_message: Option<String>,
}

/// Type hint exposed by the topology API (FLOWIP-114b).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeHintApiInfo {
    pub kind: TypeHintKindApi,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeHintKindApi {
    Unspecified,
    Exact,
    Mixed,
}

/// Stage-level membership facts for logical subgraphs (FLOWIP-086z-part-2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageSubgraphApiInfo {
    pub subgraph_id: String,
    pub kind: String,
    pub binding: String,
    pub role: String,
    pub order: u16,
    pub is_entry: bool,
    pub is_exit: bool,
}

/// Graph-level registry entry describing one logical subgraph (FLOWIP-086z-part-2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubgraphApiInfo {
    pub subgraph_id: String,
    pub kind: String,
    pub binding: String,
    pub label: String,
    pub member_stage_ids: Vec<String>,
    pub internal_edges: Vec<SubgraphInternalEdgeApiInfo>,
    pub entry_stage_ids: Vec<String>,
    pub exit_stage_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_subgraph_id: Option<String>,
    pub collapsible: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubgraphInternalEdgeApiInfo {
    pub from_stage_id: String,
    pub to_stage_id: String,
    pub role: String,
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
    /// Effective burst capacity used by runtime admission
    pub burst_capacity: f64,
    /// Raw configured burst capacity, if the user set one explicitly
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configured_burst_capacity: Option<f64>,
    /// Tokens consumed per admitted event
    pub cost_per_event: f64,
    /// Effective event-rate limit after weighting (`tokens_per_sec / cost_per_event`)
    pub limit_rate: f64,
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
    /// Derived edge payload typing projection (FLOWIP-114b).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub typing: Option<EdgeTypingApiInfo>,
}

/// Edge payload typing projection exposed by the topology API (FLOWIP-114b).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EdgeTypingApiInfo {
    pub role: EdgeTypingRoleApi,
    pub label_source: EdgeTypingLabelSourceApi,
    pub payload_type: TypeHintApiInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTypingRoleApi {
    Input,
    Reference,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EdgeTypingLabelSourceApi {
    UpstreamOutputType,
    DownstreamInputType,
    DownstreamReferenceType,
    DownstreamStreamType,
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
    /// Stage typing contracts per stage (FLOWIP-114b)
    stage_typing: Option<StageTypingMap>,
    /// Per-stage logical subgraph membership (FLOWIP-086z-part-2)
    subgraph_membership: Option<StageSubgraphMembershipMap>,
    /// Subgraph registry (FLOWIP-086z-part-2)
    subgraphs: Option<SubgraphRegistry>,
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

impl From<&StageTypingInfo> for StageTypingApiInfo {
    fn from(value: &StageTypingInfo) -> Self {
        Self {
            input_type: (&value.input_type).into(),
            output_type: (&value.output_type).into(),
            boundary_in_type: (&value.boundary_in_type).into(),
            boundary_out_type: (&value.boundary_out_type).into(),
            reference_type: (&value.reference_type).into(),
            stream_type: (&value.stream_type).into(),
            is_placeholder: value.is_placeholder,
            placeholder_message: value.placeholder_message.clone(),
        }
    }
}

impl From<&TypeHintInfo> for TypeHintApiInfo {
    fn from(value: &TypeHintInfo) -> Self {
        match value {
            TypeHintInfo::Unspecified => Self {
                kind: TypeHintKindApi::Unspecified,
                name: None,
                display_name: None,
            },
            TypeHintInfo::Exact { name } => Self {
                kind: TypeHintKindApi::Exact,
                name: Some(name.clone()),
                display_name: Some(display_name_for_type(name)),
            },
            TypeHintInfo::Mixed => Self {
                kind: TypeHintKindApi::Mixed,
                name: None,
                display_name: None,
            },
        }
    }
}

fn edge_typing_from_hint(
    role: EdgeTypingRoleApi,
    label_source: EdgeTypingLabelSourceApi,
    payload_type: &TypeHintInfo,
) -> Option<EdgeTypingApiInfo> {
    if matches!(payload_type, TypeHintInfo::Unspecified) {
        return None;
    }

    Some(EdgeTypingApiInfo {
        role,
        label_source,
        payload_type: payload_type.into(),
    })
}

fn display_name_for_type(name: &str) -> String {
    split_camel_case(&strip_rust_path_qualifiers(name))
}

fn strip_rust_path_qualifiers(name: &str) -> String {
    let mut result = String::new();
    let mut token = String::new();

    for ch in name.trim().chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
            token.push(ch);
        } else {
            push_final_path_segment(&mut result, &token);
            token.clear();
            result.push(ch);
        }
    }
    push_final_path_segment(&mut result, &token);

    result
}

fn push_final_path_segment(result: &mut String, token: &str) {
    if token.is_empty() {
        return;
    }

    result.push_str(token.rsplit("::").next().unwrap_or(token));
}

fn split_camel_case(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut result = String::with_capacity(name.len());

    for (index, ch) in chars.iter().copied().enumerate() {
        if ch == '_' {
            if !result.ends_with(' ') {
                result.push(' ');
            }
            continue;
        }

        if index > 0 && should_insert_type_word_space(&chars, index) && !result.ends_with(' ') {
            result.push(' ');
        }
        result.push(ch);
    }

    result
}

fn should_insert_type_word_space(chars: &[char], index: usize) -> bool {
    let previous = chars[index - 1];
    let current = chars[index];
    let next = chars.get(index + 1).copied();

    if !current.is_ascii_uppercase() {
        return false;
    }

    previous.is_ascii_lowercase()
        || previous.is_ascii_digit()
        || (previous.is_ascii_uppercase() && next.is_some_and(|ch| ch.is_ascii_lowercase()))
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        topology: Arc<obzenflow_topology::Topology>,
        stages_metadata: Arc<StageMetadataMap>,
        flow_name: String,
        middleware_stacks: Option<MiddlewareStacks>,
        contract_attachments: Option<ContractAttachments>,
        join_metadata: Option<JoinMetadataMap>,
        stage_typing: Option<StageTypingMap>,
        subgraph_membership: Option<StageSubgraphMembershipMap>,
        subgraphs: Option<SubgraphRegistry>,
    ) -> Self {
        Self {
            topology,
            stages_metadata,
            flow_name,
            middleware_stacks,
            contract_attachments,
            join_metadata,
            stage_typing,
            subgraph_membership,
            subgraphs,
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

                let subgraph = self
                    .subgraph_membership
                    .as_ref()
                    .and_then(|map| map.get(&core_stage_id))
                    .map(|membership| StageSubgraphApiInfo {
                        subgraph_id: membership.subgraph_id.clone(),
                        kind: membership.kind.clone(),
                        binding: membership.binding.clone(),
                        role: membership.role.clone(),
                        order: membership.order,
                        is_entry: membership.is_entry,
                        is_exit: membership.is_exit,
                    });

                let typing = self
                    .stage_typing
                    .as_ref()
                    .and_then(|map| map.get(&core_stage_id))
                    .map(StageTypingApiInfo::from);

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
                    typing,
                    subgraph,
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
                let typing = self.derive_edge_typing(edge);

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
                    typing,
                }
            })
            .collect();

        FlowTopologyResponse {
            flow_name: self.flow_name.clone(),
            // FLOWIP-114b: Topology API schema version 0.5 includes stage and edge typing.
            version: "0.5".to_string(),
            stages,
            edges,
            subgraphs: self
                .subgraphs
                .as_ref()
                .map(|list| {
                    list.iter()
                        .cloned()
                        .map(|info| SubgraphApiInfo {
                            subgraph_id: info.subgraph_id,
                            kind: info.kind,
                            binding: info.binding,
                            label: info.label,
                            member_stage_ids: info
                                .member_stage_ids
                                .iter()
                                .map(|id| id.to_topology_id().to_string())
                                .collect(),
                            internal_edges: info
                                .internal_edges
                                .iter()
                                .map(|edge| SubgraphInternalEdgeApiInfo {
                                    from_stage_id: edge.from_stage_id.to_topology_id().to_string(),
                                    to_stage_id: edge.to_stage_id.to_topology_id().to_string(),
                                    role: edge.role.clone(),
                                })
                                .collect(),
                            entry_stage_ids: info
                                .entry_stage_ids
                                .iter()
                                .map(|id| id.to_topology_id().to_string())
                                .collect(),
                            exit_stage_ids: info
                                .exit_stage_ids
                                .iter()
                                .map(|id| id.to_topology_id().to_string())
                                .collect(),
                            parent_subgraph_id: info.parent_subgraph_id,
                            collapsible: info.collapsible,
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    fn derive_edge_typing(&self, edge: &DirectedEdge) -> Option<EdgeTypingApiInfo> {
        if edge.kind != EdgeKind::Forward {
            return None;
        }

        let from_core = StageId::from_ulid(edge.from.ulid());
        let to_core = StageId::from_ulid(edge.to.ulid());

        if self.is_join_stage(edge.to) {
            return self.derive_join_edge_typing(from_core, to_core);
        }

        let stage_typing = self.stage_typing.as_ref()?;

        if let Some(upstream_typing) = stage_typing.get(&from_core) {
            if let Some(typing) = edge_typing_from_hint(
                EdgeTypingRoleApi::Input,
                EdgeTypingLabelSourceApi::UpstreamOutputType,
                &upstream_typing.output_type,
            ) {
                return Some(typing);
            }
        }

        if self.forward_upstream_count(edge.to) == 1 {
            return stage_typing.get(&to_core).and_then(|downstream_typing| {
                edge_typing_from_hint(
                    EdgeTypingRoleApi::Input,
                    EdgeTypingLabelSourceApi::DownstreamInputType,
                    &downstream_typing.input_type,
                )
            });
        }

        None
    }

    fn derive_join_edge_typing(
        &self,
        from_core: StageId,
        join_core: StageId,
    ) -> Option<EdgeTypingApiInfo> {
        let join_metadata = self
            .join_metadata
            .as_ref()
            .and_then(|map| map.get(&join_core))?;
        let join_typing = self
            .stage_typing
            .as_ref()
            .and_then(|map| map.get(&join_core))?;

        if join_metadata.catalog_source_ids.contains(&from_core) {
            return edge_typing_from_hint(
                EdgeTypingRoleApi::Reference,
                EdgeTypingLabelSourceApi::DownstreamReferenceType,
                &join_typing.reference_type,
            );
        }

        if join_metadata.stream_source_ids.contains(&from_core) {
            return edge_typing_from_hint(
                EdgeTypingRoleApi::Stream,
                EdgeTypingLabelSourceApi::DownstreamStreamType,
                &join_typing.stream_type,
            );
        }

        None
    }

    fn is_join_stage(&self, stage_id: obzenflow_topology::StageId) -> bool {
        self.topology
            .stage_info(stage_id)
            .is_some_and(|info| info.stage_type == TopologyStageType::Join)
    }

    fn forward_upstream_count(&self, stage_id: obzenflow_topology::StageId) -> usize {
        self.topology
            .edges()
            .iter()
            .filter(|edge| edge.to == stage_id && edge.kind == EdgeKind::Forward)
            .count()
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

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
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

        Ok(response.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::topology::subgraphs::{
        StageSubgraphMembership, SubgraphInternalEdge, TopologySubgraphInfo,
    };
    use obzenflow_runtime::id_conversions::StageIdExt;
    use obzenflow_runtime::pipeline::JoinMetadata;
    use obzenflow_runtime::pipeline::MiddlewareStackConfig;
    use obzenflow_topology::TopologyBuilder;
    use std::collections::HashMap;

    fn core_id(stage_id: obzenflow_topology::StageId) -> StageId {
        StageId::from_ulid(stage_id.ulid())
    }

    fn exact(name: &str) -> TypeHintInfo {
        TypeHintInfo::Exact {
            name: name.to_string(),
        }
    }

    fn stage_typing(
        input_type: TypeHintInfo,
        output_type: TypeHintInfo,
        reference_type: TypeHintInfo,
        stream_type: TypeHintInfo,
    ) -> StageTypingInfo {
        StageTypingInfo {
            input_type,
            output_type,
            boundary_in_type: TypeHintInfo::Unspecified,
            boundary_out_type: TypeHintInfo::Unspecified,
            reference_type,
            stream_type,
            is_placeholder: false,
            placeholder_message: None,
        }
    }

    fn stage_metadata(ids: &[StageId]) -> HashMap<StageId, StageMetadata> {
        ids.iter()
            .copied()
            .map(|id| {
                (
                    id,
                    StageMetadata {
                        stage_type: StageType::Transform,
                        status: StageStatus::Running,
                    },
                )
            })
            .collect()
    }

    fn edge_between<'a>(
        response: &'a FlowTopologyResponse,
        from: obzenflow_topology::StageId,
        to: obzenflow_topology::StageId,
    ) -> &'a EdgeApiInfo {
        let from = from.to_string();
        let to = to.to_string();
        response
            .edges
            .iter()
            .find(|edge| edge.from == from && edge.to == to)
            .expect("edge should be present")
    }

    #[tokio::test]
    async fn topology_endpoint_exports_weighted_rate_limiter_config() {
        let mut builder = TopologyBuilder::new();
        let stage = builder.add_stage(Some("rate_limited".to_string()));
        builder.reset_current();
        let sink = builder.add_stage(Some("sink".to_string()));
        builder.reset_current();
        builder.add_edge(stage, sink);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let core_stage_id = StageId::from_ulid(stage.ulid());
        let sink_stage_id = StageId::from_ulid(sink.ulid());

        let mut stages_metadata: HashMap<StageId, StageMetadata> = HashMap::new();
        stages_metadata.insert(
            core_stage_id,
            StageMetadata {
                stage_type: StageType::Transform,
                status: StageStatus::Running,
            },
        );
        stages_metadata.insert(
            sink_stage_id,
            StageMetadata {
                stage_type: StageType::Sink,
                status: StageStatus::Running,
            },
        );

        let mut middleware_stacks = HashMap::new();
        middleware_stacks.insert(
            core_stage_id,
            MiddlewareStackConfig {
                stack: vec!["rate_limiter".to_string()],
                circuit_breaker: None,
                rate_limiter: Some(serde_json::json!({
                    "tokens_per_sec": 2.0,
                    "burst_capacity": 5.0,
                    "cost_per_event": 5.0,
                    "limit_rate": 0.4,
                })),
                retry: None,
                backpressure: None,
            },
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stages_metadata),
            "test_flow".to_string(),
            Some(Arc::new(middleware_stacks)),
            None,
            None,
            None,
            None,
            None,
        );

        let response = endpoint
            .handle(Request::new(HttpMethod::Get, "/api/topology".to_string()))
            .await
            .expect("endpoint should handle request");
        let response = match response {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        };

        assert_eq!(response.status, 200);

        let parsed: FlowTopologyResponse =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        let stage = parsed
            .stages
            .iter()
            .find(|stage| stage.name == "rate_limited")
            .expect("stage should be present");
        let rate_limiter = stage
            .middleware
            .as_ref()
            .and_then(|middleware| middleware.rate_limiter.as_ref())
            .expect("rate limiter config should be present");

        assert_eq!(rate_limiter.tokens_per_sec, 2.0);
        assert_eq!(rate_limiter.burst_capacity, 5.0);
        assert_eq!(rate_limiter.configured_burst_capacity, None);
        assert_eq!(rate_limiter.cost_per_event, 5.0);
        assert!((rate_limiter.limit_rate - 0.4).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn topology_endpoint_exports_stage_typing_schema_v0_5() {
        let mut builder = TopologyBuilder::new();
        let stage = builder.add_stage(Some("typed_stage".to_string()));
        builder.reset_current();
        let sink = builder.add_stage(Some("sink".to_string()));
        builder.reset_current();
        builder.add_edge(stage, sink);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let core_stage_id = StageId::from_ulid(stage.ulid());
        let sink_stage_id = StageId::from_ulid(sink.ulid());

        let mut stages_metadata: HashMap<StageId, StageMetadata> = HashMap::new();
        stages_metadata.insert(
            core_stage_id,
            StageMetadata {
                stage_type: StageType::Transform,
                status: StageStatus::Running,
            },
        );
        stages_metadata.insert(
            sink_stage_id,
            StageMetadata {
                stage_type: StageType::Sink,
                status: StageStatus::Running,
            },
        );

        let mut stage_typing = HashMap::new();
        stage_typing.insert(
            core_stage_id,
            StageTypingInfo {
                input_type: TypeHintInfo::Mixed,
                output_type: TypeHintInfo::Exact {
                    name: "product_catalog::domain::EnrichedOrder".to_string(),
                },
                boundary_in_type: TypeHintInfo::Unspecified,
                boundary_out_type: TypeHintInfo::Unspecified,
                reference_type: TypeHintInfo::Unspecified,
                stream_type: TypeHintInfo::Unspecified,
                is_placeholder: false,
                placeholder_message: None,
            },
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stages_metadata),
            "test_flow".to_string(),
            None,
            None,
            None,
            Some(Arc::new(stage_typing)),
            None,
            None,
        );

        let response = endpoint
            .handle(
                Request::new(HttpMethod::Get, "/api/topology".to_string())
                    .with_query_param("include".to_string(), "middleware".to_string()),
            )
            .await
            .expect("endpoint should handle request");
        let response = match response {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        };

        let parsed: FlowTopologyResponse =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        assert_eq!(parsed.version, "0.5");

        let typed_stage = parsed
            .stages
            .iter()
            .find(|stage| stage.name == "typed_stage")
            .expect("typed stage should be present");
        let typing = typed_stage
            .typing
            .as_ref()
            .expect("typed stage should include typing");

        assert_eq!(typing.input_type.kind, TypeHintKindApi::Mixed);
        assert_eq!(typing.input_type.name, None);
        assert_eq!(typing.input_type.display_name, None);
        assert_eq!(typing.output_type.kind, TypeHintKindApi::Exact);
        assert_eq!(
            typing.output_type.name.as_deref(),
            Some("product_catalog::domain::EnrichedOrder")
        );
        assert_eq!(
            typing.output_type.display_name.as_deref(),
            Some("Enriched Order")
        );

        let sink_stage = parsed
            .stages
            .iter()
            .find(|stage| stage.name == "sink")
            .expect("sink stage should be present");
        assert!(sink_stage.typing.is_none());

        let edge = edge_between(&parsed, stage, sink);
        let edge_typing = edge
            .typing
            .as_ref()
            .expect("ordinary forward edge should include typing");
        assert_eq!(edge_typing.role, EdgeTypingRoleApi::Input);
        assert_eq!(
            edge_typing.label_source,
            EdgeTypingLabelSourceApi::UpstreamOutputType
        );
        assert_eq!(edge_typing.payload_type.kind, TypeHintKindApi::Exact);
        assert_eq!(
            edge_typing.payload_type.name.as_deref(),
            Some("product_catalog::domain::EnrichedOrder")
        );
        assert_eq!(
            edge_typing.payload_type.display_name.as_deref(),
            Some("Enriched Order")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_exports_per_edge_typing_for_fan_in() {
        let mut builder = TopologyBuilder::new();
        let source_a = builder.add_stage(Some("source_a".to_string()));
        builder.reset_current();
        let source_b = builder.add_stage(Some("source_b".to_string()));
        builder.reset_current();
        let merge = builder.add_stage(Some("merge".to_string()));
        builder.reset_current();
        builder.add_edge(source_a, merge);
        builder.add_edge(source_b, merge);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let source_a_core = core_id(source_a);
        let source_b_core = core_id(source_b);
        let merge_core = core_id(merge);

        let mut stage_typing_map = HashMap::new();
        stage_typing_map.insert(
            source_a_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("Sku"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );
        stage_typing_map.insert(
            source_b_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("Promotion"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[source_a_core, source_b_core, merge_core])),
            "test_flow".to_string(),
            None,
            None,
            None,
            Some(Arc::new(stage_typing_map)),
            None,
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());

        let source_a_edge = edge_between(&parsed, source_a, merge)
            .typing
            .as_ref()
            .expect("source_a edge should include typing");
        assert_eq!(source_a_edge.role, EdgeTypingRoleApi::Input);
        assert_eq!(
            source_a_edge.label_source,
            EdgeTypingLabelSourceApi::UpstreamOutputType
        );
        assert_eq!(source_a_edge.payload_type.name.as_deref(), Some("Sku"));
        assert_eq!(
            source_a_edge.payload_type.display_name.as_deref(),
            Some("Sku")
        );

        let source_b_edge = edge_between(&parsed, source_b, merge)
            .typing
            .as_ref()
            .expect("source_b edge should include typing");
        assert_eq!(source_b_edge.role, EdgeTypingRoleApi::Input);
        assert_eq!(
            source_b_edge.label_source,
            EdgeTypingLabelSourceApi::UpstreamOutputType
        );
        assert_eq!(
            source_b_edge.payload_type.name.as_deref(),
            Some("Promotion")
        );
        assert_eq!(
            source_b_edge.payload_type.display_name.as_deref(),
            Some("Promotion")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_can_fall_back_to_downstream_input_for_single_input_edges() {
        let mut builder = TopologyBuilder::new();
        let source = builder.add_stage(Some("source".to_string()));
        builder.reset_current();
        let transform = builder.add_stage(Some("transform".to_string()));
        builder.reset_current();
        builder.add_edge(source, transform);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let source_core = core_id(source);
        let transform_core = core_id(transform);

        let mut stage_typing_map = HashMap::new();
        stage_typing_map.insert(
            transform_core,
            stage_typing(
                exact("ValidatedOrder"),
                exact("EnrichedOrder"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[source_core, transform_core])),
            "test_flow".to_string(),
            None,
            None,
            None,
            Some(Arc::new(stage_typing_map)),
            None,
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());
        let edge_typing = edge_between(&parsed, source, transform)
            .typing
            .as_ref()
            .expect("single-input edge should use downstream input typing");

        assert_eq!(edge_typing.role, EdgeTypingRoleApi::Input);
        assert_eq!(
            edge_typing.label_source,
            EdgeTypingLabelSourceApi::DownstreamInputType
        );
        assert_eq!(
            edge_typing.payload_type.name.as_deref(),
            Some("ValidatedOrder")
        );
        assert_eq!(
            edge_typing.payload_type.display_name.as_deref(),
            Some("Validated Order")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_exports_join_edge_typing_from_downstream_join_contract() {
        let mut builder = TopologyBuilder::new();
        let catalog = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(1_u128.to_be_bytes()),
            Some("catalog".to_string()),
            TopologyStageType::FiniteSource,
        );
        builder.reset_current();
        let stream = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(2_u128.to_be_bytes()),
            Some("orders".to_string()),
            TopologyStageType::FiniteSource,
        );
        builder.reset_current();
        let join = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(3_u128.to_be_bytes()),
            Some("promo_enriched".to_string()),
            TopologyStageType::Join,
        );
        builder.reset_current();
        builder.add_edge(catalog, join);
        builder.add_edge(stream, join);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let catalog_core = core_id(catalog);
        let stream_core = core_id(stream);
        let join_core = core_id(join);

        let mut stage_typing_map = HashMap::new();
        stage_typing_map.insert(
            catalog_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("CatalogOutputShouldNotDriveJoinEdge"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );
        stage_typing_map.insert(
            stream_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("StreamOutputShouldNotDriveJoinEdge"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );
        stage_typing_map.insert(
            join_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("EnrichedOrderWithPromo"),
                exact("Promotion"),
                exact("EnrichedOrder"),
            ),
        );

        let mut join_metadata = HashMap::new();
        join_metadata.insert(
            join_core,
            JoinMetadata {
                catalog_source_ids: vec![catalog_core],
                stream_source_ids: vec![stream_core],
            },
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[catalog_core, stream_core, join_core])),
            "test_flow".to_string(),
            None,
            None,
            Some(Arc::new(join_metadata)),
            Some(Arc::new(stage_typing_map)),
            None,
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());

        let catalog_edge = edge_between(&parsed, catalog, join)
            .typing
            .as_ref()
            .expect("catalog edge should include reference typing");
        assert_eq!(catalog_edge.role, EdgeTypingRoleApi::Reference);
        assert_eq!(
            catalog_edge.label_source,
            EdgeTypingLabelSourceApi::DownstreamReferenceType
        );
        assert_eq!(catalog_edge.payload_type.name.as_deref(), Some("Promotion"));
        assert_eq!(
            catalog_edge.payload_type.display_name.as_deref(),
            Some("Promotion")
        );

        let stream_edge = edge_between(&parsed, stream, join)
            .typing
            .as_ref()
            .expect("stream edge should include stream typing");
        assert_eq!(stream_edge.role, EdgeTypingRoleApi::Stream);
        assert_eq!(
            stream_edge.label_source,
            EdgeTypingLabelSourceApi::DownstreamStreamType
        );
        assert_eq!(
            stream_edge.payload_type.name.as_deref(),
            Some("EnrichedOrder")
        );
        assert_eq!(
            stream_edge.payload_type.display_name.as_deref(),
            Some("Enriched Order")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_omits_join_edge_typing_without_join_metadata() {
        let mut builder = TopologyBuilder::new();
        let upstream = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(11_u128.to_be_bytes()),
            Some("catalog".to_string()),
            TopologyStageType::FiniteSource,
        );
        builder.reset_current();
        let join = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(12_u128.to_be_bytes()),
            Some("join".to_string()),
            TopologyStageType::Join,
        );
        builder.reset_current();
        builder.add_edge(upstream, join);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let upstream_core = core_id(upstream);
        let join_core = core_id(join);

        let mut stage_typing_map = HashMap::new();
        stage_typing_map.insert(
            upstream_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("Promotion"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );
        stage_typing_map.insert(
            join_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("EnrichedOrder"),
                exact("Promotion"),
                exact("ValidatedOrder"),
            ),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[upstream_core, join_core])),
            "test_flow".to_string(),
            None,
            None,
            None,
            Some(Arc::new(stage_typing_map)),
            None,
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());

        assert!(edge_between(&parsed, upstream, join).typing.is_none());
    }

    #[tokio::test]
    async fn topology_endpoint_omits_typing_for_backward_edges() {
        let mut builder = TopologyBuilder::new();
        let from = builder.add_stage(Some("retry_source".to_string()));
        builder.reset_current();
        let to = builder.add_stage(Some("retry_target".to_string()));
        builder.reset_current();
        builder.add_backward_edge(from, to);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let from_core = core_id(from);
        let to_core = core_id(to);

        let mut stage_typing_map = HashMap::new();
        stage_typing_map.insert(
            from_core,
            stage_typing(
                TypeHintInfo::Unspecified,
                exact("RetryPayload"),
                TypeHintInfo::Unspecified,
                TypeHintInfo::Unspecified,
            ),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[from_core, to_core])),
            "test_flow".to_string(),
            None,
            None,
            None,
            Some(Arc::new(stage_typing_map)),
            None,
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());
        let edge = edge_between(&parsed, from, to);

        assert_eq!(edge.kind, "backward");
        assert!(edge.typing.is_none());
    }

    #[tokio::test]
    async fn topology_endpoint_exports_subgraphs_schema_v0_5() {
        let mut builder = TopologyBuilder::new();
        let stage_a = builder.add_stage(Some("a".to_string()));
        builder.reset_current(); // avoid implicit chaining edges
        let stage_b = builder.add_stage(Some("b".to_string()));
        builder.reset_current(); // avoid implicit chaining edges
        builder.add_edge(stage_a, stage_b);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let a_core = StageId::from_ulid(stage_a.ulid());
        let b_core = StageId::from_ulid(stage_b.ulid());

        let mut stages_metadata: HashMap<StageId, StageMetadata> = HashMap::new();
        stages_metadata.insert(
            a_core,
            StageMetadata {
                stage_type: StageType::Transform,
                status: StageStatus::Running,
            },
        );
        stages_metadata.insert(
            b_core,
            StageMetadata {
                stage_type: StageType::Transform,
                status: StageStatus::Running,
            },
        );

        let mut membership: HashMap<StageId, StageSubgraphMembership> = HashMap::new();
        membership.insert(
            a_core,
            StageSubgraphMembership {
                subgraph_id: "ai_map_reduce:digest".to_string(),
                kind: "ai_map_reduce".to_string(),
                binding: "digest".to_string(),
                role: "chunk".to_string(),
                order: 0,
                is_entry: true,
                is_exit: false,
            },
        );

        let subgraph = TopologySubgraphInfo {
            subgraph_id: "ai_map_reduce:digest".to_string(),
            kind: "ai_map_reduce".to_string(),
            binding: "digest".to_string(),
            label: "digest".to_string(),
            member_stage_ids: vec![a_core, b_core],
            internal_edges: vec![SubgraphInternalEdge {
                from_stage_id: a_core,
                to_stage_id: b_core,
                role: "data".to_string(),
            }],
            entry_stage_ids: vec![a_core],
            exit_stage_ids: vec![b_core],
            parent_subgraph_id: None,
            collapsible: true,
        };

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stages_metadata),
            "test_flow".to_string(),
            None,
            None,
            None,
            None,
            Some(Arc::new(membership)),
            Some(Arc::new(vec![subgraph])),
        );

        let response = endpoint
            .handle(Request::new(HttpMethod::Get, "/api/topology".to_string()))
            .await
            .expect("endpoint should handle request");
        let response = match response {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        };

        assert_eq!(response.status, 200);

        let parsed: FlowTopologyResponse =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        assert_eq!(parsed.version, "0.5");
        assert_eq!(parsed.subgraphs.len(), 1);

        let stage_a_id = stage_a.to_string();
        let stage_b_id = stage_b.to_string();

        let a_api = parsed
            .stages
            .iter()
            .find(|s| s.stage_id == stage_a_id)
            .expect("stage a present");
        assert!(a_api.subgraph.is_some());

        let b_api = parsed
            .stages
            .iter()
            .find(|s| s.stage_id == stage_b_id)
            .expect("stage b present");
        assert!(b_api.subgraph.is_none());

        let expected_members = vec![
            a_core.to_topology_id().to_string(),
            b_core.to_topology_id().to_string(),
        ];
        assert_eq!(parsed.subgraphs[0].member_stage_ids, expected_members);
        assert_eq!(parsed.subgraphs[0].internal_edges.len(), 1);
        assert_eq!(parsed.subgraphs[0].internal_edges[0].role, "data");
    }
}
