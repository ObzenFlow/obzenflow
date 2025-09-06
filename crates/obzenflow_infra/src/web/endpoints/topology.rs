//! Topology HTTP endpoint for flow visualization
//!
//! Provides `/api/topology` endpoint that returns the flow structure
//! for visualization in UI tools.

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response, WebError};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use obzenflow_core::StageId;

/// JSON representation of flow topology for the API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTopologyResponse {
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
}

/// Edge information for API response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeApiInfo {
    pub from: String,
    pub to: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_per_sec: Option<f64>,
}

/// HTTP endpoint that serves flow topology
pub struct TopologyHttpEndpoint {
    topology: Arc<obzenflow_topology::Topology>,
    stages_metadata: Arc<std::collections::HashMap<StageId, StageMetadata>>,
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
    Source,      // Simplified - UI doesn't need to distinguish finite/infinite
    Transform,   // Regular transform operations
    Sink,
    Broadcast,   // One-to-many distribution (implemented as Transform)
    Merge,       // Many-to-one aggregation (implemented as Transform)
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

impl TopologyHttpEndpoint {
    /// Create a new topology endpoint
    pub fn new(
        topology: Arc<obzenflow_topology::Topology>,
        stages_metadata: Arc<std::collections::HashMap<StageId, StageMetadata>>,
    ) -> Self {
        Self {
            topology,
            stages_metadata,
        }
    }
    
    /// Convert internal topology to API response format
    fn build_response(&self) -> FlowTopologyResponse {
        let stages: Vec<StageApiInfo> = self.topology.stages()
            .map(|stage_info| {
                // Convert topology StageId to core StageId for HashMap lookup
                let core_stage_id = StageId::from_ulid(stage_info.id.ulid());
                let metadata = self.stages_metadata.get(&core_stage_id);
                StageApiInfo {
                    stage_id: stage_info.id.to_string(),
                    name: stage_info.name.clone(),
                    stage_type: metadata
                        .map(|m| m.stage_type.as_str().to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                    status: metadata
                        .map(|m| m.status.as_str().to_string())
                        .unwrap_or_else(|| "pending".to_string()),
                }
            })
            .collect();
        
        let edges: Vec<EdgeApiInfo> = self.topology.edges()
            .into_iter()
            .map(|edge| EdgeApiInfo {
                from: edge.from.to_string(),
                to: edge.to.to_string(),
                events_per_sec: None, // Could be populated from metrics later
            })
            .collect();
        
        FlowTopologyResponse {
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
    
    async fn handle(&self, _request: Request) -> Result<Response, WebError> {
        let topology_response = self.build_response();
        
        let json_body = serde_json::to_string(&topology_response)
            .map_err(|e| WebError::RequestHandlingFailed {
                message: format!("Failed to serialize topology: {}", e),
                source: None,
            })?;
        
        let mut response = Response::ok();
        response.headers.insert(
            "Content-Type".to_string(),
            "application/json".to_string(),
        );
        response.headers.insert(
            "Cache-Control".to_string(),
            "max-age=60".to_string(),
        );
        response.body = json_body.into_bytes();
        
        Ok(response)
    }
}