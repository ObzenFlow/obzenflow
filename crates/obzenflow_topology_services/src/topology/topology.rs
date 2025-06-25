use std::collections::{HashMap, HashSet};
use crate::stages::{StageId, StageInfo};
use crate::topology::DirectedEdge;
use crate::validation::TopologyError;

/// Complete topology with efficient traversal
#[derive(Debug, Clone)]
pub struct Topology {
    stages: HashMap<StageId, StageInfo>,
    edges: Vec<DirectedEdge>,

    // Cached adjacency lists for O(1) lookups
    // Using HashSet for O(1) contains() checks
    downstream: HashMap<StageId, HashSet<StageId>>,
    upstream: HashMap<StageId, HashSet<StageId>>,
}

impl Topology {
    /// Construct and validate topology
    pub fn new(stages: Vec<StageInfo>, edges: Vec<DirectedEdge>) -> Result<Self, TopologyError> {
        let stage_map: HashMap<StageId, StageInfo> = stages
            .into_iter()
            .map(|s| (s.id, s))
            .collect();

        // Build adjacency lists for efficient traversal
        let mut downstream: HashMap<StageId, HashSet<StageId>> = HashMap::new();
        let mut upstream: HashMap<StageId, HashSet<StageId>> = HashMap::new();

        for edge in &edges {
            // Validate edge references valid stages
            if !stage_map.contains_key(&edge.from) {
                return Err(TopologyError::InvalidEdge {
                    from: edge.from,
                    to: edge.to,
                    reason: format!("Source stage {} not found", edge.from),
                });
            }
            if !stage_map.contains_key(&edge.to) {
                return Err(TopologyError::InvalidEdge {
                    from: edge.from,
                    to: edge.to,
                    reason: format!("Target stage {} not found", edge.to),
                });
            }

            // Check for duplicate edges
            if let Some(existing) = downstream.get(&edge.from) {
                if existing.contains(&edge.to) {
                    return Err(TopologyError::DuplicateEdge {
                        from: edge.from,
                        to: edge.to,
                    });
                }
            }

            downstream.entry(edge.from).or_default().insert(edge.to);
            upstream.entry(edge.to).or_default().insert(edge.from);
        }

        // Validate no cycles using topological sort
        crate::validation::validate_acyclic(&stage_map, &downstream)?;

        // Detect disconnected components
        if let Some(disconnected) = crate::validation::find_disconnected_stages(&stage_map, &downstream, &upstream) {
            return Err(TopologyError::DisconnectedStages { stages: disconnected });
        }

        Ok(Self {
            stages: stage_map,
            edges,
            downstream,
            upstream
        })
    }

    /// Get stages that flow INTO this stage
    pub fn upstream_stages(&self, stage: StageId) -> Vec<StageId> {
        self.upstream.get(&stage)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get stages that this stage flows TO
    pub fn downstream_stages(&self, stage: StageId) -> Vec<StageId> {
        self.downstream.get(&stage)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get human-readable name for debugging
    pub fn stage_name(&self, stage: StageId) -> Option<&str> {
        self.stages.get(&stage).map(|info| info.name.as_str())
    }

    /// Get stage info
    pub fn stage_info(&self, stage: StageId) -> Option<&StageInfo> {
        self.stages.get(&stage)
    }

    /// Get all stages
    pub fn stages(&self) -> impl Iterator<Item = &StageInfo> {
        self.stages.values()
    }

    /// Get all edges
    pub fn edges(&self) -> &[DirectedEdge] {
        &self.edges
    }

    /// Check if topology has any stages
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Get number of stages
    pub fn num_stages(&self) -> usize {
        self.stages.len()
    }

    /// Find source stages (no upstream)
    pub fn source_stages(&self) -> Vec<StageId> {
        self.stages
            .keys()
            .filter(|&id| self.upstream_stages(*id).is_empty())
            .copied()
            .collect()
    }

    /// Find sink stages (no downstream)
    pub fn sink_stages(&self) -> Vec<StageId> {
        self.stages
            .keys()
            .filter(|&id| self.downstream_stages(*id).is_empty())
            .copied()
            .collect()
    }

    /// Get flow name (derived from source stage if single source)
    pub fn flow_name(&self) -> String {
        let sources = self.source_stages();
        if sources.len() == 1 {
            if let Some(stage_info) = self.stages.get(&sources[0]) {
                return format!("{}_flow", stage_info.name);
            }
        }
        "multi_source_flow".to_string()
    }

    /// Get flow ID (unique identifier for this flow instance)
    pub fn flow_id(&self) -> String {

        // Generate flow ID from topology structure
        // In production, this would be set during flow construction
        use ulid::Ulid;
        Ulid::new().to_string()
    }

    /// Get source stage name (assumes single source)
    pub fn source_stage_name(&self) -> String {

        let sources = self.source_stages();
        if sources.len() == 1 {
            if let Some(stage_info) = self.stages.get(&sources[0]) {
                return stage_info.name.clone();
            }
        }
        "unknown_source".to_string()
    }

    /// Get sink stage name (assumes single sink)
    pub fn sink_stage_name(&self) -> String {

        let sinks = self.sink_stages();
        if sinks.len() == 1 {
            if let Some(stage_info) = self.stages.get(&sinks[0]) {
                return stage_info.name.clone();
            }
        }
        "unknown_sink".to_string()
    }

    /// Get topology metrics for debugging and optimization
    pub fn metrics(&self) -> TopologyMetrics {
        TopologyMetrics {
            num_stages: self.stages.len(),
            num_edges: self.edges.len(),
            num_sources: self.source_stages().len(),
            num_sinks: self.sink_stages().len(),
            max_fan_out: self.downstream
                .values()
                .map(|set| set.len())
                .max()
                .unwrap_or(0),
            max_fan_in: self.upstream
                .values()
                .map(|set| set.len())
                .max()
                .unwrap_or(0),
            max_depth: self.calculate_max_depth(),
        }
    }

    /// Calculate the maximum depth (longest path) in the DAG
    fn calculate_max_depth(&self) -> usize {
        let mut depths: HashMap<StageId, usize> = HashMap::new();
        let sources = self.source_stages();

        // BFS from all sources
        let mut queue = std::collections::VecDeque::new();
        for source in sources {
            queue.push_back((source, 0));
            depths.insert(source, 0);
        }

        let mut max_depth = 0;
        while let Some((stage, depth)) = queue.pop_front() {
            max_depth = max_depth.max(depth);

            for downstream in self.downstream_stages(stage) {
                let new_depth = depth + 1;
                let should_update = depths.get(&downstream)
                    .map(|&d| new_depth > d)
                    .unwrap_or(true);

                if should_update {
                    depths.insert(downstream, new_depth);
                    queue.push_back((downstream, new_depth));
                }
            }
        }

        max_depth
    }

    /// Check if a specific edge exists
    pub fn has_edge(&self, from: StageId, to: StageId) -> bool {
        self.downstream
            .get(&from)
            .map(|set| set.contains(&to))
            .unwrap_or(false)
    }
}

/// Topology metrics for debugging and optimization
#[derive(Debug, Clone)]
pub struct TopologyMetrics {
    pub num_stages: usize,
    pub num_edges: usize,
    pub num_sources: usize,
    pub num_sinks: usize,
    pub max_fan_out: usize,
    pub max_fan_in: usize,
    pub max_depth: usize,
}

#[cfg(test)]
mod tests {
    use crate::builder::TopologyBuilder;

    #[test]
    fn test_simple_pipeline() {
        let mut builder = TopologyBuilder::new();
        let source = builder.add_stage(Some("source".to_string()));
        let transform = builder.add_stage(Some("transform".to_string()));
        let sink = builder.add_stage(Some("sink".to_string()));

        let topology = builder.build().unwrap();

        assert_eq!(topology.num_stages(), 3);
        assert_eq!(topology.upstream_stages(transform), &[source]);
        assert_eq!(topology.downstream_stages(transform), &[sink]);
        assert_eq!(topology.source_stages(), vec![source]);
        assert_eq!(topology.sink_stages(), vec![sink]);
    }

    #[test]
    fn test_fan_out_topology() {
        let mut builder = TopologyBuilder::new();
        let source = builder.add_stage(Some("source".to_string()));

        // Reset to build parallel branches
        builder.reset_current();
        let transform1 = builder.add_stage(Some("transform1".to_string()));
        let transform2 = builder.add_stage(Some("transform2".to_string()));

        // Manually connect source to both transforms
        builder.add_edge(source, transform1);
        builder.add_edge(source, transform2);

        let topology = builder.build().unwrap();

        assert_eq!(topology.downstream_stages(source).len(), 2);
        assert!(topology.downstream_stages(source).contains(&transform1));
        assert!(topology.downstream_stages(source).contains(&transform2));
    }
}
