use crate::stages::{StageId, StageInfo};
use crate::topology::DirectedEdge;
use crate::topology::Topology;
use crate::validation::TopologyError;

/// Builder for constructing pipeline topologies
pub struct TopologyBuilder {
    stages: Vec<StageInfo>,
    edges: Vec<DirectedEdge>,
    current_stage: Option<StageId>,
}

impl TopologyBuilder {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            edges: Vec::new(),
            current_stage: None,
        }
    }

    /// Add a stage and optionally connect from current stage
    pub fn add_stage(&mut self, name: Option<String>) -> StageId {
        let id = StageId::new();
        let info = match name {
            Some(n) => StageInfo::new(id, n),
            None => StageInfo::auto_named(id),
        };

        self.stages.push(info);

        // If there's a current stage, create an edge from it to this new stage
        if let Some(from) = self.current_stage {
            self.edges.push(DirectedEdge::new(from, id));
        }

        self.current_stage = Some(id);
        id
    }


    /// Add an explicit edge between stages
    pub fn add_edge(&mut self, from: StageId, to: StageId) {
        self.edges.push(DirectedEdge::new(from, to));
    }

    /// Set the current stage (for chaining)
    pub fn set_current(&mut self, stage: StageId) {
        self.current_stage = Some(stage);
    }

    /// Reset current stage (for building separate chains)
    pub fn reset_current(&mut self) {
        self.current_stage = None;
    }

    /// Build the topology
    pub fn build(self) -> Result<Topology, TopologyError> {
        Topology::new(self.stages, self.edges)
    }
}

impl Default for TopologyBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builder_chaining() {
        let mut builder = TopologyBuilder::new();

        // Build a simple chain
        let s1 = builder.add_stage(Some("stage1".to_string()));
        let s2 = builder.add_stage(Some("stage2".to_string()));
        let s3 = builder.add_stage(Some("stage3".to_string()));

        let topology = builder.build().unwrap();

        // Verify chain: s1 -> s2 -> s3
        assert_eq!(topology.downstream_stages(s1), &[s2]);
        assert_eq!(topology.downstream_stages(s2), &[s3]);
        assert_eq!(topology.upstream_stages(s3), &[s2]);
    }

    #[test]
    fn test_builder_fan_in() {
        let mut builder = TopologyBuilder::new();

        // Create two sources
        let source1 = builder.add_stage(Some("source1".to_string()));
        builder.reset_current();
        let source2 = builder.add_stage(Some("source2".to_string()));
        builder.reset_current();

        // Create a merger
        let merger = builder.add_stage(Some("merger".to_string()));

        // Connect both sources to merger
        builder.add_edge(source1, merger);
        builder.add_edge(source2, merger);

        let topology = builder.build().unwrap();

        // Verify fan-in
        let upstream = topology.upstream_stages(merger);
        assert_eq!(upstream.len(), 2);
        assert!(upstream.contains(&source1));
        assert!(upstream.contains(&source2));
    }
}
