use obzenflow_topology_services::builder::TopologyBuilder;
use obzenflow_topology_services::validation::TopologyError;

#[test]
fn test_valid_topology_creation() {
    let mut builder = TopologyBuilder::new();
    let _s1 = builder.add_stage(Some("source".to_string()));
    let _s2 = builder.add_stage(Some("transform".to_string()));
    let _s3 = builder.add_stage(Some("sink".to_string()));
    
    match builder.build() {
        Ok(topology) => {
            let metrics = topology.metrics();
            assert_eq!(metrics.num_stages, 3);
            assert_eq!(metrics.num_edges, 2);
        }
        Err(e) => panic!("Unexpected error creating valid topology: {}", e),
    }
}

#[test]
fn test_cycle_detection() {
    let mut builder = TopologyBuilder::new();
    let s1 = builder.add_stage(Some("stage1".to_string()));
    let _s2 = builder.add_stage(Some("stage2".to_string()));
    let s3 = builder.add_stage(Some("stage3".to_string()));
    builder.add_edge(s3, s1); // Create cycle: s1 -> s2 -> s3 -> s1
    
    match builder.build() {
        Err(TopologyError::CycleDetected { stages }) => {
            // Should detect the cycle
            assert!(!stages.is_empty());
            assert!(stages.len() >= 3); // At least the 3 stages in the cycle
        }
        Ok(_) => panic!("Expected cycle detection to fail"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_duplicate_edge_detection() {
    let mut builder = TopologyBuilder::new();
    let s1 = builder.add_stage(Some("source".to_string()));
    let s2 = builder.add_stage(Some("sink".to_string()));
    builder.add_edge(s1, s2); // Duplicate edge (already connected by add_stage)
    
    match builder.build() {
        Err(TopologyError::DuplicateEdge { from, to }) => {
            assert_eq!(from, s1);
            assert_eq!(to, s2);
        }
        Ok(_) => panic!("Expected duplicate edge detection to fail"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_explicit_duplicate_edge_detection() {
    let mut builder = TopologyBuilder::new();
    let s1 = builder.add_stage(Some("source".to_string()));
    builder.reset_current(); // Break the chain
    let s2 = builder.add_stage(Some("sink".to_string()));
    
    // Add the same edge twice explicitly
    builder.add_edge(s1, s2);
    builder.add_edge(s1, s2); // Duplicate
    
    match builder.build() {
        Err(TopologyError::DuplicateEdge { from, to }) => {
            assert_eq!(from, s1);
            assert_eq!(to, s2);
        }
        Ok(_) => panic!("Expected duplicate edge detection to fail"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_disconnected_stages_detection() {
    let mut builder = TopologyBuilder::new();
    let _s1 = builder.add_stage(Some("source".to_string()));
    let _s2 = builder.add_stage(Some("transform".to_string()));
    
    // Create a disconnected stage by resetting before creating it
    builder.reset_current();
    let _s3 = builder.add_stage(Some("disconnected".to_string()));
    // s3 is not connected to anything
    
    match builder.build() {
        Err(TopologyError::DisconnectedStages { stages }) => {
            assert_eq!(stages.len(), 1);
            // The disconnected stage should be s3
        }
        Ok(_) => panic!("Expected disconnected stage detection to fail"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_complex_topology_metrics() {
    let mut builder = TopologyBuilder::new();
    let source = builder.add_stage(Some("source".to_string()));
    
    // Create transforms separately (no auto-connection)
    builder.reset_current();
    let t1 = builder.add_stage(Some("transform1".to_string()));
    builder.reset_current();
    let t2 = builder.add_stage(Some("transform2".to_string()));
    builder.reset_current();
    let t3 = builder.add_stage(Some("transform3".to_string()));
    
    // Fan-out: manually connect source to all transforms
    builder.add_edge(source, t1);
    builder.add_edge(source, t2);
    builder.add_edge(source, t3);
    
    // Create sink separately
    builder.reset_current();
    let sink = builder.add_stage(Some("sink".to_string()));
    
    // Fan-in: connect all transforms to sink
    builder.add_edge(t1, sink);
    builder.add_edge(t2, sink);
    builder.add_edge(t3, sink);
    
    match builder.build() {
        Ok(topology) => {
            let metrics = topology.metrics();
            assert_eq!(metrics.num_stages, 5);
            assert_eq!(metrics.num_edges, 6);
            assert_eq!(metrics.num_sources, 1);
            assert_eq!(metrics.num_sinks, 1);
            assert_eq!(metrics.max_fan_out, 3); // source fans out to 3
            assert_eq!(metrics.max_fan_in, 3);  // sink has 3 inputs
            assert_eq!(metrics.max_depth, 2);   // source -> transform -> sink
        }
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_isolated_node_detection() {
    let mut builder = TopologyBuilder::new();
    
    // Create a connected component
    let _s1 = builder.add_stage(Some("stage1".to_string()));
    let s2 = builder.add_stage(Some("stage2".to_string()));
    
    // Create an isolated node
    builder.reset_current();
    let _isolated = builder.add_stage(Some("isolated".to_string()));
    
    // Create another connected component
    builder.reset_current();
    let s4 = builder.add_stage(Some("stage4".to_string()));
    let _s5 = builder.add_stage(Some("stage5".to_string()));
    
    // Connect the first two components
    builder.add_edge(s2, s4);
    
    // The isolated node should be detected
    match builder.build() {
        Err(TopologyError::DisconnectedStages { stages }) => {
            assert_eq!(stages.len(), 1);
        }
        Ok(_) => panic!("Expected disconnected stage detection"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}