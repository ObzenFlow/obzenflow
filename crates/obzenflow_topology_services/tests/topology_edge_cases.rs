use obzenflow_topology_services::builder::TopologyBuilder;
use obzenflow_topology_services::validation::TopologyError;
use obzenflow_topology_services::stages::StageId;

#[test]
fn test_empty_topology() {
    let builder = TopologyBuilder::new();
    
    match builder.build() {
        Ok(topology) => {
            assert!(topology.is_empty());
            assert_eq!(topology.num_stages(), 0);
            assert_eq!(topology.source_stages().len(), 0);
            assert_eq!(topology.sink_stages().len(), 0);
        }
        Err(e) => panic!("Empty topology should be valid: {}", e),
    }
}

#[test]
fn test_minimum_valid_pipeline() {
    let mut builder = TopologyBuilder::new();
    let source = builder.add_stage(Some("source".to_string()));
    let transform = builder.add_stage(Some("transform".to_string()));
    let sink = builder.add_stage(Some("sink".to_string()));
    
    // Minimum valid pipeline: source -> transform -> sink (3 nodes)
    match builder.build() {
        Ok(topology) => {
            assert_eq!(topology.num_stages(), 3);
            assert_eq!(topology.source_stages(), vec![source]);
            assert_eq!(topology.sink_stages(), vec![sink]);
            assert_eq!(topology.metrics().num_edges, 2);
            // Verify the transform is neither source nor sink
            assert!(!topology.source_stages().contains(&transform));
            assert!(!topology.sink_stages().contains(&transform));
        }
        Err(e) => panic!("Minimum pipeline (source->transform->sink) should be valid: {}", e),
    }
}

#[test]
fn test_diamond_topology() {
    let mut builder = TopologyBuilder::new();
    
    // Create diamond shape: source -> (a, b) -> sink
    let source = builder.add_stage(Some("source".to_string()));
    
    builder.reset_current();
    let a = builder.add_stage(Some("path_a".to_string()));
    builder.reset_current();
    let b = builder.add_stage(Some("path_b".to_string()));
    builder.reset_current();
    let sink = builder.add_stage(Some("sink".to_string()));
    
    // Connect the diamond
    builder.add_edge(source, a);
    builder.add_edge(source, b);
    builder.add_edge(a, sink);
    builder.add_edge(b, sink);
    
    match builder.build() {
        Ok(topology) => {
            assert_eq!(topology.num_stages(), 4);
            assert_eq!(topology.metrics().num_edges, 4);
            assert_eq!(topology.downstream_stages(source).len(), 2);
            assert_eq!(topology.upstream_stages(sink).len(), 2);
            assert_eq!(topology.metrics().max_depth, 2);
        }
        Err(e) => panic!("Diamond topology should be valid: {}", e),
    }
}

#[test]
fn test_self_loop_detection() {
    let mut builder = TopologyBuilder::new();
    let stage = builder.add_stage(Some("self_loop".to_string()));
    
    // Try to create self-loop
    builder.add_edge(stage, stage);
    
    match builder.build() {
        Err(TopologyError::CycleDetected { stages }) => {
            assert!(stages.contains(&stage));
        }
        Ok(_) => panic!("Self-loop should be detected as cycle"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_multiple_sources_and_sinks() {
    let mut builder = TopologyBuilder::new();
    
    // Create two separate pipelines
    let s1 = builder.add_stage(Some("source1".to_string()));
    let _t1 = builder.add_stage(Some("transform1".to_string()));
    let _sink1 = builder.add_stage(Some("sink1".to_string()));
    
    builder.reset_current();
    let s2 = builder.add_stage(Some("source2".to_string()));
    let _t2 = builder.add_stage(Some("transform2".to_string()));
    let _sink2 = builder.add_stage(Some("sink2".to_string()));
    
    // Connect them with a cross-edge to avoid disconnected error
    builder.add_edge(s1, s2);
    
    match builder.build() {
        Ok(topology) => {
            assert_eq!(topology.num_stages(), 6);
            assert_eq!(topology.source_stages().len(), 1); // Only s1 is a true source now
            assert_eq!(topology.sink_stages().len(), 2); // Two sinks
        }
        Err(e) => panic!("Multiple pipelines should be valid when connected: {}", e),
    }
}

#[test]
fn test_stage_name_retrieval() {
    let mut builder = TopologyBuilder::new();
    let source_id = builder.add_stage(Some("source".to_string()));
    let transform_id = builder.add_stage(Some("transform".to_string()));
    let sink_id = builder.add_stage(Some("sink".to_string()));
    
    match builder.build() {
        Ok(topology) => {
            assert_eq!(topology.stage_name(source_id), Some("source"));
            assert_eq!(topology.stage_name(transform_id), Some("transform"));
            assert_eq!(topology.stage_name(sink_id), Some("sink"));
            assert_eq!(topology.stage_name(StageId::new()), None);
        }
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test] 
fn test_flow_naming() {
    let mut builder = TopologyBuilder::new();
    let _source = builder.add_stage(Some("data_ingestion".to_string()));
    let _transform = builder.add_stage(Some("transform".to_string()));
    let _sink = builder.add_stage(Some("sink".to_string()));
    
    match builder.build() {
        Ok(topology) => {
            assert_eq!(topology.flow_name(), "data_ingestion_flow");
            assert_eq!(topology.source_stage_name(), "data_ingestion");
            assert_eq!(topology.sink_stage_name(), "sink");
        }
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

#[test]
fn test_has_edge_queries() {
    let mut builder = TopologyBuilder::new();
    let s1 = builder.add_stage(Some("s1".to_string()));
    let s2 = builder.add_stage(Some("s2".to_string()));
    let s3 = builder.add_stage(Some("s3".to_string()));
    
    match builder.build() {
        Ok(topology) => {
            assert!(topology.has_edge(s1, s2));
            assert!(topology.has_edge(s2, s3));
            assert!(!topology.has_edge(s1, s3)); // No direct edge
            assert!(!topology.has_edge(s3, s1)); // Wrong direction
        }
        Err(e) => panic!("Unexpected error: {}", e),
    }
}