//! Tests for automatic cycle detection used by the DSL.
//!
//! These tests intentionally mirror the topology used in the old
//! `cycle_guard_demo.rs` example, but in a purely mechanical way:
//! we verify that the `<|` operator produces a real graph cycle and
//! that `Topology::is_in_cycle` correctly identifies the stages that
//! participate in that cycle.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use obzenflow_topology::{EdgeKind, TopologyBuilder};

    /// Build a topology using the DSL's `parse_topology!` macro and
    /// verify that stages in the feedback loop are detected as being
    /// in a cycle, while the pure source/sink stages are not.
    #[test]
    fn feedback_loop_stages_are_marked_as_in_cycle() {
        let mut connections: Vec<(String, String, EdgeKind)> = Vec::new();

        crate::parse_topology!(
            connections,
            // Main flow
            source |> processor1;
            processor1 |> processor2;
            processor2 |> sink;

            // Feedback loop: processors feed back to each other
            processor1 <| processor2;
        );

        // Create deterministic stage IDs and names (structural only)
        let mut builder = TopologyBuilder::new();
        let mut name_to_id = HashMap::new();
        for name in ["source", "processor1", "processor2", "sink"] {
            let id = builder.add_stage(Some(name.to_string()));
            name_to_id.insert(name.to_string(), id);
            // Avoid implicit chaining edges; this test supplies all edges explicitly
            builder.reset_current();
        }

        // Convert (from_name, to_name, kind) into concrete edges, preserving EdgeKind
        for (from_name, to_name, kind) in connections {
            let from = *name_to_id
                .get(&from_name)
                .unwrap_or_else(|| panic!("Unknown from stage: {from_name}"));
            let to = *name_to_id
                .get(&to_name)
                .unwrap_or_else(|| panic!("Unknown to stage: {to_name}"));

            match kind {
                EdgeKind::Forward => builder.add_edge(from, to),
                EdgeKind::Backward => builder.add_backward_edge(from, to),
            }
        }

        // Use structural-only validation: this test cares about SCC detection,
        // not semantic source/sink roles
        let topology = builder
            .build_unchecked()
            .expect("topology should build with structural validation");

        let source = name_to_id["source"];
        let processor1 = name_to_id["processor1"];
        let processor2 = name_to_id["processor2"];
        let sink = name_to_id["sink"];

        // Only the processors participate in the feedback loop SCC.
        assert!(!topology.is_in_cycle(source));
        assert!(topology.is_in_cycle(processor1));
        assert!(topology.is_in_cycle(processor2));
        assert!(!topology.is_in_cycle(sink));
    }
}
