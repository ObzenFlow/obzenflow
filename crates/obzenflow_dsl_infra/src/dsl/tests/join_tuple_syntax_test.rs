//! Test for the join tuple syntax (reference, stream) |> join

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_topology_macro_with_join_tuple() {
        // Direct test of the parse_topology! macro
        let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();
        let mut join_connections = Vec::new();

        // Test the macro expansion
        crate::parse_topology_with_joins!(connections, join_connections,
            (reference, stream) |> joiner;
            joiner |> output;
        );

        // Verify join connections are tracked
        assert_eq!(join_connections.len(), 1);
        assert_eq!(join_connections[0].0, "joiner");
        assert_eq!(join_connections[0].1 .0, "reference");
        assert_eq!(join_connections[0].1 .1, "stream");

        // Verify regular connections are also created
        assert_eq!(connections.len(), 3);
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "reference" && to == "joiner" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "stream" && to == "joiner" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "joiner" && to == "output" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
    }

    #[test]
    fn test_parse_topology_multiple_joins() {
        let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();
        let mut join_connections = Vec::new();

        // Test multiple joins
        crate::parse_topology_with_joins!(connections, join_connections,
            (products, orders) |> order_enricher;
            (users, orders) |> user_enricher;
            order_enricher |> sink1;
            user_enricher |> sink2;
        );

        // Verify two joins are tracked
        assert_eq!(join_connections.len(), 2);

        // First join
        assert_eq!(join_connections[0].0, "order_enricher");
        assert_eq!(join_connections[0].1 .0, "products");
        assert_eq!(join_connections[0].1 .1, "orders");

        // Second join
        assert_eq!(join_connections[1].0, "user_enricher");
        assert_eq!(join_connections[1].1 .0, "users");
        assert_eq!(join_connections[1].1 .1, "orders");

        // Verify all connections
        assert_eq!(connections.len(), 6);
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "products" && to == "order_enricher" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "orders" && to == "order_enricher" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "users" && to == "user_enricher" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "orders" && to == "user_enricher" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "order_enricher" && to == "sink1" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "user_enricher" && to == "sink2" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
    }

    #[test]
    fn test_parse_topology_mixed_syntax() {
        let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();
        let mut join_connections = Vec::new();

        // Test mixing regular and join tuple syntax
        crate::parse_topology_with_joins!(connections, join_connections,
            source |> transform1;
            transform1 |> transform2;
            (reference, transform2) |> joiner;
            joiner |> sink;
        );

        // Verify one join
        assert_eq!(join_connections.len(), 1);
        assert_eq!(join_connections[0].0, "joiner");
        assert_eq!(join_connections[0].1 .0, "reference");
        assert_eq!(join_connections[0].1 .1, "transform2");

        // Verify all connections
        assert_eq!(connections.len(), 5);
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "source" && to == "transform1" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "transform1" && to == "transform2" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "reference" && to == "joiner" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "transform2" && to == "joiner" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "joiner" && to == "sink" && matches!(kind, obzenflow_topology::EdgeKind::Forward)
        }));
    }
}
