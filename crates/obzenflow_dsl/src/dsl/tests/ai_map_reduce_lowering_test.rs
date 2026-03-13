// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for FLOWIP-086z-part-2 composite lowering.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
    use obzenflow_topology::EdgeKind;
    use serde::{Deserialize, Serialize};

    use crate::dsl::composites::lower_composites;
    use crate::dsl::composites::ai_map_reduce;
    use crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};

    #[derive(Debug, Clone)]
    struct NoopTransform;

    #[async_trait::async_trait]
    impl TransformHandler for NoopTransform {
        fn process(
            &self,
            event: obzenflow_core::ChainEvent,
        ) -> Result<Vec<obzenflow_core::ChainEvent>, HandlerError> {
            Ok(vec![event])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct NoopAsyncTransform;

    #[async_trait::async_trait]
    impl AsyncTransformHandler for NoopAsyncTransform {
        async fn process(
            &self,
            _event: obzenflow_core::ChainEvent,
        ) -> Result<Vec<obzenflow_core::ChainEvent>, HandlerError> {
            Ok(vec![])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct TestIn;

    #[derive(Debug, Clone)]
    struct TestOut;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestChunk {
        chunk_index: usize,
        chunk_count: usize,
    }

    impl TypedPayload for TestChunk {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.chunk";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestPartial {
        value: u32,
    }

    impl TypedPayload for TestPartial {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.partial";
    }

    #[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
    struct TestCollected {
        values: Vec<u32>,
    }

    impl TypedPayload for TestCollected {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.collected";
    }

    fn mk_transform(name: &str) -> Box<dyn StageDescriptor> {
        Box::new(TransformDescriptor {
            name: name.to_string(),
            handler: NoopTransform,
            middleware: vec![],
        })
    }

    #[test]
    fn ai_map_reduce_lowering_expands_stages_and_rewrites_edges() {
        let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();

        stages.insert("batch".to_string(), mk_transform("batch"));
        stages.insert("out".to_string(), mk_transform("out"));

        let digest = ai_map_reduce::map_reduce::<TestIn, TestChunk, TestPartial, TestCollected, TestOut>()
            .chunker(NoopTransform)
            .map(NoopAsyncTransform)
            .collect(obzenflow_runtime::stages::stateful::CollectByInput::new(
                TestCollected::default(),
                |acc, partial: &TestPartial| acc.values.push(partial.value),
            ))
            .finalise(NoopAsyncTransform)
            .build();

        stages.insert("digest".to_string(), digest);

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        let artifacts = lower_composites(&mut stages, &mut connections)
            .expect("lowering should succeed for ai_map_reduce composite");

        // Logical binding removed, internal bindings inserted.
        assert!(!stages.contains_key("digest"));
        for internal in [
            "digest__chunk",
            "digest__map",
            "digest__collect",
            "digest__finalise",
        ] {
            assert!(
                stages.contains_key(internal),
                "missing lowered stage '{internal}'"
            );
        }

        // External edges rewritten to entry + exit stages.
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "batch" && to == "digest__chunk" && *kind == EdgeKind::Forward
        }));
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "digest__finalise" && to == "out" && *kind == EdgeKind::Forward
        }));
        assert!(
            !connections.iter().any(|(from, to, _)| from == "digest" || to == "digest"),
            "no edge should reference the logical composite binding after lowering"
        );

        // Internal edges include the direct chunk -> collect manifest edge.
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "digest__chunk" && to == "digest__collect" && *kind == EdgeKind::Forward
        }));

        // Lowering artifacts include subgraph membership + registry entry.
        assert_eq!(artifacts.stage_subgraphs.len(), 4);
        let chunk = artifacts
            .stage_subgraphs
            .get("digest__chunk")
            .expect("chunk stage membership");
        assert_eq!(chunk.binding, "digest");
        assert_eq!(chunk.kind, "ai_map_reduce");
        assert_eq!(chunk.role, "chunk");
        assert!(chunk.is_entry);

        assert_eq!(artifacts.subgraphs.len(), 1);
        let subgraph = &artifacts.subgraphs[0];
        assert_eq!(subgraph.subgraph_id, "ai_map_reduce:digest");
        assert_eq!(subgraph.kind, "ai_map_reduce");
        assert_eq!(subgraph.binding, "digest");
        assert_eq!(
            subgraph.member_stage_names,
            vec![
                "digest__chunk".to_string(),
                "digest__map".to_string(),
                "digest__collect".to_string(),
                "digest__finalise".to_string(),
            ]
        );
        assert!(
            subgraph
                .internal_edges
                .iter()
                .any(|edge| edge.from_stage == "digest__chunk"
                    && edge.to_stage == "digest__collect"
                    && edge.role == "manifest"),
            "subgraph internal edges should include the manifest path"
        );
    }

    #[test]
    fn lowering_detects_internal_stage_name_collisions() {
        let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();

        // Collision: existing stage has the same binding as the would-be lowered stage.
        stages.insert(
            "digest__chunk".to_string(),
            Box::new(TransformDescriptor {
                name: "digest__chunk".to_string(),
                handler: NoopTransform,
                middleware: vec![],
            }),
        );

        stages.insert(
            "digest".to_string(),
            ai_map_reduce::map_reduce::<TestIn, TestChunk, TestPartial, TestCollected, TestOut>()
                .chunker(NoopTransform)
                .map(NoopAsyncTransform)
                .collect(obzenflow_runtime::stages::stateful::CollectByInput::new(
                    TestCollected::default(),
                    |acc, partial: &TestPartial| acc.values.push(partial.value),
                ))
                .finalise(NoopAsyncTransform)
                .build(),
        );

        let mut connections = vec![("digest".to_string(), "out".to_string(), EdgeKind::Forward)];

        let err = lower_composites(&mut stages, &mut connections)
            .expect_err("expected name collision error from composite lowering");
        assert!(
            err.to_string().contains("lowering collision"),
            "unexpected error: {err}"
        );
    }
}
