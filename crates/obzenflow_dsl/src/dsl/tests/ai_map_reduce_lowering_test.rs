// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for FLOWIP-086z-part-2 composite lowering.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
    use obzenflow_adapters::middleware::{Middleware, MiddlewareFactory};
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
    use obzenflow_topology::EdgeKind;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use crate::dsl::composites::ai_map_reduce;
    use crate::dsl::composites::lower_composites;
    use crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
    use crate::dsl::typing::TypeHint;

    struct TestMiddleware(&'static str);

    impl Middleware for TestMiddleware {
        fn middleware_name(&self) -> &'static str {
            self.0
        }
    }

    #[derive(Clone)]
    struct TestMiddlewareFactory(&'static str);

    impl MiddlewareFactory for TestMiddlewareFactory {
        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
        ) -> Box<dyn Middleware> {
            Box::new(TestMiddleware(self.0))
        }

        fn name(&self) -> &str {
            self.0
        }
    }

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

    #[derive(Debug, Clone, Default, Serialize, Deserialize)]
    struct TestSeed {
        items: Vec<TestItem>,
    }

    impl TypedPayload for TestSeed {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.seed";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestItem {
        value: u32,
    }

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

        let digest =
            ai_map_reduce::map_reduce::<TestIn, TestChunk, TestPartial, TestCollected, TestOut>()
                .chunker(NoopTransform)
                .map(NoopAsyncTransform)
                .collect(obzenflow_runtime::stages::stateful::CollectByInput::new(
                    TestCollected::default(),
                    |acc, partial: &TestPartial| acc.values.push(partial.value),
                ))
                .finalize(NoopAsyncTransform)
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
            "digest__finalize",
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
            from == "digest__finalize" && to == "out" && *kind == EdgeKind::Forward
        }));
        assert!(
            !connections
                .iter()
                .any(|(from, to, _)| from == "digest" || to == "digest"),
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
                "digest__finalize".to_string(),
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
    fn ai_map_reduce_macro_is_typed_and_lowers_composite() {
        let digest = crate::ai_map_reduce!(
            chunk: TestIn -> TestChunk => NoopTransform,
            map: TestChunk -> TestPartial => NoopAsyncTransform,
            collect: TestPartial -> TestCollected => obzenflow_runtime::stages::stateful::CollectByInput::new(
                TestCollected::default(),
                |acc, partial: &TestPartial| acc.values.push(partial.value),
            ),
            reduce: TestCollected -> TestOut => NoopAsyncTransform,
            [
                map: TestMiddlewareFactory("test_map_mw"),
                reduce: TestMiddlewareFactory("test_reduce_mw"),
            ]
        );

        let metadata = digest
            .typing_metadata()
            .expect("ai_map_reduce! should return a typed descriptor wrapper");
        assert_eq!(metadata.input_type, TypeHint::exact("TestIn"));
        assert_eq!(metadata.output_type, TypeHint::exact("TestOut"));

        let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        stages.insert("batch".to_string(), mk_transform("batch"));
        stages.insert("out".to_string(), mk_transform("out"));
        stages.insert("digest".to_string(), digest);

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        lower_composites(&mut stages, &mut connections)
            .expect("lowering should succeed for ai_map_reduce composite from ai_map_reduce!()");

        let map_stage = stages
            .get("digest__map")
            .expect("map stage should exist after lowering");
        assert!(
            map_stage
                .stage_middleware_names()
                .contains(&"test_map_mw".to_string()),
            "map stage should include map-scoped middleware from macro surface"
        );

        let finalize_stage = stages
            .get("digest__finalize")
            .expect("finalize stage should exist after lowering");
        assert!(
            finalize_stage
                .stage_middleware_names()
                .contains(&"test_reduce_mw".to_string()),
            "finalise stage should include reduce-scoped middleware from macro surface"
        );
    }

    #[test]
    fn ai_map_reduce_cadillac_macro_is_typed_and_lowers_composite() {
        let digest = crate::ai_map_reduce!(
            TestSeed -> TestOut => {
                map: [TestItem] -> TestPartial => NoopAsyncTransform,
                reduce: (TestSeed, [TestPartial]) -> TestOut => NoopAsyncTransform,
            },
            chunking: by_budget {
                items: |seed: &TestSeed| seed.items.clone(),
                render: |item: &TestItem, _ctx| format!("{}", item.value),
                budget: ::obzenflow_core::ai::TokenCount::new(100),
                max_items: None,
                oversize: error,
            },
            middleware: {
                map: TestMiddlewareFactory("test_map_mw"),
                reduce: TestMiddlewareFactory("test_reduce_mw"),
            }
        );

        let metadata = digest
            .typing_metadata()
            .expect("ai_map_reduce! should return a typed descriptor wrapper");
        assert_eq!(metadata.input_type, TypeHint::exact("TestSeed"));
        assert_eq!(metadata.output_type, TypeHint::exact("TestOut"));

        let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        stages.insert("batch".to_string(), mk_transform("batch"));
        stages.insert("out".to_string(), mk_transform("out"));
        stages.insert("digest".to_string(), digest);

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        lower_composites(&mut stages, &mut connections).expect(
            "lowering should succeed for ai_map_reduce composite from Cadillac ai_map_reduce!()",
        );

        let map_stage = stages
            .get("digest__map")
            .expect("map stage should exist after lowering");
        assert!(
            map_stage
                .stage_middleware_names()
                .contains(&"test_map_mw".to_string()),
            "map stage should include map-scoped middleware from macro surface"
        );

        let finalize_stage = stages
            .get("digest__finalize")
            .expect("finalize stage should exist after lowering");
        assert!(
            finalize_stage
                .stage_middleware_names()
                .contains(&"test_reduce_mw".to_string()),
            "finalise stage should include reduce-scoped middleware from macro surface"
        );
    }

    #[test]
    fn ai_map_reduce_cadillac_chunked_map_macro_is_typed_and_lowers_composite() {
        let digest = crate::ai_map_reduce!(
            TestSeed -> TestOut => {
                map: chunked [TestItem] -> TestPartial => NoopAsyncTransform,
                reduce: (TestSeed, [TestPartial]) -> TestOut => NoopAsyncTransform,
            },
            chunking: by_budget {
                items: |seed: &TestSeed| seed.items.clone(),
                render: |item: &TestItem, _ctx| format!("{}", item.value),
                budget: ::obzenflow_core::ai::TokenCount::new(100),
                max_items: None,
                oversize: error,
            },
            middleware: {
                map: TestMiddlewareFactory("test_map_mw"),
                reduce: TestMiddlewareFactory("test_reduce_mw"),
            }
        );

        let metadata = digest
            .typing_metadata()
            .expect("ai_map_reduce! should return a typed descriptor wrapper");
        assert_eq!(metadata.input_type, TypeHint::exact("TestSeed"));
        assert_eq!(metadata.output_type, TypeHint::exact("TestOut"));

        let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
        stages.insert("batch".to_string(), mk_transform("batch"));
        stages.insert("out".to_string(), mk_transform("out"));
        stages.insert("digest".to_string(), digest);

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        lower_composites(&mut stages, &mut connections).expect(
            "lowering should succeed for ai_map_reduce composite from Cadillac ai_map_reduce!()",
        );

        let map_stage = stages
            .get("digest__map")
            .expect("map stage should exist after lowering");
        assert!(
            map_stage
                .stage_middleware_names()
                .contains(&"test_map_mw".to_string()),
            "map stage should include map-scoped middleware from macro surface"
        );

        let finalize_stage = stages
            .get("digest__finalize")
            .expect("finalize stage should exist after lowering");
        assert!(
            finalize_stage
                .stage_middleware_names()
                .contains(&"test_reduce_mw".to_string()),
            "finalise stage should include reduce-scoped middleware from macro surface"
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
                .finalize(NoopAsyncTransform)
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
