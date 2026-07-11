// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for FLOWIP-086z-part-2 composite lowering.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
    use obzenflow_adapters::middleware::{
        ControlMiddlewareRole, Middleware, MiddlewareFactory, MiddlewareOverrideKey,
        MiddlewarePlanContribution, SourceMiddlewarePhase, TopologyMiddlewareConfigSlot,
    };
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::{AsyncTransformHandler, TransformHandler};
    use obzenflow_topology::EdgeKind;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    use crate::dsl::composites::ai_map_reduce;
    use crate::dsl::composites::lower_composites;
    use crate::dsl::composition::{FlowMember, IntoFlowMember};
    use crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
    use crate::dsl::typing::TypeHint;
    use obzenflow_core::ai::{
        AiMapReduceChunkFailed, AiMapReducePlanningManifest, AiMapReduceTaggedPartial,
    };

    struct TestMiddleware(&'static str);

    impl Middleware for TestMiddleware {
        fn label(&self) -> &'static str {
            self.0
        }

        fn source_phase(&self) -> SourceMiddlewarePhase {
            SourceMiddlewarePhase::Ordinary
        }
    }

    #[derive(Clone)]
    struct TestMiddlewareFactory(&'static str);

    impl MiddlewareFactory for TestMiddlewareFactory {
        fn label(&self) -> &'static str {
            self.0
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            // In production, override keys are stable family identifiers independent of display
            // labels. For this test factory, the label itself is the family boundary.
            MiddlewareOverrideKey::of::<Self>(self.0)
        }

        fn control_role(&self) -> ControlMiddlewareRole {
            ControlMiddlewareRole::None
        }

        fn plan_contribution(&self) -> MiddlewarePlanContribution {
            MiddlewarePlanContribution::None
        }

        fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
            None
        }

        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
        ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
            Ok(Box::new(TestMiddleware(self.0)))
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

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestIn;

    impl TypedPayload for TestIn {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.in";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct TestOut;

    impl TypedPayload for TestOut {
        const EVENT_TYPE: &'static str = "test.ai_map_reduce.out";
    }

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
            backpressure: None,
        })
    }

    #[test]
    fn ai_map_reduce_lowering_expands_stages_and_rewrites_edges() {
        let mut members: HashMap<String, FlowMember> = HashMap::new();

        members.insert(
            "batch".to_string(),
            mk_transform("batch").into_flow_member(),
        );
        members.insert("out".to_string(), mk_transform("out").into_flow_member());

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

        members.insert("digest".to_string(), digest.into_flow_member());

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        let (stages, artifacts) = lower_composites(members, &mut connections)
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

        // The manifest follows the event-sourced data path through map. There
        // is no direct chunk -> collect fan-in edge.
        assert!(connections.iter().any(|(from, to, kind)| {
            from == "digest__chunk" && to == "digest__map" && *kind == EdgeKind::Forward
        }));
        assert!(!connections
            .iter()
            .any(|(from, to, _)| { from == "digest__chunk" && to == "digest__collect" }));

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
        assert!(subgraph.internal_edges.iter().any(|edge| {
            edge.from_stage == "digest__chunk"
                && edge.to_stage == "digest__map"
                && edge.role == "data"
        }));
        assert!(!subgraph.internal_edges.iter().any(|edge| {
            edge.from_stage == "digest__chunk" && edge.to_stage == "digest__collect"
        }));
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

        // FLOWIP-128a: the macro returns a composite descriptor; boundary
        // typing lives on the declared ports, validated at member level
        // post-lowering.
        assert_eq!(digest.kind(), "ai_map_reduce");
        assert_eq!(digest.schema_version(), 1);

        let mut members: HashMap<String, FlowMember> = HashMap::new();
        members.insert(
            "batch".to_string(),
            mk_transform("batch").into_flow_member(),
        );
        members.insert("out".to_string(), mk_transform("out").into_flow_member());
        members.insert("digest".to_string(), digest.into_flow_member());

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        let (stages, _artifacts) = lower_composites(members, &mut connections)
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

        assert_eq!(digest.kind(), "ai_map_reduce");
        assert_eq!(digest.schema_version(), 1);

        let mut members: HashMap<String, FlowMember> = HashMap::new();
        members.insert(
            "batch".to_string(),
            mk_transform("batch").into_flow_member(),
        );
        members.insert("out".to_string(), mk_transform("out").into_flow_member());
        members.insert("digest".to_string(), digest.into_flow_member());

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        let (stages, _artifacts) = lower_composites(members, &mut connections).expect(
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

    /// FLOWIP-114c Acceptance #20: composite outer-boundary invariant.
    /// The entry stage must carry an `input_type` matching the composite's
    /// declared outer input. The exit stage must carry an `output_type`
    /// matching the composite's declared outer output. Without this, the
    /// validator's composite-internal-edge skip rule would silently miss a
    /// boundary mismatch.
    #[test]
    fn ai_map_reduce_outer_boundary_input_and_output_types_match_composite_contract() {
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

        let mut members: HashMap<String, FlowMember> = HashMap::new();
        members.insert("digest".to_string(), digest.into_flow_member());

        let mut connections: Vec<(String, String, EdgeKind)> = Vec::new();
        let (stages, _artifacts) = lower_composites(members, &mut connections)
            .expect("ai_map_reduce composite should lower without error");

        let entry = stages
            .get("digest__chunk")
            .expect("entry stage 'digest__chunk' should exist after lowering");
        let entry_meta = entry
            .typing_metadata()
            .expect("entry stage should carry typing metadata");
        assert_eq!(
            entry_meta.input_type,
            TypeHint::exact_payload::<TestIn>(),
            "entry stage input_type must equal the composite's declared outer input"
        );
        assert_eq!(
            entry_meta.output_type,
            TypeHint::exact_payload::<TestChunk>(),
            "entry stage output_type remains the user-facing chunk type"
        );
        assert!(
            entry_meta
                .output_contract
                .contains(&TypeHint::exact_payload::<TestChunk>()),
            "entry output contract must include chunk envelopes"
        );
        assert!(
            entry_meta
                .output_contract
                .contains(&TypeHint::exact_payload::<AiMapReducePlanningManifest>()),
            "entry output contract must include the framework planning manifest"
        );

        let map = stages
            .get("digest__map")
            .expect("map stage 'digest__map' should exist after lowering");
        let map_meta = map
            .typing_metadata()
            .expect("map stage should carry typing metadata");
        assert!(
            map_meta
                .output_contract
                .contains(&TypeHint::exact_payload::<TestPartial>()),
            "map output contract keeps the user-facing partial type"
        );
        assert!(
            map_meta
                .output_contract
                .contains(&TypeHint::exact_payload::<AiMapReducePlanningManifest>()),
            "map output contract must include the forwarded planning manifest"
        );
        assert!(
            map_meta
                .output_contract
                .contains(&TypeHint::exact_payload::<
                    AiMapReduceTaggedPartial<serde_json::Value>,
                >()),
            "map output contract must include tagged partial transport events"
        );
        assert!(
            map_meta
                .output_contract
                .contains(&TypeHint::exact_payload::<AiMapReduceChunkFailed>()),
            "map output contract must include chunk failure transport events"
        );

        let exit = stages
            .get("digest__finalize")
            .expect("exit stage 'digest__finalize' should exist after lowering");
        let exit_meta = exit
            .typing_metadata()
            .expect("exit stage should carry typing metadata");
        assert_eq!(
            exit_meta.output_type,
            TypeHint::exact_payload::<TestOut>(),
            "exit stage output_type must equal the composite's declared outer output"
        );
    }

    /// FLOWIP-114c Acceptance #18 success case, revised by FLOWIP-128a A6:
    /// an `ai_map_reduce`-using flow with external upstream and downstream
    /// stages must build clean under `validate_edge_typing`. Declared-feed
    /// edges (chunk -> map carries chunks plus manifest; map -> collect carries
    /// forwarded manifest plus terminal/partial transport) validate their lane
    /// payloads against the upstream output contract and are exempt from
    /// downstream-input matching; the ordinary collect -> finalize edge
    /// type-checks normally; the external boundary edges are checked against
    /// the composite's outer types.
    /// Subgraph co-membership alone never skips validation.
    #[test]
    fn ai_map_reduce_external_flow_validates_clean_with_subgraph_attached() {
        use obzenflow_core::id::StageId;
        use obzenflow_runtime::id_conversions::StageIdExt;
        use obzenflow_topology::{
            DirectedEdge as TopologyDirectedEdge, StageInfo as TopologyStageInfo,
            StageType as TopologyStageType, Topology,
        };
        use std::collections::HashMap as StdHashMap;

        use crate::dsl::typing::validate_edge_typing;

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

        let mut members: HashMap<String, FlowMember> = HashMap::new();
        members.insert(
            "upstream".to_string(),
            crate::source!(name: "upstream", TestIn => placeholder!()).into_flow_member(),
        );
        members.insert(
            "downstream".to_string(),
            crate::sink!(name: "downstream", TestOut => placeholder!()).into_flow_member(),
        );
        members.insert("digest".to_string(), digest.into_flow_member());

        let mut connections = vec![
            (
                "upstream".to_string(),
                "digest".to_string(),
                EdgeKind::Forward,
            ),
            (
                "digest".to_string(),
                "downstream".to_string(),
                EdgeKind::Forward,
            ),
        ];

        let (stages, artifacts) = lower_composites(members, &mut connections)
            .expect("composite lowering succeeds with external upstream and downstream");

        // After lowering, "digest" is gone and replaced by four internal
        // stages. The external upstream and downstream survive untouched.
        assert!(stages.contains_key("upstream"));
        assert!(stages.contains_key("downstream"));
        assert!(stages.contains_key("digest__chunk"));
        assert!(stages.contains_key("digest__map"));
        assert!(stages.contains_key("digest__collect"));
        assert!(stages.contains_key("digest__finalize"));
        assert!(!stages.contains_key("digest"));

        // Build name -> StageId map for every surviving descriptor.
        let mut name_to_id: StdHashMap<String, StageId> = StdHashMap::new();
        for name in stages.keys() {
            name_to_id.insert(name.clone(), StageId::new());
        }

        // Construct `StageInfo` carrying subgraph membership on the
        // composite-internal stages. This mirrors what `build_typed_flow!`
        // should do before calling `validate_edge_typing` (the production
        // attachment happens after the validator today, which is a separate
        // issue called out in the FLOWIP closing-PR notes; this test
        // demonstrates the validator behaves correctly when fed properly).
        let mut topology_stages: Vec<TopologyStageInfo> = Vec::new();
        for (name, descriptor) in &stages {
            let id = name_to_id[name];
            let topology_id = id.to_topology_id();
            // The validator only reads typing metadata from descriptors,
            // so any structurally-valid StageType works here.
            let topology_kind = match descriptor.stage_type() {
                obzenflow_core::event::context::StageType::FiniteSource
                | obzenflow_core::event::context::StageType::InfiniteSource => {
                    TopologyStageType::FiniteSource
                }
                obzenflow_core::event::context::StageType::Sink => TopologyStageType::Sink,
                obzenflow_core::event::context::StageType::Transform => {
                    TopologyStageType::Transform
                }
                obzenflow_core::event::context::StageType::Stateful => TopologyStageType::Stateful,
                obzenflow_core::event::context::StageType::Join => TopologyStageType::Join,
            };
            let mut info = TopologyStageInfo::new(topology_id, name.clone(), topology_kind);
            if let Some(membership) = artifacts.stage_subgraphs.get(name) {
                info = info.with_subgraph(membership.clone());
            }
            topology_stages.push(info);
        }

        let mut topology_edges: Vec<TopologyDirectedEdge> = Vec::new();
        for (from, to, kind) in &connections {
            let from_id = name_to_id[from].to_topology_id();
            let to_id = name_to_id[to].to_topology_id();
            topology_edges.push(TopologyDirectedEdge::new(from_id, to_id, *kind));
        }

        let topology = Topology::new_unvalidated(topology_stages, topology_edges)
            .expect("topology construction succeeds");

        validate_edge_typing(&topology, &stages, &name_to_id, &artifacts.internal_feeds).expect(
            "FLOWIP-114c Acceptance #18: ai_map_reduce composite-internal edges must be \
             skipped when subgraph_id is attached to their StageInfo, so a fully-typed \
             external flow builds clean. If this fails, the composite-internal-edge skip \
             rule in validate_edge_typing has regressed.",
        );
    }

    /// FLOWIP-114c Acceptance #18, failure-mode case: when an external edge
    /// crosses the composite's outer boundary with a type mismatch, the
    /// validator must surface the error. This proves the skip rule does
    /// not silently swallow legitimate boundary failures.
    #[test]
    fn ai_map_reduce_external_boundary_mismatch_is_surfaced() {
        use obzenflow_core::id::StageId;
        use obzenflow_runtime::id_conversions::StageIdExt;
        use obzenflow_topology::{
            DirectedEdge as TopologyDirectedEdge, StageInfo as TopologyStageInfo,
            StageType as TopologyStageType, Topology,
        };
        use std::collections::HashMap as StdHashMap;

        use crate::dsl::error::EdgeTypingMismatchKind;
        use crate::dsl::typing::validate_edge_typing;

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

        let mut members: HashMap<String, FlowMember> = HashMap::new();
        // External upstream emits TestOut, which does NOT match the
        // composite's declared outer input (TestIn). The skip rule must
        // not paper over this; the validator must report a SingleEdge
        // mismatch on the upstream -> digest__chunk edge.
        members.insert(
            "upstream".to_string(),
            crate::source!(name: "upstream", TestOut => placeholder!()).into_flow_member(),
        );
        members.insert(
            "downstream".to_string(),
            crate::sink!(name: "downstream", TestOut => placeholder!()).into_flow_member(),
        );
        members.insert("digest".to_string(), digest.into_flow_member());

        let mut connections = vec![
            (
                "upstream".to_string(),
                "digest".to_string(),
                EdgeKind::Forward,
            ),
            (
                "digest".to_string(),
                "downstream".to_string(),
                EdgeKind::Forward,
            ),
        ];

        let (stages, artifacts) =
            lower_composites(members, &mut connections).expect("composite lowering succeeds");

        let mut name_to_id: StdHashMap<String, StageId> = StdHashMap::new();
        for name in stages.keys() {
            name_to_id.insert(name.clone(), StageId::new());
        }

        let mut topology_stages: Vec<TopologyStageInfo> = Vec::new();
        for (name, descriptor) in &stages {
            let id = name_to_id[name];
            let topology_kind = match descriptor.stage_type() {
                obzenflow_core::event::context::StageType::FiniteSource
                | obzenflow_core::event::context::StageType::InfiniteSource => {
                    TopologyStageType::FiniteSource
                }
                obzenflow_core::event::context::StageType::Sink => TopologyStageType::Sink,
                obzenflow_core::event::context::StageType::Transform => {
                    TopologyStageType::Transform
                }
                obzenflow_core::event::context::StageType::Stateful => TopologyStageType::Stateful,
                obzenflow_core::event::context::StageType::Join => TopologyStageType::Join,
            };
            let mut info = TopologyStageInfo::new(id.to_topology_id(), name.clone(), topology_kind);
            if let Some(membership) = artifacts.stage_subgraphs.get(name) {
                info = info.with_subgraph(membership.clone());
            }
            topology_stages.push(info);
        }

        let mut topology_edges: Vec<TopologyDirectedEdge> = Vec::new();
        for (from, to, kind) in &connections {
            let from_id = name_to_id[from].to_topology_id();
            let to_id = name_to_id[to].to_topology_id();
            topology_edges.push(TopologyDirectedEdge::new(from_id, to_id, *kind));
        }

        let topology = Topology::new_unvalidated(topology_stages, topology_edges)
            .expect("topology construction succeeds");

        let errors =
            validate_edge_typing(&topology, &stages, &name_to_id, &artifacts.internal_feeds)
                .expect_err(
                    "expected a SingleEdge error on the external upstream -> digest__chunk \
             boundary edge (TestOut -> TestIn mismatch)",
                );

        let boundary_error = errors
            .iter()
            .find(|e| {
                e.upstream_stage == "upstream"
                    && e.downstream_stage == "digest__chunk"
                    && matches!(e.kind, EdgeTypingMismatchKind::SingleEdge)
            })
            .expect("expected SingleEdge error on upstream -> digest__chunk boundary edge");

        assert_eq!(
            boundary_error.upstream_type,
            std::any::type_name::<TestOut>()
        );
        assert_eq!(
            boundary_error.expected_type,
            std::any::type_name::<TestIn>()
        );
    }

    #[test]
    fn lowering_detects_internal_stage_name_collisions() {
        let mut members: HashMap<String, FlowMember> = HashMap::new();

        // Collision: existing stage has the same binding as the would-be lowered stage.
        let existing: Box<dyn StageDescriptor> = Box::new(TransformDescriptor {
            name: "digest__chunk".to_string(),
            handler: NoopTransform,
            middleware: vec![],
            backpressure: None,
        });
        members.insert("digest__chunk".to_string(), existing.into_flow_member());

        members.insert(
            "digest".to_string(),
            ai_map_reduce::map_reduce::<TestIn, TestChunk, TestPartial, TestCollected, TestOut>()
                .chunker(NoopTransform)
                .map(NoopAsyncTransform)
                .collect(obzenflow_runtime::stages::stateful::CollectByInput::new(
                    TestCollected::default(),
                    |acc, partial: &TestPartial| acc.values.push(partial.value),
                ))
                .finalize(NoopAsyncTransform)
                .build()
                .into_flow_member(),
        );

        let mut connections = vec![("digest".to_string(), "out".to_string(), EdgeKind::Forward)];

        let err = match lower_composites(members, &mut connections) {
            Ok(_) => panic!("expected name collision error from composite lowering"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("lowering collision"),
            "unexpected error: {err}"
        );
    }

    fn render_visibility(visibility: obzenflow_runtime::feed_plan::FactVisibility) -> &'static str {
        match visibility {
            obzenflow_runtime::feed_plan::FactVisibility::Routable => "routable",
            obzenflow_runtime::feed_plan::FactVisibility::Unrouted => "unrouted",
        }
    }

    /// FLOWIP-128a regression snapshots for the reference `ai_map_reduce`
    /// lowering. Feed keys and visibility changing here means journal routing
    /// changed and requires an explicit Insta snapshot review.
    #[test]
    fn lowering_feed_plan_and_artifacts_match_snapshots() {
        let mut members: HashMap<String, FlowMember> = HashMap::new();
        members.insert(
            "batch".to_string(),
            mk_transform("batch").into_flow_member(),
        );
        members.insert("out".to_string(), mk_transform("out").into_flow_member());
        members.insert(
            "digest".to_string(),
            ai_map_reduce::map_reduce::<TestIn, TestChunk, TestPartial, TestCollected, TestOut>()
                .chunker(NoopTransform)
                .map(NoopAsyncTransform)
                .collect(obzenflow_runtime::stages::stateful::CollectByInput::new(
                    TestCollected::default(),
                    |acc, partial: &TestPartial| acc.values.push(partial.value),
                ))
                .finalize(NoopAsyncTransform)
                .build()
                .into_flow_member(),
        );

        let mut connections = vec![
            ("batch".to_string(), "digest".to_string(), EdgeKind::Forward),
            ("digest".to_string(), "out".to_string(), EdgeKind::Forward),
        ];

        let (stages, artifacts) =
            lower_composites(members, &mut connections).expect("lowering succeeds");

        // Membership snapshot: serialized shape must stay byte-identical for
        // flows that declare no new manifest annotations.
        let membership: std::collections::BTreeMap<_, _> =
            artifacts.stage_subgraphs.iter().collect();
        let membership_json =
            serde_json::to_string_pretty(&membership).expect("membership serializes");
        insta::assert_snapshot!(membership_json, @r###"
        {
          "digest__chunk": {
            "subgraph_id": "ai_map_reduce:digest",
            "kind": "ai_map_reduce",
            "binding": "digest",
            "role": "chunk",
            "order": 0,
            "is_entry": true,
            "is_exit": false
          },
          "digest__collect": {
            "subgraph_id": "ai_map_reduce:digest",
            "kind": "ai_map_reduce",
            "binding": "digest",
            "role": "collect",
            "order": 2,
            "is_entry": false,
            "is_exit": false
          },
          "digest__finalize": {
            "subgraph_id": "ai_map_reduce:digest",
            "kind": "ai_map_reduce",
            "binding": "digest",
            "role": "finalize",
            "order": 3,
            "is_entry": false,
            "is_exit": true
          },
          "digest__map": {
            "subgraph_id": "ai_map_reduce:digest",
            "kind": "ai_map_reduce",
            "binding": "digest",
            "role": "map",
            "order": 1,
            "is_entry": false,
            "is_exit": false
          }
        }
        "###);

        // Subgraph registry snapshot (name-keyed spec; rendered because the
        // spec type is not serde).
        let mut spec_lines = Vec::new();
        for spec in &artifacts.subgraphs {
            spec_lines.push(format!(
                "subgraph id={} kind={} binding={} label={} collapsible={}",
                spec.subgraph_id, spec.kind, spec.binding, spec.label, spec.collapsible
            ));
            spec_lines.push(format!("  members={:?}", spec.member_stage_names));
            spec_lines.push(format!(
                "  entry={:?} exit={:?} parent={:?}",
                spec.entry_stage_names, spec.exit_stage_names, spec.parent_subgraph_id
            ));
            for edge in &spec.internal_edges {
                spec_lines.push(format!(
                    "  edge {} -> {} role={}",
                    edge.from_stage, edge.to_stage, edge.role
                ));
            }
        }
        let mut spec_text = spec_lines.join("\n");
        spec_text.push('\n');
        insta::assert_snapshot!(spec_text, @r###"
        subgraph id=ai_map_reduce:digest kind=ai_map_reduce binding=digest label=digest collapsible=true
          members=["digest__chunk", "digest__map", "digest__collect", "digest__finalize"]
          entry=["digest__chunk"] exit=["digest__finalize"] parent=None
          edge digest__chunk -> digest__map role=data
          edge digest__map -> digest__collect role=data
          edge digest__collect -> digest__finalize role=data
        "###);

        // Feed-plan snapshot: build the topology the way the flow build seam
        // does (StageInfo + subgraph membership), then render feeds and
        // contracts keyed by stage NAME so the snapshot survives fresh ULIDs.
        fn to_topology_stage_type(
            core: obzenflow_core::event::context::StageType,
        ) -> obzenflow_topology::StageType {
            use obzenflow_core::event::context::StageType as S;
            use obzenflow_topology::StageType as T;
            match core {
                S::FiniteSource => T::FiniteSource,
                S::InfiniteSource => T::InfiniteSource,
                S::Transform => T::Transform,
                S::Sink => T::Sink,
                S::Stateful => T::Stateful,
                S::Join => T::Join,
            }
        }

        let mut name_to_id: HashMap<String, obzenflow_core::StageId> = HashMap::new();
        for name in stages.keys() {
            name_to_id.insert(name.clone(), obzenflow_core::StageId::new());
        }

        let mut topo_stages = Vec::new();
        for (name, descriptor) in &stages {
            let core_id = name_to_id[name];
            // Topology validation needs a source and sink; the outer fixture
            // stages play those roles for the graph without affecting feeds.
            let stage_type = match name.as_str() {
                "batch" => obzenflow_topology::StageType::FiniteSource,
                "out" => obzenflow_topology::StageType::Sink,
                _ => to_topology_stage_type(descriptor.stage_type()),
            };
            let mut info = obzenflow_topology::StageInfo::new(
                obzenflow_topology::StageId::from_ulid(core_id.as_ulid()),
                descriptor.name().to_string(),
                stage_type,
            );
            if let Some(membership) = artifacts.stage_subgraphs.get(name) {
                info = info.with_subgraph(membership.clone());
            }
            topo_stages.push(info);
        }
        let topo_edges: Vec<obzenflow_topology::DirectedEdge> = connections
            .iter()
            .map(|(from, to, kind)| {
                obzenflow_topology::DirectedEdge::new(
                    obzenflow_topology::StageId::from_ulid(name_to_id[from].as_ulid()),
                    obzenflow_topology::StageId::from_ulid(name_to_id[to].as_ulid()),
                    *kind,
                )
            })
            .collect();
        let topology = obzenflow_topology::Topology::new(topo_stages, topo_edges)
            .expect("fixture topology is valid");

        // The plan must come from the generic declared-feed path. The snapshot
        // pins the complete selected-feed and output-contract inventory.
        let plan = crate::dsl::typing::derive_feed_plan(
            &topology,
            &stages,
            &name_to_id,
            &artifacts.internal_feeds,
        );

        let id_to_name: HashMap<obzenflow_core::StageId, &String> =
            name_to_id.iter().map(|(name, id)| (*id, name)).collect();

        let mut feed_lines: Vec<String> = plan
            .all_feeds()
            .iter()
            .map(|feed| {
                format!(
                    "feed {} -> {} role={} key={} visibility={}",
                    id_to_name[&feed.key.upstream_stage],
                    id_to_name[&feed.key.downstream_stage],
                    feed.key.role.as_str(),
                    feed.key.selected_payload_key,
                    render_visibility(feed.selected_payload.visibility),
                )
            })
            .collect();
        feed_lines.sort();

        let mut contract_lines: Vec<String> = Vec::new();
        for (stage_id, contract) in &plan.stage_output_contracts {
            for output in &contract.outputs {
                contract_lines.push(format!(
                    "contract {} key={} visibility={}",
                    id_to_name[stage_id],
                    output.payload_key(),
                    render_visibility(output.visibility),
                ));
            }
        }
        contract_lines.sort();

        let mut plan_text = feed_lines.join("\n");
        plan_text.push('\n');
        plan_text.push_str(&contract_lines.join("\n"));
        plan_text.push('\n');
        insta::assert_snapshot!(plan_text, @r###"
        feed digest__chunk -> digest__map role=input key=ai.map_reduce.planning_manifest.v1 visibility=routable
        feed digest__chunk -> digest__map role=input key=test.ai_map_reduce.chunk.v1 visibility=routable
        feed digest__collect -> digest__finalize role=input key=test.ai_map_reduce.collected.v1 visibility=routable
        feed digest__map -> digest__collect role=input key=ai.map_reduce.chunk_failed.v1 visibility=routable
        feed digest__map -> digest__collect role=input key=ai.map_reduce.planning_manifest.v1 visibility=routable
        feed digest__map -> digest__collect role=input key=ai.map_reduce.tagged_partial.v1 visibility=routable
        contract digest__chunk key=ai.map_reduce.planning_manifest.v1 visibility=routable
        contract digest__chunk key=test.ai_map_reduce.chunk.v1 visibility=routable
        contract digest__collect key=test.ai_map_reduce.collected.v1 visibility=routable
        contract digest__finalize key=test.ai_map_reduce.out.v1 visibility=unrouted
        contract digest__map key=ai.map_reduce.chunk_failed.v1 visibility=routable
        contract digest__map key=ai.map_reduce.planning_manifest.v1 visibility=routable
        contract digest__map key=ai.map_reduce.tagged_partial.v1 visibility=routable
        contract digest__map key=test.ai_map_reduce.partial.v1 visibility=unrouted
        "###);
    }
}
