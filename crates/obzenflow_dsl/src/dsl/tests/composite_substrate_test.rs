// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128a substrate fixtures: the two-port test composite (D1), lane
//! and class vocabulary (D2/D3), deterministic lowering (A2), and the
//! 128B-shaped role-addressed effect-declaration passthrough.

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::effects::{EffectDeclaration, EffectSafety, IdempotencyKeyPolicy};
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::TransformHandler;
    use obzenflow_topology::EdgeKind;
    use serde::{Deserialize, Serialize};

    use crate::dsl::composites::lower_composites;
    use crate::dsl::composition::{
        CompositeBuildContext, CompositeBuildError, CompositeDescriptor, FlowMember, IntoFlowMember,
    };
    use crate::dsl::stage_descriptor::{StageDescriptor, TransformDescriptor};
    use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};

    #[derive(Debug, Clone)]
    struct Noop;

    #[async_trait::async_trait]
    impl TransformHandler for Noop {
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

    macro_rules! payload {
        ($name:ident, $event:literal) => {
            #[derive(Debug, Clone, Serialize, Deserialize)]
            struct $name;
            impl TypedPayload for $name {
                const EVENT_TYPE: &'static str = $event;
            }
        };
    }

    payload!(InP, "test.branch.in");
    payload!(MidP, "test.branch.mid");
    payload!(CompletedP, "test.branch.completed");
    payload!(FailedP, "test.branch.failed");
    payload!(UnrelatedP, "test.branch.unrelated");

    fn typed_transform(
        name: &str,
        input: TypeHint,
        output: TypeHint,
        extra_outputs: Vec<TypeHint>,
    ) -> Box<dyn StageDescriptor> {
        let descriptor = TransformDescriptor {
            name: name.to_string(),
            handler: Noop,
            middleware: vec![],
            backpressure: None,
        };
        let mut metadata = StageTypingMetadata::transform(input, output, false, None);
        if !extra_outputs.is_empty() {
            metadata = metadata.with_additional_output_contract(extra_outputs);
        }
        wrap_typed_descriptor(Box::new(descriptor), metadata)
    }

    /// 128B-shaped member: an ordinary descriptor whose effect declarations
    /// must survive lowering under its role address (D2 attachment surface).
    struct DeclaringDescriptor {
        inner: TransformDescriptor<Noop>,
    }

    #[async_trait::async_trait]
    impl StageDescriptor for DeclaringDescriptor {
        fn name(&self) -> &str {
            self.inner.name()
        }

        fn set_name(&mut self, name: String) {
            self.inner.set_name(name);
        }

        fn stage_type(&self) -> obzenflow_core::event::context::StageType {
            obzenflow_core::event::context::StageType::Transform
        }

        fn effect_declarations(&self) -> Vec<EffectDeclaration> {
            vec![EffectDeclaration {
                effect_type: "test.fixture_effect",
                safety: EffectSafety::Transactional,
                idempotency_key_policy: IdempotencyKeyPolicy::NotRequired,
                required_ports: Vec::new(),
                transactional_executor: None,
                outcome_fact_types: Vec::new(),
            }]
        }

        async fn create_handle_with_flow_middleware(
            self: Box<Self>,
            config: obzenflow_runtime::pipeline::config::StageConfig,
            resources: obzenflow_runtime::stages::StageResources,
            flow_middleware: Vec<Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>>,
            control_middleware: std::sync::Arc<
                obzenflow_adapters::middleware::control::ControlMiddlewareAggregator,
            >,
        ) -> crate::dsl::StageCreationResult<
            obzenflow_runtime::stages::common::stage_handle::BoxedStageHandle,
        > {
            Box::new(self.inner)
                .create_handle_with_flow_middleware(
                    config,
                    resources,
                    flow_middleware,
                    control_middleware,
                )
                .await
        }
    }

    /// Two-output-port composite: `intake -> done` on a data edge, an
    /// `intake -> failed` status lane, ports `completed` (default, on done)
    /// and `failed` (on the member that also carries the status feed).
    struct TestBranchComposite {
        name: String,
        with_class: bool,
        declaring_worker: bool,
    }

    impl TestBranchComposite {
        fn new() -> Self {
            Self {
                name: "test_branch".to_string(),
                with_class: false,
                declaring_worker: false,
            }
        }
    }

    impl CompositeDescriptor for TestBranchComposite {
        fn name(&self) -> &str {
            &self.name
        }

        fn set_name(&mut self, name: String) {
            self.name = name;
        }

        fn kind(&self) -> &'static str {
            "test_branch"
        }

        fn schema_version(&self) -> u32 {
            1
        }

        fn expand(
            self: Box<Self>,
            ctx: &mut CompositeBuildContext,
        ) -> Result<(), CompositeBuildError> {
            ctx.permit_class("driver");

            let intake = typed_transform(
                "intake",
                TypeHint::exact_payload::<InP>(),
                TypeHint::exact_payload::<MidP>(),
                vec![TypeHint::exact_payload::<FailedP>()],
            );
            let done = typed_transform(
                "done",
                TypeHint::exact_payload::<MidP>(),
                TypeHint::exact_payload::<CompletedP>(),
                vec![],
            );
            let failed = typed_transform(
                "failed",
                TypeHint::exact_payload::<FailedP>(),
                TypeHint::exact_payload::<FailedP>(),
                vec![],
            );

            let intake_member = ctx.member("intake").descriptor(intake);
            if self.with_class {
                intake_member.class("driver");
            }
            if self.declaring_worker {
                ctx.member("worker")
                    .descriptor(Box::new(DeclaringDescriptor {
                        inner: TransformDescriptor {
                            name: "worker".to_string(),
                            handler: Noop,
                            middleware: vec![],
                            backpressure: None,
                        },
                    }));
                ctx.edge("intake", "worker");
            }
            ctx.member("done").descriptor(done);
            ctx.member("failed").descriptor(failed);

            ctx.edge("intake", "done");
            ctx.feed("intake", "failed")
                .lane("status")
                .payload::<FailedP>();

            ctx.permit_lane("status_extra");

            ctx.boundary()
                .input("in", "intake")
                .payload::<InP>()
                .default()
                .output("completed", "done")
                .payload::<CompletedP>()
                .default()
                .output("failed", "failed")
                .payload::<FailedP>();

            Ok(())
        }
    }

    fn branch_members() -> HashMap<String, FlowMember> {
        let mut members: HashMap<String, FlowMember> = HashMap::new();
        members.insert(
            "src".to_string(),
            typed_transform(
                "src",
                TypeHint::exact_payload::<InP>(),
                TypeHint::exact_payload::<InP>(),
                vec![],
            )
            .into_flow_member(),
        );
        members.insert(
            "ok_sink".to_string(),
            typed_transform(
                "ok_sink",
                TypeHint::exact_payload::<CompletedP>(),
                TypeHint::exact_payload::<CompletedP>(),
                vec![],
            )
            .into_flow_member(),
        );
        members.insert(
            "err_sink".to_string(),
            typed_transform(
                "err_sink",
                TypeHint::exact_payload::<FailedP>(),
                TypeHint::exact_payload::<FailedP>(),
                vec![],
            )
            .into_flow_member(),
        );
        let composite: Box<dyn CompositeDescriptor> = Box::new(TestBranchComposite::new());
        members.insert("branch".to_string(), composite.into_flow_member());
        members
    }

    fn branch_connections() -> Vec<(String, String, EdgeKind)> {
        vec![
            ("src".to_string(), "branch".to_string(), EdgeKind::Forward),
            (
                "branch".to_string(),
                "ok_sink".to_string(),
                EdgeKind::Forward,
            ),
            (
                "branch".to_string(),
                "err_sink".to_string(),
                EdgeKind::Forward,
            ),
        ]
    }

    #[test]
    fn two_port_composite_rewrites_edges_by_type_and_default() {
        let mut connections = branch_connections();
        let (stages, artifacts) =
            lower_composites(branch_members(), &mut connections).expect("lowering succeeds");

        // Input edge binds the input port's member; output edges resolve by
        // the downstream's declared input type (D1).
        assert!(connections.contains(&(
            "src".to_string(),
            "branch__intake".to_string(),
            EdgeKind::Forward
        )));
        assert!(connections.contains(&(
            "branch__done".to_string(),
            "ok_sink".to_string(),
            EdgeKind::Forward
        )));
        assert!(connections.contains(&(
            "branch__failed".to_string(),
            "err_sink".to_string(),
            EdgeKind::Forward
        )));

        // The status lane is a declared feed on the member that also owns
        // the `failed` output port.
        assert_eq!(artifacts.internal_feeds.len(), 1);
        let feed = &artifacts.internal_feeds[0];
        assert_eq!(feed.from_stage, "branch__intake");
        assert_eq!(feed.to_stage, "branch__failed");
        assert_eq!(feed.lane, "status");

        // Manifest ports resolved to member stage names.
        let boundary = &artifacts.boundaries["branch"];
        assert_eq!(boundary.subgraph_id, "test_branch:branch");
        assert_eq!(boundary.input().stage_name, "branch__intake");
        assert_eq!(boundary.outputs.len(), 2);

        // Type-directed port choice survives the physical edge rewrite. The
        // runtime and Studio consume these names rather than re-inferring them
        // from member position or type metadata (B3).
        let bound_port = |from: &str, to: &str| {
            artifacts
                .boundary_edges
                .iter()
                .find(|edge| edge.from_stage == from && edge.to_stage == to)
                .and_then(|edge| edge.ports.first())
                .map(|port| (port.subgraph_id.as_str(), port.port_name.as_str()))
        };
        assert_eq!(
            bound_port("src", "branch__intake"),
            Some(("test_branch:branch", "in"))
        );
        assert_eq!(
            bound_port("branch__done", "ok_sink"),
            Some(("test_branch:branch", "completed"))
        );
        assert_eq!(
            bound_port("branch__failed", "err_sink"),
            Some(("test_branch:branch", "failed"))
        );

        // Feed plan and edge validation through the generic path.
        let mut name_to_id: HashMap<String, obzenflow_core::StageId> = HashMap::new();
        for name in stages.keys() {
            name_to_id.insert(name.clone(), obzenflow_core::StageId::new());
        }
        let mut topo_stages = Vec::new();
        for (name, descriptor) in &stages {
            let stage_type = match name.as_str() {
                "src" => obzenflow_topology::StageType::FiniteSource,
                "ok_sink" | "err_sink" => obzenflow_topology::StageType::Sink,
                _ => obzenflow_topology::StageType::Transform,
            };
            let mut info = obzenflow_topology::StageInfo::new(
                obzenflow_topology::StageId::from_ulid(name_to_id[name].as_ulid()),
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

        crate::dsl::typing::validate_edge_typing(
            &topology,
            &stages,
            &name_to_id,
            &artifacts.internal_feeds,
        )
        .expect("declared feeds validate; ordinary edges type-check");

        let plan = crate::dsl::typing::derive_feed_plan(
            &topology,
            &stages,
            &name_to_id,
            &artifacts.internal_feeds,
        );
        let status_feed = plan.all_feeds().iter().find(|feed| {
            feed.key.upstream_stage == name_to_id["branch__intake"]
                && feed.key.downstream_stage == name_to_id["branch__failed"]
        });
        assert!(
            status_feed.is_some_and(|feed| feed
                .key
                .selected_payload_key
                .starts_with("test.branch.failed")),
            "declared status lane must produce a selected feed keyed by its payload"
        );
    }

    #[test]
    fn ambiguous_output_port_fails_with_locked_diagnostic() {
        struct AmbiguousComposite {
            name: String,
        }

        impl CompositeDescriptor for AmbiguousComposite {
            fn name(&self) -> &str {
                &self.name
            }
            fn set_name(&mut self, name: String) {
                self.name = name;
            }
            fn kind(&self) -> &'static str {
                "test_ambiguous"
            }
            fn schema_version(&self) -> u32 {
                1
            }
            fn expand(
                self: Box<Self>,
                ctx: &mut CompositeBuildContext,
            ) -> Result<(), CompositeBuildError> {
                let a = typed_transform(
                    "a",
                    TypeHint::exact_payload::<InP>(),
                    TypeHint::exact_payload::<FailedP>(),
                    vec![],
                );
                let b = typed_transform(
                    "b",
                    TypeHint::exact_payload::<InP>(),
                    TypeHint::exact_payload::<FailedP>(),
                    vec![],
                );
                ctx.member("a").descriptor(a);
                ctx.member("b").descriptor(b);
                ctx.boundary()
                    .input("in", "a")
                    .payload::<InP>()
                    .default()
                    .output("first", "a")
                    .payload::<FailedP>()
                    .default()
                    .output("second", "b")
                    .payload::<FailedP>();
                Ok(())
            }
        }

        let mut members: HashMap<String, FlowMember> = HashMap::new();
        let composite: Box<dyn CompositeDescriptor> =
            Box::new(AmbiguousComposite { name: "amb".into() });
        members.insert("amb".to_string(), composite.into_flow_member());
        members.insert(
            "dup_sink".to_string(),
            typed_transform(
                "dup_sink",
                TypeHint::exact_payload::<FailedP>(),
                TypeHint::exact_payload::<FailedP>(),
                vec![],
            )
            .into_flow_member(),
        );

        let mut connections = vec![("amb".to_string(), "dup_sink".to_string(), EdgeKind::Forward)];
        let err = match lower_composites(members, &mut connections) {
            Ok(_) => panic!("expected ambiguity error"),
            Err(err) => err,
        };
        let message = err.to_string();
        assert!(
            message.contains("composite 'amb': ambiguous output port for downstream 'dup_sink'"),
            "unexpected: {message}"
        );
        assert!(
            message.contains("matches ports 'first' and 'second'"),
            "unexpected: {message}"
        );
        assert!(
            message.contains("bind explicitly via the composite's branch clause"),
            "unexpected: {message}"
        );
    }

    #[test]
    fn unmatched_downstream_binds_default_port() {
        let mut members = branch_members();
        members.insert(
            "odd_sink".to_string(),
            typed_transform(
                "odd_sink",
                TypeHint::exact_payload::<UnrelatedP>(),
                TypeHint::exact_payload::<UnrelatedP>(),
                vec![],
            )
            .into_flow_member(),
        );
        let mut connections = branch_connections();
        connections.push((
            "branch".to_string(),
            "odd_sink".to_string(),
            EdgeKind::Forward,
        ));

        let (_stages, _artifacts) =
            lower_composites(members, &mut connections).expect("lowering succeeds");

        // No port carries UnrelatedP, so the edge binds the default output
        // port; the type mismatch then surfaces at the member level through
        // ordinary edge validation (exercised in the flow-build tests).
        assert!(connections.contains(&(
            "branch__done".to_string(),
            "odd_sink".to_string(),
            EdgeKind::Forward
        )));
    }

    #[test]
    fn two_composites_lower_deterministically_sorted() {
        let render = |artifacts: &crate::dsl::composites::LoweringArtifacts| {
            artifacts
                .subgraphs
                .iter()
                .map(|s| s.binding.clone())
                .collect::<Vec<_>>()
        };

        let build_members = || {
            let mut members: HashMap<String, FlowMember> = HashMap::new();
            let beta: Box<dyn CompositeDescriptor> = Box::new(TestBranchComposite::new());
            let alpha: Box<dyn CompositeDescriptor> = Box::new(TestBranchComposite::new());
            members.insert("beta".to_string(), beta.into_flow_member());
            members.insert("alpha".to_string(), alpha.into_flow_member());
            members
        };

        let mut connections_one: Vec<(String, String, EdgeKind)> = Vec::new();
        let (_, artifacts_one) =
            lower_composites(build_members(), &mut connections_one).expect("lowering succeeds");
        let mut connections_two: Vec<(String, String, EdgeKind)> = Vec::new();
        let (_, artifacts_two) =
            lower_composites(build_members(), &mut connections_two).expect("lowering succeeds");

        assert_eq!(render(&artifacts_one), vec!["alpha", "beta"]);
        assert_eq!(render(&artifacts_one), render(&artifacts_two));
        assert_eq!(connections_one, connections_two);
    }

    #[test]
    fn composite_to_composite_edge_retains_both_named_cut_bindings() {
        let mut members: HashMap<String, FlowMember> = HashMap::new();
        for binding in ["alpha", "beta"] {
            let composite: Box<dyn CompositeDescriptor> = Box::new(TestBranchComposite::new());
            members.insert(binding.to_string(), composite.into_flow_member());
        }
        let mut connections = vec![("alpha".to_string(), "beta".to_string(), EdgeKind::Forward)];

        let (_, artifacts) =
            lower_composites(members, &mut connections).expect("lowering succeeds");

        assert!(connections.contains(&(
            "alpha__done".to_string(),
            "beta__intake".to_string(),
            EdgeKind::Forward,
        )));
        let edge = artifacts
            .boundary_edges
            .iter()
            .find(|edge| edge.from_stage == "alpha__done" && edge.to_stage == "beta__intake")
            .expect("rewritten boundary edge");
        let refs: HashSet<_> = edge
            .ports
            .iter()
            .map(|port| (port.subgraph_id.as_str(), port.port_name.as_str()))
            .collect();
        assert_eq!(
            refs,
            HashSet::from([
                ("test_branch:alpha", "completed"),
                ("test_branch:beta", "in"),
            ])
        );
    }

    #[test]
    fn member_class_reaches_membership_manifest() {
        let mut members: HashMap<String, FlowMember> = HashMap::new();
        let composite: Box<dyn CompositeDescriptor> = Box::new(TestBranchComposite {
            name: "classy".into(),
            with_class: true,
            declaring_worker: false,
        });
        members.insert("classy".to_string(), composite.into_flow_member());

        let mut connections: Vec<(String, String, EdgeKind)> = Vec::new();
        let (_stages, artifacts) =
            lower_composites(members, &mut connections).expect("lowering succeeds");

        let membership = &artifacts.stage_subgraphs["classy__intake"];
        assert_eq!(membership.class.as_deref(), Some("driver"));
        let rendered = serde_json::to_string(membership).expect("membership serializes");
        assert!(rendered.contains("\"class\":\"driver\""));

        // Classless members keep the byte-compatible shape (no class key).
        let plain = serde_json::to_string(&artifacts.stage_subgraphs["classy__done"])
            .expect("membership serializes");
        assert!(!plain.contains("\"class\""));
    }

    /// 128B-shaped fixture: role-scoped effect declarations resolve to the
    /// lowered member stage through the generic `ctx.member(role)` surface,
    /// with no kind-specific plumbing.
    #[test]
    fn effect_declarations_resolve_by_role_through_member_surface() {
        let mut members: HashMap<String, FlowMember> = HashMap::new();
        let composite: Box<dyn CompositeDescriptor> = Box::new(TestBranchComposite {
            name: "fx".into(),
            with_class: false,
            declaring_worker: true,
        });
        members.insert("fx".to_string(), composite.into_flow_member());

        let mut connections: Vec<(String, String, EdgeKind)> = Vec::new();
        let (stages, artifacts) =
            lower_composites(members, &mut connections).expect("lowering succeeds");

        assert_eq!(artifacts.stage_subgraphs["fx__worker"].role, "worker");
        let declarations = stages["fx__worker"].effect_declarations();
        assert_eq!(declarations.len(), 1);
        assert_eq!(declarations[0].effect_type, "test.fixture_effect");
    }
}
