// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Main flow! macro - the primary API for building ObzenFlow pipelines
//!
//! This is now a clean implementation using the let bindings approach that
//! separates stage declaration from topology definition.

/// Parse topology edges supporting both |> and <| operators (single collector)
/// Note: Join stages only accept stream inputs in topology.
/// Reference stages are specified via `join!(catalog ...)` (or the join tuple syntax).
#[macro_export]
macro_rules! parse_topology {
    // Base case - no more edges
    ($connections:expr,) => {};

    // Join tuple when only collecting connections (no join metadata collector)
    ($connections:expr, ($reference:ident, $stream:ident) |> $join:ident; $($rest:tt)*) => {
        $connections.extend([
            (
                stringify!($reference).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
            (
                stringify!($stream).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
        ]);
        $crate::parse_topology!($connections, $($rest)*);
    };

    // Forward edge: from |> to;
    ($connections:expr, $from:ident |> $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($from).to_string(),
            stringify!($to).to_string(),
            obzenflow_topology::EdgeKind::Forward,
        )]);
        $crate::parse_topology!($connections, $($rest)*);
    };

    // Backward edge: from <| to; (reversed)
    ($connections:expr, $from:ident <| $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($to).to_string(),    // Note: reversed!
            stringify!($from).to_string(),
            obzenflow_topology::EdgeKind::Backward,
        )]);
        $crate::parse_topology!($connections, $($rest)*);
    };
}

/// Parse topology edges while also collecting join input metadata
#[macro_export]
macro_rules! parse_topology_with_joins {
    // Base case with both regular and join-connection collectors
    ($connections:expr, $join_connections:expr,) => {};

    // Join tuple: (reference, stream) |> joiner; with join metadata collector
    ($connections:expr, $join_connections:expr, ($reference:ident, $stream:ident) |> $join:ident; $($rest:tt)*) => {
        $join_connections.extend([(
            stringify!($join).to_string(),
            (
                stringify!($reference).to_string(),
                stringify!($stream).to_string()
            )
        )]);
        $connections.extend([
            (
                stringify!($reference).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
            (
                stringify!($stream).to_string(),
                stringify!($join).to_string(),
                obzenflow_topology::EdgeKind::Forward,
            ),
        ]);
        $crate::parse_topology_with_joins!($connections, $join_connections, $($rest)*);
    };

    // Forward edge while tracking join connections separately
    ($connections:expr, $join_connections:expr, $from:ident |> $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($from).to_string(),
            stringify!($to).to_string(),
            obzenflow_topology::EdgeKind::Forward,
        )]);
        $crate::parse_topology_with_joins!($connections, $join_connections, $($rest)*);
    };

    // Backward edge while tracking join connections separately
    ($connections:expr, $join_connections:expr, $from:ident <| $to:ident; $($rest:tt)*) => {
        $connections.extend([(
            stringify!($to).to_string(),    // Note: reversed!
            stringify!($from).to_string(),
            obzenflow_topology::EdgeKind::Backward,
        )]);
        $crate::parse_topology_with_joins!($connections, $join_connections, $($rest)*);
    };
}

#[doc(hidden)]
#[macro_export]
macro_rules! __obzenflow_effect_ports_or_default {
    () => {
        ::obzenflow_runtime::effects::EffectPortRegistry::new()
    };
    ($effect_ports:expr) => {
        $effect_ports
    };
}

/// Declare an ObzenFlow pipeline as a single expression.
///
/// `flow!` is the primary API for building pipelines. It takes five sections
/// and returns a [`FlowDefinition`](crate::FlowDefinition) that can be handed
/// to `FlowApplication::run()`.
///
/// ## Sections
///
/// | Section | Purpose |
/// |---------|---------|
/// | `name:` | String identifier used for journal directories and metrics. |
/// | `journals:` | Journal factory (`disk_journals(path)` or `memory_journals()`). The factory declares its substrate via the required `run_state()`: durable with a run location, or ephemeral with none (FLOWIP-120u). |
/// | `middleware:` | Flow-level middleware applied to every stage by default. |
/// | `stages:` | Let-bindings producing stage descriptors via stage macros. |
/// | `topology:` | Edges connecting stages (`a \|> b;` forward, `a <\| b;` backward). |
///
/// ## Minimal example
///
/// ```rust,ignore
/// use obzenflow_dsl::{flow, source, transform, sink};
/// use obzenflow_infra::journal::memory_journals;
/// use obzenflow::typed::{sources, transforms};
///
/// let pipeline = flow! {
///     name: "example",
///     journals: memory_journals(),
///     middleware: [],
///
///     stages: {
///         src = source!(MyEvent => sources::finite(vec![MyEvent { value: 1 }]));
///         map = transform!(MyEvent -> Doubled => transforms::map(|e: MyEvent| Doubled { value: e.value * 2 }));
///         out = sink!(Doubled => |d| { println!("{}", d.value); });
///     },
///
///     topology: {
///         src |> map;
///         map |> out;
///     }
/// };
/// ```
///
/// The `name:` section is optional. If omitted, the flow is named `"default"`.
#[macro_export]
macro_rules! flow {
    // Pattern with explicit flow name (stage descriptors as expressions)
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],
        $(backpressure: $flow_bp:expr,)?
        $(effect_ports: $effect_ports:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        // FLOWIP-010 §7: the build receives the resolved config snapshot as
        // an explicit input; a context-less build no longer exists.
        $crate::FlowDefinition::new(move |__build_ctx: obzenflow_runtime::run_context::FlowBuildContext| async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::collections::HashMap;

            let journals = $journals;
            let effect_ports = $crate::__obzenflow_effect_ports_or_default!($($effect_ports)?);

            // Collect flow members (FLOWIP-128a D5): stages and composites
            // both enter through IntoFlowMember; user syntax is unchanged.
            let mut members: HashMap<String, $crate::dsl::composition::FlowMember> = HashMap::new();

            $(
                let member = $crate::dsl::composition::IntoFlowMember::into_flow_member($descriptor);
                members.insert(stringify!($stage_name).to_string(), member);
            )*

            // FLOWIP-105g-part-2: resolve binding-derived runtime stage names.
            //
            // Stage macros can request name derivation by setting the descriptor name to
            // `BINDING_DERIVED_NAME_SENTINEL`. Resolve those to the left-hand binding
            // before uniqueness checks and build phases run.
            for (binding, member) in members.iter_mut() {
                if member.name() == BINDING_DERIVED_NAME_SENTINEL {
                    member.set_name(binding.clone());
                }
            }

            // Create connections
            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();

            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);

            // FLOWIP-128a: expand composites into concrete stages + edges
            // before topology validation and runtime build. Post-lowering,
            // only stages exist; the signature enforces it.
            let (stages, lowering_artifacts) =
                $crate::dsl::composites::lower_composites(members, &mut connections)?;

            // Create closure for flow middleware
            let create_flow_middleware =
                || -> Vec<Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>> {
                    vec![
                        $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
                    ]
                };

            // Build the flow
            $crate::build_typed_flow!(
                $flow_name,
                journals,
                stages,
                connections,
                create_flow_middleware,
                lowering_artifacts,
                effect_ports = effect_ports,
                context = __build_ctx
                $(, backpressure = $flow_bp)?
            )
        })
    }};

    // Pattern without explicit flow name (uses "default")
    {
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],
        $(backpressure: $flow_bp:expr,)?
        $(effect_ports: $effect_ports:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        // FLOWIP-010 §7: the build receives the resolved config snapshot as
        // an explicit input; a context-less build no longer exists.
        $crate::FlowDefinition::new(move |__build_ctx: obzenflow_runtime::run_context::FlowBuildContext| async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::collections::HashMap;

            let journals = $journals;
            let effect_ports = $crate::__obzenflow_effect_ports_or_default!($($effect_ports)?);

            // Collect flow members (FLOWIP-128a D5): stages and composites
            // both enter through IntoFlowMember; user syntax is unchanged.
            let mut members: HashMap<String, $crate::dsl::composition::FlowMember> = HashMap::new();

            $(
                let member = $crate::dsl::composition::IntoFlowMember::into_flow_member($descriptor);
                members.insert(stringify!($stage_name).to_string(), member);
            )*

            // FLOWIP-105g-part-2: resolve binding-derived runtime stage names.
            //
            // Stage macros can request name derivation by setting the descriptor name to
            // `BINDING_DERIVED_NAME_SENTINEL`. Resolve those to the left-hand binding
            // before uniqueness checks and build phases run.
            for (binding, member) in members.iter_mut() {
                if member.name() == BINDING_DERIVED_NAME_SENTINEL {
                    member.set_name(binding.clone());
                }
            }

            // Create connections
            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();

            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);

            // FLOWIP-128a: expand composites into concrete stages + edges
            // before topology validation and runtime build. Post-lowering,
            // only stages exist; the signature enforces it.
            let (stages, lowering_artifacts) =
                $crate::dsl::composites::lower_composites(members, &mut connections)?;

            // Create closure for flow middleware
            let create_flow_middleware =
                || -> Vec<Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>> {
                    vec![
                        $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
                    ]
                };

            // Build the flow
            $crate::build_typed_flow!(
                "default",
                journals,
                stages,
                connections,
                create_flow_middleware,
                lowering_artifacts,
                effect_ports = effect_ports,
                context = __build_ctx
                $(, backpressure = $flow_bp)?
            )
        })
    }};
}

/// Build a flow for tests, returning a `FlowTestHarness` (FLOWIP-114h).
///
/// This macro is gated behind `#[cfg(any(test, feature = "test-support"))]`.
/// It reuses the normal `flow!` build path, but retains the stage data journals
/// produced during construction so deterministic test helpers can observe them.
#[cfg(any(test, feature = "test-support"))]
#[macro_export]
macro_rules! test_flow {
    // Pattern with explicit flow name (stage descriptors as expressions)
    {
        name: $flow_name:literal,
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],
        $(backpressure: $flow_bp:expr,)?
        $(effect_ports: $effect_ports:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::collections::HashMap;

            let journals = $journals;
            let effect_ports = $crate::__obzenflow_effect_ports_or_default!($($effect_ports)?);

            let mut members: HashMap<String, $crate::dsl::composition::FlowMember> = HashMap::new();
            $(
                let member = $crate::dsl::composition::IntoFlowMember::into_flow_member($descriptor);
                members.insert(stringify!($stage_name).to_string(), member);
            )*

            for (binding, member) in members.iter_mut() {
                if member.name() == BINDING_DERIVED_NAME_SENTINEL {
                    member.set_name(binding.clone());
                }
            }

            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();
            $crate::parse_topology!(connections, $($edge)*);

            let (stages, lowering_artifacts) =
                $crate::dsl::composites::lower_composites(members, &mut connections)?;

            let create_flow_middleware =
                || -> Vec<Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>> {
                    vec![
                        $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
                    ]
                };

            // Explicit built-in-defaults context: test_flow! builds without
            // a host (FLOWIP-010 §7).
            let __build_ctx = obzenflow_runtime::run_context::FlowBuildContext::for_tests();

            $crate::build_typed_flow!(
                $flow_name,
                journals,
                stages,
                connections,
                create_flow_middleware,
                lowering_artifacts,
                effect_ports = effect_ports,
                context = __build_ctx,
                finish: |handle, stage_data_journals| {
                    obzenflow_runtime::testing::FlowTestHarness::from_parts(handle, stage_data_journals)
                        .map_err(|e| $crate::dsl::FlowBuildError::StageResourcesFailed(format!(
                            "Failed to build FlowTestHarness: {e}"
                        )))
                }
                $(, backpressure = $flow_bp)?
            )
        }
    }};

    // Pattern without explicit flow name (uses "default")
    {
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],
        $(backpressure: $flow_bp:expr,)?
        $(effect_ports: $effect_ports:expr,)?

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        $crate::test_flow! {
            name: "default",
            journals: $journals,
            middleware: [$($flow_mw),*],
            $(backpressure: $flow_bp,)?
            $(
                effect_ports: $effect_ports,
            )?

            stages: {
                $($stage_name = $descriptor;)*
            },

            topology: {
                $($edge)*
            }
        }
    }};
}

/// Build the actual flow from collected stages and connections
#[macro_export]
macro_rules! build_typed_flow {
    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, $create_flow_middleware:expr, $lowering_artifacts:expr, effect_ports = $effect_ports:expr, context = $build_ctx:expr $(, backpressure = $flow_bp:expr)?) => {{
        $crate::build_typed_flow!(
            $flow_name,
            $journals,
            $stages,
            $connections,
            $create_flow_middleware,
            $lowering_artifacts,
            effect_ports = $effect_ports,
            context = $build_ctx,
            finish: |handle, _stage_data_journals| Ok::<FlowHandle, FlowBuildError>(handle)
            $(, backpressure = $flow_bp)?
        )
    }};

    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, $create_flow_middleware:expr, $lowering_artifacts:expr, effect_ports = $effect_ports:expr, context = $build_ctx:expr, finish: $finish:expr $(, backpressure = $flow_bp:expr)?) => {{
        use $crate::prelude::*;
        use $crate::dsl::FlowBuildError;
        use std::collections::{HashMap, HashSet};
        use obzenflow_topology::{StageInfo as TopologyStageInfo, DirectedEdge, EdgeKind};
        use std::sync::Arc;

        fn to_topology_id(core_id: StageId) -> obzenflow_topology::StageId {
            // Convert standard ulid crate's Ulid to topology's idkit Id
            obzenflow_topology::StageId::from_ulid(core_id.as_ulid())
        }

        fn to_topology_stage_type(
            core_stage_type: obzenflow_core::event::context::StageType,
        ) -> obzenflow_topology::StageType {
            match core_stage_type {
                obzenflow_core::event::context::StageType::FiniteSource => {
                    obzenflow_topology::StageType::FiniteSource
                }
                obzenflow_core::event::context::StageType::InfiniteSource => {
                    obzenflow_topology::StageType::InfiniteSource
                }
                obzenflow_core::event::context::StageType::Transform => {
                    obzenflow_topology::StageType::Transform
                }
                obzenflow_core::event::context::StageType::Sink => {
                    obzenflow_topology::StageType::Sink
                }
                obzenflow_core::event::context::StageType::Stateful => {
                    obzenflow_topology::StageType::Stateful
                }
                obzenflow_core::event::context::StageType::Join => {
                    obzenflow_topology::StageType::Join
                }
            }
        }

        let journal_factory_provider = $journals;
        let stages = $stages;
        let connections = $connections;
        let create_flow_middleware = $create_flow_middleware;
        let lowering_artifacts = $lowering_artifacts;
        // FLOWIP-010 §7: the resolved snapshot arrives as build input.
        let __runtime_config = $build_ctx.runtime_config().clone();

        // FLOWIP-120u F2: pair the build result with the substrate state known
        // at the failure point. Set once at the factory seam; None means the
        // build failed before substrate selection, so no run directory exists.
        let mut __run_state: Option<obzenflow_runtime::journal::RunSubstrateState> = None;
        let __build_result: Result<_, FlowBuildError> = async {

        // Build topology - Two-pass approach for join stages:
        // Pass 1: Create all stage IDs and build name_to_id mapping
        // Pass 2: Resolve join reference names and build StageInfo + DirectedEdge sets

        let mut name_to_id = HashMap::new();
        let mut descriptors = HashMap::new();

        // Phase 0 prerequisite (FLOWIP-095a): descriptor.name() must be unique.
        // This is the stable cross-run replay key used by `run_manifest.json`.
        let mut seen_descriptor_names: HashMap<String, String> = HashMap::new();
        for (var_name, descriptor) in stages.iter() {
            let desc_name = descriptor.name().to_string();
            if let Some(first_var) = seen_descriptor_names.insert(desc_name.clone(), var_name.clone()) {
                return Err(FlowBuildError::DuplicateStageName {
                    name: desc_name,
                    first_var,
                    second_var: var_name.clone(),
                });
            }
        }

        // Pass 1: Create stage IDs
        for (name, descriptor) in &stages {
            // Generate a real ULID using the core crate's StageId::new()
            let core_id = StageId::new();
            name_to_id.insert(name.clone(), core_id);
        }

        // Move descriptors into a mutable map we can reuse later
        for (name, descriptor) in stages {
            descriptors.insert(name, descriptor);
        }

        // FLOWIP-114c: build the subgraph membership map from lowering
        // artifacts BEFORE topology stages are assembled, so the StageInfo
        // entries passed to `validate_edge_typing` carry `subgraph_id`.
        // Without this, the validator's composite-internal-edge skip rule
        // cannot fire (it reads `topology.stage_info(...).subgraph`), and
        // any flow using `ai_map_reduce!` would surface its mixed internal
        // selected feeds as a SingleEdge mismatch at the validator boundary.
        // The same map is reused downstream when assembling the
        // annotated topology returned to the caller.
        let mut subgraph_membership_map: HashMap<StageId, obzenflow_topology::StageSubgraphMembership> =
            HashMap::new();
        for (stage_name, membership) in lowering_artifacts.stage_subgraphs {
            let stage_id = *name_to_id.get(&stage_name).ok_or_else(|| {
                FlowBuildError::StageResourcesFailed(format!(
                    "Lowering artifacts reference unknown stage '{stage_name}'"
                ))
            })?;
            subgraph_membership_map.insert(stage_id, membership);
        }

        // Build StageInfo list, attaching composite subgraph membership where
        // applicable so the validator sees a fully-annotated topology.
        let mut topology_stages: Vec<TopologyStageInfo> = Vec::new();
        let descriptor_names: Vec<String> = descriptors.keys().cloned().collect();
        for name in &descriptor_names {
            let descriptor = descriptors
                .get(name)
                .expect("descriptor map should contain all stage names");
            let core_id = *name_to_id
                .get(name)
                .expect("name_to_id should contain all stage names");
            let topology_id = to_topology_id(core_id);
            let topology_stage_type = to_topology_stage_type(descriptor.stage_type());
            let mut info = TopologyStageInfo::new(
                topology_id,
                descriptor.name().to_string(),
                topology_stage_type,
            );
            if let Some(membership) = subgraph_membership_map.get(&core_id) {
                info = info.with_subgraph(membership.clone());
            }
            topology_stages.push(info);
        }

        // Build edges, including join reference edges and explicit topology edges
        let mut topology_edges: Vec<DirectedEdge> = Vec::new();

        // Precompute explicit forward edges so join reference edges can avoid duplication (e.g.
        // when using join tuple syntax in the topology block).
        let explicit_forward_edges: HashSet<(String, String)> = connections
            .iter()
            .filter(|(_, _, kind)| matches!(kind, EdgeKind::Forward))
            .map(|(from, to, _)| (from.clone(), to.clone()))
            .collect();

        // Pass 2: Resolve join references and add reference edges
        for name in &descriptor_names {
            let descriptor = descriptors
                .get_mut(name)
                .expect("descriptor map should contain all stage names");
            let core_id = *name_to_id
                .get(name)
                .expect("name_to_id should contain all stage names");
            let topology_id = to_topology_id(core_id);

            // If this is a join stage with a DSL reference variable, resolve it
            if let Some(ref_var) = descriptor.reference_stage_name().map(|s| s.to_string()) {
                // FLOWIP-128a A4: a reference variable naming a composite
                // resolves through its boundary by the join's declared
                // reference type (D1), with default-port fallback.
                let (resolved_ref_name, resolved_composite_port) = if name_to_id.contains_key(ref_var.as_str()) {
                    (ref_var.clone(), None)
                } else if let Some(boundary) =
                    lowering_artifacts.boundaries.get(ref_var.as_str())
                {
                    let reference_hint = descriptor
                        .typing_metadata()
                        .map(|metadata| metadata.reference_type.clone())
                        .filter(|hint| {
                            matches!(hint, $crate::dsl::typing::TypeHint::Exact { .. })
                        });
                    let port = boundary.resolve_output(reference_hint.as_ref()).map_err(|err| {
                        match err {
                            $crate::dsl::composition::PortResolveError::Ambiguous {
                                ports,
                                input_display,
                            } => {
                                let listed = ports
                                    .iter()
                                    .map(|p| format!("'{p}'"))
                                    .collect::<Vec<_>>()
                                    .join(" and ");
                                FlowBuildError::StageResourcesFailed(format!(
                                    "join '{name}' references composite '{ref_var}': ambiguous output port (reference '{input_display}' matches ports {listed})"
                                ))
                            }
                            $crate::dsl::composition::PortResolveError::NoDefaultPort => {
                                FlowBuildError::StageResourcesFailed(format!(
                                    "join '{name}' references composite '{ref_var}': no output port matches and no default port exists; available ports: {}",
                                    boundary.describe_outputs()
                                ))
                            }
                        }
                    })?;
                    (
                        port.stage_name.clone(),
                        Some(obzenflow_topology::CompositePortRef::new(
                            boundary.subgraph_id.clone(),
                            port.name.clone(),
                        )),
                    )
                } else {
                    return Err(FlowBuildError::StageResourcesFailed(format!(
                        "Join stage '{}' references unknown stage variable '{}'",
                        name, ref_var
                    )));
                };

                // Explicit tuple-syntax edges were rewritten by lowering, so
                // dedup against the RESOLVED reference stage name.
                let reference_edge_explicit =
                    explicit_forward_edges.contains(&(resolved_ref_name.clone(), name.clone()));

                let ref_id = name_to_id
                    .get(resolved_ref_name.as_str())
                    .copied()
                    .ok_or_else(|| {
                        FlowBuildError::StageResourcesFailed(format!(
                            "Join stage '{}' resolved reference '{}' to missing stage '{}'",
                            name, ref_var, resolved_ref_name
                        ))
                    })?;

                tracing::debug!("Join stage '{}' resolved reference variable '{}' to ID {:?}",
                               name, ref_var, ref_id);
                descriptor.set_reference_stage_id(ref_id);

                // Add topology edge from reference stage to join stage (forward) unless already
                // declared in the user's explicit topology (e.g. join tuple syntax).
                if !reference_edge_explicit {
                    let mut edge = DirectedEdge::new(
                        to_topology_id(ref_id),
                        topology_id,
                        EdgeKind::Forward,
                    );
                    if let Some(port) = resolved_composite_port {
                        edge = edge.with_composite_ports(vec![port]);
                    }
                    topology_edges.push(edge);
                }
            }
        }

        // Add connections from the DSL (|> and <|) with correct EdgeKind.
        // Borrowed: `connections` is consumed again by the FLOWIP-010 config
        // phase and the backpressure materialization below.
        for (from, to, kind) in &connections {
            if let (Some(&from_id), Some(&to_id)) =
                (name_to_id.get(from), name_to_id.get(to)) {
                let ports = lowering_artifacts
                    .boundary_edges
                    .iter()
                    .find(|binding| {
                        binding.from_stage == *from
                            && binding.to_stage == *to
                            && binding.kind == *kind
                    })
                    .map(|binding| binding.ports.clone())
                    .unwrap_or_default();
                topology_edges.push(DirectedEdge::new(
                    to_topology_id(from_id),
                    to_topology_id(to_id),
                    *kind,
                ).with_composite_ports(ports));
            }
        }

        let topology = Arc::new(
            obzenflow_topology::Topology::new(topology_stages.clone(), topology_edges.clone())
                .map_err(FlowBuildError::TopologyValidationFailed)?,
        );

        // FLOWIP-114c PR D: stage-level metadata check first (None metadata,
        // Unspecified on applicable slot), then per-edge / per-slot validator.
        // Both surface as structured FlowBuildError variants so downstream
        // tooling sees a typed error rather than a tracing warning.
        $crate::dsl::typing::validate_stage_typing_metadata(&descriptors)?;

        // Producer-side effect-fact containment is unconditional and fails
        // the build before the first live I/O.
        $crate::dsl::typing::validate_effect_fact_containment(&descriptors)?;

        if let Err(edge_errors) = $crate::dsl::typing::validate_edge_typing(
            &topology,
            &descriptors,
            &name_to_id,
            &lowering_artifacts.internal_feeds,
        ) {
            // Surface every error to tracing so the build log captures the full set.
            for err in &edge_errors {
                tracing::error!(
                    upstream_stage = %err.upstream_stage,
                    downstream_stage = %err.downstream_stage,
                    upstream_type = %err.upstream_type,
                    expected_type = %err.expected_type,
                    input_role = %err.input_role.as_str(),
                    kind = ?err.kind,
                    "Typed edge compatibility mismatch"
                );
            }
            let first = edge_errors
                .into_iter()
                .next()
                .expect("Err carries at least one error");
            // FLOWIP-128a D1 diagnostics: when the failing edge crosses a
            // composite boundary port, say so by composite and port name
            // instead of leaving the user to decode the mangled member stage.
            let composite_context =
                lowering_artifacts
                    .boundaries
                    .iter()
                    .find_map(|(binding, boundary)| {
                        boundary
                            .inputs
                            .iter()
                            .chain(boundary.outputs.iter())
                            .find_map(|port| {
                                if port.stage_name == first.upstream_stage
                                    || port.stage_name == first.downstream_stage
                                {
                                    Some(format!(
                                        "This edge crosses composite '{binding}' boundary port '{}'. ",
                                        port.name
                                    ))
                                } else {
                                    None
                                }
                            })
                    });
            let suggested_fix = match first.kind {
                $crate::dsl::error::EdgeTypingMismatchKind::SingleEdge => {
                    "Align the upstream and downstream types, or insert an alignment transform. \
                     See FLOWIP-114c \"How to handle heterogeneous fan-in\"."
                        .to_string()
                }
                $crate::dsl::error::EdgeTypingMismatchKind::HeterogeneousFanIn { .. } => {
                    "Insert per-branch alignment transforms so the fan-in is homogeneous on a \
                     common envelope type, or use a join for two-input typed fan-in. See \
                     FLOWIP-114c \"How to handle heterogeneous fan-in\"."
                        .to_string()
                }
                $crate::dsl::error::EdgeTypingMismatchKind::DeclaredFeedPayloadMissing => {
                    "Add the payload type to the member's output contract, or remove it from \
                     the composite's internal feed declaration (FLOWIP-128a D3)."
                        .to_string()
                }
            };
            let suggested_fix = match composite_context {
                Some(prefix) => format!("{prefix}{suggested_fix}"),
                None => suggested_fix,
            };
            return Err($crate::dsl::error::FlowBuildError::EdgeTypingMismatch {
                upstream_stage: first.upstream_stage,
                downstream_stage: first.downstream_stage,
                role: first.input_role,
                expected_type: first.expected_type,
                actual_type: first.upstream_type,
                kind: first.kind,
                suggested_fix,
            });
        }

        // FLOWIP-095m: an order observer (an effect, a stateful fold, or a
        // live/symmetric join) below a multi-source fan-in needs the canonical
        // merge for faithful reconstruction. Compute observers from the
        // unwrapped descriptors, because the live-vs-hydrating join test reads
        // is_deterministic_input_orderer(), which wrap_deterministic_orderers
        // flips to true on every marked stage; then seed both the marking walk
        // and the guard from the same set. The guard stays the safety net for
        // cycles and externally constructed descriptors.
        let order_observers =
            $crate::dsl::typing::order_observer_stage_ids(&descriptors, &name_to_id);
        let deterministic_fan_in_stages = $crate::dsl::typing::derive_deterministic_fan_in_stages(
            &topology,
            &descriptors,
            &name_to_id,
        );
        $crate::dsl::typing::wrap_deterministic_orderers(
            &mut descriptors,
            &name_to_id,
            &deterministic_fan_in_stages,
        );
        // FLOWIP-120n F18: source-fed ordered fan-ins compare by the recorded
        // admission sequence and need no Kahn wait on a quiet input.
        let seq_ordered_fan_ins = $crate::dsl::typing::derive_seq_ordered_fan_ins(
            &topology,
            &deterministic_fan_in_stages,
        );

        $crate::dsl::typing::validate_order_observer_deterministic_input_order(
            &topology,
            &descriptors,
            &name_to_id,
            &order_observers,
        )
        .map_err(|err| *err)?;

        let feed_plan = $crate::dsl::typing::derive_feed_plan(
            &topology,
            &descriptors,
            &name_to_id,
            &lowering_artifacts.internal_feeds,
        );

        if let Some(replay) = obzenflow_runtime::bootstrap::replay_bootstrap() {
            // FLOWIP-120n F13: cyclic resume is a v1 non-goal; the
            // condensation-frontier follow-on lifts it. Resume-only.
            if replay.verb == obzenflow_runtime::bootstrap::ReplayVerb::Resume {
                let cycle_members: Vec<String> = topology
                    .stages()
                    .filter(|stage| topology.is_in_cycle(stage.id))
                    .map(|stage| stage.name.as_str().to_string())
                    .collect();
                if !cycle_members.is_empty() {
                    return Err(FlowBuildError::UnsupportedCycleTopology(format!(
                        "--resume-from does not support cyclic topologies (FLOWIP-120n F13); cycle members: {}",
                        cycle_members.join(", ")
                    )));
                }
            }

            // FLOWIP-120n F16, FLOWIP-120v: both archive verbs re-execute
            // recorded work at sinks, so every sink must declare its delivery
            // safety.
            $crate::dsl::typing::validate_archive_sink_delivery_safety(
                &descriptors,
                replay.verb,
                replay.allow_duplicate_sink_delivery,
            )?;
        }

        // FLOWIP-051l (P0): backflow cycles are currently only supported for transform stages.
        // Reject any topology where a cycle-member stage is not a transform so we do not silently
        // drop cycle protection for other stage types when the middleware-based guard is removed.
        let unsupported_cycle_members: Vec<String> = topology
            .stages()
            .filter(|stage| {
                topology.is_in_cycle(stage.id)
                    && stage.stage_type != obzenflow_topology::StageType::Transform
            })
            .map(|stage| format!("{} ({})", stage.name.as_str(), stage.stage_type))
            .collect();

        if !unsupported_cycle_members.is_empty() {
            return Err(FlowBuildError::UnsupportedCycleTopology(format!(
                "backflow cycles are only supported for transform stages in this release; non-transform cycle members: {}",
                unsupported_cycle_members.join(", ")
            )));
        }

        // FLOWIP-051n (P0): SCCs must have exactly one entry point.
        //
        // An SCC entry point is a cycle-member stage that has at least one upstream outside its SCC.
        // For P0, multi-entry SCCs are rejected at build time to avoid ambiguous EOF/Drain gating.
        let mut scc_to_members: HashMap<obzenflow_topology::SccId, Vec<obzenflow_topology::StageId>> = HashMap::new();
        for stage in topology.stages() {
            if let Some(scc_id) = topology.scc_id(stage.id) {
                scc_to_members.entry(scc_id).or_default().push(stage.id);
            }
        }

        let mut scc_entry_points: HashMap<obzenflow_topology::SccId, obzenflow_topology::StageId> = HashMap::new();
        for (scc_id, members) in &scc_to_members {
            let mut entry_points = Vec::new();
            for &member in members {
                let has_external_upstream = topology
                    .upstream_stages(member)
                    .iter()
                    .any(|upstream| topology.scc_id(*upstream) != Some(*scc_id));
                if has_external_upstream {
                    entry_points.push(member);
                }
            }

            if entry_points.len() != 1 {
                let entry_names: Vec<String> = entry_points
                    .iter()
                    .filter_map(|id| topology.stage_name(*id).map(|name| name.to_string()))
                    .collect();

                let found = if entry_names.is_empty() {
                    "<none>".to_string()
                } else {
                    entry_names.join(", ")
                };

                return Err(FlowBuildError::UnsupportedCycleTopology(format!(
                    "SCC {scc_id} must have exactly one entry point (cycle-member stage with at least one external upstream); found {}: {found}",
                    entry_points.len()
                )));
            }

            scc_entry_points.insert(*scc_id, entry_points[0]);
        }

        // FLOWIP-010 Phase B: collect DSL-declared candidates at their
        // declaration sites (§4a), then run the full scope ladder against
        // the topology (§4c). Policy-attachment guards ride this walk (they
        // previously ran during plan extraction, after journal creation;
        // failing before substrate selection is strictly earlier).
        // FLOWIP-115e: the flow-level `backpressure:` clause, when declared.
        #[allow(unused_mut)]
        let mut __flow_backpressure_clause: Option<
            $crate::dsl::backpressure_clause::BackpressureClause,
        > = None;
        $( __flow_backpressure_clause = Some($flow_bp); )?

        let __flow_effective = {
            use obzenflow_core::config::ConfigScope;
            use obzenflow_runtime::runtime_config::DslCandidates;
            use $crate::middleware_resolution::MiddlewareSourceScope;

            let mut __dsl_candidates = DslCandidates::default();
            for (name, descriptor) in descriptors.iter() {
                let flow_middleware = create_flow_middleware();
                for factory in &flow_middleware {
                    if factory.declaration().is_control() {
                        return Err(FlowBuildError::PolicyMiddlewareOnFlowScope {
                            middleware: factory.label().to_string(),
                        }
                        .into());
                    }
                }
                let resolved_view = $crate::middleware_resolution::resolve_middleware_view_with_scope(
                    &flow_middleware,
                    descriptor.stage_middleware_factories(),
                    name,
                )
                .map_err(|e| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Failed to resolve middleware for stage '{name}' while collecting config candidates: {e}"
                    ))
                })?;

                for (scope, factory) in resolved_view {
                    // FLOWIP-120c H1: policy middleware attaches to live I/O
                    // units only. Flow-level broadcast policy rejects above,
                    // pure sync surfaces hard-reject, effectful-stateful rejects
                    // until FLOWIP-120l installs its boundary, and the deprecated
                    // async-non-effectful surface warns until FLOWIP-120f deletes
                    // it (FLOWIP-128b migrates its AI legs to effects).
                    if factory.declaration().is_control() {
                        use $crate::dsl::stage_descriptor::PolicyGuardSurface;
                        match descriptor.policy_guard_surface() {
                            PolicyGuardSurface::PureSync => {
                                return Err(FlowBuildError::PolicyMiddlewareOnPureStage {
                                    stage_name: name.clone(),
                                    middleware: factory.label().to_string(),
                                }
                                .into());
                            }
                            PolicyGuardSurface::EffectfulStatefulPendingBoundary => {
                                return Err(
                                    FlowBuildError::PolicyMiddlewareOnPendingEffectfulStateful {
                                        stage_name: name.clone(),
                                        middleware: factory.label().to_string(),
                                    }
                                    .into(),
                                );
                            }
                            PolicyGuardSurface::AsyncNonEffectful => {
                                return Err(FlowBuildError::PolicyMiddlewareOnPureStage {
                                    stage_name: name.clone(),
                                    middleware: factory.label().to_string(),
                                }
                                .into());
                            }
                            PolicyGuardSurface::Effectful
                            | PolicyGuardSurface::Source
                            | PolicyGuardSurface::Sink => {}
                        }
                    }
                    for default in factory.dsl_config_defaults() {
                        let exact_effect = if matches!(
                            descriptor.policy_guard_surface(),
                            $crate::dsl::stage_descriptor::PolicyGuardSurface::Effectful
                        ) && factory.declaration().is_control()
                        {
                            let effects = descriptor.effect_declarations();
                            (effects.len() == 1).then(|| effects[0].effect_type)
                        } else {
                            None
                        };
                        if let Some(effect_type) = exact_effect {
                            __dsl_candidates.declare_for_effect(
                                default.key_path,
                                obzenflow_core::StageKey::from(descriptor.name()),
                                obzenflow_core::event::EffectType::from(effect_type),
                                default.value,
                            );
                        } else {
                            let candidate_scope = match scope {
                                MiddlewareSourceScope::Flow => ConfigScope::Flow,
                                MiddlewareSourceScope::Stage => {
                                    ConfigScope::stage(descriptor.name())
                                }
                            };
                            __dsl_candidates.declare(
                                default.key_path,
                                candidate_scope,
                                default.value,
                            );
                        }
                    }
                }

                for attachment in descriptor.effect_policy_attachments() {
                    for factory in &attachment.factories {
                        for default in factory.dsl_config_defaults() {
                            __dsl_candidates.declare_for_effect(
                                default.key_path,
                                obzenflow_core::StageKey::from(descriptor.name()),
                                obzenflow_core::event::EffectType::from(attachment.effect_type),
                                default.value,
                            );
                        }
                    }
                }

                // FLOWIP-115e: the stage's `backpressure:` clause is pure
                // DSL-tier candidate sugar at the declaring scope.
                if let Some(clause) = descriptor.backpressure_clause() {
                    clause.declare(&mut __dsl_candidates, ConfigScope::stage(descriptor.name()));
                }
            }
            if let Some(clause) = &__flow_backpressure_clause {
                clause.declare(&mut __dsl_candidates, ConfigScope::Flow);
            }
            let __resolution_ctx = obzenflow_runtime::runtime_config::FlowResolutionContext {
                flow_name: $flow_name.to_string(),
                stages: descriptors
                    .values()
                    .map(|d| obzenflow_core::StageKey::from(d.name()))
                    .collect(),
                edges: connections
                    .iter()
                    .map(|(from_var, to_var, _kind)| {
                        let from = descriptors
                            .get(from_var)
                            .expect("connections reference known stages")
                            .name();
                        let to = descriptors
                            .get(to_var)
                            .expect("connections reference known stages")
                            .name();
                        (
                            obzenflow_core::StageKey::from(from),
                            obzenflow_core::StageKey::from(to),
                        )
                    })
                    .collect(),
                declared_effects: descriptors
                    .values()
                    .map(|descriptor| {
                        (
                            obzenflow_core::StageKey::from(descriptor.name()),
                            descriptor
                                .effect_declarations()
                                .into_iter()
                                .map(|effect| {
                                    obzenflow_core::event::EffectType::from(effect.effect_type)
                                })
                                .collect(),
                        )
                    })
                    .collect(),
                dsl: __dsl_candidates,
            };
            std::sync::Arc::new(
                obzenflow_runtime::runtime_config::materialize_flow_config(
                    &__runtime_config,
                    __resolution_ctx,
                )
                .map_err(FlowBuildError::ConfigResolution)?,
            )
        };

        // FLOWIP-010: `runtime.cycle_max_iterations` is build-resolved; the
        // registry range check (1..=65535) replaced the old env warn/clamp.
        use obzenflow_runtime::pipeline::MaxIterations;
        let cycle_max_iterations: MaxIterations =
            MaxIterations::try_from_usize(__flow_effective.cycle_max_iterations() as usize)
                .unwrap_or(MaxIterations::DEFAULT);

        // Collect join metadata per stage (FLOWIP-082a). The canonical
        // `JoinMetadataInfo` lives in `obzenflow-topology` and uses topology
        // `StageId`s; we convert at this collection boundary.
        use obzenflow_topology::JoinMetadataInfo;
        let mut join_metadata_map: HashMap<StageId, JoinMetadataInfo> = HashMap::new();
        for stage in topology.stages() {
            if matches!(stage.stage_type, obzenflow_topology::StageType::Join) {
                let join_topology_id = stage.id;
                let join_core_id = StageId::from_ulid(join_topology_id.ulid());

                // Look up descriptor so we can get the reference_stage_id set earlier
                if let Some(descriptor_name) = descriptor_names
                    .iter()
                    .find(|name| {
                        let id = name_to_id.get(*name).copied();
                        id.map(|core_id| {
                            let topo_id = to_topology_id(core_id);
                            topo_id == join_topology_id
                        })
                        .unwrap_or(false)
                    })
                {
                    if let Some(descriptor) = descriptors.get(descriptor_name) {
                        if let Some(ref_stage_id) = descriptor.reference_stage_id() {
                            // Convert reference and stream upstream stages to topology IDs
                            // for the canonical wire payload.
                            let catalog_sources = vec![to_topology_id(ref_stage_id)];
                            let mut stream_sources = Vec::new();
                            for upstream in topology.upstream_stages(join_topology_id) {
                                let core_upstream = StageId::from_ulid(upstream.ulid());
                                if core_upstream == ref_stage_id {
                                    continue;
                                }
                                stream_sources.push(upstream);
                            }

                            join_metadata_map.insert(
                                join_core_id,
                                JoinMetadataInfo::new(catalog_sources, stream_sources),
                            );
                        }
                    }
                }
            }
        }

        // Collect stage typing metadata per stage for topology export (FLOWIP-114b).
        let stage_typing_map =
            $crate::dsl::typing::collect_stage_typing_info(&descriptors, &name_to_id);

        // Collect logical subgraph metadata (FLOWIP-086z-part-2).
        // Canonical types live in `obzenflow-topology`; subgraph payload uses
        // topology `StageId`s so we convert from core IDs at this boundary.
        // The `subgraph_membership_map` was built earlier (above the topology
        // assembly) so the validator at flow-build time sees subgraph IDs on
        // composite-internal stages; it is reused here to enrich the final
        // annotated topology.
        use obzenflow_topology::{SubgraphInternalEdge, TopologySubgraphInfo};

        let mut subgraphs: Vec<TopologySubgraphInfo> = Vec::new();
        for spec in lowering_artifacts.subgraphs {
            let mut member_stage_ids = Vec::with_capacity(spec.member_stage_names.len());
            for name in &spec.member_stage_names {
                let id = *name_to_id.get(name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{name}'"
                    ))
                })?;
                member_stage_ids.push(to_topology_id(id));
            }

            let mut internal_edges = Vec::with_capacity(spec.internal_edges.len());
            for edge in &spec.internal_edges {
                let from_stage_id = *name_to_id.get(&edge.from_stage).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{}'",
                        edge.from_stage
                    ))
                })?;
                let to_stage_id = *name_to_id.get(&edge.to_stage).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{}'",
                        edge.to_stage
                    ))
                })?;
                internal_edges.push(SubgraphInternalEdge::new(
                    to_topology_id(from_stage_id),
                    to_topology_id(to_stage_id),
                    edge.role.clone(),
                ));
            }

            let mut entry_stage_ids = Vec::with_capacity(spec.entry_stage_names.len());
            for name in &spec.entry_stage_names {
                let id = *name_to_id.get(name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{name}'"
                    ))
                })?;
                entry_stage_ids.push(to_topology_id(id));
            }

            let mut exit_stage_ids = Vec::with_capacity(spec.exit_stage_names.len());
            for name in &spec.exit_stage_names {
                let id = *name_to_id.get(name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{name}'"
                    ))
                })?;
                exit_stage_ids.push(to_topology_id(id));
            }

            // Boundary ports resolve to member stage ids here at the
            // name-to-id seam (FLOWIP-128a D1/D4). Payload strings use the
            // payload-key convention (`event_type.vN`).
            let mut boundary_ports = Vec::with_capacity(spec.boundary_ports.len());
            for port in &spec.boundary_ports {
                let member_id = *name_to_id.get(&port.stage_name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{}'",
                        port.stage_name
                    ))
                })?;
                let direction = match port.direction {
                    $crate::dsl::composition::PortDirection::Input => {
                        obzenflow_topology::PortDirection::Input
                    }
                    $crate::dsl::composition::PortDirection::Output => {
                        obzenflow_topology::PortDirection::Output
                    }
                };
                let payload_event_types = port
                    .payloads
                    .iter()
                    .filter_map(|hint| match hint {
                        $crate::dsl::typing::TypeHint::Exact {
                            event_type: Some(event_type),
                            ..
                        } => Some(event_type.clone()),
                        _ => None,
                    })
                    .collect();
                boundary_ports.push(obzenflow_topology::BoundaryPortSpec::new(
                    port.name.clone(),
                    direction,
                    to_topology_id(member_id),
                    payload_event_types,
                    port.default,
                ));
            }

            let mut info = TopologySubgraphInfo::new(
                spec.subgraph_id,
                spec.kind,
                spec.binding,
                spec.label,
                member_stage_ids,
                internal_edges,
                entry_stage_ids,
                exit_stage_ids,
                spec.collapsible,
            );
            info.schema_version = spec.schema_version;
            if let Some(parent) = spec.parent_subgraph_id {
                info = info.with_parent_subgraph_id(parent);
            }
            if !boundary_ports.is_empty() {
                info = info.with_boundary_ports(boundary_ports);
            }
            subgraphs.push(info);
        }

        // Runtime resource wiring consumes the composite graph cut too: input
        // subscriptions use it to stamp durable activation provenance at the
        // exact boundary edge. Attach and validate the structural manifest
        // before StageResourcesBuilder sees the topology. The later topology
        // pass enriches this same structure with middleware and derived typing;
        // it must not be the first place where subgraphs become visible.
        let topology = Arc::new(
            topology
                .as_ref()
                .clone()
                .with_subgraphs(subgraphs.clone()),
        );
        topology
            .validate_composite_boundaries()
            .map_err(FlowBuildError::TopologyValidationFailed)?;

        // Create services
        use obzenflow_runtime::pipeline::config::StageConfig;
        use obzenflow_runtime::metrics::DefaultMetricsConfig;
        use obzenflow_core::{SystemId, FlowId};
        use obzenflow_adapters::monitoring::exporters::MetricsExporterBuilder;

        // Create stage-local journals using the builder pattern (FLOWIP-008)
        let flow_id = FlowId::new();
        let pipeline_id = SystemId::new();

        // Get the journal factory for this specific flow
        let mut journal_factory = journal_factory_provider(flow_id.clone())
            .map_err(|e| FlowBuildError::JournalFactoryFailed(format!("{:?}", e)))?;

        // FLOWIP-120u: capture the substrate declaration, then run the
        // provider's resource preflight before any journal is created.
        let __substrate =
            obzenflow_runtime::journal::FlowJournalFactory::run_state(&journal_factory);
        __run_state = Some(__substrate.clone());
        obzenflow_runtime::journal::FlowJournalFactory::resource_preflight(
            &journal_factory,
            &obzenflow_runtime::journal::RunResourcePlan {
                stage_count: topology.stages().count(),
                edge_count: topology.edges().len(),
                metrics_enabled: DefaultMetricsConfig::default().is_enabled(),
            },
        )
        .map_err(|e| FlowBuildError::ResourcePreflightFailed(format!("{e}")))?;

        // FLOWIP-120u F13: resume requires a located current run; an ephemeral
        // continuation is unreachable and its live effects would re-execute on
        // the next resume of the original archive.
        if matches!(
            __substrate,
            obzenflow_runtime::journal::RunSubstrateState::Ephemeral
        ) && obzenflow_runtime::bootstrap::replay_bootstrap()
            .is_some_and(|replay| replay.verb == obzenflow_runtime::bootstrap::ReplayVerb::Resume)
        {
            return Err(FlowBuildError::ResumeRefusedEphemeralRun);
        }

        // Create all journals upfront with proper ownership
        use obzenflow_core::journal::journal_name::JournalName;
        use obzenflow_core::journal::journal_owner::JournalOwner;

        let control_journal = obzenflow_runtime::journal::FlowJournalFactory::create_system_journal(
                &mut journal_factory,
                JournalName::System,
                JournalOwner::system(pipeline_id.clone()),
            )
            .map_err(|e| {
                FlowBuildError::JournalFactoryFailed(format!(
                    "Failed to create system journal: {:?}",
                    e
                ))
            })?;

        let mut stage_journals = HashMap::new();
        let mut error_journals = HashMap::new();
        let mut manifest_stages: HashMap<String, obzenflow_core::journal::run_manifest::RunManifestStage> = HashMap::new();
        // FLOWIP-095j: per-stage delivery metadata for the manifest, derived after
        // wrap_deterministic_orderers so orderer answers match the runtime.
        let manifest_delivery_metadata = $crate::dsl::typing::derive_manifest_delivery_metadata(
            &topology,
            &descriptors,
            &name_to_id,
        );
        for (name, &stage_id) in name_to_id.iter() {
            // Get the descriptor to access stage type and name
            let descriptor = descriptors.get(name).ok_or_else(|| {
                FlowBuildError::StageResourcesFailed(format!(
                    "Missing descriptor for stage {}",
                    name
                ))
            })?;

            let journal = obzenflow_runtime::journal::FlowJournalFactory::create_chain_journal(
                    &mut journal_factory,
                    JournalName::Stage {
                        id: stage_id,
                        stage_type: descriptor.stage_type(),
                        name: descriptor.name().to_string(),
                    },
                    JournalOwner::stage(stage_id),
                )
                .map_err(|e| {
                    FlowBuildError::JournalFactoryFailed(format!(
                        "Failed to create journal for stage {:?}: {:?}",
                        stage_id, e
                    ))
            })?;
            stage_journals.insert(stage_id, journal);

            // Create error journal for this stage (FLOWIP-082e)
            let error_journal = obzenflow_runtime::journal::FlowJournalFactory::create_chain_journal(
                    &mut journal_factory,
                    JournalName::Stage {
                        id: stage_id,
                        stage_type: descriptor.stage_type(),
                        name: format!("{}_error", descriptor.name()),
                    },
                    JournalOwner::stage(stage_id),
                )
                .map_err(|e| {
                    FlowBuildError::JournalFactoryFailed(format!(
                        "Failed to create error journal for stage {:?}: {:?}",
                        stage_id, e
                    ))
            })?;
            error_journals.insert(stage_id, error_journal);

            // Record static mapping for replay lookup (FLOWIP-095a).
            let stage_key = descriptor.name().to_string();
            let data_journal_file = JournalName::Stage {
                id: stage_id,
                stage_type: descriptor.stage_type(),
                name: descriptor.name().to_string(),
            }
            .to_filename();
            let error_journal_file = JournalName::Stage {
                id: stage_id,
                stage_type: descriptor.stage_type(),
                name: format!("{}_error", descriptor.name()),
            }
            .to_filename();
            let (inbound, ordered_delivery) = manifest_delivery_metadata
                .get(&stage_id)
                .cloned()
                .unwrap_or((Vec::new(), true));
            manifest_stages.insert(
                stage_key,
                obzenflow_core::journal::run_manifest::RunManifestStage {
                    dsl_var: name.clone(),
                    stage_type: descriptor.stage_type(),
                    stage_id: stage_id.to_string(),
                    stage_logic_version: descriptor.stage_logic_version(),
                    data_journal_file,
                    error_journal_file,
                    inbound,
                    ordered_delivery,
                },
            );
        }

        // FLOWIP-120n: take the replay archive before the manifest is written,
        // so a resume records the generation it enters (max recorded + 1).
        // FLOWIP-120u: the host opened it into the bootstrap snapshot; the
        // factory only consumes it, to seed the F18 admission sequencer.
        let replay_archive = obzenflow_runtime::bootstrap::replay_archive();
        if let Some(archive) = replay_archive.as_ref() {
            obzenflow_runtime::journal::FlowJournalFactory::seed_admission_from_archive(
                &journal_factory,
                archive.as_ref(),
            );
        }

        // Write run manifest (disk journals persist; memory journals no-op) - FLOWIP-095a.
        let replay_manifest = obzenflow_runtime::bootstrap::replay_bootstrap().map(|replay| {
            obzenflow_core::journal::run_manifest::RunManifestReplayConfig {
                replay_from: replay.archive_path.to_string_lossy().to_string(),
                allow_incomplete_archive: replay.allow_incomplete_archive,
            }
        });

        // FLOWIP-120n: a resume run records the archive it resumed from and the
        // generation it enters; high-water marks are filled by the catch-up read (PR-D).
        let resume_manifest = obzenflow_runtime::bootstrap::replay_bootstrap()
            .filter(|replay| replay.verb == obzenflow_runtime::bootstrap::ReplayVerb::Resume)
            .zip(replay_archive.as_ref())
            .map(|(replay, archive)| {
                obzenflow_core::journal::run_manifest::RunManifestResumeConfig {
                    resumed_from: replay.archive_path,
                    resume_generation: archive.max_recorded_generation().0 + 1,
                    high_water_by_stage: std::collections::BTreeMap::new(),
                }
            });

        let run_manifest = obzenflow_core::journal::run_manifest::RunManifest {
            manifest_version: obzenflow_core::journal::run_manifest::RUN_MANIFEST_VERSION.to_string(),
            journal_format_version: obzenflow_core::journal::run_manifest::JOURNAL_FORMAT_VERSION,
            obzenflow_version: obzenflow_core::build_info::OBZENFLOW_VERSION.to_string(),
            flow_id: flow_id.to_string(),
            flow_name: $flow_name.to_string(),
            created_at: obzenflow_core::chrono::Utc::now(),
            replay: replay_manifest,
            resume: resume_manifest,
            stages: manifest_stages,
            system_journal_file: JournalName::System.to_filename(),
            // FLOWIP-010 §6a: the redacted effective config is run evidence.
            effective_config: Some(__flow_effective.manifest_evidence()),
        };
        // FLOWIP-120u: the manifest is a durable-run artifact; ephemeral runs
        // have no location to persist it within.
        if matches!(
            __substrate,
            obzenflow_runtime::journal::RunSubstrateState::Durable(_)
        ) {
            obzenflow_runtime::journal::FlowJournalFactory::write_run_manifest(
                &journal_factory,
                &run_manifest,
            )
            .map_err(|e| {
                FlowBuildError::JournalFactoryFailed(format!(
                    "Failed to write run_manifest.json: {:?}",
                    e
                ))
            })?;
        }

        // Backpressure materialization (FLOWIP-115e): the resolved mode
        // decides each edge exactly. `enforce` lands an edge override with
        // the resolved window and stall ceiling, `track` lands TrackOnly,
        // and `off` lands nothing, with one exception: a cycle-internal
        // edge whose `off` is only the built-in default upgrades to
        // TrackOnly (provenance-aware SCC auto-enable), so an explicit
        // `off` from any tier is respected.
        use obzenflow_runtime::backpressure::BackpressurePlan;

        let mut backpressure_plan = BackpressurePlan::disabled();
        {
            use obzenflow_runtime::runtime_config::BackpressureMode;
            for (from_var, to_var, _kind) in &connections {
                let from_key = obzenflow_core::StageKey::from(
                    descriptors
                        .get(from_var)
                        .expect("connections reference known stages")
                        .name(),
                );
                let to_key = obzenflow_core::StageKey::from(
                    descriptors
                        .get(to_var)
                        .expect("connections reference known stages")
                        .name(),
                );
                let up_id = *name_to_id
                    .get(from_var)
                    .expect("name_to_id should contain stage ids");
                let down_id = *name_to_id
                    .get(to_var)
                    .expect("name_to_id should contain stage ids");
                let (mode, mode_source) =
                    __flow_effective.backpressure_mode_for(&from_key, &to_key);
                let up_scc = topology.scc_id(to_topology_id(up_id));
                let cycle_internal =
                    up_scc.is_some() && up_scc == topology.scc_id(to_topology_id(down_id));
                use $crate::dsl::backpressure_clause::{
                    resolve_edge_backpressure, EdgeBackpressure,
                };
                match resolve_edge_backpressure(mode, mode_source, cycle_internal) {
                    EdgeBackpressure::Enforce => {
                        let window = __flow_effective
                            .backpressure_window_for(&from_key, &to_key)
                            .and_then(|resolved| resolved.value.as_u64())
                            .and_then(std::num::NonZeroU64::new)
                            .expect(
                                "required-where-enforce validated the window at materialization",
                            );
                        let stall_timeout = std::time::Duration::from_millis(
                            __flow_effective
                                .backpressure_stall_timeout_for(&from_key, &to_key)
                                .expect(
                                    "required-where-enforce validated the stall timeout at \
                                     materialization",
                                ),
                        );
                        backpressure_plan = backpressure_plan.with_edge_enforced(
                            up_id,
                            down_id,
                            window,
                            stall_timeout,
                        );
                    }
                    EdgeBackpressure::Track => {
                        backpressure_plan = backpressure_plan.track_only_edge(up_id, down_id);
                    }
                    EdgeBackpressure::Disabled => {}
                }
            }
        }

        // Use StageResourcesBuilder to handle all the complex wiring
        use obzenflow_runtime::stages::resources_builder::StageResourcesBuilder;

        // FLOWIP-010 §7: per-stage build-resolved lineage policies and
        // heartbeat intervals ride stage resources into the builders
        // (never a global read).
        let lineage_policies: HashMap<StageId, obzenflow_core::config::LineagePolicy> = name_to_id
            .iter()
            .map(|(name, id)| {
                (
                    *id,
                    __flow_effective
                        .lineage_policy_for(&obzenflow_core::StageKey(name.clone())),
                )
            })
            .collect();
        let heartbeat_intervals: HashMap<StageId, u64> = name_to_id
            .iter()
            .map(|(name, id)| {
                (
                    *id,
                    __flow_effective
                        .heartbeat_interval_for(&obzenflow_core::StageKey(name.clone())),
                )
            })
            .collect();

        let resources_builder = StageResourcesBuilder::new(
            flow_id.clone(),
            pipeline_id.clone(),
            topology.clone(),
            control_journal,
            stage_journals,
            error_journals,
        )
        .with_backpressure_plan(backpressure_plan)
        .with_feed_plan(feed_plan)
        .with_deterministic_fan_in_stages(deterministic_fan_in_stages)
        .with_seq_ordered_fan_ins(seq_ordered_fan_ins)
        .with_lineage_policies(lineage_policies)
        .with_heartbeat_intervals(heartbeat_intervals)
        .with_effect_ports($effect_ports)
        .with_replay_archive(replay_archive);

        let stage_resources_set = resources_builder
            .build()
            .await
            .map_err(|e| {
                FlowBuildError::StageResourcesFailed(format!(
                    "Failed to build stage resources: {:?}",
                    e
                ))
            })?;

        // Flow-scoped control middleware aggregator (FLOWIP-059a-3).
        let control_middleware = std::sync::Arc::new(
            obzenflow_adapters::middleware::control::ControlMiddlewareAggregator::new(),
        );

        // Create metrics exporter using the builder pattern
        let metrics_exporter = if DefaultMetricsConfig::default().is_enabled() {
            Some(MetricsExporterBuilder::from_bootstrap().build())
        } else {
            None
        };

        // Create stage supervisors using resources from StageResourcesBuilder
        let mut stages = Vec::new();
        let mut sources = Vec::new();
        let mut stage_resources = stage_resources_set.stage_resources;

        // Structural: track final middleware stack config per stage (core StageId) - FLOWIP-059
        use obzenflow_runtime::pipeline::MiddlewareStackConfig;
        let mut middleware_stacks: HashMap<StageId, MiddlewareStackConfig> = HashMap::new();

        // We need to create wrapped descriptors that will merge middleware
        // This is done by wrapping the existing descriptors
        for (name, id) in &name_to_id {
            if let Some(descriptor) = descriptors.remove(name) {
                let stage_type = descriptor.stage_type();
                let is_in_cycle = topology.is_in_cycle(to_topology_id(*id));
                let topology_stage_id = to_topology_id(*id);
                let config = StageConfig {
                    stage_id: *id,
                    name: descriptor.name().to_string(),
                    flow_name: $flow_name.to_string(),
                    lineage: __flow_effective
                        .lineage_policy_for(&obzenflow_core::StageKey(name.clone())),
                    effective_config: __flow_effective.clone(),
                    cycle_guard: if is_in_cycle
                        && matches!(
                            descriptor.stage_type(),
                            obzenflow_core::event::context::StageType::Transform
                        ) {
                        let Some(scc_id) = topology.scc_id(topology_stage_id) else {
                            return Err(FlowBuildError::UnsupportedCycleTopology(format!(
                                "stage '{}' is marked as in-cycle, but has no scc_id",
                                descriptor.name()
                            )));
                        };

                        let mut external_upstreams: HashSet<StageId> = HashSet::new();
                        let mut internal_upstreams: HashSet<StageId> = HashSet::new();
                        for upstream in topology.upstream_stages(topology_stage_id) {
                            let upstream_core = StageId::from_ulid(upstream.ulid());
                            if topology.scc_id(upstream) == Some(scc_id) {
                                internal_upstreams.insert(upstream_core);
                            } else {
                                external_upstreams.insert(upstream_core);
                            }
                        }

                        let is_entry_point = scc_entry_points
                            .get(&scc_id)
                            .copied()
                            .is_some_and(|entry| entry == topology_stage_id);

                        let scc_internal_edges: Vec<(StageId, StageId)> = if is_entry_point {
                            topology
                                .edges()
                                .iter()
                                .filter(|edge| {
                                    topology.scc_id(edge.from) == Some(scc_id)
                                        && topology.scc_id(edge.to) == Some(scc_id)
                                })
                                .map(|edge| {
                                    (
                                        StageId::from_ulid(edge.from.ulid()),
                                        StageId::from_ulid(edge.to.ulid()),
                                    )
                                })
                                .collect()
                        } else {
                            Vec::new()
                        };

                        use obzenflow_runtime::id_conversions::SccIdExt;
                        Some(obzenflow_runtime::pipeline::config::CycleGuardConfig {
                            max_iterations: cycle_max_iterations,
                            scc_id: scc_id.to_core_scc_id(),
                            external_upstreams,
                            internal_upstreams,
                            is_entry_point,
                            scc_internal_edges,
                        })
                    } else {
                        None
                    },
                };

                // Get the pre-built resources for this stage
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %name,
                    stage_id = ?id,
                    "DSL: Removing resources for stage"
                );
                let mut resources = stage_resources.remove(id).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "No resources found for stage {:?}",
                        id
                    ))
                })?;

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %name,
                    stage_id = ?id,
                    upstream_journals_count = resources.upstream_journals.len(),
                    upstream_stage_ids = ?resources.upstream_journals.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                    "DSL: Got resources for stage"
                );

                // Special handling for join stages: add reference journal to upstream_journals
                // Join descriptors store reference_stage_id from the builder
                if let Some(ref_stage_id) = descriptor.reference_stage_id() {
                    // Find the reference journal
                    let ref_journal = stage_resources_set
                        .stage_journals
                        .iter()
                        .find(|(sid, _)| sid == &ref_stage_id)
                        .map(|(_, j)| (ref_stage_id, j.clone()))
                        .ok_or_else(|| {
                            FlowBuildError::StageResourcesFailed(format!(
                                "No journal found for reference stage {:?}",
                                ref_stage_id
                            ))
                        })?;

                    // Prepend reference journal to upstream_journals (reference must be first)
                    // Filter out any duplicate reference journals to prevent it appearing in stream_journals
                    let mut updated_journals = vec![ref_journal];
                    updated_journals.extend(
                        resources.upstream_journals.into_iter()
                            .filter(|(id, _)| *id != ref_stage_id)
                    );
                    resources.upstream_journals = updated_journals;

                    // Add reference to upstream_stages if not already present
                    if !resources.upstream_stages.contains(&ref_stage_id) {
                        let mut updated_stages = vec![ref_stage_id];
                        updated_stages.extend(&resources.upstream_stages);
                        resources.upstream_stages = updated_stages;
                    }

                    tracing::info!(
                        "Configured join stage '{}' with reference={:?}",
                        name, ref_stage_id
                    );
                }

                // Flow-level middleware list (stage-local middleware is resolved later).
                let flow_middleware = create_flow_middleware();

                // Structural: compute the effective middleware stack config for this stage (FLOWIP-059).
                //
                // Important: this uses the resolved middleware view (flow + stage with typed overrides),
                // not an independent merge of two name vectors. That keeps topology annotations consistent
                // with runtime execution and plan extraction (FLOWIP-114p).
                let resolved_middleware_view = $crate::middleware_resolution::resolve_middleware_view(
                    &flow_middleware,
                    descriptor.stage_middleware_factories(),
                    &config.name,
                )
                .map_err(|e| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Failed to resolve middleware for stage '{}': {e}",
                        name
                    ))
                })?;

                let merged_names: Vec<String> = resolved_middleware_view
                    .iter()
                    .map(|f| f.label().to_string())
                    .collect();

                // Extract config snapshots from all factories (FLOWIP-059)
                let mut circuit_breaker_config: Option<(&'static str, serde_json::Value)> = None;
                let mut rate_limiter_config: Option<(&'static str, serde_json::Value)> = None;
                let mut retry_config: Option<(&'static str, serde_json::Value)> = None;

                for factory in &resolved_middleware_view {
                    if let Some(snapshot) = factory.config_snapshot() {
                        if let Some(slot) = factory.topology_config_slot() {
                            match slot {
                                obzenflow_adapters::middleware::TopologyMiddlewareConfigSlot::CircuitBreaker => {
                                    if let Some((existing, _)) = &circuit_breaker_config {
                                        return Err(FlowBuildError::StageResourcesFailed(format!(
                                            "Stage '{name}' has multiple middleware claiming the CircuitBreaker topology config slot: '{existing}' and '{}'",
                                            factory.label()
                                        )));
                                    }
                                    circuit_breaker_config = Some((factory.label(), snapshot))
                                }
                                obzenflow_adapters::middleware::TopologyMiddlewareConfigSlot::RateLimiter => {
                                    if let Some((existing, _)) = &rate_limiter_config {
                                        return Err(FlowBuildError::StageResourcesFailed(format!(
                                            "Stage '{name}' has multiple middleware claiming the RateLimiter topology config slot: '{existing}' and '{}'",
                                            factory.label()
                                        )));
                                    }
                                    rate_limiter_config = Some((factory.label(), snapshot))
                                }
                                obzenflow_adapters::middleware::TopologyMiddlewareConfigSlot::Retry => {
                                    if let Some((existing, _)) = &retry_config {
                                        return Err(FlowBuildError::StageResourcesFailed(format!(
                                            "Stage '{name}' has multiple middleware claiming the Retry topology config slot: '{existing}' and '{}'",
                                            factory.label()
                                        )));
                                    }
                                    retry_config = Some((factory.label(), snapshot))
                                }
                            }
                        }
                    }
                }

                middleware_stacks.insert(*id, MiddlewareStackConfig {
                    stack: merged_names,
                    circuit_breaker: circuit_breaker_config.map(|(_, snapshot)| snapshot),
                    rate_limiter: rate_limiter_config.map(|(_, snapshot)| snapshot),
                    retry: retry_config.map(|(_, snapshot)| snapshot),
                });

                // Create handle with flow middleware (cycle protection is configured via StageConfig for transforms).
                let handle = descriptor
                    .create_handle_with_flow_middleware(
                        config,
                        resources,
                        flow_middleware,
                        control_middleware.clone(),
                    )
                    .await
                    .map_err(|source| FlowBuildError::StageCreationFailed {
                        stage_name: name.to_string(),
                        source,
                    })?;

                if stage_type.is_source() {
                    sources.push(handle);
                } else {
                    stages.push(handle);
                }
            }
        }

        // FLOWIP-114b: bake DSL-time annotations into the canonical Topology.
        // Stage typing, join metadata, subgraph membership, and middleware
        // configuration become annotation fields on the corresponding
        // `StageInfo`; edge typing is then derived from the attached stage
        // typings and join metadata. The resulting `Topology` is what the
        // runtime carries through `FlowHandle::topology()` and what the
        // `/api/topology` endpoint serialises.
        let topology = {
            use obzenflow_runtime::id_conversions::StageIdExt;

            let mut middleware_decoded: HashMap<StageId, obzenflow_topology::MiddlewareInfo> =
                HashMap::new();
            for (id, config) in &middleware_stacks {
                middleware_decoded.insert(
                    *id,
                    obzenflow_topology::MiddlewareInfo {
                        stack: config.stack.clone(),
                        circuit_breaker: config
                            .circuit_breaker
                            .as_ref()
                            .and_then(|v| serde_json::from_value(v.clone()).ok()),
                        rate_limiter: config
                            .rate_limiter
                            .as_ref()
                            .and_then(|v| serde_json::from_value(v.clone()).ok()),
                        retry: config
                            .retry
                            .as_ref()
                            .and_then(|v| serde_json::from_value(v.clone()).ok()),
                    },
                );
            }

            let annotated_stages: Vec<obzenflow_topology::StageInfo> = topology
                .stages()
                .cloned()
                .map(|mut s| {
                    let core_id = StageId::from_ulid(s.id.ulid());
                    if let Some(typing) = stage_typing_map.get(&core_id) {
                        s.typing = Some(typing.clone());
                    }
                    if let Some(jm) = join_metadata_map.get(&core_id) {
                        s.join_metadata = Some(jm.clone());
                    }
                    if let Some(sub) = subgraph_membership_map.get(&core_id) {
                        s.subgraph = Some(sub.clone());
                    }
                    if let Some(mw) = middleware_decoded.get(&core_id) {
                        s.middleware = Some(mw.clone());
                    }
                    s
                })
                .collect();

            let annotated_edges: Vec<obzenflow_topology::DirectedEdge> =
                topology.edges().to_vec();

            let annotated =
                obzenflow_topology::Topology::new_unvalidated(annotated_stages, annotated_edges)
                    .map_err(FlowBuildError::TopologyValidationFailed)?
                    .with_flow_name($flow_name)
                    .with_api_version("0.5.1")
                    .with_subgraphs(subgraphs.clone())
                    .populate_derived_stage_annotations()
                    .derive_edge_typings();
            annotated
                .validate_composite_boundaries()
                .map_err(FlowBuildError::TopologyValidationFailed)?;
            Arc::new(annotated)
        };

        // Create flow handle using builder pattern
        use $crate::prelude::{PipelineBuilder, FlowHandle};
        use obzenflow_runtime::supervised_base::SupervisorBuilder;

        // FLOWIP-114b: stage_typing_map, join_metadata_map,
        // subgraph_membership_map, subgraphs, and middleware_stacks have
        // already been folded into the canonical `topology` above as
        // `StageInfo`/`Topology` annotations, so PipelineBuilder no longer
        // needs to thread them through.
        let _ = (
            &stage_typing_map,
            &join_metadata_map,
            &subgraph_membership_map,
            &subgraphs,
            &middleware_stacks,
        );

        let builder = PipelineBuilder::new(
                topology.clone(),
                stage_resources_set.system_journal.clone(),
                stage_resources_set.flow_id.clone(),
            )
            .with_flow_name($flow_name)
            .with_stages(stages)
            .with_sources(sources)
            .with_stage_journals(stage_resources_set.stage_journals.clone())
            .with_error_journals(stage_resources_set.error_journals.clone())
            .with_backpressure_registry(stage_resources_set.backpressure_registry.clone())
            .with_feed_plan(stage_resources_set.feed_plan.clone())
            .with_liveness_snapshots(stage_resources_set.liveness_snapshots.clone())
            .with_run_substrate(__substrate.clone())
            .with_flow_effective_config(__flow_effective.clone());

        let builder = if let Some(exporter) = metrics_exporter {
            builder.with_metrics(exporter)
        } else {
            builder
        };

        let handle = builder
            .build()
            .await
            .map_err(|e| FlowBuildError::PipelineBuildFailed(format!("{:?}", e)))?;

        let stage_data_journals = stage_resources_set.stage_journals;
        $finish(handle, stage_data_journals)
        }
        .await;
        __build_result.map_err(|error| $crate::FlowBuildFailure {
            error,
            run: __run_state,
        })
    }};
}
