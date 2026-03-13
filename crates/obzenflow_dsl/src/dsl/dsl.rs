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
/// | `journals:` | Journal factory (`disk_journals(path)` or `memory_journals()`). |
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
///         out = sink!(Doubled => |d: Doubled| { println!("{}", d.value); });
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

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        $crate::FlowDefinition::new(async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::collections::HashMap;

            let journals = $journals;

            // Create stages
            let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();

            $(
                let descriptor = $descriptor;
                stages.insert(stringify!($stage_name).to_string(), descriptor);
            )*

            // FLOWIP-105g-part-2: resolve binding-derived runtime stage names.
            //
            // Stage macros can request name derivation by setting the descriptor name to
            // `BINDING_DERIVED_NAME_SENTINEL`. Resolve those to the left-hand binding
            // before uniqueness checks and build phases run.
            for (binding, descriptor) in stages.iter_mut() {
                if descriptor.name() == BINDING_DERIVED_NAME_SENTINEL {
                    descriptor.set_name(binding.clone());
                }
            }

            // Create connections
            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();

            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);

            // FLOWIP-086z-part-2: lower composite descriptors (e.g. ai_map_reduce) into
            // concrete stages + edges before topology validation and runtime build.
            let lowering_artifacts =
                $crate::dsl::composites::lower_composites(&mut stages, &mut connections)?;

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
                lowering_artifacts
            )
        })
    }};

    // Pattern without explicit flow name (uses "default")
    {
        journals: $journals:expr,
        middleware: [$($flow_mw:expr),*],

        stages: {
            $($stage_name:ident = $descriptor:expr;)*
        },

        topology: {
            $(
                $edge:tt
            )*
        }
    } => {{
        $crate::FlowDefinition::new(async move {
            use $crate::prelude::*;
            use $crate::dsl::stage_descriptor::*;
            use std::collections::HashMap;

            let journals = $journals;

            // Create stages
            let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();

            $(
                let descriptor = $descriptor;
                stages.insert(stringify!($stage_name).to_string(), descriptor);
            )*

            // FLOWIP-105g-part-2: resolve binding-derived runtime stage names.
            //
            // Stage macros can request name derivation by setting the descriptor name to
            // `BINDING_DERIVED_NAME_SENTINEL`. Resolve those to the left-hand binding
            // before uniqueness checks and build phases run.
            for (binding, descriptor) in stages.iter_mut() {
                if descriptor.name() == BINDING_DERIVED_NAME_SENTINEL {
                    descriptor.set_name(binding.clone());
                }
            }

            // Create connections
            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();

            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);

            // FLOWIP-086z-part-2: lower composite descriptors (e.g. ai_map_reduce) into
            // concrete stages + edges before topology validation and runtime build.
            let lowering_artifacts =
                $crate::dsl::composites::lower_composites(&mut stages, &mut connections)?;

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
                lowering_artifacts
            )
        })
    }};
}

/// Build the actual flow from collected stages and connections
#[macro_export]
macro_rules! build_typed_flow {
    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, $create_flow_middleware:expr) => {{
        $crate::build_typed_flow!(
            $flow_name,
            $journals,
            $stages,
            $connections,
            $create_flow_middleware,
            $crate::dsl::composites::LoweringArtifacts::default()
        )
    }};

    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, $create_flow_middleware:expr, $lowering_artifacts:expr) => {{
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

        // Build StageInfo list
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
            topology_stages.push(TopologyStageInfo::new(
                topology_id,
                descriptor.name().to_string(),
                topology_stage_type,
            ));
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
                // Compute this before mutating the descriptor; `reference_stage_name()` returns a
                // `&str` tied to the descriptor borrow, and we may still need the value later.
                let reference_edge_explicit =
                    explicit_forward_edges.contains(&(ref_var.clone(), name.clone()));

                let ref_id = name_to_id
                    .get(ref_var.as_str())
                    .copied()
                    .ok_or_else(|| {
                        FlowBuildError::StageResourcesFailed(format!(
                            "Join stage '{}' references unknown stage variable '{}'",
                            name, ref_var
                        ))
                    })?;

                tracing::debug!("Join stage '{}' resolved reference variable '{}' to ID {:?}",
                               name, ref_var, ref_id);
                descriptor.set_reference_stage_id(ref_id);

                // Add topology edge from reference stage to join stage (forward) unless already
                // declared in the user's explicit topology (e.g. join tuple syntax).
                if !reference_edge_explicit {
                    topology_edges.push(DirectedEdge::new(
                        to_topology_id(ref_id),
                        topology_id,
                        EdgeKind::Forward,
                    ));
                }
            }
        }

        // Add connections from the DSL (|> and <|) with correct EdgeKind
        for (from, to, kind) in connections {
            if let (Some(&from_id), Some(&to_id)) =
                (name_to_id.get(&from), name_to_id.get(&to)) {
                topology_edges.push(DirectedEdge::new(
                    to_topology_id(from_id),
                    to_topology_id(to_id),
                    kind,
                ));
            }
        }

        let topology = Arc::new(
            obzenflow_topology::Topology::new(topology_stages.clone(), topology_edges.clone())
                .map_err(FlowBuildError::TopologyValidationFailed)?,
        );

        let edge_warnings = $crate::dsl::typing::collect_edge_warnings(
            &topology,
            &descriptors,
            &name_to_id,
        );
        for warning in &edge_warnings {
            tracing::warn!(
                upstream_stage = %warning.upstream_stage,
                downstream_stage = %warning.downstream_stage,
                upstream_type = %warning.upstream_type,
                expected_type = %warning.expected_type,
                input_role = %warning.input_role.as_str(),
                "Typed edge compatibility mismatch"
            );
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

        use obzenflow_runtime::pipeline::MaxIterations;
        let cycle_max_iterations: MaxIterations = match std::env::var("OBZENFLOW_CYCLE_MAX_ITERATIONS") {
            Ok(raw) => match raw.parse::<usize>() {
                Ok(0) => {
                    tracing::warn!(
                        value = %raw,
                        "OBZENFLOW_CYCLE_MAX_ITERATIONS must be > 0; using default of {}",
                        MaxIterations::DEFAULT,
                    );
                    MaxIterations::DEFAULT
                }
                Ok(value) => {
                    if value > u16::MAX as usize {
                        tracing::warn!(
                            value = value,
                            clamped = u16::MAX,
                            "OBZENFLOW_CYCLE_MAX_ITERATIONS exceeds u16::MAX; clamping (FLOWIP-051p)"
                        );
                    }
                    // try_from_usize clamps to u16::MAX; None case (0) already handled above.
                    MaxIterations::try_from_usize(value).unwrap_or(MaxIterations::DEFAULT)
                }
                Err(err) => {
                    tracing::warn!(
                        value = %raw,
                        error = %err,
                        "Invalid OBZENFLOW_CYCLE_MAX_ITERATIONS; using default of {}",
                        MaxIterations::DEFAULT,
                    );
                    MaxIterations::DEFAULT
                }
            },
            Err(_) => MaxIterations::DEFAULT,
        };

        // Collect join metadata per stage (FLOWIP-082a)
        use obzenflow_runtime::pipeline::JoinMetadata;
        let mut join_metadata_map: HashMap<StageId, JoinMetadata> = HashMap::new();
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
                        // Reference stage id is stored on join descriptors after resolution
                        if let Some(ref_stage_id) = descriptor.reference_stage_id() {
                            // Upstream stages of join in topology; convert to core IDs and
                            // treat all except the reference as stream sources.
                            let mut catalog_sources = vec![ref_stage_id];
                            let mut stream_sources = Vec::new();
                            for upstream in topology.upstream_stages(join_topology_id) {
                                let core_upstream = StageId::from_ulid(upstream.ulid());
                                if core_upstream == ref_stage_id {
                                    continue;
                                }
                                stream_sources.push(core_upstream);
                            }

                            join_metadata_map.insert(
                                join_core_id,
                                JoinMetadata {
                                    catalog_source_ids: catalog_sources,
                                    stream_source_ids: stream_sources,
                                },
                            );
                        }
                    }
                }
            }
        }

        // Collect logical subgraph metadata (FLOWIP-086z-part-2).
        use obzenflow_core::topology::subgraphs::{StageSubgraphMembership, SubgraphInternalEdge, TopologySubgraphInfo};
        let mut subgraph_membership_map: HashMap<StageId, StageSubgraphMembership> =
            HashMap::new();
        for (stage_name, membership) in lowering_artifacts.stage_subgraphs {
            let stage_id = *name_to_id.get(&stage_name).ok_or_else(|| {
                FlowBuildError::StageResourcesFailed(format!(
                    "Lowering artifacts reference unknown stage '{stage_name}'"
                ))
            })?;
            subgraph_membership_map.insert(stage_id, membership);
        }

        let mut subgraphs: Vec<TopologySubgraphInfo> = Vec::new();
        for spec in lowering_artifacts.subgraphs {
            let mut member_stage_ids = Vec::with_capacity(spec.member_stage_names.len());
            for name in &spec.member_stage_names {
                let id = *name_to_id.get(name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{name}'"
                    ))
                })?;
                member_stage_ids.push(id);
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
                internal_edges.push(SubgraphInternalEdge {
                    from_stage_id,
                    to_stage_id,
                    role: edge.role.clone(),
                });
            }

            let mut entry_stage_ids = Vec::with_capacity(spec.entry_stage_names.len());
            for name in &spec.entry_stage_names {
                let id = *name_to_id.get(name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{name}'"
                    ))
                })?;
                entry_stage_ids.push(id);
            }

            let mut exit_stage_ids = Vec::with_capacity(spec.exit_stage_names.len());
            for name in &spec.exit_stage_names {
                let id = *name_to_id.get(name).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Lowering artifacts reference unknown stage '{name}'"
                    ))
                })?;
                exit_stage_ids.push(id);
            }

            subgraphs.push(TopologySubgraphInfo {
                subgraph_id: spec.subgraph_id,
                kind: spec.kind,
                binding: spec.binding,
                label: spec.label,
                member_stage_ids,
                internal_edges,
                entry_stage_ids,
                exit_stage_ids,
                parent_subgraph_id: spec.parent_subgraph_id,
                collapsible: spec.collapsible,
            });
        }

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
            manifest_stages.insert(
                stage_key,
                obzenflow_core::journal::run_manifest::RunManifestStage {
                    dsl_var: name.clone(),
                    stage_type: descriptor.stage_type(),
                    stage_id: stage_id.to_string(),
                    data_journal_file,
                    error_journal_file,
                },
            );
        }

        // Write run manifest (disk journals persist; memory journals no-op) - FLOWIP-095a.
        let replay_manifest = std::env::var_os("OBZENFLOW_REPLAY_FROM").map(|path| {
            let allow_incomplete_archive = std::env::var("OBZENFLOW_ALLOW_INCOMPLETE_ARCHIVE")
                .ok()
                .is_some_and(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"));

            obzenflow_core::journal::run_manifest::RunManifestReplayConfig {
                replay_from: path.to_string_lossy().to_string(),
                allow_incomplete_archive,
            }
        });

        let run_manifest = obzenflow_core::journal::run_manifest::RunManifest {
            manifest_version: obzenflow_core::journal::run_manifest::RUN_MANIFEST_VERSION.to_string(),
            obzenflow_version: obzenflow_core::build_info::OBZENFLOW_VERSION.to_string(),
            flow_id: flow_id.to_string(),
            flow_name: $flow_name.to_string(),
            created_at: obzenflow_core::chrono::Utc::now(),
            replay: replay_manifest,
            stages: manifest_stages,
            system_journal_file: JournalName::System.to_filename(),
        };
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

        // Backpressure is opt-in (FLOWIP-086k): extract per-stage windows from middleware configs.
        use obzenflow_runtime::backpressure::BackpressurePlan;
        use std::num::NonZeroU64;

        let mut backpressure_plan = BackpressurePlan::disabled();
        for (name, descriptor) in descriptors.iter() {
            let stage_id = *name_to_id.get(name).expect("name_to_id should contain stage ids");
            for factory in descriptor.stage_middleware_factories() {
                if factory.name() != "backpressure" {
                    continue;
                }
                let snapshot = factory.config_snapshot().ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Stage '{}' enables backpressure but did not provide a config snapshot",
                        name
                    ))
                })?;
                let window = snapshot
                    .get("window")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| {
                        FlowBuildError::StageResourcesFailed(format!(
                            "Stage '{}' backpressure config is missing a numeric 'window' field",
                            name
                        ))
                    })?;
                let window = NonZeroU64::new(window).ok_or_else(|| {
                    FlowBuildError::StageResourcesFailed(format!(
                        "Stage '{}' backpressure window must be > 0",
                        name
                    ))
                })?;
                backpressure_plan = backpressure_plan.with_stage_window(stage_id, window);
            }
        }

        // Use StageResourcesBuilder to handle all the complex wiring
        use obzenflow_runtime::stages::resources_builder::StageResourcesBuilder;

        let resources_builder = StageResourcesBuilder::new(
            flow_id.clone(),
            pipeline_id.clone(),
            topology.clone(),
            control_journal,
            stage_journals,
            error_journals,
        )
        .with_backpressure_plan(backpressure_plan)
        .with_replay_archive(
            obzenflow_runtime::journal::FlowJournalFactory::replay_archive_from_env(
                &mut journal_factory,
            )
            .await
                .map_err(|e| FlowBuildError::StageResourcesFailed(format!("Failed to open replay archive: {e}")))?,
        );

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
            Some(MetricsExporterBuilder::from_env().build())
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

                // Structural: compute final middleware stack config for this stage (FLOWIP-059)
                let flow_names: Vec<String> = flow_middleware
                    .iter()
                    .map(|f| f.name().to_string())
                    .collect();
                let stage_names = descriptor.stage_middleware_names();
                // Merge roughly like resolve_middleware (flow first, then stage overrides/extends).
                let mut merged_names: Vec<String> = Vec::new();
                for mw_name in flow_names.into_iter().chain(stage_names.into_iter()) {
                    if !merged_names.iter().any(|n| n == &mw_name) {
                        merged_names.push(mw_name);
                    }
                }

                // Extract config snapshots from all factories (FLOWIP-059)
                let mut circuit_breaker_config: Option<serde_json::Value> = None;
                let mut rate_limiter_config: Option<serde_json::Value> = None;
                let mut retry_config: Option<serde_json::Value> = None;
                let mut backpressure_config: Option<serde_json::Value> = None;

                // Check flow-level middleware
                for factory in &flow_middleware {
                    if let Some(snapshot) = factory.config_snapshot() {
                        match factory.name() {
                            "circuit_breaker" => circuit_breaker_config = Some(snapshot),
                            "rate_limiter" => rate_limiter_config = Some(snapshot),
                            "retry" => retry_config = Some(snapshot),
                            "backpressure" => backpressure_config = Some(snapshot),
                            _ => {}
                        }
                    }
                }

                // Check stage-level middleware (overrides flow-level)
                for factory in descriptor.stage_middleware_factories() {
                    if let Some(snapshot) = factory.config_snapshot() {
                        match factory.name() {
                            "circuit_breaker" => circuit_breaker_config = Some(snapshot),
                            "rate_limiter" => rate_limiter_config = Some(snapshot),
                            "retry" => retry_config = Some(snapshot),
                            "backpressure" => backpressure_config = Some(snapshot),
                            _ => {}
                        }
                    }
                }

                middleware_stacks.insert(*id, MiddlewareStackConfig {
                    stack: merged_names,
                    circuit_breaker: circuit_breaker_config,
                    rate_limiter: rate_limiter_config,
                    retry: retry_config,
                    backpressure: backpressure_config,
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
                    .map_err(|e| FlowBuildError::StageCreationFailed {
                        stage_name: name.to_string(),
                        message: e,
                    })?;

                if stage_type.is_source() {
                    sources.push(handle);
                } else {
                    stages.push(handle);
                }
            }
        }

        // Create flow handle using builder pattern
        use $crate::prelude::{PipelineBuilder, FlowHandle};
        use obzenflow_runtime::supervised_base::SupervisorBuilder;

        let mut builder = PipelineBuilder::new(
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
            .with_middleware_stacks(middleware_stacks);

        if !join_metadata_map.is_empty() {
            builder = builder.with_join_metadata(join_metadata_map);
        }

        if !subgraph_membership_map.is_empty() || !subgraphs.is_empty() {
            builder = builder.with_subgraphs(subgraph_membership_map, subgraphs);
        }

        let builder = if let Some(exporter) = metrics_exporter {
            builder.with_metrics(exporter)
        } else {
            builder
        };

        let handle = builder
            .build()
            .await
            .map_err(|e| FlowBuildError::PipelineBuildFailed(format!("{:?}", e)))?;

        Ok::<FlowHandle, FlowBuildError>(handle)
    }};
}
