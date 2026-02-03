// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Main flow! macro - the primary API for building ObzenFlow pipelines
//!
//! This is now a clean implementation using the let bindings approach that
//! separates stage declaration from topology definition.

/// Parse topology edges supporting both |> and <| operators (single collector)
/// Note: Join stages only accept stream inputs in topology.
/// Reference stages are specified via with_reference() in the join builder.
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

/// The main flow! macro using the clean typed approach
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

            // Create connections
            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();

            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);

            // Create closure for flow middleware
            let create_flow_middleware = || vec![
                $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
            ];

            // Build the flow
            $crate::build_typed_flow!($flow_name, journals, stages, connections, create_flow_middleware)
        })
    }};

    // Pattern without explicit flow name (uses "default")
    // (stage descriptors as expressions)
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

            // Create connections
            let mut connections: Vec<(String, String, obzenflow_topology::EdgeKind)> = Vec::new();

            // Parse topology edges
            $crate::parse_topology!(connections, $($edge)*);

            // Create closure for flow middleware
            let create_flow_middleware = || vec![
                $(Box::new($flow_mw) as Box<dyn obzenflow_adapters::middleware::MiddlewareFactory>),*
            ];

            // Build the flow
            $crate::build_typed_flow!("default", journals, stages, connections, create_flow_middleware)
        })
    }};
}

/// Build the actual flow from collected stages and connections
#[macro_export]
macro_rules! build_typed_flow {
    ($flow_name:expr, $journals:expr, $stages:expr, $connections:expr, $create_flow_middleware:expr) => {{
        use $crate::prelude::*;
        use $crate::dsl::FlowBuildError;
        use std::collections::HashMap;
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
        use std::collections::HashSet;
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

        // Collect join metadata per stage (FLOWIP-082a)
        use obzenflow_runtime_services::pipeline::JoinMetadata;
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

        // Create services
        use obzenflow_runtime_services::pipeline::config::StageConfig;
        use obzenflow_runtime_services::metrics::DefaultMetricsConfig;
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

        let control_journal = obzenflow_runtime_services::journal::FlowJournalFactory::create_system_journal(
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

            let journal = obzenflow_runtime_services::journal::FlowJournalFactory::create_chain_journal(
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
            let error_journal = obzenflow_runtime_services::journal::FlowJournalFactory::create_chain_journal(
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
        obzenflow_runtime_services::journal::FlowJournalFactory::write_run_manifest(
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
        use obzenflow_runtime_services::backpressure::BackpressurePlan;
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
        use obzenflow_runtime_services::stages::resources_builder::StageResourcesBuilder;

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
            obzenflow_runtime_services::journal::FlowJournalFactory::replay_archive_from_env(
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
        use obzenflow_runtime_services::pipeline::MiddlewareStackConfig;
        let mut middleware_stacks: HashMap<StageId, MiddlewareStackConfig> = HashMap::new();

        // We need to create wrapped descriptors that will merge middleware
        // This is done by wrapping the existing descriptors
        for (name, id) in &name_to_id {
            if let Some(descriptor) = descriptors.remove(name) {
                let stage_type = descriptor.stage_type();
                let config = StageConfig {
                    stage_id: *id,
                    name: descriptor.name().to_string(),
                    flow_name: $flow_name.to_string(),
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

                // Check if this stage is in a cycle and needs CycleGuard
                let mut flow_middleware = create_flow_middleware();
                let is_in_cycle = topology.is_in_cycle(to_topology_id(*id));
                tracing::info!(
                    "Checking stage '{}' (id={:?}) for cycles: {}",
                    name,
                    id,
                    is_in_cycle
                );

                if is_in_cycle {
                    tracing::info!(
                        "Stage '{}' detected in cycle, auto-attaching CycleGuard middleware",
                        name
                    );
                    // Add CycleGuard middleware with a reasonable default (10 iterations)
                    use obzenflow_adapters::middleware::control::cycle_guard;
                    flow_middleware.push(cycle_guard(10));
                }

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

                // Create handle with flow middleware (including CycleGuard if needed)
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
        use obzenflow_runtime_services::supervised_base::SupervisorBuilder;

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
