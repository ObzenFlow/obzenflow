// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline builder pattern for creating supervisors with proper FSM lifecycle
//!
//! This builder ensures supervisors are created and started correctly according
//! to the FSM architecture patterns, returning only a FlowHandle for control.

use super::{
    fsm::{PipelineContext, PipelineFsmEvent, PipelineFsmState},
    handle::{FlowHandle, FlowHandleExtras},
    metrics::prepare_metrics,
    supervisor::PipelineSupervisor,
    PipelineState,
};
use crate::journal::RunSubstrateState;
use crate::{
    backpressure::BackpressureRegistry,
    feed_plan::{FeedKey, FeedPlan},
    id_conversions::StageIdExt,
    stages::common::stage_handle::BoxedStageHandle,
    stages::LivenessSnapshots,
    supervised_base::{BuilderError, ChannelBuilder, HandleBuilder, SupervisorTaskBuilder},
};
use obzenflow_core::event::{ChainEvent, SystemEvent, WriterId};
use obzenflow_core::id::{FlowId, SystemId};
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::MetricsSnapshotExporter;
use obzenflow_core::StageId;
use obzenflow_core::{DeliveryContract, SourceContract, TransportContract};
use obzenflow_topology::Topology;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

type StageJournalList = Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>;

fn derive_expected_contract_keys(topology: &Topology, feed_plan: &FeedPlan) -> HashSet<FeedKey> {
    let mut keys: HashSet<FeedKey> = feed_plan
        .all_feeds()
        .iter()
        .map(|feed| feed.key.clone())
        .collect();

    for edge in topology.edges() {
        let upstream = StageId::from_topology_id(edge.from);
        let downstream = StageId::from_topology_id(edge.to);
        if keys
            .iter()
            .any(|key| key.matches_stage_pair(upstream, downstream))
        {
            continue;
        }
        keys.insert(FeedKey::legacy_stage_pair(upstream, downstream));
    }

    keys
}

/// Builder for creating a pipeline with proper FSM lifecycle
pub struct PipelineBuilder {
    topology: Arc<Topology>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    flow_id: FlowId,
    stages: Vec<BoxedStageHandle>,
    sources: Vec<BoxedStageHandle>,
    metrics_exporter: Option<Arc<dyn MetricsSnapshotExporter>>,
    stage_journals: Option<StageJournalList>,
    error_journals: Option<StageJournalList>,
    flow_name: Option<String>,
    contract_attachments: Option<HashMap<(StageId, StageId), Vec<String>>>,
    backpressure_registry: Option<Arc<BackpressureRegistry>>,
    liveness_snapshots: Option<LivenessSnapshots>,
    feed_plan: FeedPlan,
    run_substrate: Option<RunSubstrateState>,
    flow_effective_config: Option<Arc<crate::runtime_config::FlowEffectiveConfig>>,
}

impl PipelineBuilder {
    /// Create a new pipeline builder
    pub fn new(
        topology: Arc<Topology>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        flow_id: FlowId,
    ) -> Self {
        Self {
            topology,
            system_journal,
            flow_id,
            stages: Vec::new(),
            sources: Vec::new(),
            metrics_exporter: None,
            stage_journals: None,
            error_journals: None,
            flow_name: None,
            contract_attachments: None,
            backpressure_registry: None,
            liveness_snapshots: None,
            feed_plan: FeedPlan::default(),
            run_substrate: None,
            flow_effective_config: None,
        }
    }

    /// Attach the selected run substrate (FLOWIP-120u). The DSL always sets
    /// this; a builder used directly defaults to `Ephemeral` at build.
    pub fn with_run_substrate(mut self, run_substrate: RunSubstrateState) -> Self {
        self.run_substrate = Some(run_substrate);
        self
    }

    /// Attach the build-resolved effective config (FLOWIP-010), carried out
    /// through the flow handle for the host's read surface.
    pub fn with_flow_effective_config(
        mut self,
        config: Arc<crate::runtime_config::FlowEffectiveConfig>,
    ) -> Self {
        self.flow_effective_config = Some(config);
        self
    }

    /// Add stages to the pipeline
    pub fn with_stages(mut self, stages: Vec<BoxedStageHandle>) -> Self {
        self.stages = stages;
        self
    }

    /// Add source stages to the pipeline
    pub fn with_sources(mut self, sources: Vec<BoxedStageHandle>) -> Self {
        self.sources = sources;
        self
    }

    /// Inject the run-owned destination for application observations.
    pub fn with_metrics_exporter(mut self, exporter: Arc<dyn MetricsSnapshotExporter>) -> Self {
        self.metrics_exporter = Some(exporter);
        self
    }

    /// Add stage journals for metrics aggregator
    pub fn with_stage_journals(mut self, journals: StageJournalList) -> Self {
        self.stage_journals = Some(journals);
        self
    }

    /// Add error journals for error sink
    pub fn with_error_journals(mut self, journals: StageJournalList) -> Self {
        self.error_journals = Some(journals);
        self
    }

    /// Set the user-specified flow name from the flow! macro
    pub fn with_flow_name(mut self, name: impl Into<String>) -> Self {
        self.flow_name = Some(name.into());
        self
    }

    /// Attach structural contract names per edge (for topology observability).
    ///
    /// Note: as of FLOWIP-114b, stage typing, join metadata, subgraph
    /// membership, and middleware configuration are baked into the
    /// canonical `Topology` at flow build time, so they are no longer
    /// threaded through `PipelineBuilder` as side maps. Contracts remain a
    /// side map because they are derived in `PipelineBuilder::build` from
    /// the topology shape and are not yet baked into the canonical
    /// `Topology`.
    pub fn with_contract_attachments(
        mut self,
        attachments: HashMap<(StageId, StageId), Vec<String>>,
    ) -> Self {
        self.contract_attachments = Some(attachments);
        self
    }

    /// Provide the flow-scoped backpressure registry for observability (FLOWIP-086k).
    pub fn with_backpressure_registry(mut self, registry: Arc<BackpressureRegistry>) -> Self {
        self.backpressure_registry = Some(registry);
        self
    }

    /// Add flow-scoped stage liveness snapshots for continuous heartbeat metrics (FLOWIP-063e).
    pub fn with_liveness_snapshots(mut self, snapshots: LivenessSnapshots) -> Self {
        self.liveness_snapshots = Some(snapshots);
        self
    }

    /// Add flow-scoped logical feed metadata for contract gating (FLOWIP-120b).
    pub fn with_feed_plan(mut self, feed_plan: FeedPlan) -> Self {
        self.feed_plan = feed_plan;
        self
    }
}

impl PipelineBuilder {
    /// Build and start the pipeline, returning a FlowHandle
    pub async fn build(self) -> Result<FlowHandle, BuilderError> {
        // FD preflight for disk journals runs at the factory seam via
        // FlowJournalFactory::resource_preflight (FLOWIP-086n, moved by
        // FLOWIP-120u), before any journal is created.

        // ErrorSink will be automatically created by the flow DSL
        // similar to how MetricsAggregator is created

        // Create unique stage ID for the pipeline supervisor
        let _stage_id = StageId::new();

        // Prepare stage supervisors map
        let mut stage_map = HashMap::new();
        for stage in self.stages {
            let stage_id = stage.stage_id();
            stage_map.insert(
                stage_id,
                Arc::<dyn crate::stages::common::stage_handle::StageHandle>::from(stage),
            );
        }

        // Prepare source supervisors map
        let mut source_map = HashMap::new();
        for source in self.sources {
            let stage_id = source.stage_id();
            source_map.insert(
                stage_id,
                Arc::<dyn crate::stages::common::stage_handle::StageHandle>::from(source),
            );
        }

        // Create pipeline context with all mutable state
        let system_id = SystemId::new();

        // DEBUG: Print topology information
        tracing::debug!("=== TOPOLOGY DEBUG ===");
        let stages: Vec<_> = self.topology.stages().collect();
        tracing::debug!("Topology stages count: {}", stages.len());
        for stage in stages {
            let upstreams = self.topology.upstream_stages(stage.id);
            let downstreams = self.topology.downstream_stages(stage.id);
            tracing::debug!(
                "Stage '{}' (id={:?}): upstreams={:?}, downstreams={:?}",
                stage.name,
                stage.id,
                upstreams,
                downstreams
            );
        }
        tracing::debug!("=== END TOPOLOGY DEBUG ===");

        // Identify source stages (no upstreams)
        let expected_sources: Vec<StageId> = self
            .topology
            .stages()
            .filter(|stage| self.topology.upstream_stages(stage.id).is_empty())
            .map(|stage| StageId::from_topology_id(stage.id))
            .collect();

        // Identify sink stages by semantic type so we can attach delivery contracts
        // for UI/observability.
        let sink_stages: HashSet<StageId> = self
            .topology
            .stages()
            .filter(|stage| stage.stage_type == obzenflow_topology::StageType::Sink)
            .map(|stage| StageId::from_topology_id(stage.id))
            .collect();

        let delivery_contract_pairs: HashSet<(StageId, StageId)> = self
            .topology
            .edges()
            .iter()
            .filter(|edge| edge.kind == obzenflow_topology::EdgeKind::Forward)
            .map(|edge| {
                (
                    StageId::from_topology_id(edge.from),
                    StageId::from_topology_id(edge.to),
                )
            })
            .filter(|(_, downstream)| sink_stages.contains(downstream))
            .collect();

        // Track every logical feed so we can require ContractStatus evidence for
        // each upstream->reader selected payload/role pair. Legacy/direct
        // callers without a feed plan get one fallback key per topology edge.
        let expected_contract_pairs =
            derive_expected_contract_keys(&self.topology, &self.feed_plan);
        let expected_contract_stage_pairs: HashSet<(StageId, StageId)> = expected_contract_pairs
            .iter()
            .map(|key| (key.upstream_stage, key.downstream_stage))
            .collect();

        // Structural contract attachments for topology observability:
        // - Every edge gets TransportContract.
        // - Edges whose upstream is a source stage also get SourceContract.
        // - Forward edges into sink stages also get DeliveryContract.
        let mut contract_attachments_map: HashMap<(StageId, StageId), Vec<String>> =
            self.contract_attachments.unwrap_or_default();
        for (upstream, downstream) in &expected_contract_stage_pairs {
            let entry = contract_attachments_map
                .entry((*upstream, *downstream))
                .or_default();
            if !entry.iter().any(|n| n == TransportContract::NAME) {
                entry.push(TransportContract::NAME.to_string());
            }
            if expected_sources.contains(upstream)
                && !entry.iter().any(|n| n == SourceContract::NAME)
            {
                entry.push(SourceContract::NAME.to_string());
            }
            if delivery_contract_pairs.contains(&(*upstream, *downstream))
                && !entry.iter().any(|n| n == DeliveryContract::NAME)
            {
                entry.push(DeliveryContract::NAME.to_string());
            }
        }

        // Prefer the user-provided flow name (from `flow!`); fall back to a stable default.
        let flow_name = self
            .flow_name
            .clone()
            .unwrap_or_else(|| "unnamed_flow".to_string());

        // Retain the existing stage teardown handles for emergency cleanup if
        // the pipeline supervisor itself must be aborted during publication.
        let stage_cleanup: Vec<_> = stage_map
            .values()
            .chain(source_map.values())
            .cloned()
            .collect();
        let mut pipeline_context = PipelineContext {
            system_id,
            topology: self.topology.clone(),
            flow_name: flow_name.clone(),
            flow_id: self.flow_id,
            system_journal: self.system_journal.clone(),
            stage_supervisors: stage_map,
            source_supervisors: source_map,
            completed_stages: Vec::new(),
            running_stages: std::collections::HashSet::new(),
            stage_data_journals: self.stage_journals.unwrap_or_default(),
            stage_error_journals: self.error_journals.unwrap_or_default(),
            backpressure_registry: self.backpressure_registry.clone(),
            completion_subscription: None,
            metrics_exporter: self.metrics_exporter.clone(),
            resources: Default::default(),
            progress: Default::default(),
            contract_status: HashMap::new(),
            contract_pairs: HashMap::new(),
            expected_contract_pairs,
            expected_sources,
            stage_lifecycle_metrics: HashMap::new(),
            flow_start_time: None,
            last_system_event_id_seen: None,
            stop_intent: Default::default(),
            termination: Default::default(),
            // FLOWIP-010: global knobs from the build-resolved effective
            // config; registry defaults when no snapshot is threaded (tests).
            source_contract_strict: self
                .flow_effective_config
                .as_ref()
                .map(|cfg| {
                    super::config::SourceContractStrictMode::from_token(
                        cfg.source_contract_strict_mode(),
                    )
                })
                .unwrap_or_default(),
            metrics_drain_timeout_ms: self
                .flow_effective_config
                .as_ref()
                .map(|cfg| cfg.metrics_drain_timeout_ms())
                .unwrap_or(5_000),
        };

        // Establish context Drop ownership before the first fallible await.
        // A returned failure joins supplied stages; dropping this build future
        // requests cancellation through that same context fallback.
        let preparation = async {
            pipeline_context.completion_subscription =
                Some(crate::messaging::SystemSubscription::new(
                    self.system_journal
                        .reader()
                        .await
                        .map_err(|e| BuilderError::ContextCreationError(e.to_string()))?,
                    "pipeline_supervisor".into(),
                ));
            pipeline_context.resources.prepared_metrics =
                prepare_metrics(&pipeline_context).await?;
            Ok::<(), BuilderError>(())
        }
        .await;
        if let Err(error) = preparation {
            for handle in &stage_cleanup {
                handle.request_abort();
            }
            for handle in &stage_cleanup {
                if let Err(join_error) = handle.abort_and_join().await {
                    tracing::error!(%join_error, "Stage failed during pipeline construction cleanup");
                }
            }
            return Err(error);
        }
        let published_outcome = pipeline_context.termination.published.clone();
        let metrics = pipeline_context.resources.metrics.clone();
        let operational_failure = pipeline_context.resources.failure.clone();
        let publications = pipeline_context.resources.publications.clone();
        let (event_sender, event_receiver, state_watcher) =
            ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Created);
        let supervisor = PipelineSupervisor::new(
            system_id,
            event_receiver,
            state_watcher.clone(),
            operational_failure.clone(),
        );
        let supervisor_task = SupervisorTaskBuilder::new("pipeline_supervisor")
            .with_publications(publications)
            .spawn_self_supervised(supervisor, PipelineFsmState::Created, pipeline_context);

        // Build the standard handle first
        let standard_handle = HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))?;

        // Wrap it in FlowHandle with pipeline-specific extras.
        // Clone topology for the handle (topology is Arc, so this is cheap);
        // it already carries the FLOWIP-114b annotation fields (typing,
        // join_metadata, middleware, subgraph membership, subgraph
        // registry).
        let topology = Some(self.topology.clone());
        let contract_attachments = Some(
            Arc::new(contract_attachments_map) as Arc<HashMap<(StageId, StageId), Vec<String>>>
        );

        Ok(FlowHandle::new(
            standard_handle,
            FlowHandleExtras {
                stage_cleanup,
                metrics,
                operational_failure,
                published_outcome,
                topology,
                flow_name,
                contract_attachments,
                system_journal: Some(self.system_journal.clone()),
                pipeline_writer_id: WriterId::from(system_id),
                liveness_snapshots: self.liveness_snapshots.clone(),
                run_substrate: self
                    .run_substrate
                    .clone()
                    .unwrap_or(RunSubstrateState::Ephemeral),
                flow_effective_config: self.flow_effective_config.clone(),
            },
        ))
    }
}

#[cfg(test)]
#[path = "tests/builder.rs"]
mod tests;
