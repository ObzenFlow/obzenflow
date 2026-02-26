// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for creating stage resources with all their dependencies
//!
//! This module handles the complex wiring of stage-local journals, upstream journals,
//! control journals, message bus, and other resources that stages need.

use crate::backpressure::{
    BackpressurePlan, BackpressureReader, BackpressureRegistry, BackpressureWriter,
};
use crate::id_conversions::StageIdExt;
use crate::message_bus::FsmMessageBus;
use crate::messaging::upstream_subscription::{ContractsWiring, UpstreamSubscription};
use crate::replay::ReplayArchive;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, StageId, SystemId};
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::sync::Arc;

/// Factory for creating subscriptions with pre-computed metadata
/// This allows deferred subscription creation with the correct journal subsets
#[derive(Clone)]
pub struct SubscriptionFactory {
    /// Pre-computed stage names for all potential upstreams
    stage_names: HashMap<StageId, String>,
}

/// A subscription factory that is already bound to a specific set of journals
/// and resolved stage names. Stages can build subscriptions without re-plumbing
/// upstream details.
#[derive(Clone)]
pub struct BoundSubscriptionFactory {
    /// Owner label for logging/attribution
    pub owner_label: String,
    journals_with_names: Vec<(StageId, String, Arc<dyn Journal<ChainEvent>>)>,
}

impl SubscriptionFactory {
    /// Create a new factory with pre-computed metadata
    pub fn new(stage_names: HashMap<StageId, String>) -> Self {
        Self { stage_names }
    }

    /// Create a subscription for the given journals with pre-computed names
    pub async fn create_subscription(
        &self,
        journals: &[(StageId, Arc<dyn Journal<ChainEvent>>)],
    ) -> Result<UpstreamSubscription<ChainEvent>, String> {
        // Build the tuples with names
        let journals_with_names: Vec<(StageId, String, Arc<dyn Journal<ChainEvent>>)> = journals
            .iter()
            .map(|(id, journal)| {
                let name = self
                    .stage_names
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| format!("{id:?}"));
                (*id, name, journal.clone())
            })
            .collect();

        UpstreamSubscription::new_with_names("unknown_owner", &journals_with_names)
            .await
            .map_err(|e| format!("Failed to create subscription: {e:?}"))
    }

    /// Bind this factory to a concrete set of journals (capturing stage IDs/names)
    pub fn bind(
        &self,
        journals: &[(StageId, Arc<dyn Journal<ChainEvent>>)],
    ) -> BoundSubscriptionFactory {
        let journals_with_names: Vec<(StageId, String, Arc<dyn Journal<ChainEvent>>)> = journals
            .iter()
            .map(|(id, journal)| {
                let name = self
                    .stage_names
                    .get(id)
                    .cloned()
                    .unwrap_or_else(|| format!("{id:?}"));
                (*id, name, journal.clone())
            })
            .collect();

        BoundSubscriptionFactory {
            owner_label: "unknown_owner".to_string(),
            journals_with_names,
        }
    }
}

impl BoundSubscriptionFactory {
    /// Build a subscription from the bound journals
    pub async fn build(&self) -> Result<UpstreamSubscription<ChainEvent>, String> {
        UpstreamSubscription::new_with_names(&self.owner_label, &self.journals_with_names)
            .await
            .map(|sub| sub.transport_only())
            .map_err(|e| format!("Failed to create subscription: {e:?}"))
    }

    /// Build a subscription with contracts configured from the bound journals
    pub async fn build_with_contracts(
        &self,
        wiring: ContractsWiring,
    ) -> Result<UpstreamSubscription<ChainEvent>, String> {
        let subscription =
            UpstreamSubscription::new_with_names(&self.owner_label, &self.journals_with_names)
                .await
                .map_err(|e| format!("Failed to create subscription: {e:?}"))?
                .with_contracts(wiring)
                .transport_only();

        Ok(subscription)
    }

    /// Whether there are any upstream journals bound
    pub fn is_empty(&self) -> bool {
        self.journals_with_names.is_empty()
    }

    /// Convenience for logging
    pub fn upstream_stage_ids(&self) -> Vec<StageId> {
        self.journals_with_names
            .iter()
            .map(|(id, _, _)| *id)
            .collect()
    }
}

/// Resources provided to stage creation
pub struct StageResources {
    /// Flow execution ID (from pipeline)
    pub flow_id: FlowId,

    /// Stage's own journal for writing data events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Stage's own journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// Shared system journal for lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Upstream journals for reading events
    pub upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Friendly upstream stage names keyed by StageId (for diagnostics/instrumentation)
    pub upstream_stage_names: std::collections::HashMap<StageId, String>,

    /// Factory for creating subscriptions with pre-computed metadata
    /// All stages must use this factory to create their subscriptions
    pub subscription_factory: SubscriptionFactory,

    /// Stage-scoped factory already bound to this stage's upstream journals
    pub upstream_subscription_factory: BoundSubscriptionFactory,

    /// Message bus for FSM communication
    pub message_bus: Arc<FsmMessageBus>,

    /// List of upstream stage IDs
    pub upstream_stages: Vec<StageId>,

    /// All error journals from all stages (only populated for ErrorSink)
    pub error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Backpressure writer handle for this stage's data journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Backpressure readers keyed by upstream stage ID (FLOWIP-086k).
    pub backpressure_readers: HashMap<StageId, BackpressureReader>,

    /// Flow-scoped backpressure registry (FLOWIP-086k).
    ///
    /// Used by cycle entry points to evaluate SCC quiescence (FLOWIP-051n).
    pub backpressure_registry: Arc<BackpressureRegistry>,

    /// Optional replay archive injection (FLOWIP-095a). Sources use this to
    /// read archived journals instead of calling external systems.
    pub replay_archive: Option<Arc<dyn ReplayArchive>>,
}

/// Builder for creating all stage resources with proper wiring
pub struct StageResourcesBuilder {
    flow_id: FlowId,
    pipeline_system_id: SystemId,
    topology: Arc<Topology>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    stage_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
    error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
    backpressure_plan: BackpressurePlan,
    replay_archive: Option<Arc<dyn ReplayArchive>>,
}

impl StageResourcesBuilder {
    /// Create a new builder
    pub fn new(
        flow_id: FlowId,
        pipeline_system_id: SystemId,
        topology: Arc<Topology>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        stage_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
        error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>>,
    ) -> Self {
        Self {
            flow_id,
            pipeline_system_id,
            topology,
            system_journal,
            stage_journals,
            error_journals,
            backpressure_plan: BackpressurePlan::disabled(),
            replay_archive: None,
        }
    }

    /// Configure a flow-scoped backpressure plan (FLOWIP-086k).
    pub fn with_backpressure_plan(mut self, plan: BackpressurePlan) -> Self {
        self.backpressure_plan = plan;
        self
    }

    /// Inject a replay archive implementation (FLOWIP-095a).
    pub fn with_replay_archive(mut self, replay_archive: Option<Arc<dyn ReplayArchive>>) -> Self {
        self.replay_archive = replay_archive;
        self
    }

    /// Build all resources for all stages
    pub async fn build(self) -> Result<StageResourcesSet, String> {
        // Create shared message bus
        let message_bus = Arc::new(FsmMessageBus::new());

        // Build backpressure registry once per flow (Phase 1: in-process).
        let mut backpressure_plan = self.backpressure_plan;
        backpressure_plan.auto_enable_scc_internal_edges(self.topology.as_ref());
        let backpressure_registry = Arc::new(BackpressureRegistry::new(
            self.topology.as_ref(),
            &backpressure_plan,
        ));

        // Build stage resources for each stage
        let mut stage_resources = HashMap::new();

        // Keep track of all stage journals for metrics aggregator
        let mut all_stage_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> = Vec::new();

        // Keep track of all error journals for ErrorSink
        let mut all_error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> = Vec::new();

        for stage_info in self.topology.stages() {
            let stage_ulid = stage_info.id;
            let stage_id = StageId::from_topology_id(stage_ulid);

            // Get the stage's own journal
            let data_journal = self
                .stage_journals
                .get(&stage_id)
                .ok_or_else(|| format!("No journal found for stage {stage_id:?}"))?
                .clone();

            // Get the stage's error journal
            let error_journal = self
                .error_journals
                .get(&stage_id)
                .ok_or_else(|| format!("No error journal found for stage {stage_id:?}"))?
                .clone();

            // Keep a reference for metrics aggregator
            all_stage_journals.push((stage_id, data_journal.clone()));

            // Keep a reference for ErrorSink
            all_error_journals.push((stage_id, error_journal.clone()));

            // Get upstream journals
            let upstream_ulids = self.topology.upstream_stages(stage_ulid);
            let upstream_names: Vec<String> = upstream_ulids
                .iter()
                .map(|upstream| {
                    self.topology
                        .stage_name(*upstream)
                        .unwrap_or("unknown")
                        .to_string()
                })
                .collect();

            tracing::info!(
                target: "flowip-080o",
                stage_name = %stage_info.name,
                stage_id = ?stage_id,
                upstream_ulids = ?upstream_ulids,
                upstream_names = ?upstream_names,
                "Building upstream_journals for stage"
            );

            let upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)> = upstream_ulids
                .iter()
                .filter_map(|upstream_ulid| {
                    let upstream_id = StageId::from_topology_id(*upstream_ulid);
                    let journal_opt = self.stage_journals.get(&upstream_id);

                    if let Some(journal) = journal_opt {
                        tracing::info!(
                            target: "flowip-080o",
                            stage_name = %stage_info.name,
                            upstream_id = ?upstream_id,
                            upstream_stage_name = %self
                                .topology
                                .stage_name(upstream_id.to_topology_id())
                                .unwrap_or("unknown"),
                            upstream_journal_id = ?journal.id(),
                            "Found upstream journal"
                        );
                        Some((upstream_id, journal.clone()))
                    } else {
                        tracing::warn!(
                            target: "flowip-080o",
                            stage_name = %stage_info.name,
                            upstream_id = ?upstream_id,
                            upstream_stage_name = %self
                                .topology
                                .stage_name(upstream_id.to_topology_id())
                                .unwrap_or("unknown"),
                            "Missing upstream journal in stage_journals map!"
                        );
                        None
                    }
                })
                .collect();

            let upstream_stage_names: std::collections::HashMap<StageId, String> = upstream_ulids
                .iter()
                .map(|upstream_ulid| {
                    let upstream_id = StageId::from_topology_id(*upstream_ulid);
                    let name = self
                        .topology
                        .stage_name(*upstream_ulid)
                        .unwrap_or("unknown")
                        .to_string();
                    (upstream_id, name)
                })
                .collect();

            // No longer pre-build subscriptions - all stages use factory

            if upstream_journals.len() != upstream_ulids.len() {
                tracing::error!(
                    target: "flowip-080o",
                    stage_name = %stage_info.name,
                    stage_id = ?stage_id,
                    expected_upstreams = upstream_ulids.len(),
                    wired_upstreams = upstream_journals.len(),
                    "Mismatch between topology upstreams and wired upstream journals"
                );
            }

            // Check if this is the ErrorSink stage (FLOWIP-082g)
            let error_journals_for_stage = if stage_info.name == "__error_sink" {
                all_error_journals.clone()
            } else {
                Vec::new()
            };

            let backpressure_writer = backpressure_registry.writer(stage_id);
            let mut backpressure_readers: HashMap<StageId, BackpressureReader> = HashMap::new();
            for upstream_stage in upstream_journals.iter().map(|(id, _)| *id) {
                backpressure_readers.insert(
                    upstream_stage,
                    backpressure_registry.reader(upstream_stage, stage_id),
                );
            }

            // Create subscription factory with ALL stage names (not just upstreams)
            // This allows join stages to create subscriptions after DSL adds reference
            let mut all_stage_names = upstream_stage_names.clone();

            // Add ALL stages to the factory's stage names map
            for stage_info in self.topology.stages() {
                let stage_id = StageId::from_topology_id(stage_info.id);
                all_stage_names
                    .entry(stage_id)
                    .or_insert_with(|| stage_info.name.clone());
            }

            // Keep a copy for logging before moving into the factory
            let all_stage_names_for_log = all_stage_names.clone();
            let subscription_factory = SubscriptionFactory::new(all_stage_names);
            let mut upstream_subscription_factory = subscription_factory.bind(&upstream_journals);
            upstream_subscription_factory.owner_label = stage_info.name.clone();

            let resources = StageResources {
                flow_id: self.flow_id,
                data_journal,
                error_journal,
                system_journal: self.system_journal.clone(),
                upstream_journals: upstream_journals.clone(),
                upstream_stage_names,
                subscription_factory,
                upstream_subscription_factory,
                message_bus: message_bus.clone(),
                upstream_stages: upstream_ulids
                    .into_iter()
                    .map(StageId::from_topology_id)
                    .collect(),
                error_journals: error_journals_for_stage,
                backpressure_writer,
                backpressure_readers,
                backpressure_registry: backpressure_registry.clone(),
                replay_archive: self.replay_archive.clone(),
            };

            tracing::info!(
                target: "flowip-080o",
                stage_name = %stage_info.name,
                stage_id = ?stage_id,
                upstream_journals_count = upstream_journals.len(),
                upstream_stage_ids = ?upstream_journals.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                upstream_stage_names = ?upstream_journals.iter().map(|(id, _)| {
                    (id, all_stage_names_for_log.get(id).cloned().unwrap_or_else(|| "<unknown>".to_string()))
                }).collect::<Vec<_>>(),
                "Inserting StageResources for stage"
            );

            stage_resources.insert(stage_id, resources);
        }

        Ok(StageResourcesSet {
            flow_id: self.flow_id,
            pipeline_system_id: self.pipeline_system_id,
            system_journal: self.system_journal,
            backpressure_registry,
            stage_journals: all_stage_journals,
            error_journals: all_error_journals,
            stage_resources,
            message_bus,
        })
    }
}

/// Complete set of resources for all stages in a flow
pub struct StageResourcesSet {
    /// Flow execution ID
    pub flow_id: FlowId,

    /// Pipeline system ID
    pub pipeline_system_id: SystemId,

    /// System journal for orchestration events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Flow-scoped backpressure registry for observability (FLOWIP-086k).
    pub backpressure_registry: Arc<BackpressureRegistry>,

    /// All stage journals (for metrics aggregator to read)
    pub stage_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// All error journals (for ErrorSink to read) (FLOWIP-082e)
    pub error_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Resources for each stage
    pub stage_resources: HashMap<StageId, StageResources>,

    /// Shared message bus
    pub message_bus: Arc<FsmMessageBus>,
}

impl StageResourcesSet {
    /// Get resources for a specific stage
    pub fn get_stage_resources(&self, stage_id: StageId) -> Option<&StageResources> {
        self.stage_resources.get(&stage_id)
    }

    /// Take resources for a specific stage (removes from set)
    pub fn take_stage_resources(&mut self, stage_id: StageId) -> Option<StageResources> {
        self.stage_resources.remove(&stage_id)
    }

    /// Get the shared message bus
    pub fn message_bus(&self) -> &Arc<FsmMessageBus> {
        &self.message_bus
    }
}
