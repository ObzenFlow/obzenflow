// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Builder for a per-composite monitor-supervisor.
//!
//! Mirrors `MetricsAggregatorBuilder`: constructs the supervisor with its own
//! system-journal subscription (from run start, so it sees every member
//! lifecycle), spawns it, and returns a handle the pipeline retains. One builder
//! per composite, wired from topology subgraph membership by the pipeline.

use super::fsm::{CompositeSupervisorContext, CompositeSupervisorEvent, CompositeSupervisorState};
use super::rollup::CompositeRollup;
use super::supervisor::CompositeSupervisor;
use crate::messaging::system_subscription::SystemSubscription;
use crate::supervised_base::{
    BuilderError, ChannelBuilder, HandleBuilder, SelfSupervisedExt, StandardHandle,
    SupervisorBuilder, SupervisorTaskBuilder,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::id::{CompositeId, RoleId, StageId, SystemId};
use obzenflow_core::journal::Journal;
use std::sync::Arc;

/// The handle a spawned composite supervisor returns; the pipeline retains it so
/// the supervisor task is not dropped.
pub type CompositeSupervisorHandle =
    StandardHandle<CompositeSupervisorEvent, CompositeSupervisorState>;

/// Builder for one composite's monitor-supervisor.
pub struct CompositeSupervisorBuilder {
    composite_id: CompositeId,
    members: Vec<(StageId, RoleId)>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
}

impl CompositeSupervisorBuilder {
    /// Create a builder for `composite_id` over its `(member stage, role)` set.
    pub fn new(
        composite_id: CompositeId,
        members: Vec<(StageId, RoleId)>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
    ) -> Self {
        Self {
            composite_id,
            members,
            system_journal,
        }
    }
}

#[async_trait::async_trait]
impl SupervisorBuilder for CompositeSupervisorBuilder {
    type Handle = CompositeSupervisorHandle;
    type Error = BuilderError;

    async fn build(self) -> Result<Self::Handle, Self::Error> {
        let system_id = SystemId::new();
        let name = format!("composite_supervisor:{}", self.composite_id);
        let sub_name = format!("composite:{}", self.composite_id);

        // Read the system journal from the run start so every member lifecycle
        // transition is folded, live and on replay.
        let reader = self
            .system_journal
            .reader_from(0)
            .await
            .map_err(|e| BuilderError::Other(e.to_string()))?;
        let subscription = SystemSubscription::new(reader, sub_name);

        let rollup = CompositeRollup::new(self.composite_id.clone(), self.members);

        let (event_sender, _event_receiver, state_watcher) =
            ChannelBuilder::<CompositeSupervisorEvent, CompositeSupervisorState>::new()
                .with_event_buffer(4)
                .build(CompositeSupervisorState::Running);

        let supervisor = CompositeSupervisor {
            name,
            composite_id: self.composite_id,
            system_id,
            system_journal: self.system_journal,
            subscription: Some(subscription),
            rollup,
        };

        let supervisor_task = SupervisorTaskBuilder::<CompositeSupervisor>::new(
            "composite_supervisor",
        )
        .spawn(move || async move {
            SelfSupervisedExt::run(
                supervisor,
                CompositeSupervisorState::Running,
                CompositeSupervisorContext,
            )
            .await
        });

        HandleBuilder::new()
            .with_event_sender(event_sender)
            .with_state_watcher(state_watcher)
            .with_supervisor_task(supervisor_task)
            .build_standard()
            .map_err(|e| BuilderError::Other(e.to_string()))
    }
}
