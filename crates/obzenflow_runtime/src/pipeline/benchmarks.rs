// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only fixture for the real parent FSM and publication scope.

use super::fsm::{
    build_pipeline_fsm_with_initial, PipelineContext, PipelineFsm, PipelineFsmEvent,
    PipelineFsmState,
};
use super::tests::support::test_context;
use crate::id_conversions::StageIdExt;
use crate::supervised_base::publication::{self, PublicationScope};
use crate::supervised_base::{SupervisorJournal, SupervisorRecord};
use obzenflow_core::event::{CausalFrontier, SystemEvent};
use obzenflow_core::{Journal, JournalOwner, StageId};
use std::sync::Arc;

pub struct ParentAdmission {
    context: PipelineContext,
    machine: PipelineFsm,
    scope: Arc<PublicationScope>,
}

impl ParentAdmission {
    pub fn topology(count: usize) -> (Arc<obzenflow_topology::Topology>, Vec<StageId>) {
        let mut builder = obzenflow_topology::TopologyBuilder::new();
        let sink = builder.add_stage(Some("quiet_sink".into()));
        let stages = (0..count)
            .map(|index| {
                let stage = builder.add_stage(Some(format!("child_{index}")));
                builder.add_edge(stage, sink);
                StageId::from_topology_id(stage)
            })
            .collect();
        (
            Arc::new(builder.build_unchecked().expect("benchmark topology")),
            stages,
        )
    }

    pub fn new(
        journal: Arc<dyn Journal<SystemEvent>>,
        topology: Arc<obzenflow_topology::Topology>,
    ) -> Self {
        let Some(JournalOwner::System { system_id }) = journal.owner() else {
            panic!("benchmark parent requires a system-owned journal");
        };
        Self {
            context: test_context(topology, *system_id, journal, None),
            machine: build_pipeline_fsm_with_initial(PipelineFsmState::Running),
            scope: PublicationScope::pipeline(),
        }
    }

    pub async fn admit(&mut self, rows: Vec<SupervisorRecord>) -> usize {
        let scope = self.scope.clone();
        scope
            .enter(async {
                let mut actions = 0;
                for row in rows {
                    actions += self
                        .machine
                        .handle(PipelineFsmEvent::Journal(Box::new(row)), &mut self.context)
                        .await
                        .expect("benchmark parent admission")
                        .len();
                }
                actions
            })
            .await
    }

    pub fn validate(&self, expected: &[SupervisorRecord]) {
        assert_eq!(self.machine.state(), &PipelineFsmState::Running);
        assert_eq!(self.context.running_stages.len(), expected.len());
        assert_eq!(self.context.report_coverage.len(), expected.len());
        let frontier = self.scope.capture();
        for row in expected {
            assert_eq!(
                self.context.report_coverage[&row.journal_id()],
                row.position()
            );
            for (coordinate, sequence) in &row.journal().vector_clock.clocks {
                assert!(frontier.clock().get(coordinate) >= *sequence);
            }
        }
    }

    pub fn incorporate(&self, frontier: &CausalFrontier) {
        self.scope
            .incorporate(frontier)
            .expect("benchmark frontier");
    }

    pub async fn publish(&self, events: Vec<SystemEvent>) -> Vec<SupervisorRecord> {
        let journal = SupervisorJournal::System(self.context.system_journal.clone());
        self.scope
            .enter(async {
                let mut records = Vec::with_capacity(events.len());
                for event in events {
                    records.push(
                        publication::report(&journal, event, Default::default())
                            .await
                            .expect("benchmark parent publication"),
                    );
                }
                records
            })
            .await
    }

    pub async fn settle(&self) {
        self.scope
            .join()
            .await
            .expect("benchmark publication settlement");
    }
}

/// Real readiness fold and publication handoff, without starting business stages.
/// Only available through the development test-support feature.
pub struct ReportParent {
    inner: ParentAdmission,
    pub actions_executed: usize,
}

impl ReportParent {
    pub fn new(
        journal: Arc<dyn Journal<SystemEvent>>,
        topology: Arc<obzenflow_topology::Topology>,
        children: &[StageId],
    ) -> Self {
        let mut inner = ParentAdmission::new(journal, topology);
        inner.machine = build_pipeline_fsm_with_initial(PipelineFsmState::AwaitingStageReadiness);
        inner.scope = inner.context.resources.publications.clone();
        for child in children {
            inner.context.stage_supervisors.insert(
                *child,
                super::tests::support::TestPipelineStageHandle::boxed(
                    *child,
                    "benchmark_child",
                    obzenflow_core::event::context::StageType::Transform,
                ),
            );
        }
        Self {
            inner,
            actions_executed: 0,
        }
    }

    pub async fn apply(&mut self, read: crate::supervised_base::report_reader::ReportRead) {
        use crate::supervised_base::report_reader::ReportRead;
        use obzenflow_fsm::FsmAction;
        let scope = self.inner.scope.clone();
        if !scope.has_capacity(4) {
            scope.wait_for_capacity(4).await.unwrap();
        }
        scope
            .enter(async {
                match read {
                    ReportRead::Coverage { journal, through } => {
                        self.inner.context.report_coverage.insert(journal, through);
                    }
                    ReportRead::Record(row) => {
                        let actions = self
                            .inner
                            .machine
                            .handle(PipelineFsmEvent::Journal(row), &mut self.inner.context)
                            .await
                            .unwrap();
                        for action in actions {
                            action.execute(&mut self.inner.context).await.unwrap();
                            self.actions_executed += 1;
                        }
                    }
                }
            })
            .await;
    }

    pub fn ready(&self) -> bool {
        self.inner.machine.state() == &PipelineFsmState::ReadyForRun
    }

    pub fn covered(&self, journal: &obzenflow_core::JournalId) -> u64 {
        self.inner
            .context
            .report_coverage
            .get(journal)
            .copied()
            .unwrap_or(0)
    }

    pub async fn settle(&self) {
        self.inner.settle().await;
    }
}
