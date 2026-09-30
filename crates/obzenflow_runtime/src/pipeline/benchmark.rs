// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only access to retained child results and the production FSM.
//! The benchmark crate owns workload selection, timing and resource accounting.
//! Metadata handles describe the topology; no synthetic handle drives a child.

use super::fsm::{
    build_pipeline_fsm_with_initial, PipelineAction, PipelineContext, PipelineFsm,
    PipelineFsmEvent as Event, PipelineFsmState as State,
};
use super::tests::support::{test_context, TestPipelineStageHandle};
use crate::id_conversions::StageIdExt;
use crate::stages::common::stage_handle::{StageAck, StageExit, StageFailure};
use crate::stages::common::stage_lifecycle::{
    LifecyclePhase, LifecycleResults, Results, StageMilestone,
};
use crate::supervised_base::publication::PublicationScope;
use futures::StreamExt;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::{CausalFrontier, SystemEvent};
use obzenflow_core::{Journal, JournalOwner, StageId};
use obzenflow_fsm::FsmAction;
use obzenflow_topology::TopologyBuilder;
use std::sync::Arc;
use tokio::sync::watch;

mod pressure;
pub use pressure::{HostCongestion, PressureControl, PressureObservation};

#[derive(Clone, Copy, Debug)]
pub enum AvailableResult {
    Initialized,
    Completed,
    /// One child fails; every other child has already completed its work.
    Failed {
        child: usize,
    },
}

struct Child {
    id: StageId,
    frontier: CausalFrontier,
    results: Arc<LifecycleResults>,
    observer: watch::Receiver<Results>,
}

pub struct ParentLifecycle {
    context: PipelineContext,
    fsm: PipelineFsm,
    children: Vec<Child>,
    selected: AvailableResult,
    expected_frontier: CausalFrontier,
    selected_actions: Vec<PipelineAction>,
    available: bool,
    applied: bool,
}

impl ParentLifecycle {
    pub fn new(
        journal: Arc<dyn Journal<SystemEvent>>,
        inputs: Vec<(StageId, CausalFrontier)>,
        retained: CausalFrontier,
        selected: AvailableResult,
    ) -> Self {
        assert!(inputs.len() >= 2, "source and consumer are required");
        if let AvailableResult::Failed { child } = selected {
            assert!(child < inputs.len());
        }
        let Some(JournalOwner::System { system_id }) = journal.owner() else {
            panic!("parent requires a system-owned journal");
        };
        let mut topology = TopologyBuilder::new();
        let mut handles = Vec::new();
        let last = inputs.len() - 1;
        for (index, (id, _)) in inputs.iter().enumerate() {
            let (kind, topology_kind) = if index == 0 {
                (
                    StageType::FiniteSource,
                    obzenflow_topology::StageType::FiniteSource,
                )
            } else if index == last {
                (StageType::Sink, obzenflow_topology::StageType::Sink)
            } else {
                (
                    StageType::Transform,
                    obzenflow_topology::StageType::Transform,
                )
            };
            let name = format!("capacity_child_{index}");
            topology.add_stage_with_id(id.to_topology_id(), Some(name.clone()), topology_kind);
            handles.push(TestPipelineStageHandle::boxed(*id, name, kind));
        }
        let mut context = test_context(
            Arc::new(topology.build().expect("valid source/consumer topology")),
            *system_id,
            journal.clone(),
        );
        for (index, handle) in handles.into_iter().enumerate() {
            let id = handle.stage_id();
            if index == 0 {
                context.source_supervisors.insert(id, handle);
            } else {
                context.stage_supervisors.insert(id, handle);
            }
            context.outstanding_children.insert(id);
            if matches!(selected, AvailableResult::Initialized) {
                context.outstanding_milestones.insert(id.into());
            }
        }
        context.flow_start_time = Some(std::time::Instant::now());
        context
            .resources
            .publications
            .incorporate(&retained)
            .unwrap();
        let mut expected_frontier = retained;
        let children = inputs
            .into_iter()
            .map(|(id, frontier)| {
                expected_frontier.merge(&frontier).unwrap();
                let results = LifecycleResults::new();
                let observer = results.subscribe();
                Child {
                    id,
                    frontier,
                    results,
                    observer,
                }
            })
            .collect();
        let initial = match selected {
            AvailableResult::Initialized => State::InitializingStages,
            _ => State::Running,
        };
        Self {
            context,
            fsm: build_pipeline_fsm_with_initial(initial),
            children,
            selected,
            expected_frontier,
            selected_actions: Vec::new(),
            available: false,
            applied: false,
        }
    }

    /// Commit the assigned phases to the same retained watch results used by
    /// stage handles. Each child contributes one result; none is replayed.
    pub async fn make_available(&mut self) {
        assert!(!self.available, "child results may be produced only once");
        for (index, child) in self.children.iter().enumerate() {
            let scope = PublicationScope::new();
            scope.incorporate(&child.frontier).unwrap();
            scope.enter(child.results.enter(async {
                let accounting = ExecutionAccounting {
                    events_processed_total: 1,
                    events_emitted_total: 1,
                    ..Default::default()
                };
                LifecycleResults::observe(&LifecyclePhase::Initialized, accounting.clone(), None);
                if !matches!(self.selected, AvailableResult::Initialized) {
                    LifecycleResults::observe(&LifecyclePhase::Ready, accounting.clone(), None);
                    LifecycleResults::observe(&LifecyclePhase::Active, accounting.clone(), None);
                    let phase = if matches!(self.selected, AvailableResult::Failed { child } if child == index) {
                        LifecyclePhase::Failed("capacity fixture failure".into())
                    } else {
                        LifecyclePhase::Completed
                    };
                    LifecycleResults::observe(&phase, accounting, None);
                }
            })).await;
            scope.join().await.unwrap();
        }
        self.available = true;
    }

    /// Available retained results -> parent state and selected actions.
    /// Physical publication is deliberately a separate operation below.
    pub async fn apply_available(&mut self) {
        assert!(self.available && !self.applied);
        let scope = self.context.resources.publications.clone();
        scope
            .enter(async {
                for child in &mut self.children {
                    assert!(child.observer.has_changed().unwrap());
                    let result = child.observer.borrow_and_update().clone();
                    let event = if matches!(self.selected, AvailableResult::Initialized) {
                        let ack = result
                            .initialized
                            .expect("retained initialisation acknowledgement");
                        assert_eq!(ack.milestone, StageMilestone::Initialized);
                        Event::ChildAcknowledged(StageAck {
                            stage_id: child.id,
                            milestone: ack.milestone,
                            snapshot: ack.snapshot,
                        })
                    } else {
                        if let Some(failure) = result.failure {
                            self.selected_actions.extend(
                                self.fsm
                                    .handle(
                                        Event::ChildFailed(StageFailure {
                                            stage_id: child.id,
                                            cause: failure.cause,
                                            snapshot: failure.snapshot,
                                        }),
                                        &mut self.context,
                                    )
                                    .await
                                    .unwrap(),
                            );
                        }
                        Event::ChildExited(StageExit {
                            stage_id: child.id,
                            outcome: child.results.settled(child.frontier.clone()),
                        })
                    };
                    self.selected_actions
                        .extend(self.fsm.handle(event, &mut self.context).await.unwrap());
                }
                self.selected_actions.extend(
                    self.fsm
                        .handle(Event::PhaseSatisfied, &mut self.context)
                        .await
                        .unwrap(),
                );
                if self.fsm.state() == &State::Draining {
                    self.selected_actions.extend(
                        self.fsm
                            .handle(Event::PhaseSatisfied, &mut self.context)
                            .await
                            .unwrap(),
                    );
                }
            })
            .await;
        self.applied = true;
    }

    pub fn verify_applied(&self) {
        assert!(self.applied);
        assert_eq!(
            self.context.stage_lifecycle_metrics.len(),
            self.children.len()
        );
        for accounting in self.context.stage_lifecycle_metrics.values() {
            assert_eq!(accounting.events_processed_total, 1);
            assert_eq!(accounting.events_emitted_total, 1);
        }
        assert_eq!(
            self.context.resources.publications.capture().clock(),
            self.expected_frontier.clock()
        );
        match self.selected {
            AvailableResult::Initialized => {
                assert!(self.context.outstanding_milestones.is_empty());
                assert_eq!(self.fsm.state(), &State::StartingConsumers);
                assert!(matches!(
                    self.selected_actions.as_slice(),
                    [PipelineAction::StartConsumers]
                ));
            }
            AvailableResult::Completed | AvailableResult::Failed { .. } => {
                assert!(self.context.outstanding_children.is_empty());
                assert_eq!(self.fsm.state(), &State::PublishingTerminal);
                let failed = matches!(self.selected, AvailableResult::Failed { .. });
                assert_eq!(
                    self.context.completed_stages.len(),
                    self.children.len() - usize::from(failed)
                );
                assert_eq!(self.context.termination.failure.is_some(), failed);
                assert_eq!(
                    self.selected_actions
                        .iter()
                        .filter(|a| matches!(a, PipelineAction::PublishTerminal { .. }))
                        .count(),
                    1
                );
                assert_eq!(
                    self.selected_actions
                        .iter()
                        .filter(|a| matches!(a, PipelineAction::CancelChildren))
                        .count(),
                    usize::from(failed)
                );
            }
        }
    }

    /// Execute the actual selected terminal publication, retain its receipt and
    /// return it through the FSM. Other action costs stay outside this boundary.
    pub async fn publish_terminal(&mut self) {
        assert!(self.applied);
        assert!(!matches!(self.selected, AvailableResult::Initialized));
        let scope = self.context.resources.publications.clone();
        scope
            .enter(async {
                self.enqueue_terminal().await;
                self.settle_terminal().await;
            })
            .await;
        scope.join().await.unwrap();
        assert_eq!(self.fsm.state(), &State::FinalisingMetrics);
    }

    async fn enqueue_terminal(&mut self) {
        let action = self
            .selected_actions
            .iter()
            .find(|a| matches!(a, PipelineAction::PublishTerminal { .. }))
            .unwrap();
        action.execute(&mut self.context).await.unwrap();
    }

    async fn settle_terminal(&mut self) {
        let mut terminal_count = 0;
        while let Some(event) = self
            .context
            .resources
            .publication_results
            .get_mut()
            .unwrap()
            .next()
            .await
        {
            let terminal = matches!(event, Event::TerminalPublished);
            assert!(
                terminal || matches!(event, Event::ObservationEnded),
                "unexpected publication receipt: {event:?}"
            );
            let actions = self.fsm.handle(event, &mut self.context).await.unwrap();
            if terminal {
                terminal_count += 1;
                assert!(matches!(
                    actions.as_slice(),
                    [PipelineAction::FinaliseMetrics]
                ));
            } else {
                assert!(actions.is_empty());
            }
        }
        assert_eq!(terminal_count, 1);
    }
}
