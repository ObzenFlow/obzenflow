// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Real host admission, reserved stop admission and terminal publication under
//! a held physical append. Only the storage delay is injected by the fixture.

use super::{AvailableResult, Event, ParentLifecycle, PipelineAction, State};
use crate::pipeline::ingress::IngressRefusalWriter;
use crate::pipeline::tests::support::{ControlledJournal, TerminalAppendGate};
use crate::supervised_base::publication::{is_admission_closed, BoxError};
use futures::{future::BoxFuture, FutureExt};
use obzenflow_core::event::SystemPayload;
use obzenflow_core::ingress::{IngressAttemptSeq, IngressKey, IngressRefusalReason};
use obzenflow_core::StageKey;
use obzenflow_fsm::FsmAction;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub struct HostCongestion {
    gate: Arc<TerminalAppendGate>,
    accepted: Vec<BoxFuture<'static, Result<(), BoxError>>>,
    waiting: Option<BoxFuture<'static, Result<(), BoxError>>>,
    hosts: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum PressureControl {
    None,
    GracefulStop,
    Cancel,
}

#[derive(serde::Serialize)]
pub struct PressureObservation {
    pub accepted_host_commands: usize,
    pub cancelled_receipt_waiters: usize,
    pub rejected_unadmitted_host_commands: usize,
    pub reserved_stop_publications: usize,
    pub applied_child_results: usize,
    pub application_and_action_selection: Duration,
    pub required_publication_and_settlement: Duration,
    pub requested_storage_hold: Duration,
}

impl ParentLifecycle {
    /// Admit real typed ingress-refusal commands through the parent's host
    /// pool. One extra waiter at eight hosts demonstrates bounded admission.
    pub async fn congest_hosts(&mut self, hosts: usize) -> HostCongestion {
        assert!((1..=8).contains(&hosts));
        assert!(!self.applied && !matches!(self.selected, AvailableResult::Initialized));
        let gate = Arc::new(TerminalAppendGate {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            fail: false,
        });
        let mut journal = ControlledJournal::new(self.context.system_journal.clone());
        journal.gate_event = Some("system.ingress.refusal");
        journal.gate = Some(gate.clone());
        self.context.system_journal = Arc::new(journal);
        let writer = IngressRefusalWriter {
            journal: self.context.system_journal.clone(),
            owner: self.context.resources.publications.clone(),
            writer: self.context.system_id.into(),
        };
        let mut accepted = Vec::new();
        let mut waiting = None;
        for index in 0..hosts + usize::from(hosts == 8) {
            let writer = writer.clone();
            let stage_id = self.children[0].id;
            let mut command = async move {
                writer
                    .record_ingress_refusal(SystemPayload::IngressRefusal {
                        ingress_key: IngressKey("capacity".into()),
                        stage_id,
                        stage_key: StageKey("capacity_child_0".into()),
                        reason: IngressRefusalReason::BufferFull,
                        attempt_seq: IngressAttemptSeq(index as u64),
                        request_count: 1,
                        event_count: 1,
                        batch_count: 0,
                        http_status: 503,
                        retry_after_ms_bucket: None,
                    })
                    .await
            }
            .boxed();
            assert!(futures::poll!(&mut command).is_pending());
            if index < hosts {
                accepted.push(command);
            } else {
                waiting = Some(command);
            }
        }
        gate.entered.notified().await;
        HostCongestion {
            gate,
            accepted,
            waiting,
            hosts,
        }
    }
}

impl HostCongestion {
    /// Child results are already available. Apply a real graceful-stop control
    /// if requested, then their retained outcomes. Stage-command execution is
    /// outside this isolated boundary; selected cancellation/drain is checked.
    pub async fn settle(
        mut self,
        parent: &mut ParentLifecycle,
        hold: Duration,
        control: PressureControl,
    ) -> PressureObservation {
        assert!(parent.available && !parent.applied);
        let scope = parent.context.resources.publications.clone();
        let application = Instant::now();
        if control != PressureControl::None {
            let event = if control == PressureControl::Cancel {
                Event::Cancel
            } else {
                Event::GracefulStop {
                    timeout: Duration::from_secs(30),
                }
            };
            let actions = scope
                .enter(parent.fsm.handle(event, &mut parent.context))
                .await
                .unwrap();
            assert!(actions
                .iter()
                .any(|a| if control == PressureControl::Cancel {
                    matches!(a, PipelineAction::CancelChildren)
                } else {
                    matches!(a, PipelineAction::StopSources)
                }));
            for action in actions {
                if matches!(action, PipelineAction::Publish { .. }) {
                    scope
                        .enter(action.execute(&mut parent.context))
                        .await
                        .unwrap();
                }
            }
        }
        parent.apply_available().await;
        parent.verify_applied();
        let application_and_action_selection = application.elapsed();
        let publication = Instant::now();
        scope.enter(parent.enqueue_terminal()).await;
        // Cancelling a receipt waiter cannot revoke its accepted physical work.
        drop(self.accepted.remove(0));
        scope.close();
        let rejected = if let Some(waiting) = self.waiting.take() {
            let error = waiting
                .await
                .expect_err("closed host admission must reject the ninth waiter");
            assert!(is_admission_closed(error.as_ref()));
            1
        } else {
            0
        };
        let hosts = self.hosts;
        let gate = self.gate;
        let release = tokio::spawn(async move {
            tokio::time::sleep(hold).await;
            for index in 0..hosts {
                if index > 0 {
                    gate.entered.notified().await;
                }
                gate.release.notify_one();
            }
        });
        scope.enter(parent.settle_terminal()).await;
        for accepted in self.accepted {
            accepted.await.unwrap();
        }
        release.await.unwrap();
        scope.join().await.unwrap();
        assert_eq!(parent.fsm.state(), &State::FinalisingMetrics);
        assert!(parent
            .context
            .resources
            .publication_results
            .get_mut()
            .unwrap()
            .is_empty());
        PressureObservation {
            accepted_host_commands: hosts,
            cancelled_receipt_waiters: 1,
            rejected_unadmitted_host_commands: rejected,
            reserved_stop_publications: usize::from(control != PressureControl::None),
            applied_child_results: parent.children.len(),
            application_and_action_selection,
            required_publication_and_settlement: publication.elapsed(),
            requested_storage_hold: hold,
        }
    }
}
