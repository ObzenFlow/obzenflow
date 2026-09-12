// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Supervisor dispatch fairness, retained journal reads and deadline service.

use crate::bootstrap::{
    bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig, StartupMode,
};
use crate::journal::FlowJournalFactory;
use crate::messaging::SystemSubscription;
use crate::pipeline::fsm::{PipelineAction, PipelineFsmEvent, PipelineFsmState};
use crate::pipeline::resources::ProducerTail;
use crate::pipeline::supervisor::PipelineSupervisor;
use crate::pipeline::tests::support::new_system_journal;
use crate::pipeline::tests::support::{
    empty_system_subscription, make_fsm_context, source_sink_topology,
    source_sink_topology_with_source, spawn_supervisor_loop, test_context, test_supervisor,
    TestPipelineStageHandle,
};
use crate::pipeline::{FlowStopMode, PipelineControl, PipelineState};
use crate::supervised_base::{ChannelBuilder, EventLoopDirective, SelfSupervised};
use async_trait::async_trait;
use futures::FutureExt;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::{EventEnvelope, StageId, SystemId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct PausedReader {
    row: Option<EventEnvelope<SystemEvent>>,
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl JournalReader<SystemEvent> for PausedReader {
    async fn next(&mut self) -> Result<Option<EventEnvelope<SystemEvent>>, JournalError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        // Moving the cursor before suspension intentionally makes cancellation
        // unsafe. A recreated read would skip this committed envelope.
        let row = self.row.take();
        if row.is_some() {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(row)
    }
    fn position(&self) -> u64 {
        u64::from(self.row.is_none())
    }
}

#[test]
fn pipeline_supervisor_has_no_inline_fsm_definition() {
    const SUPERVISOR_MOD: &str = include_str!("../supervisor.rs");
    assert!(
        !SUPERVISOR_MOD.contains("fsm!"),
        "pipeline supervisor must not contain an inline fsm! definition; keep the FSM single-sourced in pipeline/fsm/mod.rs"
    );
}

pub async fn graceful_deadline_bounds_a_stalled_source_control_send(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, _) = source_sink_topology();
    let subscription = empty_system_subscription(&journal).await;
    let mut context = test_context(topology, system_id, journal.clone(), Some(subscription));
    let stage_id = StageId::new();
    context.source_supervisors.insert(
        stage_id,
        Arc::new(TestPipelineStageHandle {
            id: stage_id,
            name: "stalled_source_control".into(),
            stage_type: StageType::FiniteSource,
            start_gate: None,
            shutdown_probe: None,
            stall_drain: true,
            panic_on_start: false,
        }),
    );
    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Running);
    let task = spawn_supervisor_loop(
        PipelineState::Running,
        test_supervisor(system_id, journal.clone()),
        context,
        receiver,
        watcher,
    );
    sender
        .send(PipelineFsmEvent::from(PipelineControl::Stop {
            mode: FlowStopMode::Graceful {
                timeout: std::time::Duration::from_millis(20),
            },
        }))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(500), task)
        .await
        .expect("a full source control queue cannot hold the pipeline beyond its graceful deadline")
        .unwrap()
        .unwrap();
    let facts = journal.read_all_unordered().await.unwrap();
    let admissions: Vec<_> = facts
        .iter()
        .filter_map(|envelope| match &envelope.event.event {
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::StopAdmitted { admission },
            ) => Some(admission.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        admissions,
        [
            obzenflow_core::event::PipelineStopAdmission::Graceful {
                timeout_ms: obzenflow_core::event::types::DurationMs(20)
            },
            obzenflow_core::event::PipelineStopAdmission::Cancel {
                cause: obzenflow_core::event::PipelineCancellationCause::GracefulTimeout
            },
        ]
    );
}

pub async fn persistent_controls_cannot_starve_command_delivery_or_stage_joins(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use obzenflow_fsm::FsmAction;
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal, None);
    ctx.source_supervisors.insert(
        source,
        TestPipelineStageHandle::boxed(source, "source", StageType::FiniteSource),
    );
    ctx.stage_supervisors.insert(
        sink,
        TestPipelineStageHandle::boxed(sink, "sink", StageType::Sink),
    );
    PipelineAction::StartSources
        .execute(&mut ctx)
        .await
        .unwrap();
    PipelineAction::ObserveStages
        .execute(&mut ctx)
        .await
        .unwrap();
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Running);
    for _ in 0..32 {
        sender.send(PipelineFsmEvent::Start).await.unwrap();
    }
    let mut supervisor =
        PipelineSupervisor::new(system_id, receiver, watcher, ctx.resources.failure.clone());
    let mut controls_observed = 0;
    for _ in 0..16 {
        if ctx.resources.delivery.is_empty() && ctx.resources.stages_joined {
            break;
        }
        if matches!(
            supervisor
                .dispatch_state(&PipelineFsmState::Running, &mut ctx)
                .await
                .unwrap(),
            EventLoopDirective::Transition(PipelineFsmEvent::Start)
        ) {
            controls_observed += 1;
        }
    }
    assert!(
        ctx.resources.delivery.is_empty(),
        "authorised commands need bounded service"
    );
    assert!(
        ctx.resources.stages_joined,
        "every stage join needs bounded service"
    );
    assert!(
        controls_observed > 0 && controls_observed < 32,
        "controls must share dispatch with owned work"
    );
}

pub async fn queued_controls_cannot_starve_bootstrap_or_automatic_start(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..BootstrapConfig::default()
    });
    for state in [PipelineFsmState::Created, PipelineFsmState::ReadyForRun] {
        let mut ctx = make_fsm_context(make_journals);
        let (sender, receiver, watcher) = ChannelBuilder::new().build(state.public_state(&ctx));
        // Exercise dispatch without applying these control transitions. Stops
        // distinguish queued controls from the automatically generated Start.
        for _ in 0..2 {
            sender
                .send(PipelineFsmEvent::from(PipelineControl::Stop {
                    mode: FlowStopMode::Cancel,
                }))
                .await
                .unwrap();
        }
        let mut supervisor = PipelineSupervisor::new(
            ctx.system_id,
            receiver,
            watcher,
            ctx.resources.failure.clone(),
        );
        assert!(matches!(
            supervisor.dispatch_state(&state, &mut ctx).await.unwrap(),
            EventLoopDirective::Transition(PipelineFsmEvent::Cancel)
        ));
        let directive = supervisor.dispatch_state(&state, &mut ctx).await.unwrap();
        assert!(
            matches!(
                (&state, directive),
                (
                    PipelineFsmState::Created,
                    EventLoopDirective::Transition(PipelineFsmEvent::Bootstrap)
                ) | (
                    PipelineFsmState::ReadyForRun,
                    EventLoopDirective::Transition(PipelineFsmEvent::Start)
                )
            ),
            "startup in {state:?} must get a turn while controls are still queued"
        );
        assert!(matches!(
            supervisor.dispatch_state(&state, &mut ctx).await.unwrap(),
            EventLoopDirective::Transition(PipelineFsmEvent::Cancel)
        ));
    }
}

pub async fn ready_stage_joins_cannot_starve_other_resource_completions(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    // One turn per ready resource must suffice, even with more stage joins
    // ready than the supervisor can consume within that budget.
    const DISPATCH_BUDGET: usize = 4;
    let mut ctx = make_fsm_context(make_journals);
    ctx.resources.stage_joins = Some(Mutex::new(
        (0..=DISPATCH_BUDGET)
            .map(|_| futures::future::ready(Ok(())).boxed())
            .collect(),
    ));
    ctx.resources.refresh_publications();
    ctx.resources.producer_tail =
        ProducerTail::Reading(Mutex::new(futures::future::ready(Ok(None)).boxed()));
    ctx.resources.metrics_join = Some(Mutex::new(futures::future::ready(Ok(())).boxed()));
    let (_sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Running);
    let mut supervisor = PipelineSupervisor::new(
        ctx.system_id,
        receiver,
        watcher,
        ctx.resources.failure.clone(),
    );

    for _ in 0..DISPATCH_BUDGET {
        assert!(matches!(
            supervisor
                .dispatch_state(&PipelineFsmState::Running, &mut ctx)
                .await
                .unwrap(),
            EventLoopDirective::Continue
        ));
    }

    assert!(ctx.resources.publication_settlement.is_none());
    assert!(matches!(ctx.resources.producer_tail, ProducerTail::Reached));
    assert!(ctx.resources.metrics_joined);
    assert!(ctx.resources.metrics_join.is_none());
    assert!(
        !ctx.resources
            .stage_joins
            .as_mut()
            .expect("some stage joins must remain unobserved")
            .get_mut()
            .unwrap()
            .is_empty(),
        "other resources must finish before the ready stage joins are exhausted"
    );
}

pub async fn completed_action_failure_gateway_does_not_report_the_original_error_again(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, _) = source_sink_topology();
    let mut ctx = test_context(topology, system_id, journal, None);
    ctx.resources
        .retain_failure(Box::new(std::io::Error::other("handoff failed")));
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    let mut supervisor =
        PipelineSupervisor::new(system_id, receiver, watcher, ctx.resources.failure.clone());
    // This hook follows successful execution of the shared runner's failure
    // actions. Dispatch must retain the error for completion without routing it
    // through that gateway a second time.
    supervisor
        .after_transition(&PipelineFsmState::SettlingStages, &ctx)
        .await
        .unwrap();
    sender.send(PipelineFsmEvent::Start).await.unwrap();
    assert!(matches!(
        supervisor
            .dispatch_state(&PipelineFsmState::SettlingStages, &mut ctx)
            .await
            .unwrap(),
        EventLoopDirective::Transition(PipelineFsmEvent::Start)
    ));
    assert_eq!(
        ctx.resources.failure.get().unwrap().to_string(),
        "handoff failed"
    );
}

pub async fn pending_journal_read_survives_controls_and_gets_bounded_service(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, sink) = source_sink_topology();
    let row = journal
        .append(SystemEvent::stage_running(sink), None)
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let subscription = SystemSubscription::new(
        Box::new(PausedReader {
            row: Some(row.clone()),
            calls: calls.clone(),
            entered: entered.clone(),
            release: release.clone(),
        }),
        "paused reader".into(),
    );
    let mut context = test_context(topology, system_id, journal, Some(subscription));
    let (sender, receiver, watcher) = ChannelBuilder::new()
        .with_event_buffer(32)
        .build(PipelineState::Running);
    let mut supervisor = PipelineSupervisor::new(
        system_id,
        receiver,
        watcher,
        context.resources.failure.clone(),
    );
    let mut first = Box::pin(supervisor.dispatch_state(&PipelineFsmState::Running, &mut context));
    assert!(futures::poll!(&mut first).is_pending());
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = entered.notified() => {},
            result = &mut first => panic!("read unexpectedly completed: {result:?}"),
        }
    })
    .await
    .unwrap();
    sender.send(PipelineFsmEvent::Start).await.unwrap();
    assert!(matches!(
        first.await.unwrap(),
        EventLoopDirective::Transition(PipelineFsmEvent::Start)
    ));
    for _ in 0..32 {
        sender.send(PipelineFsmEvent::Start).await.unwrap();
    }
    for _ in 0..8 {
        assert!(matches!(
            supervisor
                .dispatch_state(&PipelineFsmState::Running, &mut context)
                .await
                .unwrap(),
            EventLoopDirective::Transition(PipelineFsmEvent::Start)
        ));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    release.notify_one();
    let mut delivered = false;
    for _ in 0..4 {
        if let EventLoopDirective::Transition(PipelineFsmEvent::Journal(envelope)) = supervisor
            .dispatch_state(&PipelineFsmState::Running, &mut context)
            .await
            .unwrap()
        {
            assert_eq!(envelope.event.id, row.event.id);
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "ready journal input must be served despite the full control queue"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

pub async fn expired_stop_is_dispatched_before_a_full_external_control_queue(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::supervised_base::{ChannelBuilder, SelfSupervised};
    let mut context = make_fsm_context(make_journals);
    context.stop_intent.apply_request(
        FlowStopMode::Graceful {
            timeout: std::time::Duration::ZERO,
        },
        None,
    );
    let (sender, receiver, watcher) = ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
        .with_event_buffer(32)
        .build(PipelineState::Draining);
    for _ in 0..32 {
        sender.send(PipelineFsmEvent::Start).await.unwrap();
    }
    let mut supervisor = PipelineSupervisor::new(
        context.system_id,
        receiver,
        watcher,
        context.resources.failure.clone(),
    );
    let directive = supervisor
        .dispatch_state(&PipelineFsmState::Draining, &mut context)
        .await
        .unwrap();
    assert!(matches!(
        directive,
        crate::supervised_base::EventLoopDirective::Transition(
            PipelineFsmEvent::GracefulStopExpired
        )
    ));
}
