// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::HashMap;
use std::sync::Arc;

use futures::FutureExt;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{ChainEvent, FlowId, StageId};

use super::fsm::{MergeContext, MergeEvent, MergeResources, MergeState};
use super::handle::{MergeHandle, MergeHandleExt};
use super::supervisor::MergeSupervisor;
use crate::backpressure::BackpressureWriter;
use crate::execution::{RuntimeExecution, RuntimeMode};
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::JonestownSignalStrategy;
use crate::stages::common::stage_handle::FORCE_SHUTDOWN_MESSAGE;
use crate::stages::common::stage_lifecycle::{LifecycleExit, StageMilestone};
use crate::stages::common::supervision::catch_up::test_support::context;
use crate::stages::merges::MergeStrategyKind;
use crate::stages::resources_builder::SubscriptionFactory;
use crate::stages::source::finite::fsm::tests::TestJournal;
use crate::supervised_base::with_external_events::stage_commands;
use crate::supervised_base::{
    ChannelBuilder, HandleBuilder, HandlerSupervisedWithExternalEvents, SupervisorHandle,
    SupervisorTaskBuilder,
};

struct Fixture {
    handle: MergeHandle,
    data: Arc<TestJournal<ChainEvent>>,
    batch: Arc<TestJournal<ChainEvent>>,
    batch_id: StageId,
}

fn fixture() -> Fixture {
    let stage_id = StageId::new();
    let batch_id = StageId::new();
    let live_id = StageId::new();
    let data = Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
    let batch = Arc::new(TestJournal::new(JournalOwner::stage(batch_id)));
    let live: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(live_id)));
    let execution = RuntimeExecution::new(RuntimeMode::Live, None);
    let flow_id = FlowId::new();
    let instrumentation = Arc::new(StageInstrumentation::new());
    instrumentation.bind_observations(flow_id, stage_id.into(), &execution);
    let resources = MergeResources {
        stage_id,
        stage_name: "trades".to_string(),
        flow_name: "flow".to_string(),
        flow_id,
        strategy: MergeStrategyKind::ThenLive,
        writer_id: None,
        runtime_execution: execution.clone(),
        lineage_policy: Default::default(),
        data_journal: data.clone(),
        instrumentation,
        control_strategy: Arc::new(JonestownSignalStrategy),
        processing_context: Default::default(),
        first_phase_subscription_factory: SubscriptionFactory::new(HashMap::new())
            .bind(&[(batch_id, batch.clone() as Arc<dyn Journal<ChainEvent>>)]),
        held_subscription_factory: SubscriptionFactory::new(HashMap::new())
            .bind(&[(live_id, live)]),
        first_phase: Default::default(),
        held: Default::default(),
        held_producers: vec![live_id],
        backpressure_writer: BackpressureWriter::disabled(),
        backpressure_readers: HashMap::new(),
        output_contract: StageOutputContract::empty(),
        pending: None,
        backpressure_pulse: BackpressureActivityPulse::new(),
        backpressure_stall: Default::default(),
        heartbeat: None,
        catch_up: context(stage_id, "trades", execution, None),
    };
    let (sender, receiver, watcher) = ChannelBuilder::new().build(MergeState::Created);
    let supervisor = HandlerSupervisedWithExternalEvents::new(
        MergeSupervisor::new("trades".to_string(), stage_id),
        receiver,
        watcher.clone(),
        stage_commands(data.clone(), FlowContext::new("trades", stage_id)),
    );
    let task = SupervisorTaskBuilder::new("merge_trades").spawn_handler_supervised(
        supervisor,
        MergeState::Created,
        MergeContext::new(resources),
    );
    let handle = HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
    Fixture {
        handle,
        data,
        batch,
        batch_id,
    }
}

#[tokio::test]
async fn failure_is_visible_while_a_merge_row_and_its_failure_publication_are_blocked() {
    let fixture = fixture();
    fixture
        .batch
        .append(
            ChainEventFactory::data_event(
                fixture.batch_id.into(),
                "trades.row",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"trade": 1}),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    let (writing, release_row) = fixture
        .data
        .block_matching_append(|event| matches!(event.payload, ChainPayload::Fact(_)));
    fixture.handle.initialize().await.unwrap();
    fixture
        .handle
        .wait_for_milestone(StageMilestone::Initialized)
        .await
        .unwrap();
    fixture.handle.ready().await.unwrap();
    fixture
        .handle
        .wait_for_milestone(StageMilestone::Started)
        .await
        .unwrap();
    writing.notified().await;
    assert!(matches!(
        fixture.handle.current_state(),
        MergeState::FirstPhase(_)
    ));

    fixture
        .handle
        .send_event(MergeEvent::Error("original failure".into()))
        .await
        .unwrap();
    let failure = fixture.handle.wait_for_failure().await.unwrap();
    assert!(failure.cause.to_string().contains("original failure"));
    assert_eq!(
        fixture.handle.current_state(),
        MergeState::Failing("original failure".into())
    );

    let (publishing, release_failure) = fixture.data.block_matching_append(|event| {
        matches!(
            event.payload,
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Failed { .. }
            ))
        )
    });
    fixture
        .handle
        .send_event(MergeEvent::Error(FORCE_SHUTDOWN_MESSAGE.into()))
        .await
        .unwrap();
    assert!(fixture
        .handle
        .wait_for_stage_exit()
        .now_or_never()
        .is_none());

    release_row.notify_one();
    publishing.notified().await;
    assert_eq!(
        fixture.handle.current_state(),
        MergeState::Failing("original failure".into())
    );
    assert_eq!(
        fixture
            .data
            .read_all_unordered()
            .await
            .unwrap()
            .iter()
            .filter(|record| matches!(record.payload, ChainPayload::Fact(_)))
            .count(),
        1
    );
    assert!(fixture
        .handle
        .wait_for_stage_exit()
        .now_or_never()
        .is_none());

    release_failure.notify_one();
    let LifecycleExit::Failed(failure) = fixture.handle.wait_for_stage_exit().await else {
        panic!("a cancel during settlement leaves the failure in place")
    };
    assert!(failure.cause.to_string().contains("original failure"));
    assert_eq!(
        fixture.handle.current_state(),
        MergeState::Failed("original failure".into())
    );
}

#[tokio::test]
async fn failure_is_visible_while_the_running_publication_is_blocked() {
    let fixture = fixture();
    let (publishing, release) = fixture.data.block_matching_append(|event| {
        matches!(
            event.payload,
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Running { .. }
            ))
        )
    });
    fixture.handle.initialize().await.unwrap();
    fixture
        .handle
        .wait_for_milestone(StageMilestone::Initialized)
        .await
        .unwrap();
    fixture.handle.ready().await.unwrap();
    publishing.notified().await;
    assert_eq!(fixture.handle.current_state(), MergeState::Starting);

    fixture
        .handle
        .send_event(MergeEvent::Error("original failure".into()))
        .await
        .unwrap();
    let failure = fixture.handle.wait_for_failure().await.unwrap();
    assert!(failure.cause.to_string().contains("original failure"));
    assert_eq!(
        fixture.handle.current_state(),
        MergeState::Failing("original failure".into())
    );

    release.notify_one();
    let LifecycleExit::Failed(failure) = fixture.handle.wait_for_stage_exit().await else {
        panic!("the failure decided while the publication was blocked stands")
    };
    assert!(failure.cause.to_string().contains("original failure"));
}
