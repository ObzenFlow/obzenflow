// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A stateful stage records an input its handler rejects in its own error
//! journal. One whose input journal holds an error-marked record, as an
//! archive recorded before rejections left data journals can, fails on the
//! refusal and never folds the record into its state.

mod replay_testkit;

use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{ChainPayload, SystemEvent};
use obzenflow_core::journal::factory::{FlowJournalFactory, RunResourcePlan, RunSubstrateState};
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::{AppendOptions, Journal, JournalError, JournalReader, RunManifest};
use obzenflow_core::{EventId, FlowId, JournalId, JournalRecord, StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_stateful, flow, sink, source, stateful, transform, FlowDefinition};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::{disk_journals, DiskJournalFactory};
use obzenflow_runtime::effects::{Effects, SinkRedeliverySafety, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulStatefulHandler, InlineSink, SinkDescription, SinkWriteFailure, StatefulEmission,
    TypedFiniteSourceHandler, TypedStatefulHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    value: u64,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "stateful_pre_error.input";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Aggregate {
    count: usize,
}

impl TypedPayload for Aggregate {
    const EVENT_TYPE: &'static str = "stateful_pre_error.aggregate";
}

#[derive(Clone, Debug)]
struct ThreeRows {
    next: u64,
}

impl ThreeRows {
    fn new() -> Self {
        Self { next: 1 }
    }
}

impl TypedFiniteSourceHandler for ThreeRows {
    type Output = Input;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![Input { value }]))
    }
}

#[derive(Clone, Debug)]
struct Validate;

impl TypedTransformHandler for Validate {
    type Input = Input;
    type Output = Input;

    fn process(&self, input: Input) -> Result<Input, HandlerError> {
        Ok(input)
    }
}

struct RecordedBeforeTheRefusal {
    inner: Arc<dyn Journal<ChainEvent>>,
}

#[async_trait]
impl obzenflow_core::journal::JournalStorage<ChainEvent> for RecordedBeforeTheRefusal {
    fn storage_id(&self) -> &JournalId {
        self.inner.id()
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn storage_append(
        &self,
        mut event: ChainEvent,
        options: AppendOptions<ChainEvent>,
    ) -> Result<JournalRecord<ChainPayload>, JournalError> {
        if Input::from_event(&event).is_some_and(|input| input.value == 2) {
            event.processing.status =
                ProcessingStatus::error_with_kind("rejected value 2", Some(ErrorKind::Domain));
        }
        self.inner.append(event, options).await
    }

    async fn storage_append_group(
        &self,
        group_id: &str,
        events: Vec<ChainEvent>,
        options: AppendOptions<ChainEvent>,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.append_group(group_id, events, options).await
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.read_all_unordered().await
    }

    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.read_event(event_id).await
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        self.inner.reader_from(position).await
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
        self.inner.read_last_n(count).await
    }
}

struct MarkingJournalFactory {
    inner: DiskJournalFactory,
}

impl FlowJournalFactory for MarkingJournalFactory {
    fn run_state(&self) -> RunSubstrateState {
        FlowJournalFactory::run_state(&self.inner)
    }

    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        let marks = matches!(&name, JournalName::Stage { name, .. } if name == "validate");
        let inner = FlowJournalFactory::create_chain_journal(&mut self.inner, name, owner)?;
        Ok(if marks {
            Arc::new(RecordedBeforeTheRefusal { inner })
        } else {
            inner
        })
    }

    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        FlowJournalFactory::create_system_journal(&mut self.inner, name, owner)
    }

    fn resource_preflight(&self, plan: &RunResourcePlan) -> Result<(), JournalError> {
        FlowJournalFactory::resource_preflight(&self.inner, plan)
    }

    fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        FlowJournalFactory::write_run_manifest(&self.inner, manifest)
    }
}

fn marking_disk_journals(
    base_path: PathBuf,
) -> impl Fn(FlowId, Option<&Path>) -> Result<MarkingJournalFactory, JournalError> {
    move |flow_id, root| {
        Ok(MarkingJournalFactory {
            inner: DiskJournalFactory::new(
                root.map_or_else(|| base_path.clone(), Path::to_path_buf),
                flow_id,
            )?,
        })
    }
}

#[derive(Clone, Debug)]
struct GuardedAccumulator {
    calls: Arc<AtomicUsize>,
}

impl TypedStatefulHandler for GuardedAccumulator {
    type State = usize;
    type Input = Input;
    type Output = Aggregate;

    fn accumulate(&self, state: &mut Self::State, _input: Input) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *state += 1;
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn emit(&self, state: &Self::State) -> Result<StatefulEmission<Self::Output>, HandlerError> {
        Ok(StatefulEmission::RetainEpoch {
            outputs: vec![Aggregate { count: *state }],
        })
    }

    fn post_emit(&self, _: &mut Self::State) {}
}

#[derive(Clone, Debug)]
struct CollectSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

#[async_trait]
impl InlineSink for CollectSink {
    type Input = Aggregate;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, event: Aggregate) -> Result<(), SinkWriteFailure> {
        self.events
            .lock()
            .expect("collector lock")
            .push(event.to_event(WriterId::from(StageId::new())));
        Ok(())
    }
}

fn build_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    collected: Arc<Mutex<Vec<ChainEvent>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let input_handler = ThreeRows::new();
        let validate_handler = Validate;
        let aggregate_handler = GuardedAccumulator { calls };
        let output_handler = CollectSink { events: collected };

        Ok(flow! {
            name: "stateful_pre_error_bypass",
            journals: marking_disk_journals(journal_base),

            stages: {
                input = source!(Input => input_handler);
                validate = transform!(Input -> Input => validate_handler);
                aggregate = stateful!(Input -> Aggregate => aggregate_handler);
                output = sink!(Aggregate => output_handler);
            },

            topology: {
                input |> validate;
                validate |> aggregate;
                aggregate |> output;
            }
        })
    })
}

#[tokio::test]
async fn a_stateful_stage_refuses_an_error_marked_input() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let calls = Arc::new(AtomicUsize::new(0));
    let collected = Arc::new(Mutex::new(Vec::new()));

    let outcome = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            calls.clone(),
            collected.clone(),
        ))
        .await;
    assert!(
        matches!(outcome, Err(ApplicationError::FlowExecutionFailed(_))),
        "the refusal fails the run: {outcome:?}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "only the row before the error-marked one reaches stateful accumulation"
    );

    let run_dir = replay_testkit::latest_run_dir(&journal_base);
    let failures: Vec<String> =
        replay_testkit::read_stage_envelopes_appended(&run_dir, "aggregate")
            .await
            .into_iter()
            .filter_map(|record| match &record.payload {
                ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                    StageLifecycleFact::Failed { error, .. },
                )) => Some(error.clone()),
                _ => None,
            })
            .collect();
    assert_eq!(
        failures,
        ["Subscription error: edge 'validate' delivered error-marked `stateful_pre_error.input`; \
          a rejected record belongs in the error journal of the stage that rejected it"]
    );
}

#[derive(Clone, Debug, obzenflow::effects::EffectHandler)]
struct Tally;

#[async_trait]
impl EffectfulStatefulHandler for Tally {
    type State = usize;
    type Input = Input;
    type Output = Aggregate;
    type AllowedEffects = obzenflow_runtime::effect_set![];

    fn initial_state(&self) -> Self::State {
        0
    }

    async fn decide(
        &mut self,
        state: &Self::State,
        input: &Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        if input.value == 2 {
            return Err(HandlerError::Validation("value 2 is out of range".into()));
        }
        fx.emit(Aggregate { count: state + 1 }).await?;
        Ok(fx.complete()?)
    }

    fn apply(&mut self, state: &mut Self::State, fact: Self::Output) -> Result<(), HandlerError> {
        *state = fact.count;
        Ok(())
    }
}

#[tokio::test]
async fn a_stateful_stage_records_the_input_its_handler_rejects() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let journals = journal_base.clone();
    let collected = Arc::new(Mutex::new(Vec::new()));
    let output_handler = CollectSink {
        events: collected.clone(),
    };
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(FlowDefinition::materialize(move |_runtime_config| {
            let input_handler = ThreeRows::new();
            let output_handler = output_handler.clone();
            Ok(flow! {
                name: "stateful_rejects_an_input",
                journals: disk_journals(journals.clone()),

                stages: {
                    input = source!(Input => input_handler);
                    tally = effectful_stateful!(Input -> Aggregate => Tally);
                    output = sink!(Aggregate => output_handler);
                },

                topology: {
                    input |> tally;
                    tally |> output;
                }
            })
        }))
        .await
        .expect("a stateful stage that rejects one input completes");

    let counts: Vec<_> = collected
        .lock()
        .expect("collector lock")
        .iter()
        .filter_map(Aggregate::from_event)
        .map(|aggregate| aggregate.count)
        .collect();
    assert_eq!(counts, [1, 2], "the rejected input never folds into state");

    let run_dir = replay_testkit::latest_run_dir(&journal_base);
    let data = replay_testkit::read_stage_envelopes_appended(&run_dir, "tally").await;
    assert!(!data.iter().any(|record| matches!(
        record.envelope.provenance.event.processing.status,
        ProcessingStatus::Error { .. }
    )));
    let failures = replay_testkit::processing_failures(&run_dir, "tally").await;
    let [(record, failure)] = failures.as_slice() else {
        panic!("the stateful stage records its one rejection: {failures:?}");
    };
    assert_eq!(
        record.envelope.provenance.event.writer_id,
        replay_testkit::stage_writer(&run_dir, "tally")
    );
    assert_eq!(failure.kind, ErrorKind::Validation);
    assert_eq!(failure.detail, "value 2 is out of range");
    assert_eq!(failure.input.payload, serde_json::json!({ "value": 2 }));
    let inputs = replay_testkit::read_stage_envelopes_appended(&run_dir, "input").await;
    let rejected = inputs
        .iter()
        .find(|row| Input::from_event(&row.authored()).is_some_and(|input| input.value == 2))
        .expect("the rejected input");
    assert_eq!(
        record.envelope.provenance.event.causality.parent_ids[0],
        rejected.envelope.provenance.event.id
    );
}
