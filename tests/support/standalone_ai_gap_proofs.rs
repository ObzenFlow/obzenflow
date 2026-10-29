// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use obzenflow::ai::{AiClientError, EmbeddingResponse};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
use obzenflow_runtime::stages::SourceError;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

const MAX_MEASURED_EMBEDDING_DIMENSIONS: u32 = 3_072;

fn failing_chat() -> FakeChatModels {
    FakeChatModels::with_reply(|_| {
        Err(AiClientError::Remote {
            message: "fixture provider unavailable".to_string(),
        })
    })
}

fn wide_embedding() -> FakeEmbeddingModels {
    FakeEmbeddingModels::with_reply(|request| {
        let width = request
            .params
            .dimensions
            .expect("the measured fixture requests its width");
        Ok(EmbeddingResponse {
            vectors: request
                .inputs
                .iter()
                .map(|_| vec![f32::MAX; width.get() as usize])
                .collect(),
            vector_dim: width,
            usage: None,
        })
    })
}

fn summarize_with(failure: MapperFailure, counters: MapperCounters) -> SummarizeTicket {
    SummarizeTicket {
        failure,
        counters,
        ..SummarizeTicket::baseline()
    }
}

fn embed_with(failure: MapperFailure, width: u32) -> EmbedSummary {
    EmbedSummary {
        empty_inputs: failure == MapperFailure::EmptyEmbeddingInputs,
        ..EmbedSummary::of_width(width)
    }
}

fn ticket(id: u64, description: &str) -> Vec<TicketRaised> {
    vec![TicketRaised {
        id,
        description: description.to_string(),
    }]
}

#[derive(Clone, Debug, Default)]
struct TwoTicketSource {
    step: u8,
}

impl TypedFiniteSourceHandler for TwoTicketSource {
    type Output = TicketRaised;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let ticket = match self.step {
            0 => TicketRaised {
                id: 1,
                description: "first".to_string(),
            },
            1 => TicketRaised {
                id: 2,
                description: "second".to_string(),
            },
            _ => return Ok(None),
        };
        self.step += 1;
        Ok(Some(vec![ticket]))
    }
}

fn control_interleaving_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
    mapper_counters: MapperCounters,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let summarize = ai::infer(summarize_with(MapperFailure::None, mapper_counters));
        let embed = ai::infer(EmbedSummary::of_width(3));
        let chat_breaker = circuit_breaker().consecutive_failures(5);
        let embedding_breaker = circuit_breaker().consecutive_failures(5);
        let input = TwoTicketSource::default();
        let collected = CollectEmbedded { outputs };

        Ok(flow! {
            name: "standalone_ai_control_interleaving",
            journals: disk_journals(journal_base),
            stages: {
                input = source!(TicketRaised => input);
                chat = ai!(TicketRaised -> TicketSummarised with chat_breaker => summarize);
                embedding = ai!(TicketSummarised -> TicketEmbedded with embedding_breaker => embed);
                collected = sink!(TicketEmbedded => collected);
            },

            topology: {
                input |> chat;
                chat |> embedding;
                embedding |> collected;
            }
        })
    })
}

async fn stage_error_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(run_dir.join("run_manifest.json")).unwrap())
            .unwrap();
    let relative = manifest["stages"][stage_key]["error_journal_file"]
        .as_str()
        .unwrap();
    replay_testkit::read_journal_envelopes::<ChainEvent>(&run_dir.join(relative))
        .await
        .into_iter()
        .map(|envelope| envelope.authored())
        .collect()
}

fn error_projection(events: &[ChainEvent]) -> Vec<(Option<ErrorKind>, String)> {
    events
        .iter()
        .filter_map(|event| match &event.processing.status {
            ProcessingStatus::Error { message, kind } => Some((kind.clone(), message.clone())),
            ProcessingStatus::Success => None,
        })
        .collect()
}

async fn stage_processing_errors(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<(Option<ErrorKind>, String)> {
    let mut events = stage_events(run_dir, stage_key).await;
    events.extend(stage_error_events(run_dir, stage_key).await);
    error_projection(&events)
}

fn failed_effect_record(event: &ChainEvent, effect_type: &str) -> Option<EffectRecord> {
    let ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) = &event.payload else {
        return None;
    };
    let record = record.clone();
    (record.descriptor.effect_type.as_str() == effect_type
        && matches!(record.outcome, EffectOutcomePayload::Failed { .. }))
    .then_some(record)
}

async fn wait_for_atomic(counter: &AtomicUsize, expected: usize, reason: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while counter.load(Ordering::SeqCst) < expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {reason}"));
}

/// Aborting the run's task only requests cancellation; its stages close their
/// journals as they unwind, where a killed process would release them at once.
async fn wait_for_writers_released(archive: &Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while DiskReplayArchive::open_for_resume(archive.to_path_buf(), true)
            .await
            .is_ok_and(|opened| opened.evidence().live_writer)
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the aborted run releases its journal writers");
}

#[tokio::test(flavor = "multi_thread")]
async fn ambiguity_recovery_repeats_only_the_in_doubt_embedding_occurrence() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let config = ai_config(temp.path(), None);
    let physical_success = Arc::new(Notify::new());
    let initial_chat = summarizing();
    let initial_embed = FakeEmbeddingModels::hanging(physical_success.clone());

    let live_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let config = config.clone();
        let providers = fake_ai::providers(&initial_chat, &initial_embed);
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .with_config_file(config)
                .with_ai_models(providers)
                .run_async(chat_and_embedding_flow(
                    journal_base,
                    ticket(1, "ambiguous embedding"),
                    Arc::new(Mutex::new(Vec::new())),
                    SummarizeTicket::baseline(),
                    EmbedSummary::of_width(3),
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), physical_success.notified())
        .await
        .expect("the embedding operation reaches physical success after Start(1)");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(initial_chat.call_count(), 1);
    assert_eq!(initial_embed.call_count(), 1);
    assert!(
        !live_task.is_finished(),
        "the port performs no hidden retry"
    );
    live_task.abort();
    let _ = live_task.await;

    let in_doubt_archive = latest_run_dir(&journal_base);
    wait_for_writers_released(&in_doubt_archive).await;
    let in_doubt_events = stage_events(&in_doubt_archive, "embedding").await;
    assert_eq!(
        in_doubt_events
            .iter()
            .filter(|event| EffectAttemptStarted::matches_event_type(&event.event_type()))
            .count(),
        1
    );
    assert!(in_doubt_events.iter().all(|event| successful_effect_record(
        event,
        EMBEDDING_EFFECT_TYPE
    )
    .is_none()));
    assert!(
        in_doubt_events
            .iter()
            .all(|event| event.event_type() != EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE),
        "the process cut leaves durable attempt evidence but no terminal reply"
    );

    let resumed_outputs = Arc::new(Mutex::new(Vec::new()));
    let (resumed_chat, resumed_embed) = (summarizing(), embedding());
    let mut resume = replay_args(&in_doubt_archive, "--resume-from");
    resume.push(OsString::from("--allow-incomplete-archive"));
    FlowApplication::builder()
        .with_cli_args(resume)
        .with_config_file(config)
        .with_ai_models(fake_ai::providers(&resumed_chat, &resumed_embed))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            ticket(1, "ambiguous embedding"),
            resumed_outputs.clone(),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("the stage's breaker authorizes one recovery attempt");

    assert_eq!(resumed_chat.connect_count(), 0);
    assert_eq!(resumed_chat.call_count(), 0);
    assert_eq!(resumed_embed.connect_count(), 1);
    assert_eq!(resumed_embed.call_count(), 1);
    assert_eq!(resumed_outputs.lock().unwrap().len(), 1);

    let recovered = stage_events(&latest_run_dir(&journal_base), "embedding").await;
    assert_eq!(
        recovered
            .iter()
            .filter_map(EffectAttemptStarted::from_event)
            .map(|attempt| attempt.attempt.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(
        recovered
            .iter()
            .filter(|event| successful_effect_record(event, EMBEDDING_EFFECT_TYPE).is_some())
            .count(),
        1
    );
}

#[tokio::test]
async fn recorded_provider_failure_replays_with_the_same_public_classification() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let config = ai_config(temp.path(), None);
    let live_chat = failing_chat();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(config.clone())
        .with_ai_models(fake_ai::providers(&live_chat, &embedding()))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            ticket(1, "provider failure"),
            Arc::new(Mutex::new(Vec::new())),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("an unavailable model reaches the role, which routes it as a remote error");
    assert_eq!(live_chat.call_count(), 1);

    let live_archive = latest_run_dir(&journal_base);
    let live_data = stage_events(&live_archive, "chat").await;
    let live_failures = live_data
        .iter()
        .filter_map(|event| failed_effect_record(event, CHAT_EFFECT_TYPE))
        .collect::<Vec<_>>();
    assert_eq!(live_failures.len(), 1);
    let live_errors = error_projection(&stage_error_events(&live_archive, "chat").await);
    assert_eq!(live_errors.len(), 1);
    assert_eq!(live_errors[0].0, Some(ErrorKind::Remote));
    assert!(
        live_errors[0].1.contains("fixture provider unavailable"),
        "{live_errors:?}"
    );

    FlowApplication::builder()
        .with_cli_args(replay_args(&live_archive, "--replay-from"))
        .with_config_file(config)
        .with_ai_models(fake_ai::providers(
            &FakeChatModels::forbidden(),
            &FakeEmbeddingModels::forbidden(),
        ))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            ticket(1, "provider failure"),
            Arc::new(Mutex::new(Vec::new())),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("strict replay rehydrates the recorded provider failure without a client");

    let replay_archive = latest_run_dir(&journal_base);
    let replay_data = stage_events(&replay_archive, "chat").await;
    let replay_failures = replay_data
        .iter()
        .filter_map(|event| failed_effect_record(event, CHAT_EFFECT_TYPE))
        .collect::<Vec<_>>();
    assert_eq!(replay_failures.len(), 1);
    assert_eq!(replay_failures[0].outcome, live_failures[0].outcome);
    assert_eq!(
        error_projection(&stage_error_events(&replay_archive, "chat").await),
        live_errors
    );
}

#[tokio::test]
async fn deterministic_role_failures_round_trip_on_their_distinct_error_routes() {
    for (failure, expected_kind, expected_chat_calls, expected_records) in [
        (MapperFailure::BeforeEffect, ErrorKind::Validation, 0, 0),
        (MapperFailure::AfterEffect, ErrorKind::Domain, 1, 1),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let journal_base = temp.path().join("journals");
        let config = ai_config(temp.path(), None);
        let (chat, embed) = (summarizing(), embedding());
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_config_file(config.clone())
            .with_ai_models(fake_ai::providers(&chat, &embed))
            .run_async(chat_and_embedding_flow(
                journal_base.clone(),
                ticket(1, "deterministic role failure"),
                Arc::new(Mutex::new(Vec::new())),
                summarize_with(failure, MapperCounters::default()),
                EmbedSummary::of_width(3),
            ))
            .await
            .expect("deterministic role failures are journalled and routed");
        assert_eq!(chat.call_count(), expected_chat_calls);
        assert_eq!(embed.call_count(), 0);
        let live_archive = latest_run_dir(&journal_base);
        let live_errors = stage_processing_errors(&live_archive, "chat").await;
        assert_eq!(live_errors.len(), 1);
        assert_eq!(live_errors[0].0, Some(expected_kind));
        assert_eq!(
            stage_events(&live_archive, "chat")
                .await
                .iter()
                .filter(|event| successful_effect_record(event, CHAT_EFFECT_TYPE).is_some())
                .count(),
            expected_records
        );

        FlowApplication::builder()
            .with_cli_args(replay_args(&live_archive, "--replay-from"))
            .with_config_file(config)
            .with_ai_models(fake_ai::providers(
                &FakeChatModels::forbidden(),
                &FakeEmbeddingModels::forbidden(),
            ))
            .run_async(chat_and_embedding_flow(
                journal_base.clone(),
                ticket(1, "deterministic role failure"),
                Arc::new(Mutex::new(Vec::new())),
                summarize_with(failure, MapperCounters::default()),
                EmbedSummary::of_width(3),
            ))
            .await
            .expect("strict replay reproduces the deterministic role failure");
        assert_eq!(
            stage_processing_errors(&latest_run_dir(&journal_base), "chat").await,
            live_errors
        );
    }
}

#[tokio::test]
async fn empty_embedding_inputs_fail_validation_before_connect_attempt_or_effect_record() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let (chat, embed) = (summarizing(), embedding());

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(ai_config(temp.path(), None))
        .with_ai_models(fake_ai::providers(&chat, &embed))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            ticket(1, "empty embedding request"),
            Arc::new(Mutex::new(Vec::new())),
            SummarizeTicket::baseline(),
            embed_with(MapperFailure::EmptyEmbeddingInputs, 3),
        ))
        .await
        .expect("request construction failures follow the deterministic handler-error route");

    assert_eq!(chat.connect_count(), 1);
    assert_eq!(chat.call_count(), 1);
    assert_eq!(embed.connect_count(), 0);
    assert_eq!(embed.call_count(), 0);

    let archive = latest_run_dir(&journal_base);
    let embedding_events = stage_events(&archive, "embedding").await;
    assert_eq!(
        embedding_events
            .iter()
            .filter(|event| EffectAttemptStarted::matches_event_type(&event.event_type()))
            .count(),
        0
    );
    assert!(embedding_events
        .iter()
        .all(|event| event.event_type() != EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE));
    let errors = stage_processing_errors(&archive, "embedding").await;
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].0, Some(ErrorKind::Validation));
    assert!(errors[0].1.contains("requires at least one input"));
}

#[tokio::test(flavor = "multi_thread")]
async fn held_provider_serializes_data_and_keeps_eof_out_of_roles() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let release = Arc::new(Semaphore::new(0));
    let chat = FakeChatModels::gated(release.clone(), "concise summary");
    let embed = embedding();
    let mapper_counters = MapperCounters::default();
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let run = tokio::spawn({
        let journal_base = journal_base.clone();
        let outputs = outputs.clone();
        let mapper_counters = mapper_counters.clone();
        let config = ai_config(temp.path(), None);
        let providers = fake_ai::providers(&chat, &embed);
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .with_config_file(config)
                .with_ai_models(providers)
                .run_async(control_interleaving_flow(
                    journal_base,
                    outputs,
                    mapper_counters,
                ))
                .await
        }
    });

    wait_for_atomic(&chat.calls, 1, "the first held chat call").await;
    let active_archive = latest_run_dir(&journal_base);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let input_events = stage_events(&active_archive, "input").await;
            let has_eof = input_events.iter().any(|event| {
                matches!(
                    &event.payload,
                    ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
                )
            });
            if has_eof {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both typed data rows and the runtime-owned EOF arrive while the provider is held");

    assert!(!run.is_finished());
    assert_eq!(chat.call_count(), 1);
    assert_eq!(embed.call_count(), 0);
    assert_eq!(mapper_counters.request.load(Ordering::SeqCst), 1);
    assert_eq!(mapper_counters.response.load(Ordering::SeqCst), 0);
    assert!(outputs.lock().unwrap().is_empty());

    release.add_permits(1);
    wait_for_atomic(&chat.calls, 2, "serial admission of the second chat call").await;
    wait_for_atomic(&embed.calls, 1, "the first embedding call").await;
    assert!(!run.is_finished());
    assert_eq!(mapper_counters.request.load(Ordering::SeqCst), 2);
    assert_eq!(mapper_counters.response.load(Ordering::SeqCst), 1);

    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("the released flow drains")
        .expect("the flow task joins")
        .expect("the flow completes");
    assert_eq!(chat.call_count(), 2);
    assert_eq!(embed.call_count(), 2);
    assert_eq!(mapper_counters.request.load(Ordering::SeqCst), 2);
    assert_eq!(mapper_counters.response.load(Ordering::SeqCst), 2);
    assert_eq!(
        outputs
            .lock()
            .unwrap()
            .iter()
            .map(|output| output.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    let chat_events = stage_events(&latest_run_dir(&journal_base), "chat").await;
    let output_positions = chat_events
        .iter()
        .enumerate()
        .filter_map(|(position, event)| {
            TicketSummarised::matches_event_type(&event.event_type()).then_some(position)
        })
        .collect::<Vec<_>>();
    let chat_writer = chat_events
        .iter()
        .find(|event| TicketSummarised::matches_event_type(&event.event_type()))
        .map(|event| event.writer_id)
        .expect("chat authors typed output");
    let eof_position = chat_events
        .iter()
        .rposition(|event| {
            event.writer_id == chat_writer
                && matches!(
                    &event.payload,
                    ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
                )
        })
        .expect("chat authors EOF after its in-flight work");
    assert_eq!(output_positions.len(), 2);
    assert!(
        output_positions
            .iter()
            .all(|position| *position < eof_position),
        "chat outputs {output_positions:?} must precede its authored EOF at {eof_position}"
    );
}

#[tokio::test]
async fn maximum_release_fixture_measures_one_normalised_reply_and_one_domain_vector_copy() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let wide = wide_embedding();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(ai_config(temp.path(), None))
        .with_ai_models(fake_ai::providers(&summarizing(), &wide))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            ticket(1, "maximum measured embedding"),
            Arc::new(Mutex::new(Vec::new())),
            SummarizeTicket::baseline(),
            embed_with(MapperFailure::None, MAX_MEASURED_EMBEDDING_DIMENSIONS),
        ))
        .await
        .expect("the maximum release measurement fixture completes");
    assert_eq!(wide.call_count(), 1);

    let events = stage_events(&latest_run_dir(&journal_base), "embedding").await;
    let records = events
        .iter()
        .filter_map(|event| successful_effect_record(event, EMBEDDING_EFFECT_TYPE))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    let EffectOutcomePayload::Succeeded { output } = &records[0].outcome else {
        unreachable!()
    };
    assert!(output.pointer("/response/raw").is_none());
    assert_eq!(count_json_key(output, "vectors"), 1);
    assert_eq!(
        output.pointer("/response/vector_dim"),
        Some(&serde_json::json!(MAX_MEASURED_EMBEDDING_DIMENSIONS))
    );

    let output_events = events
        .iter()
        .filter(|event| TicketEmbedded::matches_event_type(&event.event_type()))
        .collect::<Vec<_>>();
    assert_eq!(output_events.len(), 1);
    let all_rows = serde_json::to_value(&events).unwrap();
    assert_eq!(
        count_json_key(&all_rows, "vectors"),
        2,
        "history owns one normalised reply and one caller-selected domain output, with no adapter-private copy"
    );
    let reply_bytes = serde_json::to_vec(&records[0]).unwrap().len();
    let output_bytes = serde_json::to_vec(output_events[0]).unwrap().len();
    assert!(reply_bytes > MAX_MEASURED_EMBEDDING_DIMENSIONS as usize);
    assert!(output_bytes > MAX_MEASURED_EMBEDDING_DIMENSIONS as usize);
}
