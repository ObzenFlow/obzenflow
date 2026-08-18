// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Remaining FLOWIP-128b acceptance proofs for T2, T4, T7, T8, and T9.

use super::*;
use obzenflow::ai::{ChatTransform, EmbeddingTransform};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::EffectAttemptStarted;
use obzenflow_dsl::infinite_source;
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_runtime::bootstrap::{
    install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use std::future::pending;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

const MAX_MEASURED_EMBEDDING_DIMENSIONS: u32 = 3_072;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum MapperFailure {
    #[default]
    None,
    BeforeEffect,
    AfterEffect,
}

#[derive(Clone, Debug, Default)]
struct MapperCounters {
    request: Arc<AtomicUsize>,
    response: Arc<AtomicUsize>,
}

type GapHandlers = (
    ChatTransform<TicketRaised, TicketSummarised>,
    EmbeddingTransform<TicketSummarised, TicketEmbedded>,
);
type GapHandlerBuildResult = Result<GapHandlers, Box<FlowBuildError>>;

#[derive(Debug)]
struct FailingChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ChatClient for FailingChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.target().logically_matches(&self.target));
        Err(AiClientError::Remote {
            message: "fixture provider unavailable".to_string(),
        })
    }
}

#[derive(Debug)]
struct GatedChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl ChatClient for GatedChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.target().logically_matches(&self.target));
        self.release
            .acquire()
            .await
            .expect("the T8 provider gate remains open")
            .forget();
        Ok(ChatResponse {
            text: "concise summary".to_string(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

#[derive(Debug)]
struct AmbiguousEmbeddingClient {
    target: EmbeddingTarget,
    calls: Arc<AtomicUsize>,
    physical_success: Arc<Notify>,
}

#[async_trait]
impl EmbeddingClient for AmbiguousEmbeddingClient {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        assert!(request.logically_targets(&self.target));
        self.calls.fetch_add(1, Ordering::SeqCst);
        // The externally visible operation has succeeded, but its response has
        // not crossed back into the effect runtime. Cutting the run here is
        // the honest execute-to-commit ambiguity window.
        self.physical_success.notify_one();
        pending().await
    }
}

#[derive(Debug)]
struct WideEmbeddingClient {
    target: EmbeddingTarget,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EmbeddingClient for WideEmbeddingClient {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        assert!(request.logically_targets(&self.target));
        self.calls.fetch_add(1, Ordering::SeqCst);
        let width = request
            .params
            .dimensions
            .expect("the T9 fixture requests its measured width");
        Ok(EmbeddingResponse {
            vectors: request
                .inputs
                .iter()
                .map(|_| vec![f32::MAX; width.get() as usize])
                .collect(),
            vector_dim: width,
            usage: None,
        })
    }
}

fn fixture_targets() -> (ChatTarget, EmbeddingTarget) {
    let (chat, embedding) = base_bindings(None).expect("fixture bindings");
    (
        chat.evidence().target().clone(),
        embedding.evidence().target().clone(),
    )
}

fn successful_authority(counters: AuthorityCounters) -> AiAuthority {
    live_authority(None, Some(counters)).expect("fixture authority")
}

fn empty_authority() -> AiAuthority {
    live_authority(None, None).expect("fixture replay authority")
}

fn handlers(
    mapper_failure: MapperFailure,
    mapper_counters: MapperCounters,
    embedding_dimensions: EmbeddingDimensions,
) -> GapHandlerBuildResult {
    let request_calls = mapper_counters.request;
    let response_calls = mapper_counters.response;
    let chat = ChatTransformBuilder::new()
        .logic_version("gap-proof-chat-v1")
        .system("Summarise support tickets concisely.")
        .build_typed::<TicketRaised, TicketSummarised>(
            move |ticket| {
                request_calls.fetch_add(1, Ordering::SeqCst);
                if mapper_failure == MapperFailure::BeforeEffect {
                    return Err(HandlerError::Validation(
                        "fixture request mapping failed".to_string(),
                    ));
                }
                Ok(ticket.description.clone())
            },
            move |ticket, response| {
                response_calls.fetch_add(1, Ordering::SeqCst);
                if mapper_failure == MapperFailure::AfterEffect {
                    return Err(HandlerError::Domain(
                        "fixture response mapping failed".to_string(),
                    ));
                }
                Ok(TicketSummarised {
                    id: ticket.id,
                    summary: response.text,
                })
            },
        )
        .map_err(|error| Box::new(binding_error("chat_handler", error)))?;
    let embedding = EmbeddingTransformBuilder::new()
        .logic_version("gap-proof-embedding-v1")
        .dimensions(embedding_dimensions)
        .build_typed::<TicketSummarised, TicketEmbedded>(
            |ticket| Ok(vec![ticket.summary.clone()]),
            |ticket, response| {
                Ok(TicketEmbedded {
                    id: ticket.id,
                    vectors: response.vectors,
                })
            },
        )
        .map_err(|error| Box::new(binding_error("embedding_handler", error)))?;
    Ok((chat, embedding))
}

fn finite_flow(
    journal_base: PathBuf,
    tickets: Vec<TicketRaised>,
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
    authority: AiAuthority,
    mapper_failure: MapperFailure,
    mapper_counters: MapperCounters,
    embedding_dimensions: EmbeddingDimensions,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = authority.chat.clone();
        let embedding = authority.embedding.clone();
        let (chat_handler, embedding_handler) = handlers(
            mapper_failure,
            mapper_counters.clone(),
            embedding_dimensions,
        )
        .map_err(|error| *error)?;
        let input = sources::finite(tickets.clone());
        let collected = CollectEmbedded {
            outputs: outputs.clone(),
        };

        Ok(flow! {
            name: "standalone_ai_gap_proof",
            journals: disk_journals(journal_base.clone()),
            stages: {
                input = source!(TicketRaised => input);
                chat = effectful_transform!(
                    TicketRaised -> TicketSummarised
                    uses at_least_once(ChatCompletion)
                        via chat
                        with ai_resilience()
                    => chat_handler,
                    observers: [],
                );
                embedding = effectful_transform!(
                    TicketSummarised -> TicketEmbedded
                    uses at_least_once(EmbeddingGeneration)
                        via embedding
                        with ai_resilience()
                    => embedding_handler,
                    observers: [],
                );
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

#[derive(Clone, Debug)]
struct BoundedTicketSource {
    next_id: u64,
    remaining: u64,
}

impl BoundedTicketSource {
    fn new(first_id: u64, count: u64) -> Self {
        Self {
            next_id: first_id,
            remaining: count,
        }
    }
}

impl TypedInfiniteSourceHandler for BoundedTicketSource {
    type Output = TicketRaised;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        let id = self.next_id;
        self.next_id += 1;
        self.remaining -= 1;
        Ok(vec![TicketRaised {
            id,
            description: format!("ticket {id}"),
        }])
    }
}

fn resumable_flow(
    journal_base: PathBuf,
    first_id: u64,
    count: u64,
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
    authority: AiAuthority,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = authority.chat.clone();
        let embedding = authority.embedding.clone();
        let (chat_handler, embedding_handler) = handlers(
            MapperFailure::None,
            MapperCounters::default(),
            EmbeddingDimensions::try_from(3).unwrap(),
        )
        .map_err(|error| *error)?;
        let input = BoundedTicketSource::new(first_id, count);
        let collected = CollectEmbedded {
            outputs: outputs.clone(),
        };

        Ok(flow! {
            name: "standalone_ai_resume_tail",
            journals: disk_journals(journal_base.clone()),
            stages: {
                input = infinite_source!(TicketRaised => input);
                chat = effectful_transform!(
                    TicketRaised -> TicketSummarised
                    uses at_least_once(ChatCompletion)
                        via chat
                        with ai_resilience()
                    => chat_handler,
                    observers: [],
                );
                embedding = effectful_transform!(
                    TicketSummarised -> TicketEmbedded
                    uses at_least_once(EmbeddingGeneration)
                        via embedding
                        with ai_resilience()
                    => embedding_handler,
                    observers: [],
                );
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
    authority: AiAuthority,
    mapper_counters: MapperCounters,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = authority.chat.clone();
        let embedding = authority.embedding.clone();
        let (chat_handler, embedding_handler) = handlers(
            MapperFailure::None,
            mapper_counters.clone(),
            EmbeddingDimensions::try_from(3).unwrap(),
        )
        .map_err(|error| *error)?;
        let input = TwoTicketSource::default();
        let collected = CollectEmbedded {
            outputs: outputs.clone(),
        };

        Ok(flow! {
            name: "standalone_ai_control_interleaving",
            journals: disk_journals(journal_base.clone()),
            stages: {
                input = source!(TicketRaised => input);
                chat = effectful_transform!(
                    TicketRaised -> TicketSummarised
                    uses at_least_once(ChatCompletion)
                        via chat
                        with ai_resilience()
                    => chat_handler,
                    observers: [],
                );
                embedding = effectful_transform!(
                    TicketSummarised -> TicketEmbedded
                    uses at_least_once(EmbeddingGeneration)
                        via embedding
                        with ai_resilience()
                    => embedding_handler,
                    observers: [],
                );
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
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(relative),
        JournalOwner::stage(StageId::new()),
    )
    .unwrap();
    journal
        .read_causally_ordered()
        .await
        .unwrap()
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
}

fn error_projection(events: &[ChainEvent]) -> Vec<(Option<ErrorKind>, String)> {
    events
        .iter()
        .filter_map(|event| match &event.processing_info.status {
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
    let ChainEventContent::Data {
        event_type,
        payload,
    } = &event.content
    else {
        return None;
    };
    if event_type != EFFECT_RECORD_EVENT_TYPE {
        return None;
    }
    let record: EffectRecord = serde_json::from_value(payload.clone()).ok()?;
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

async fn wait_for_outputs(outputs: &Mutex<Vec<TicketEmbedded>>, expected: usize) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while outputs.lock().unwrap().len() < expected {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("timed out waiting for standalone AI outputs");
}

async fn wait_for_running(handle: &FlowHandle) {
    let mut states = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(*states.borrow(), PipelineState::Running) {
                return;
            }
            states.changed().await.expect("pipeline state remains open");
        }
    })
    .await
    .expect("standalone AI flow reaches Running");
}

async fn run_resumable_until(
    flow: FlowDefinition,
    outputs: &Mutex<Vec<TicketEmbedded>>,
    expected: usize,
) {
    let handle = flow
        .build(FlowBuildContext::for_tests())
        .await
        .expect("resumable standalone AI flow builds");
    wait_for_running(&handle).await;
    wait_for_outputs(outputs, expected).await;
    handle.stop().await.expect("test flow accepts stop");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("test flow stops within its deadline")
        .expect("test flow stops cleanly");
}

async fn bootstrap_for_resume(archive_path: PathBuf) -> BootstrapConfig {
    let replay = ReplayBootstrap {
        archive_path: archive_path.clone(),
        allow_incomplete_archive: true,
        allow_duplicate_sink_delivery: false,
        verb: ReplayVerb::Resume,
    };
    let archive = DiskReplayArchive::open(archive_path, true)
        .await
        .expect("the T2 resume archive opens");
    BootstrapConfig {
        replay: Some(replay),
        replay_archive: Some(Arc::new(archive)),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_suppresses_the_committed_prefix_and_executes_one_live_tail_occurrence() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let recorded_outputs = Arc::new(Mutex::new(Vec::new()));
    let recorded_counters = AuthorityCounters::default();
    run_resumable_until(
        resumable_flow(
            journal_base.clone(),
            1,
            1,
            recorded_outputs.clone(),
            successful_authority(recorded_counters.clone()),
        ),
        &recorded_outputs,
        1,
    )
    .await;
    assert_eq!(recorded_counters.chat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(recorded_counters.embedding_calls.load(Ordering::SeqCst), 1);
    let recorded_archive = latest_run_dir(&journal_base);

    let resumed_outputs = Arc::new(Mutex::new(Vec::new()));
    let resumed_counters = AuthorityCounters::default();
    let _bootstrap = install_bootstrap_config(bootstrap_for_resume(recorded_archive).await);
    run_resumable_until(
        resumable_flow(
            journal_base.clone(),
            2,
            1,
            resumed_outputs.clone(),
            successful_authority(resumed_counters.clone()),
        ),
        &resumed_outputs,
        2,
    )
    .await;

    assert_eq!(
        resumed_outputs
            .lock()
            .unwrap()
            .iter()
            .map(|output| output.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(resumed_counters.chat_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(resumed_counters.chat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        resumed_counters
            .embedding_resolutions
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(resumed_counters.embedding_calls.load(Ordering::SeqCst), 1);

    let resumed_archive = latest_run_dir(&journal_base);
    for (stage, effect_type) in [
        ("chat", "obzenflow.ai.chat_completion"),
        ("embedding", "obzenflow.ai.embedding_generation"),
    ] {
        assert_eq!(
            stage_events(&resumed_archive, stage)
                .await
                .iter()
                .filter(|event| successful_effect_record(event, effect_type).is_some())
                .count(),
            2,
            "the resumed journal contains the suppressed prefix and one live tail reply"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn ambiguity_recovery_repeats_only_the_in_doubt_embedding_occurrence() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let (chat_target, embedding_target) = fixture_targets();
    let initial_chat_calls = Arc::new(AtomicUsize::new(0));
    let initial_embedding_calls = Arc::new(AtomicUsize::new(0));
    let physical_success = Arc::new(Notify::new());
    let ports = eager_authority(
        Arc::new(FixtureChatClient {
            target: chat_target,
            calls: initial_chat_calls.clone(),
        }),
        Arc::new(AmbiguousEmbeddingClient {
            target: embedding_target,
            calls: initial_embedding_calls.clone(),
            physical_success: physical_success.clone(),
        }),
    );

    let live_task = tokio::spawn({
        let journal_base = journal_base.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(finite_flow(
                    journal_base,
                    vec![TicketRaised {
                        id: 1,
                        description: "ambiguous embedding".to_string(),
                    }],
                    Arc::new(Mutex::new(Vec::new())),
                    ports,
                    MapperFailure::None,
                    MapperCounters::default(),
                    EmbeddingDimensions::try_from(3).unwrap(),
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), physical_success.notified())
        .await
        .expect("the embedding operation reaches physical success after Start(1)");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(initial_chat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(initial_embedding_calls.load(Ordering::SeqCst), 1);
    assert!(
        !live_task.is_finished(),
        "the port performs no hidden retry"
    );
    live_task.abort();
    let _ = live_task.await;

    let in_doubt_archive = latest_run_dir(&journal_base);
    let in_doubt_events = stage_events(&in_doubt_archive, "embedding").await;
    assert_eq!(
        in_doubt_events
            .iter()
            .filter(|event| EffectAttemptStarted::event_type_matches(&event.event_type()))
            .count(),
        1
    );
    assert!(in_doubt_events.iter().all(|event| successful_effect_record(
        event,
        "obzenflow.ai.embedding_generation"
    )
    .is_none()));
    assert!(
        in_doubt_events
            .iter()
            .all(|event| event.event_type() != EFFECT_RECORD_EVENT_TYPE),
        "the process cut leaves durable attempt evidence but no terminal reply"
    );

    let resumed_outputs = Arc::new(Mutex::new(Vec::new()));
    let resumed_counters = AuthorityCounters::default();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            in_doubt_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(finite_flow(
            journal_base.clone(),
            vec![TicketRaised {
                id: 1,
                description: "ambiguous embedding".to_string(),
            }],
            resumed_outputs.clone(),
            successful_authority(resumed_counters.clone()),
            MapperFailure::None,
            MapperCounters::default(),
            EmbeddingDimensions::try_from(3).unwrap(),
        ))
        .await
        .expect("the resilience boundary authorises one recovery attempt");

    assert_eq!(resumed_counters.chat_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(resumed_counters.chat_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        resumed_counters
            .embedding_resolutions
            .load(Ordering::SeqCst),
        1
    );
    assert_eq!(resumed_counters.embedding_calls.load(Ordering::SeqCst), 1);
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
            .filter(
                |event| successful_effect_record(event, "obzenflow.ai.embedding_generation")
                    .is_some()
            )
            .count(),
        1
    );
}

#[tokio::test]
async fn recorded_provider_failure_replays_with_the_same_public_classification() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let (chat_target, embedding_target) = fixture_targets();
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_ports = eager_authority(
        Arc::new(FailingChatClient {
            target: chat_target,
            calls: live_calls.clone(),
        }),
        Arc::new(FixtureEmbeddingClient {
            target: embedding_target,
            calls: Arc::new(AtomicUsize::new(0)),
        }),
    );

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(finite_flow(
            journal_base.clone(),
            vec![TicketRaised {
                id: 1,
                description: "provider failure".to_string(),
            }],
            Arc::new(Mutex::new(Vec::new())),
            live_ports,
            MapperFailure::None,
            MapperCounters::default(),
            EmbeddingDimensions::try_from(3).unwrap(),
        ))
        .await
        .expect("an ordinary remote handler failure is journalled and routed");
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);

    let live_archive = latest_run_dir(&journal_base);
    let live_data = stage_events(&live_archive, "chat").await;
    let live_failures = live_data
        .iter()
        .filter_map(|event| failed_effect_record(event, "obzenflow.ai.chat_completion"))
        .collect::<Vec<_>>();
    assert_eq!(live_failures.len(), 1);
    let live_errors = error_projection(&stage_error_events(&live_archive, "chat").await);
    assert_eq!(live_errors.len(), 1);
    assert_eq!(live_errors[0].0, Some(ErrorKind::Remote));
    assert!(live_errors[0].1.contains("fixture provider unavailable"));

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(finite_flow(
            journal_base.clone(),
            vec![TicketRaised {
                id: 1,
                description: "provider failure".to_string(),
            }],
            Arc::new(Mutex::new(Vec::new())),
            empty_authority(),
            MapperFailure::None,
            MapperCounters::default(),
            EmbeddingDimensions::try_from(3).unwrap(),
        ))
        .await
        .expect("strict replay rehydrates the recorded provider failure without a port");

    let replay_archive = latest_run_dir(&journal_base);
    let replay_data = stage_events(&replay_archive, "chat").await;
    let replay_failures = replay_data
        .iter()
        .filter_map(|event| failed_effect_record(event, "obzenflow.ai.chat_completion"))
        .collect::<Vec<_>>();
    assert_eq!(replay_failures.len(), 1);
    assert_eq!(replay_failures[0].outcome, live_failures[0].outcome);
    assert_eq!(
        error_projection(&stage_error_events(&replay_archive, "chat").await),
        live_errors
    );
}

#[tokio::test]
async fn deterministic_mapper_failures_round_trip_on_their_distinct_error_routes() {
    for (failure, expected_kind, expected_chat_calls, expected_records) in [
        (MapperFailure::BeforeEffect, ErrorKind::Validation, 0, 0),
        (MapperFailure::AfterEffect, ErrorKind::Domain, 1, 1),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let journal_base = temp.path().join("journals");
        let live_counters = AuthorityCounters::default();
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(finite_flow(
                journal_base.clone(),
                vec![TicketRaised {
                    id: 1,
                    description: "deterministic mapper failure".to_string(),
                }],
                Arc::new(Mutex::new(Vec::new())),
                successful_authority(live_counters.clone()),
                failure,
                MapperCounters::default(),
                EmbeddingDimensions::try_from(3).unwrap(),
            ))
            .await
            .expect("deterministic mapper failures are journalled and routed");
        assert_eq!(
            live_counters.chat_calls.load(Ordering::SeqCst),
            expected_chat_calls
        );
        assert_eq!(live_counters.embedding_calls.load(Ordering::SeqCst), 0);
        let live_archive = latest_run_dir(&journal_base);
        let live_errors = stage_processing_errors(&live_archive, "chat").await;
        assert_eq!(live_errors.len(), 1);
        assert_eq!(live_errors[0].0, Some(expected_kind));
        assert_eq!(
            stage_events(&live_archive, "chat")
                .await
                .iter()
                .filter(
                    |event| successful_effect_record(event, "obzenflow.ai.chat_completion")
                        .is_some()
                )
                .count(),
            expected_records
        );

        FlowApplication::builder()
            .with_cli_args(vec![
                OsString::from("obzenflow"),
                OsString::from("--replay-from"),
                live_archive.as_os_str().to_os_string(),
            ])
            .run_async(finite_flow(
                journal_base.clone(),
                vec![TicketRaised {
                    id: 1,
                    description: "deterministic mapper failure".to_string(),
                }],
                Arc::new(Mutex::new(Vec::new())),
                empty_authority(),
                failure,
                MapperCounters::default(),
                EmbeddingDimensions::try_from(3).unwrap(),
            ))
            .await
            .expect("strict replay reproduces the deterministic mapper failure");
        assert_eq!(
            stage_processing_errors(&latest_run_dir(&journal_base), "chat").await,
            live_errors
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn held_provider_serialises_data_and_keeps_eof_out_of_mappers() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let (chat_target, embedding_target) = fixture_targets();
    let chat_calls = Arc::new(AtomicUsize::new(0));
    let embedding_calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let mapper_counters = MapperCounters::default();
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let ports = eager_authority(
        Arc::new(GatedChatClient {
            target: chat_target,
            calls: chat_calls.clone(),
            release: release.clone(),
        }),
        Arc::new(FixtureEmbeddingClient {
            target: embedding_target,
            calls: embedding_calls.clone(),
        }),
    );

    let run = tokio::spawn({
        let journal_base = journal_base.clone();
        let outputs = outputs.clone();
        let mapper_counters = mapper_counters.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(control_interleaving_flow(
                    journal_base,
                    outputs,
                    ports,
                    mapper_counters,
                ))
                .await
        }
    });

    wait_for_atomic(&chat_calls, 1, "the first held chat call").await;
    let active_archive = latest_run_dir(&journal_base);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let input_events = stage_events(&active_archive, "input").await;
            let has_eof = input_events.iter().any(|event| {
                matches!(&event.content, ChainEventContent::FlowControl(obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::Eof { .. }))
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
    assert_eq!(chat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(embedding_calls.load(Ordering::SeqCst), 0);
    assert_eq!(mapper_counters.request.load(Ordering::SeqCst), 1);
    assert_eq!(mapper_counters.response.load(Ordering::SeqCst), 0);
    assert!(outputs.lock().unwrap().is_empty());

    release.add_permits(1);
    wait_for_atomic(&chat_calls, 2, "serial admission of the second chat call").await;
    wait_for_atomic(&embedding_calls, 1, "the first embedding call").await;
    assert!(!run.is_finished());
    assert_eq!(mapper_counters.request.load(Ordering::SeqCst), 2);
    assert_eq!(mapper_counters.response.load(Ordering::SeqCst), 1);

    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(10), run)
        .await
        .expect("the released flow drains")
        .expect("the flow task joins")
        .expect("the flow completes");
    assert_eq!(chat_calls.load(Ordering::SeqCst), 2);
    assert_eq!(embedding_calls.load(Ordering::SeqCst), 2);
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
            TicketSummarised::event_type_matches(&event.event_type()).then_some(position)
        })
        .collect::<Vec<_>>();
    let chat_writer = chat_events
        .iter()
        .find(|event| TicketSummarised::event_type_matches(&event.event_type()))
        .map(|event| event.writer_id)
        .expect("chat authors typed output");
    let eof_position = chat_events
        .iter()
        .rposition(|event| {
            event.writer_id == chat_writer
                && matches!(&event.content, ChainEventContent::FlowControl(obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::Eof { .. }))
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
    let (chat_target, embedding_target) = fixture_targets();
    let wide_calls = Arc::new(AtomicUsize::new(0));
    let ports = eager_authority(
        Arc::new(FixtureChatClient {
            target: chat_target,
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        Arc::new(WideEmbeddingClient {
            target: embedding_target,
            calls: wide_calls.clone(),
        }),
    );
    let dimensions = EmbeddingDimensions::try_from(MAX_MEASURED_EMBEDDING_DIMENSIONS).unwrap();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(finite_flow(
            journal_base.clone(),
            vec![TicketRaised {
                id: 1,
                description: "maximum measured embedding".to_string(),
            }],
            Arc::new(Mutex::new(Vec::new())),
            ports,
            MapperFailure::None,
            MapperCounters::default(),
            dimensions,
        ))
        .await
        .expect("the maximum release measurement fixture completes");
    assert_eq!(wide_calls.load(Ordering::SeqCst), 1);

    let events = stage_events(&latest_run_dir(&journal_base), "embedding").await;
    let records = events
        .iter()
        .filter_map(|event| successful_effect_record(event, "obzenflow.ai.embedding_generation"))
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
        .filter(|event| TicketEmbedded::event_type_matches(&event.event_type()))
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
    eprintln!(
        "FLOWIP-128b maximum measured embedding row ({MAX_MEASURED_EMBEDDING_DIMENSIONS} dimensions): effect_reply={reply_bytes} bytes, typed_output={output_bytes} bytes"
    );
    assert!(reply_bytes > MAX_MEASURED_EMBEDDING_DIMENSIONS as usize);
    assert!(output_bytes > MAX_MEASURED_EMBEDDING_DIMENSIONS as usize);
}
