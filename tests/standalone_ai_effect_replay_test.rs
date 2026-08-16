// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128b live/replay witness for the ordinary standalone AI surface.

use async_trait::async_trait;
use obzenflow::ai::{
    ChatCompletion, ChatEffectBinding, ChatResponse, ChatResponseFormat, ChatTransformBuilder,
    EmbeddingDimensions, EmbeddingEffectBinding, EmbeddingGeneration, EmbeddingResponse,
    EmbeddingTransformBuilder,
};
use obzenflow::typed::sources;
use obzenflow_adapters::ai::{CHAT_CLIENT, EMBEDDING_CLIENT};
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_core::ai::{
    AiClientError, ChatClient, ChatRequest, ChatTarget, EmbeddingClient, EmbeddingRequest,
    EmbeddingTarget,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, EffectAttemptStarted, EffectOutcomePayload, EffectRecord,
};
use obzenflow_core::http_client::Url;
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_dsl::dsl::error::FlowBuildError;
use obzenflow_dsl::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::effects::{
    EffectBinding, EffectPortRegistry, EffectPortResolverWithMetadata, EffectRegistrationBuilder,
    LogicalEffectBindingName, ResolvedEffectPort, SinkRedeliverySafety, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TicketRaised {
    id: u64,
    description: String,
}

impl TypedPayload for TicketRaised {
    const EVENT_TYPE: &'static str = "flowip_128b.ticket_raised";
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct TicketSummarised {
    id: u64,
    summary: String,
}

impl TypedPayload for TicketSummarised {
    const EVENT_TYPE: &'static str = "flowip_128b.ticket_summarised";
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TicketEmbedded {
    id: u64,
    vectors: Vec<Vec<f32>>,
}

impl TypedPayload for TicketEmbedded {
    const EVENT_TYPE: &'static str = "flowip_128b.ticket_embedded";
}

#[derive(Clone, Default)]
struct AuthorityCounters {
    chat_resolutions: Arc<AtomicUsize>,
    chat_calls: Arc<AtomicUsize>,
    embedding_resolutions: Arc<AtomicUsize>,
    embedding_calls: Arc<AtomicUsize>,
}

#[derive(Debug)]
struct FixtureChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ChatClient for FixtureChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.target().logically_matches(&self.target));
        Ok(ChatResponse {
            text: "concise summary".to_string(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

#[derive(Debug)]
struct FixtureEmbeddingClient {
    target: EmbeddingTarget,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EmbeddingClient for FixtureEmbeddingClient {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(request.logically_targets(&self.target));
        let dimensions = EmbeddingDimensions::try_from(3).unwrap();
        Ok(EmbeddingResponse {
            vectors: request.inputs.iter().map(|_| vec![0.1, 0.2, 0.3]).collect(),
            vector_dim: dimensions,
            usage: None,
        })
    }
}

#[derive(Clone, Debug)]
struct CollectEmbedded {
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
}

#[async_trait]
impl InlineSink for CollectEmbedded {
    type Input = TicketEmbedded;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified().with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(
        &mut self,
        output: TicketEmbedded,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.outputs.lock().expect("output lock").push(output);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("CollectEmbedded".to_string()),
            Some(1),
        )))
    }
}

fn binding_error(binding: &str, error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::BindingConfiguration {
        binding: binding.to_string(),
        detail: error.to_string(),
    }
}

#[derive(Clone)]
struct AiAuthority {
    chat: EffectBinding<ChatCompletion>,
    embedding: EffectBinding<EmbeddingGeneration>,
    effect_ports: EffectPortRegistry,
}

// Keep fixture materialisation on FlowDefinition's concrete build-error API.
#[allow(clippy::result_large_err)]
fn base_bindings(
    chat_endpoint: Option<Url>,
) -> Result<
    (
        EffectBinding<ChatCompletion>,
        EffectBinding<EmbeddingGeneration>,
    ),
    FlowBuildError,
> {
    let (chat, _) = ChatEffectBinding::ollama("fixture-chat", chat_endpoint)
        .map_err(|error| binding_error("chat", error))?
        .into_parts()
        .map_err(|error| binding_error("chat", error))?;
    let (embedding, _) = EmbeddingEffectBinding::ollama("fixture-embedding", None)
        .map_err(|error| binding_error("embedding", error))?
        .into_parts()
        .map_err(|error| binding_error("embedding", error))?;
    Ok((chat, embedding))
}

// Keep fixture materialisation on FlowDefinition's concrete build-error API.
#[allow(clippy::result_large_err)]
fn live_authority(
    chat_endpoint: Option<Url>,
    counters: Option<AuthorityCounters>,
) -> Result<AiAuthority, FlowBuildError> {
    let (chat_seed, embedding_seed) = base_bindings(chat_endpoint)?;
    let Some(counters) = counters else {
        return Ok(AiAuthority {
            chat: chat_seed,
            embedding: embedding_seed,
            effect_ports: EffectPortRegistry::new(),
        });
    };
    let chat_target = chat_seed.evidence().target().clone();
    let embedding_target = embedding_seed.evidence().target().clone();
    let chat_resolver: EffectPortResolverWithMetadata<dyn ChatClient, ChatTarget> = Arc::new({
        let resolutions = counters.chat_resolutions.clone();
        let calls = counters.chat_calls.clone();
        move || {
            let target = chat_target.clone();
            resolutions.fetch_add(1, Ordering::SeqCst);
            let client = Arc::new(FixtureChatClient {
                target,
                calls: calls.clone(),
            }) as Arc<dyn ChatClient>;
            let metadata = Arc::new(client.target().clone());
            Ok(ResolvedEffectPort::new(client, metadata))
        }
    });
    let embedding_resolver: EffectPortResolverWithMetadata<dyn EmbeddingClient, EmbeddingTarget> =
        Arc::new({
            let resolutions = counters.embedding_resolutions.clone();
            let calls = counters.embedding_calls.clone();
            move || {
                let target = embedding_target.clone();
                resolutions.fetch_add(1, Ordering::SeqCst);
                let client = Arc::new(FixtureEmbeddingClient {
                    target,
                    calls: calls.clone(),
                }) as Arc<dyn EmbeddingClient>;
                let metadata = Arc::new(client.target().clone());
                Ok(ResolvedEffectPort::new(client, metadata))
            }
        });
    let (chat, chat_registration) = EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat").expect("valid fixture binding name"),
        chat_seed.evidence().clone(),
    )
    .bind_deferred_with_metadata(CHAT_CLIENT, chat_resolver)
    .and_then(|builder| builder.finish())
    .map_err(|error| binding_error("chat", error))?;
    let (embedding, embedding_registration) =
        EffectRegistrationBuilder::<EmbeddingGeneration>::new(
            LogicalEffectBindingName::new("embedding").expect("valid fixture binding name"),
            embedding_seed.evidence().clone(),
        )
        .bind_deferred_with_metadata(EMBEDDING_CLIENT, embedding_resolver)
        .and_then(|builder| builder.finish())
        .map_err(|error| binding_error("embedding", error))?;
    let mut effect_ports = EffectPortRegistry::new();
    effect_ports
        .install(chat_registration)
        .map_err(|error| binding_error("chat", error))?;
    effect_ports
        .install(embedding_registration)
        .map_err(|error| binding_error("embedding", error))?;
    Ok(AiAuthority {
        chat,
        embedding,
        effect_ports,
    })
}

fn eager_authority(chat: Arc<dyn ChatClient>, embedding: Arc<dyn EmbeddingClient>) -> AiAuthority {
    let (chat_seed, embedding_seed) = base_bindings(None).expect("fixture evidence");
    let (chat_binding, chat_registration) = EffectRegistrationBuilder::<ChatCompletion>::new(
        LogicalEffectBindingName::new("chat").unwrap(),
        chat_seed.evidence().clone(),
    )
    .bind_eager_with_metadata(
        CHAT_CLIENT,
        ResolvedEffectPort::new(chat.clone(), Arc::new(chat.target().clone())),
    )
    .and_then(|builder| builder.finish())
    .unwrap();
    let (embedding_binding, embedding_registration) =
        EffectRegistrationBuilder::<EmbeddingGeneration>::new(
            LogicalEffectBindingName::new("embedding").unwrap(),
            embedding_seed.evidence().clone(),
        )
        .bind_eager_with_metadata(
            EMBEDDING_CLIENT,
            ResolvedEffectPort::new(embedding.clone(), Arc::new(embedding.target().clone())),
        )
        .and_then(|builder| builder.finish())
        .unwrap();
    let mut effect_ports = EffectPortRegistry::new();
    effect_ports.install(chat_registration).unwrap();
    effect_ports.install(embedding_registration).unwrap();
    AiAuthority {
        chat: chat_binding,
        embedding: embedding_binding,
        effect_ports,
    }
}

struct FlowScenario {
    live_authority: Option<AuthorityCounters>,
    prompt_suffix: String,
    chat_logic_version: String,
    chat_endpoint: Option<Url>,
    json_response: bool,
    embedding_dimensions: EmbeddingDimensions,
}

impl FlowScenario {
    fn baseline(live_authority: Option<AuthorityCounters>) -> Self {
        Self {
            live_authority,
            prompt_suffix: String::new(),
            chat_logic_version: "ticket-summary-v1".to_string(),
            chat_endpoint: None,
            json_response: false,
            embedding_dimensions: EmbeddingDimensions::try_from(3).unwrap(),
        }
    }
}

fn build_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
    scenario: FlowScenario,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let authority = live_authority(
            scenario.chat_endpoint.clone(),
            scenario.live_authority.clone(),
        )?;
        let chat = authority.chat;
        let embedding = authority.embedding;
        let effect_ports = authority.effect_ports;

        let suffix = scenario.prompt_suffix.clone();
        let chat_builder = ChatTransformBuilder::from_binding(chat.clone())
            .logic_version(scenario.chat_logic_version.clone())
            .system("Summarise support tickets concisely.")
            .temperature(0.2);
        let chat_builder = if scenario.json_response {
            chat_builder.response_format(ChatResponseFormat::JsonObject)
        } else {
            chat_builder
        };
        let chat_handler = chat_builder
            .build_typed::<TicketRaised, TicketSummarised>(
                move |ticket| Ok(format!("{}{}", ticket.description, suffix)),
                |ticket, response| {
                    Ok(TicketSummarised {
                        id: ticket.id,
                        summary: response.text,
                    })
                },
            )
            .map_err(|error| binding_error("chat_handler", error))?;
        let embedding_handler = EmbeddingTransformBuilder::from_binding(embedding.clone())
            .logic_version("ticket-embedding-v1")
            .dimensions(scenario.embedding_dimensions)
            .build_typed::<TicketSummarised, TicketEmbedded>(
                |ticket| Ok(vec![ticket.summary.clone()]),
                |ticket, response| {
                    Ok(TicketEmbedded {
                        id: ticket.id,
                        vectors: response.vectors,
                    })
                },
            )
            .map_err(|error| binding_error("embedding_handler", error))?;
        let input = sources::finite([TicketRaised {
            id: 7,
            description: "Customer cannot sign in".to_string(),
        }]);
        let collected = CollectEmbedded { outputs };

        Ok(flow! {
            name: "standalone_ai_effect_replay",
            journals: disk_journals(journal_base),
            effect_ports,

            stages: {
                input = source!(TicketRaised => input);
                chat = effectful_transform!(
                    TicketRaised ->{ at_least_once(ChatCompletion) via chat with ai_resilience() } TicketSummarised => chat_handler,
                    observers: [],
                );
                embedding = effectful_transform!(
                    TicketSummarised ->{ at_least_once(EmbeddingGeneration) via embedding with ai_resilience() } TicketEmbedded => embedding_handler,
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

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut entries = std::fs::read_dir(base.join("flows"))
        .expect("flow archives")
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    entries.sort();
    entries.pop().expect("completed flow archive")
}

fn mark_archive_incomplete(run_dir: &Path) {
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(run_dir.join("run_manifest.json")).unwrap())
            .unwrap();
    let system_journal = manifest["system_journal_file"].as_str().unwrap();
    std::fs::write(run_dir.join(system_journal), "").unwrap();
}

async fn stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(run_dir.join("run_manifest.json")).unwrap())
            .unwrap();
    let relative = manifest["stages"][stage_key]["data_journal_file"]
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

fn successful_effect_record(event: &ChainEvent, effect_type: &str) -> Option<EffectRecord> {
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
        && matches!(record.outcome, EffectOutcomePayload::Succeeded { .. }))
    .then_some(record)
}

fn count_json_key(value: &serde_json::Value, key: &str) -> usize {
    match value {
        serde_json::Value::Object(fields) => {
            usize::from(fields.contains_key(key))
                + fields
                    .values()
                    .map(|value| count_json_key(value, key))
                    .sum::<usize>()
        }
        serde_json::Value::Array(values) => {
            values.iter().map(|value| count_json_key(value, key)).sum()
        }
        _ => 0,
    }
}

#[tokio::test]
async fn live_and_strict_replay_record_replies_and_replay_with_zero_authority() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let counters = AuthorityCounters::default();
    let live_outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_outputs.clone(),
            FlowScenario::baseline(Some(counters.clone())),
        ))
        .await
        .expect("live standalone AI flow succeeds");

    let expected = vec![TicketEmbedded {
        id: 7,
        vectors: vec![vec![0.1, 0.2, 0.3]],
    }];
    assert_eq!(*live_outputs.lock().unwrap(), expected);
    assert_eq!(counters.chat_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(counters.chat_calls.load(Ordering::SeqCst), 1);
    assert_eq!(counters.embedding_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(counters.embedding_calls.load(Ordering::SeqCst), 1);

    let live_archive = latest_run_dir(&journal_base);
    for (stage, effect_type, label, schema, output_type) in [
        (
            "chat",
            "obzenflow.ai.chat_completion",
            "standalone.chat_completion",
            3,
            TicketSummarised::versioned_event_type(),
        ),
        (
            "embedding",
            "obzenflow.ai.embedding_generation",
            "standalone.embedding_generation",
            1,
            TicketEmbedded::versioned_event_type(),
        ),
    ] {
        let events = stage_events(&live_archive, stage).await;
        let attempt_positions = events
            .iter()
            .enumerate()
            .filter_map(|(position, event)| {
                EffectAttemptStarted::event_type_matches(&event.event_type()).then_some(position)
            })
            .collect::<Vec<_>>();
        assert_eq!(attempt_positions.len(), 1);
        let records = events
            .iter()
            .enumerate()
            .filter_map(|(position, event)| {
                successful_effect_record(event, effect_type).map(|record| (position, record))
            })
            .collect::<Vec<_>>();
        let [(record_position, record)] = records.as_slice() else {
            panic!("{stage} must contain one recorded reply");
        };
        let output_positions = events
            .iter()
            .enumerate()
            .filter_map(|(position, event)| (event.event_type() == output_type).then_some(position))
            .collect::<Vec<_>>();
        assert_eq!(output_positions.len(), 1);
        assert!(attempt_positions[0] < *record_position);
        assert!(*record_position < output_positions[0]);
        assert_eq!(record.descriptor.label.as_str(), label);
        assert_eq!(record.descriptor.schema_version, schema);
        if effect_type == "obzenflow.ai.embedding_generation" {
            let EffectOutcomePayload::Succeeded { output } = &record.outcome else {
                unreachable!()
            };
            assert!(output.pointer("/response/raw").is_none());
            assert_eq!(
                count_json_key(output, "vectors"),
                1,
                "the recorded reply must contain one normalised vector copy"
            );
            assert_eq!(
                output.pointer("/response/vector_dim"),
                Some(&serde_json::json!(3))
            );
            eprintln!(
                "FLOWIP-128b representative embedding rows: effect_reply={} bytes, typed_output={} bytes",
                serde_json::to_vec(record).unwrap().len(),
                serde_json::to_vec(&events[output_positions[0]]).unwrap().len(),
            );
        }
    }

    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            replay_outputs.clone(),
            FlowScenario::baseline(None),
        ))
        .await
        .expect("strict replay succeeds with an empty effect registry");
    assert_eq!(*replay_outputs.lock().unwrap(), expected);

    mark_archive_incomplete(&live_archive);
    let resume_counters = AuthorityCounters::default();
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            live_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            journal_base,
            resume_outputs.clone(),
            FlowScenario::baseline(Some(resume_counters.clone())),
        ))
        .await
        .expect("committed resume prefix completes without live AI authority");
    assert_eq!(*resume_outputs.lock().unwrap(), expected);
    assert_eq!(resume_counters.chat_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(resume_counters.chat_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        resume_counters.embedding_resolutions.load(Ordering::SeqCst),
        0
    );
    assert_eq!(resume_counters.embedding_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn descriptor_drift_fails_before_live_authority_is_resolved() {
    let alternate_endpoint = Url::parse("http://127.0.0.1:11435").unwrap();
    for (suffix, logic_version, endpoint, json_response, dimensions) in [
        (
            " changed",
            "ticket-summary-v1",
            None,
            false,
            EmbeddingDimensions::try_from(3).unwrap(),
        ),
        (
            "",
            "ticket-summary-v2",
            None,
            false,
            EmbeddingDimensions::try_from(3).unwrap(),
        ),
        (
            "",
            "ticket-summary-v1",
            Some(alternate_endpoint),
            false,
            EmbeddingDimensions::try_from(3).unwrap(),
        ),
        (
            "",
            "ticket-summary-v1",
            None,
            true,
            EmbeddingDimensions::try_from(3).unwrap(),
        ),
        (
            "",
            "ticket-summary-v1",
            None,
            false,
            EmbeddingDimensions::try_from(2).unwrap(),
        ),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let journal_base = temp.path().join("journals");
        let live_counters = AuthorityCounters::default();
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_flow(
                journal_base.clone(),
                Arc::new(Mutex::new(Vec::new())),
                FlowScenario::baseline(Some(live_counters)),
            ))
            .await
            .unwrap();
        let archive = latest_run_dir(&journal_base);
        let replay_counters = AuthorityCounters::default();
        let result = FlowApplication::builder()
            .with_cli_args(vec![
                OsString::from("obzenflow"),
                OsString::from("--replay-from"),
                archive.as_os_str().to_os_string(),
            ])
            .run_async(build_flow(
                journal_base,
                Arc::new(Mutex::new(Vec::new())),
                FlowScenario {
                    live_authority: Some(replay_counters.clone()),
                    prompt_suffix: suffix.to_string(),
                    chat_logic_version: logic_version.to_string(),
                    chat_endpoint: endpoint,
                    json_response,
                    embedding_dimensions: dimensions,
                },
            ))
            .await;
        assert!(result.is_err(), "descriptor drift must fail strict replay");
        assert_eq!(replay_counters.chat_resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(replay_counters.chat_calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            replay_counters.embedding_resolutions.load(Ordering::SeqCst),
            0
        );
        assert_eq!(replay_counters.embedding_calls.load(Ordering::SeqCst), 0);
    }
}

#[path = "support/standalone_ai_gap_proofs.rs"]
mod gap_proofs;
