// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit;

#[path = "support/fake_ai.rs"]
pub mod fake_ai;

use fake_ai::{FakeChatModels, FakeEmbeddingModels};
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec,
    ChatResponseFormat, Chunk, EmbeddingDimensions, EmbeddingGeneration, EmbeddingGenerationReply,
    EmbeddingParams, EmbeddingRequestSpec, Partials,
};
use obzenflow::stages::sources;
use obzenflow_adapters::middleware::{circuit_breaker, MiddlewareFactory};
use obzenflow_core::ai::AiClientError;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{
    ChainEvent, ChainPayload, EffectAttemptStarted, EffectOutcomePayload, EffectRecord,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{SinkRedeliverySafety, EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const CHAT_EFFECT_TYPE: &str = "obzenflow.ai.chat_completion";
const EMBEDDING_EFFECT_TYPE: &str = "obzenflow.ai.embedding_generation";

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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum MapperFailure {
    #[default]
    None,
    BeforeEffect,
    AfterEffect,
    EmptyEmbeddingInputs,
}

#[derive(Clone, Debug, Default)]
struct MapperCounters {
    request: Arc<AtomicUsize>,
    response: Arc<AtomicUsize>,
}

#[derive(Clone, Debug)]
struct SummarizeTicket {
    logic_version: String,
    prompt_suffix: String,
    json_response: bool,
    failure: MapperFailure,
    counters: MapperCounters,
}

impl SummarizeTicket {
    fn baseline() -> Self {
        Self {
            logic_version: "ticket-summary-v1".to_string(),
            prompt_suffix: String::new(),
            json_response: false,
            failure: MapperFailure::None,
            counters: MapperCounters::default(),
        }
    }
}

impl AiRole<TicketRaised> for SummarizeTicket {
    type Effect = ChatCompletion;
    type Out = TicketSummarised;

    fn stage_logic_version(&self) -> &str {
        &self.logic_version
    }

    fn prepare(&self, ticket: &TicketRaised) -> Result<ChatRequestSpec, HandlerError> {
        self.counters.request.fetch_add(1, Ordering::SeqCst);
        if self.failure == MapperFailure::BeforeEffect {
            return Err(HandlerError::Validation(
                "fixture request mapping failed".to_string(),
            ));
        }
        let mut request = ChatRequestSpec::new(
            "Summarize support tickets concisely.",
            format!("{}{}", ticket.description, self.prompt_suffix),
        )
        .temperature(0.2);
        if self.json_response {
            request.response_format = Some(ChatResponseFormat::JsonObject);
        }
        Ok(request)
    }

    fn interpret(
        &self,
        ticket: TicketRaised,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<TicketSummarised, HandlerError> {
        self.counters.response.fetch_add(1, Ordering::SeqCst);
        let reply = outcome.replied()?;
        if self.failure == MapperFailure::AfterEffect {
            return Err(HandlerError::Domain(
                "fixture response mapping failed".to_string(),
            ));
        }
        Ok(TicketSummarised {
            id: ticket.id,
            summary: reply.text().to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct EmbedSummary {
    dimensions: EmbeddingDimensions,
    empty_inputs: bool,
}

impl EmbedSummary {
    fn of_width(width: u32) -> Self {
        Self {
            dimensions: EmbeddingDimensions::try_from(width).unwrap(),
            empty_inputs: false,
        }
    }
}

impl AiRole<TicketSummarised> for EmbedSummary {
    type Effect = EmbeddingGeneration;
    type Out = TicketEmbedded;

    fn stage_logic_version(&self) -> &str {
        "ticket-embedding-v1"
    }

    fn prepare(&self, ticket: &TicketSummarised) -> Result<EmbeddingRequestSpec, HandlerError> {
        Ok(EmbeddingRequestSpec {
            inputs: if self.empty_inputs {
                Vec::new()
            } else {
                vec![ticket.summary.clone()]
            },
            params: EmbeddingParams {
                dimensions: Some(self.dimensions),
            },
        })
    }

    fn interpret(
        &self,
        ticket: TicketSummarised,
        outcome: AiOutcome<EmbeddingGenerationReply>,
    ) -> Result<TicketEmbedded, HandlerError> {
        let reply = outcome.replied()?;
        Ok(TicketEmbedded {
            id: ticket.id,
            vectors: reply.response.vectors,
        })
    }
}

#[derive(Clone, Debug)]
struct CollectEmbedded {
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
}

#[async_trait::async_trait]
impl obzenflow_runtime::stages::sink::SinkConnector for CollectEmbedded {
    type Input = TicketEmbedded;
    type Writer = Self;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("CollectEmbedded".to_string()))
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn open(
        &self,
        _context: obzenflow_runtime::stages::sink::SinkWriterInitContext,
    ) -> obzenflow_runtime::stages::sink::SinkOperationResult<Self::Writer> {
        Ok(self.clone())
    }
}

#[async_trait::async_trait]
impl obzenflow_runtime::stages::sink::SinkWriter for CollectEmbedded {
    type Input = TicketEmbedded;

    async fn write(
        &mut self,
        output: TicketEmbedded,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.outputs.lock().expect("output lock").push(output);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("CollectEmbedded".to_string()),
            Some(1),
        )))
    }
}

fn summarizing() -> FakeChatModels {
    FakeChatModels::replying(|_| "concise summary".to_string())
}

fn embedding() -> FakeEmbeddingModels {
    FakeEmbeddingModels::constant(vec![0.1, 0.2, 0.3])
}

fn ai_config(dir: &Path, chat_endpoint: Option<&str>) -> PathBuf {
    let path = dir.join("standalone_ai.toml");
    let endpoint = match chat_endpoint {
        Some(url) => format!(
            "[ai.endpoints.alternate]\nprovider = \"ollama\"\nbase_url = \"{url}\"\n\n\
             [ai.models]\nendpoint = \"alternate\"\nmodel = \"fixture-chat\"\n"
        ),
        None => "[ai.models]\nmodel = \"fixture-chat\"\n".to_string(),
    };
    std::fs::write(
        &path,
        format!("{endpoint}\n[ai.embeddings]\nmodel = \"fixture-embedding\"\n"),
    )
    .expect("write the AI configuration");
    path
}

fn chat_and_embedding_flow(
    journal_base: PathBuf,
    tickets: Vec<TicketRaised>,
    outputs: Arc<Mutex<Vec<TicketEmbedded>>>,
    summarize: SummarizeTicket,
    embed: EmbedSummary,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_| {
        let summarize = ai::infer(summarize);
        let embed = ai::infer(embed);
        let chat_breaker = circuit_breaker().consecutive_failures(5);
        let embedding_breaker = circuit_breaker().consecutive_failures(5);
        let input = sources::ValuesSource::new(tickets);
        let collected = CollectEmbedded { outputs };

        Ok(flow! {
            name: "standalone_ai_effect_replay",
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

fn one_ticket() -> Vec<TicketRaised> {
    vec![TicketRaised {
        id: 7,
        description: "Customer cannot sign in".to_string(),
    }]
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
    obzenflow_infra::testing::journal::retain_archive_frames(run_dir, |path, _| {
        path.file_name().and_then(|name| name.to_str()) != Some(system_journal)
    })
    .expect("incomplete fixture must retain readable stage journals");
}

async fn stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(run_dir.join("run_manifest.json")).unwrap())
            .unwrap();
    let relative = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap();
    replay_testkit::read_journal_envelopes::<ChainEvent>(&run_dir.join(relative))
        .await
        .into_iter()
        .map(|envelope| envelope.authored())
        .collect()
}

fn successful_effect_record(event: &ChainEvent, effect_type: &str) -> Option<EffectRecord> {
    let ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) = &event.payload else {
        return None;
    };
    let record = record.clone();
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

fn replay_args(archive: &Path, verb: &str) -> Vec<OsString> {
    vec![
        OsString::from("obzenflow"),
        OsString::from(verb),
        archive.as_os_str().to_os_string(),
    ]
}

#[derive(Clone, Copy)]
struct SummarizeTickets;

impl AiRole<Chunk<TicketRaised>> for SummarizeTickets {
    type Effect = ChatCompletion;
    type Out = TicketSummarised;

    fn prepare(&self, chunk: &Chunk<TicketRaised>) -> Result<ChatRequestSpec, HandlerError> {
        let descriptions = chunk
            .iter()
            .map(|ticket| ticket.description.as_str())
            .collect::<Vec<_>>();
        Ok(ChatRequestSpec::new(
            "Summarize support tickets concisely.",
            descriptions.join("\n"),
        ))
    }

    fn interpret(
        &self,
        chunk: Chunk<TicketRaised>,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<TicketSummarised, HandlerError> {
        let id = chunk.iter().map(|ticket| ticket.id).sum();
        if outcome.unavailable().is_some() {
            return Ok(TicketSummarised {
                id,
                summary: "summary unavailable".to_string(),
            });
        }
        Ok(TicketSummarised {
            id,
            summary: outcome.replied()?.text().to_string(),
        })
    }
}

#[derive(Clone, Copy)]
struct EmbedSummaries;

impl AiRole<Partials<TicketRaised, TicketSummarised>> for EmbedSummaries {
    type Effect = EmbeddingGeneration;
    type Out = TicketEmbedded;

    fn prepare(
        &self,
        input: &Partials<TicketRaised, TicketSummarised>,
    ) -> Result<EmbeddingRequestSpec, HandlerError> {
        Ok(EmbeddingRequestSpec {
            inputs: input
                .partials()
                .iter()
                .map(|partial| partial.summary.clone())
                .collect(),
            params: EmbeddingParams {
                dimensions: Some(EmbeddingDimensions::try_from(3).unwrap()),
            },
        })
    }

    fn interpret(
        &self,
        input: Partials<TicketRaised, TicketSummarised>,
        outcome: AiOutcome<EmbeddingGenerationReply>,
    ) -> Result<TicketEmbedded, HandlerError> {
        Ok(TicketEmbedded {
            id: input.partials().iter().map(|partial| partial.id).sum(),
            vectors: outcome.replied()?.response.vectors,
        })
    }
}

fn two_tickets() -> Vec<TicketRaised> {
    vec![
        TicketRaised {
            id: 1,
            description: "Customer cannot sign in".to_string(),
        },
        TicketRaised {
            id: 2,
            description: "Invoice shows the wrong total".to_string(),
        },
    ]
}

#[tokio::test]
async fn a_stage_whose_roles_perform_two_effects_refuses_an_unscoped_breaker() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let (chat, embed) = (summarizing(), embedding());

    let error = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(ai_config(temp.path(), None))
        .with_ai_models(fake_ai::providers(&chat, &embed))
        .run_async(FlowDefinition::materialize(move |_| {
            let tickets = sources::ValuesSource::new(two_tickets());
            let digest = ai::map(SummarizeTickets)
                .chunks(1)
                .reduce(EmbedSummaries)
                .on_eof();
            let ai_breaker = circuit_breaker().consecutive_failures(5);
            let collected = CollectEmbedded {
                outputs: Arc::new(Mutex::new(Vec::new())),
            };

            Ok(flow! {
                name: "two_effect_unscoped",
                journals: disk_journals(journal_base),
                stages: {
                    tickets = source!(TicketRaised => tickets);
                    digest = ai!(TicketRaised -> TicketEmbedded with ai_breaker => digest);
                    collected = sink!(TicketEmbedded => collected);
                },

                topology: {
                    tickets |> digest;
                    digest |> collected;
                }
            })
        }))
        .await
        .expect_err("one breaker cannot guard two effects");
    assert!(
        error.to_string().contains(
            "stage `digest`: control 'circuit_breaker' requires exactly one declared effect; build it with `.for_effect::<E>()` for one of [obzenflow.ai.chat_completion, obzenflow.ai.embedding_generation]"
        ),
        "{error}"
    );
    assert_eq!(chat.connect_count() + embed.connect_count(), 0);
}

#[tokio::test]
async fn a_stage_whose_roles_perform_two_effects_builds_one_breaker_per_effect() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let chat = FakeChatModels::with_reply(|_| {
        Err(AiClientError::Timeout {
            message: "endpoint down".to_string(),
        })
    });
    let embed = embedding();
    let outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(ai_config(temp.path(), None))
        .with_ai_models(fake_ai::providers(&chat, &embed))
        .run_async(FlowDefinition::materialize({
            let outputs = outputs.clone();
            move |_| {
                let tickets = sources::ValuesSource::new(two_tickets());
                let digest = ai::map(SummarizeTickets)
                    .chunks(1)
                    .reduce(EmbedSummaries)
                    .on_eof();
                let chat_breaker = circuit_breaker()
                    .consecutive_failures(1)
                    .open_for(std::time::Duration::from_secs(60))
                    .for_effect::<ChatCompletion>();
                let embedding_breaker = circuit_breaker()
                    .consecutive_failures(1)
                    .open_for(std::time::Duration::from_secs(60))
                    .for_effect::<EmbeddingGeneration>();
                let collected = CollectEmbedded { outputs };

                Ok(flow! {
                    name: "two_effect_scoped",
                    journals: disk_journals(journal_base),
                    stages: {
                        tickets = source!(TicketRaised => tickets);
                        digest = ai!(TicketRaised -> TicketEmbedded with { chat_breaker, embedding_breaker } => digest);
                        collected = sink!(TicketEmbedded => collected);
                    },

                    topology: {
                        tickets |> digest;
                        digest |> collected;
                    }
                })
            }
        }))
        .await
        .expect("each effect has its own breaker");

    assert_eq!(
        chat.call_count(),
        1,
        "the first map failure opens the chat breaker, which rejects the second map call"
    );
    assert_eq!(
        embed.call_count(),
        1,
        "the embedding breaker stays closed, so the reduce call runs"
    );
    assert_eq!(
        *outputs.lock().unwrap(),
        vec![TicketEmbedded {
            id: 3,
            vectors: vec![vec![0.1, 0.2, 0.3], vec![0.1, 0.2, 0.3]],
        }]
    );
}

#[tokio::test]
async fn an_embedding_endpoint_whose_provider_serves_only_chat_fails_the_build() {
    let temp = tempfile::tempdir().unwrap();
    let config = temp.path().join("chat_only.toml");
    std::fs::write(
        &config,
        "[ai.endpoints.chatbox]\nprovider = \"chat_only\"\nbase_url = \"https://chat.example/v1\"\n\n\
         [ai.models]\nmodel = \"fixture-chat\"\n\n\
         [ai.embeddings]\nendpoint = \"chatbox\"\nmodel = \"fixture-embedding\"\n",
    )
    .unwrap();
    let (chat, embed) = (summarizing(), FakeEmbeddingModels::serving_only("ollama"));

    let error = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(config)
        .with_ai_models(fake_ai::providers(&chat, &embed))
        .run_async(chat_and_embedding_flow(
            temp.path().join("journals"),
            one_ticket(),
            Arc::new(Mutex::new(Vec::new())),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect_err("no installed embedding provider serves `chat_only`");
    assert!(
        error.to_string().contains(
            "config: stage `embedding` names endpoint `chatbox`, whose provider `chat_only` cannot serve obzenflow.ai.embedding_generation"
        ),
        "{error}"
    );
    assert_eq!(
        chat.call_count() + embed.connect_count() + embed.call_count(),
        0
    );
}

#[tokio::test]
async fn live_and_strict_replay_record_replies_and_replay_with_zero_authority() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    let config = ai_config(temp.path(), None);
    let (chat, embed) = (summarizing(), embedding());
    let live_outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(config.clone())
        .with_ai_models(fake_ai::providers(&chat, &embed))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            one_ticket(),
            live_outputs.clone(),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("live chat and embedding stages succeed");

    let expected = vec![TicketEmbedded {
        id: 7,
        vectors: vec![vec![0.1, 0.2, 0.3]],
    }];
    assert_eq!(*live_outputs.lock().unwrap(), expected);
    assert_eq!(chat.connect_count(), 1);
    assert_eq!(chat.call_count(), 1);
    assert_eq!(embed.connect_count(), 1);
    assert_eq!(embed.call_count(), 1);

    let live_archive = latest_run_dir(&journal_base);
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(live_archive.join("run_manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        manifest["stages"]["chat"]["ai_models"][0]["model"],
        "fixture-chat"
    );
    assert_eq!(
        manifest["stages"]["embedding"]["ai_models"][0]["model"], "fixture-embedding",
        "embeddings resolve from their own section"
    );
    for (stage, effect_type, schema, output_type) in [
        (
            "chat",
            CHAT_EFFECT_TYPE,
            3,
            TicketSummarised::event_type_name(),
        ),
        (
            "embedding",
            EMBEDDING_EFFECT_TYPE,
            1,
            TicketEmbedded::event_type_name(),
        ),
    ] {
        let events = stage_events(&live_archive, stage).await;
        let attempt_positions = events
            .iter()
            .enumerate()
            .filter_map(|(position, event)| {
                EffectAttemptStarted::matches_event_type(&event.event_type()).then_some(position)
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
        assert_eq!(record.descriptor.label.as_str(), "ai.infer");
        assert_eq!(record.descriptor.schema_version, schema);
        if effect_type == EMBEDDING_EFFECT_TYPE {
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
        }
    }

    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let (replay_chat, replay_embed) = (
        FakeChatModels::forbidden(),
        FakeEmbeddingModels::forbidden(),
    );
    FlowApplication::builder()
        .with_cli_args(replay_args(&live_archive, "--replay-from"))
        .with_config_file(config.clone())
        .with_ai_models(fake_ai::providers(&replay_chat, &replay_embed))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            one_ticket(),
            replay_outputs.clone(),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("strict replay succeeds without connecting either family");
    assert_eq!(*replay_outputs.lock().unwrap(), expected);
    assert_eq!(
        replay_chat.connect_count() + replay_embed.connect_count(),
        0
    );

    mark_archive_incomplete(&live_archive);
    let (resume_chat, resume_embed) = (summarizing(), embedding());
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    let mut resume = replay_args(&live_archive, "--resume-from");
    resume.push(OsString::from("--allow-incomplete-archive"));
    FlowApplication::builder()
        .with_cli_args(resume)
        .with_config_file(config)
        .with_ai_models(fake_ai::providers(&resume_chat, &resume_embed))
        .run_async(chat_and_embedding_flow(
            journal_base,
            one_ticket(),
            resume_outputs.clone(),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("committed resume prefix completes without live AI authority");
    assert_eq!(*resume_outputs.lock().unwrap(), expected);
    assert_eq!(resume_chat.connect_count(), 0);
    assert_eq!(resume_chat.call_count(), 0);
    assert_eq!(resume_embed.connect_count(), 0);
    assert_eq!(resume_embed.call_count(), 0);
}

#[tokio::test]
async fn request_drift_fails_strict_replay_before_any_client_is_connected() {
    let drifted = |suffix: &str, version: &str, json: bool| SummarizeTicket {
        logic_version: version.to_string(),
        prompt_suffix: suffix.to_string(),
        json_response: json,
        ..SummarizeTicket::baseline()
    };
    for (summarize, embed) in [
        (
            drifted(" changed", "ticket-summary-v1", false),
            EmbedSummary::of_width(3),
        ),
        (
            drifted("", "ticket-summary-v2", false),
            EmbedSummary::of_width(3),
        ),
        (
            drifted("", "ticket-summary-v1", true),
            EmbedSummary::of_width(3),
        ),
        (SummarizeTicket::baseline(), EmbedSummary::of_width(2)),
    ] {
        let temp = tempfile::tempdir().unwrap();
        let journal_base = temp.path().join("journals");
        let config = ai_config(temp.path(), None);
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_config_file(config.clone())
            .with_ai_models(fake_ai::providers(&summarizing(), &embedding()))
            .run_async(chat_and_embedding_flow(
                journal_base.clone(),
                one_ticket(),
                Arc::new(Mutex::new(Vec::new())),
                SummarizeTicket::baseline(),
                EmbedSummary::of_width(3),
            ))
            .await
            .unwrap();
        let archive = latest_run_dir(&journal_base);
        let (chat_models, embedding_models) = (summarizing(), embedding());
        let result = FlowApplication::builder()
            .with_cli_args(replay_args(&archive, "--replay-from"))
            .with_config_file(config)
            .with_ai_models(fake_ai::providers(&chat_models, &embedding_models))
            .run_async(chat_and_embedding_flow(
                journal_base,
                one_ticket(),
                Arc::new(Mutex::new(Vec::new())),
                summarize,
                embed,
            ))
            .await;
        assert!(result.is_err(), "request drift must fail strict replay");
        assert_eq!(chat_models.connect_count() + chat_models.call_count(), 0);
        assert_eq!(
            embedding_models.connect_count() + embedding_models.call_count(),
            0
        );
    }
}

#[tokio::test]
async fn strict_replay_keeps_the_recorded_endpoint_when_configuration_moves() {
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(ai_config(temp.path(), None))
        .with_ai_models(fake_ai::providers(&summarizing(), &embedding()))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            one_ticket(),
            Arc::new(Mutex::new(Vec::new())),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .unwrap();
    let archive = latest_run_dir(&journal_base);

    let outputs = Arc::new(Mutex::new(Vec::new()));
    let (chat, embed) = (
        FakeChatModels::forbidden(),
        FakeEmbeddingModels::forbidden(),
    );
    FlowApplication::builder()
        .with_cli_args(replay_args(&archive, "--replay-from"))
        .with_config_file(ai_config(temp.path(), Some("http://127.0.0.1:11435")))
        .with_ai_models(fake_ai::providers(&chat, &embed))
        .run_async(chat_and_embedding_flow(
            journal_base.clone(),
            one_ticket(),
            outputs.clone(),
            SummarizeTicket::baseline(),
            EmbedSummary::of_width(3),
        ))
        .await
        .expect("strict replay binds the recorded endpoint, not the configured one");
    assert_eq!(outputs.lock().unwrap().len(), 1);
    assert_eq!(chat.connect_count() + embed.connect_count(), 0);

    let replay_manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(latest_run_dir(&journal_base).join("run_manifest.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(
        replay_manifest["stages"]["chat"]["ai_models"][0]["endpoint"], "ollama",
        "the replay records the model it replayed"
    );
}

#[path = "support/standalone_ai_gap_proofs.rs"]
mod gap_proofs;
