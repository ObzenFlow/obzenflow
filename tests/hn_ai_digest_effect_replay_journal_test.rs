// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128g checked fixture for the generated AI map-reduce effect path.
//!
//! A live disk-journal run executes six chat port invocations, then strict
//! replay uses the recorded effect history with a resolver that must never be
//! polled. The fixture compares the framework evidence and domain effect fact
//! identities emitted by both runs.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_core::ai::{
    AiClientError, AiFinaliseRole, AiMapReduceChunkFailed, AiMapReducePlanningManifest,
    AiMapReduceRoleFailure, AiMapReduceTaggedPartial, AiMapRole, AiProviderFailureKind,
    AiRoleLogicFailure, ChatClient, ChatCompletionCompleted, ChatMessage, ChatParams, ChatRequest,
    ChatResponse, ChatTarget, HeuristicTokenEstimator, Many, ResolvedTokenEstimator, TokenCount,
    TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerHealthClassification, MiddlewareLifecycle,
    ObservabilityPayload,
};
use obzenflow_core::event::{
    ChainEventContent, EffectAttemptStarted, EffectFailureDetail, EffectOutcomePayload,
    EffectRecord,
};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{id::StageId, EventId, TypedPayload, WriterId};
use obzenflow_dsl::{ai_map_reduce, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
use obzenflow_runtime::effects::{
    EffectPortRegistry, EffectPortResolver, SinkDeliverySafety, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DigestSeed {
    n: u64,
}

impl TypedPayload for DigestSeed {
    const EVENT_TYPE: &'static str = "flowip_128g.digest_seed";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DigestItem {
    value: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DigestPartial {
    value: u64,
}

impl TypedPayload for DigestPartial {
    const EVENT_TYPE: &'static str = "flowip_128g.digest_partial";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DigestOut {
    total: u64,
}

impl TypedPayload for DigestOut {
    const EVENT_TYPE: &'static str = "flowip_128g.digest_out";
}

struct MapRole {
    target: ChatTarget,
    fail_prepare: bool,
    fail_interpret: bool,
}

impl AiMapRole<DigestItem, DigestPartial> for MapRole {
    type Prepared = ();

    fn prepare(
        &self,
        items: &[DigestItem],
        _chunk: &obzenflow_core::ai::ChunkInfo,
    ) -> Result<(ChatRequest, Self::Prepared), AiRoleLogicFailure> {
        if self.fail_prepare {
            return Err(AiRoleLogicFailure::Prompt {
                message: "fixture preparation failure".to_string(),
            });
        }
        Ok((
            ChatRequest {
                provider: self.target.provider.clone(),
                model: self.target.model.clone(),
                messages: vec![ChatMessage::user(format!("{} items", items.len()))],
                params: ChatParams::default(),
                tools: Vec::new(),
                response_format: None,
            },
            (),
        ))
    }

    fn interpret(
        &self,
        items: Vec<DigestItem>,
        _prepared: Self::Prepared,
        _completion: ChatCompletionCompleted,
    ) -> Result<DigestPartial, AiRoleLogicFailure> {
        if self.fail_interpret {
            return Err(AiRoleLogicFailure::Parse {
                message: "fixture interpretation failure".to_string(),
            });
        }
        Ok(DigestPartial {
            value: items.into_iter().map(|item| item.value).sum(),
        })
    }
}

struct FinaliseRole {
    target: ChatTarget,
}

impl AiFinaliseRole<DigestSeed, Many<DigestPartial>, DigestOut> for FinaliseRole {
    type Prepared = ();

    fn prepare(
        &self,
        _seed: &DigestSeed,
        collected: &Many<DigestPartial>,
    ) -> Result<(ChatRequest, Self::Prepared), AiRoleLogicFailure> {
        Ok((
            ChatRequest {
                provider: self.target.provider.clone(),
                model: self.target.model.clone(),
                messages: vec![ChatMessage::user(format!(
                    "{} partials",
                    collected.items.len()
                ))],
                params: ChatParams::default(),
                tools: Vec::new(),
                response_format: None,
            },
            (),
        ))
    }

    fn interpret(
        &self,
        _seed: DigestSeed,
        collected: Many<DigestPartial>,
        _prepared: Self::Prepared,
        _completion: ChatCompletionCompleted,
    ) -> Result<DigestOut, AiRoleLogicFailure> {
        Ok(DigestOut {
            total: collected
                .items
                .into_iter()
                .map(|partial| partial.value)
                .sum(),
        })
    }
}

struct CountingChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
    forbidden: bool,
    response_error: Option<AiClientError>,
}

struct InternallyRetryingChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
    downstream_attempts: Arc<AtomicUsize>,
}

#[async_trait]
impl ChatClient for InternallyRetryingChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.downstream_attempts.fetch_add(2, Ordering::SeqCst);
        Ok(ChatResponse {
            text: "returned after an internal retry".to_string(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

#[async_trait]
impl ChatClient for CountingChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.forbidden,
            "strict replay invoked the physical chat port"
        );
        if let Some(error) = self.response_error.clone() {
            return Err(error);
        }
        Ok(ChatResponse {
            text: "recorded".to_string(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

fn target() -> ChatTarget {
    ChatTarget::new("fixture", "deterministic")
}

fn estimator() -> ResolvedTokenEstimator {
    ResolvedTokenEstimator::new(
        Arc::new(HeuristicTokenEstimator::default()),
        TokenEstimatorResolutionInfo::heuristic(
            "deterministic",
            TokenEstimatorFallbackReason::ExplicitHeuristic,
            None,
        ),
    )
}

fn deferred_chat_port(
    resolutions: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    forbidden: bool,
) -> EffectPortRegistry {
    let resolver: EffectPortResolver<dyn ChatClient> = Arc::new(move || {
        let resolutions = resolutions.clone();
        let calls = calls.clone();
        Box::pin(async move {
            resolutions.fetch_add(1, Ordering::SeqCst);
            assert!(!forbidden, "strict replay resolved the deferred chat port");
            Ok(Arc::new(CountingChatClient {
                target: target(),
                calls,
                forbidden,
                response_error: None,
            }) as Arc<dyn ChatClient>)
        })
    });
    EffectPortRegistry::new()
        .with_deferred::<dyn ChatClient>("chat", resolver)
        .expect("one chat resolver is registered")
}

fn post_start_mismatch_port(
    resolutions: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
) -> EffectPortRegistry {
    let resolver: EffectPortResolver<dyn ChatClient> = Arc::new(move || {
        let resolutions = resolutions.clone();
        let calls = calls.clone();
        Box::pin(async move {
            resolutions.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(CountingChatClient {
                target: target(),
                calls,
                forbidden: false,
                response_error: Some(AiClientError::TargetMismatch {
                    requested: ChatTarget::new("fixture", "mutated-after-start"),
                    bound: target(),
                }),
            }) as Arc<dyn ChatClient>)
        })
    });
    EffectPortRegistry::new()
        .with_deferred::<dyn ChatClient>("chat", resolver)
        .expect("one mismatching chat resolver is registered")
}

fn eager_chat_port(calls: Arc<AtomicUsize>, forbidden: bool) -> EffectPortRegistry {
    eager_chat_port_for_target(calls, forbidden, target())
}

fn eager_chat_port_for_target(
    calls: Arc<AtomicUsize>,
    forbidden: bool,
    client_target: ChatTarget,
) -> EffectPortRegistry {
    EffectPortRegistry::new()
        .with_port::<dyn ChatClient>(
            "chat",
            Arc::new(CountingChatClient {
                target: client_target,
                calls,
                forbidden,
                response_error: None,
            }),
        )
        .expect("one eager chat port is registered")
}

fn error_chat_port(calls: Arc<AtomicUsize>, error: AiClientError) -> EffectPortRegistry {
    EffectPortRegistry::new()
        .with_port::<dyn ChatClient>(
            "chat",
            Arc::new(CountingChatClient {
                target: target(),
                calls,
                forbidden: false,
                response_error: Some(error),
            }),
        )
        .expect("one failing chat port is registered")
}

fn internally_retrying_chat_port(
    calls: Arc<AtomicUsize>,
    downstream_attempts: Arc<AtomicUsize>,
) -> EffectPortRegistry {
    EffectPortRegistry::new()
        .with_port::<dyn ChatClient>(
            "chat",
            Arc::new(InternallyRetryingChatClient {
                target: target(),
                calls,
                downstream_attempts,
            }),
        )
        .expect("one internally retrying chat port is registered")
}

#[derive(Clone, Debug)]
struct OneSeed {
    emitted: bool,
    writer: WriterId,
}

impl OneSeed {
    fn new() -> Self {
        Self {
            emitted: false,
            writer: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for OneSeed {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted {
            return Ok(None);
        }
        self.emitted = true;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer,
            DigestSeed::versioned_event_type(),
            json!(DigestSeed { n: 5 }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct CollectOut {
    outputs: Arc<Mutex<Vec<DigestOut>>>,
}

#[async_trait]
impl SinkHandler for CollectOut {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(output) = DigestOut::from_event(&event) {
            self.outputs
                .lock()
                .expect("output collector lock")
                .push(output);
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("FLOWIP-128g fixture".to_string()),
            None,
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn build_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    effect_ports: EffectPortRegistry,
    backpressure_window: u64,
    map_request_target: ChatTarget,
    map_prepare_failure: bool,
) -> FlowDefinition {
    build_flow_with_behaviour(
        journal_base,
        outputs,
        effect_ports,
        backpressure_window,
        map_request_target,
        map_prepare_failure,
        false,
        estimator(),
    )
}

#[allow(clippy::too_many_arguments)]
fn build_flow_with_behaviour(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    effect_ports: EffectPortRegistry,
    backpressure_window: u64,
    map_request_target: ChatTarget,
    map_prepare_failure: bool,
    map_interpret_failure: bool,
    chat_estimator: ResolvedTokenEstimator,
) -> FlowDefinition {
    let chat_target = target();
    flow! {
        name: "hn_ai_digest_effect_replay_journal",
        journals: disk_journals(journal_base),
        middleware: [],
        backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(backpressure_window)
            .stall_timeout_ms(5_000),
        effect_ports: effect_ports,

        stages: {
            seed = source!(DigestSeed => OneSeed::new());
            digest = ai_map_reduce!(
                DigestSeed -> DigestOut => {
                    map: [DigestItem] -> DigestPartial => MapRole {
                        target: map_request_target,
                        fail_prepare: map_prepare_failure,
                        fail_interpret: map_interpret_failure,
                    },
                    reduce: (DigestSeed, [DigestPartial]) -> DigestOut => FinaliseRole {
                        target: chat_target.clone(),
                    },
                },
                chunking: by_budget {
                    estimator: chat_estimator.estimator(),
                    items: |seed: &DigestSeed| {
                        (1..=seed.n)
                            .map(|value| DigestItem { value })
                            .collect::<Vec<_>>()
                    },
                    render: |item: &DigestItem, _ctx| item.value.to_string(),
                    budget: TokenCount::new(100),
                    max_items: Some(1),
                    oversize: error,
                },
                effects: {
                    chat_target: chat_target,
                    chat_estimator: chat_estimator,
                    map: [at_least_once(ChatCompletion) with [ai_resilience()]],
                    reduce: [at_least_once(ChatCompletion) with [ai_resilience()]],
                }
            );
            collected = sink!(DigestOut => CollectOut { outputs });
        },

        topology: {
            seed |> digest;
            digest |> collected;
        }
    }
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut entries = std::fs::read_dir(base.join("flows"))
        .expect("flow archive directory")
        .map(|entry| entry.expect("flow archive entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    entries.sort();
    entries.pop().expect("completed flow archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run manifest is readable"),
    )
    .expect("run manifest is valid JSON")
}

async fn stage_envelopes(run_dir: &Path, stage_key: &str) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let relative = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .expect("stage data journal path");
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(relative),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("stage journal is readable")
}

async fn assert_archive_contract_rejected_before_port_resolution(
    journal_base: &Path,
    archive: &Path,
    expected_detail: &str,
) {
    let resolutions = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.to_path_buf(),
            Arc::new(Mutex::new(Vec::new())),
            deferred_chat_port(resolutions.clone(), calls.clone(), true),
            3,
            target(),
            false,
        ))
        .await;

    let error = result.expect_err("an incompatible generated-effect archive must fail closed");
    assert!(
        error.to_string().contains(expected_detail),
        "archive rejection should name '{expected_detail}': {error}"
    );
    assert_eq!(resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

fn effect_evidence_ids(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<EventId> {
    let mut ids = envelopes
        .iter()
        .filter(|envelope| {
            EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
                || ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
        })
        .map(|envelope| envelope.event.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn assert_atomic_completion_groups(envelopes: &[EventEnvelope<ChainEvent>], expected: usize) {
    let completions = envelopes
        .iter()
        .filter(|envelope| {
            ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
        })
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), expected);
    for completion in completions {
        assert!(
            completion
                .journal_group_id
                .as_deref()
                .is_some_and(|group| group.starts_with("effect-outcome:v1:")),
            "completion must be committed under its cursor-derived outcome group"
        );
        let member = completion
            .journal_group_member
            .expect("atomic outcome carries physical-frame membership");
        assert!(member.size > 0);
        assert!(member.index < member.size);
    }
}

fn assert_completion_contract(
    envelopes: &[EventEnvelope<ChainEvent>],
    expected: usize,
    expected_label: &str,
) {
    let completions = envelopes
        .iter()
        .filter_map(|envelope| {
            ChatCompletionCompleted::from_event(&envelope.event)
                .map(|completion| (envelope, completion))
        })
        .collect::<Vec<_>>();
    assert_eq!(completions.len(), expected);
    for (envelope, completion) in completions {
        let provenance = envelope
            .event
            .effect_provenance
            .as_ref()
            .expect("completion carries effect provenance");
        assert_eq!(provenance.descriptor.label.as_str(), expected_label);
        assert_eq!(completion.observability.provider, target().provider);
        assert_eq!(completion.observability.model, target().model);
        assert_eq!(
            completion.observability.hashes.version,
            obzenflow_core::ai::LLM_HASH_VERSION_SHA256_V1
        );
        assert!(!completion.observability.hashes.prompt_hash.is_empty());
        assert!(!completion.observability.hashes.params_hash.is_empty());
        assert!(completion.observability.estimated_input_tokens.is_some());
        assert_eq!(
            completion
                .observability
                .estimated_input_resolution
                .as_ref()
                .expect("completion records estimator resolution")
                .model,
            target().model
        );
        assert_eq!(
            completion.observability.usage, completion.response.usage,
            "the durable observation retains the exact returned usage, including None"
        );
    }
    assert!(
        envelopes.iter().all(|envelope| {
            envelope
                .event
                .observability
                .as_ref()
                .and_then(|observability| observability.custom.as_ref())
                .and_then(serde_json::Value::as_object)
                .is_none_or(|custom| !custom.contains_key("llm"))
        }),
        "128g keeps LLM observation on the completion payload and does not copy custom[\"llm\"] onto generated facts"
    );
}

fn circuit_breaker_event_count(
    envelopes: &[EventEnvelope<ChainEvent>],
    predicate: impl Fn(&CircuitBreakerEvent) -> bool,
) -> usize {
    envelopes
        .iter()
        .filter(|envelope| {
            matches!(
                &envelope.event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(event)
                )) if predicate(event)
            )
        })
        .count()
}

fn chunk_failures(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<AiMapReduceChunkFailed> {
    envelopes
        .iter()
        .filter_map(|envelope| AiMapReduceChunkFailed::from_event(&envelope.event))
        .collect()
}

#[test]
fn hn_witness_source_uses_the_snapshot_binding_and_deferred_port_contract() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let flow_source = std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/flow.rs"))
        .expect("HN witness flow source is readable");
    let checked_config =
        std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/obzenflow.toml"))
            .expect("HN witness config is readable");

    for required in [
        "bindings: |runtime_config| {",
        "let ai_models = runtime_config.ai_models();",
        "ChatEffectBinding::from_config(&ai_models)",
        ".with_deferred::<dyn ChatClient>(\"chat\", chat.into_resolver())",
        "effect_ports: effect_ports,",
        "chat_target: chat_target,",
        "chat_estimator: resolved_estimator.clone(),",
        "map: [at_least_once(ChatCompletion) with [ai_resilience()]],",
        "reduce: [at_least_once(ChatCompletion) with [ai_resilience()]],",
    ] {
        assert!(
            flow_source.contains(required),
            "HN witness must retain the locked source clause: {required}"
        );
    }
    for forbidden in [
        "ModelConfig",
        "RigChatClient",
        "LazyRigChatClient",
        "config.ai.",
        "HN_AI_PROVIDER",
        "HN_AI_MODEL",
    ] {
        assert!(
            !flow_source.contains(forbidden),
            "HN witness may not regain the retired/eager source spelling: {forbidden}"
        );
    }
    assert!(checked_config.contains("[runtime.backpressure.flow]"));
    assert!(checked_config.contains("mode = \"enforce\""));
    assert!(checked_config.contains("window = 3"));
    assert!(checked_config.contains("[ai.models]"));
    assert!(checked_config.contains("provider = \"ollama\""));
    assert!(checked_config.contains("model = \"llama3.1:8b\""));
}

#[tokio::test]
async fn generated_map_failure_branches_preserve_their_distinct_durable_contracts() {
    const MAP_CHUNKS: usize = 5;

    let prepare_temp = tempfile::tempdir().expect("temporary preparation journal root");
    let prepare_resolutions = Arc::new(AtomicUsize::new(0));
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let prepare_outputs = Arc::new(Mutex::new(Vec::new()));
    let prepare_base = prepare_temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_with_behaviour(
            prepare_base.clone(),
            prepare_outputs.clone(),
            deferred_chat_port(prepare_resolutions.clone(), prepare_calls.clone(), false),
            3,
            target(),
            true,
            false,
            estimator(),
        ))
        .await
        .expect("pure preparation failures close the generated collector job");
    let prepare_map = stage_envelopes(&latest_run_dir(&prepare_base), "digest__map").await;
    let prepare_failures = chunk_failures(&prepare_map);
    assert_eq!(prepare_failures.len(), MAP_CHUNKS);
    assert!(prepare_failures.iter().all(|failure| matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Logic {
            logic: AiRoleLogicFailure::Prompt { .. }
        }
    )));
    assert_eq!(prepare_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
    assert!(prepare_map.iter().all(|envelope| {
        !EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            && !ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
    }));
    assert!(prepare_outputs
        .lock()
        .expect("preparation outputs lock")
        .is_empty());

    let interpretation_temp = tempfile::tempdir().expect("temporary interpretation journal root");
    let interpretation_resolutions = Arc::new(AtomicUsize::new(0));
    let interpretation_calls = Arc::new(AtomicUsize::new(0));
    let interpretation_outputs = Arc::new(Mutex::new(Vec::new()));
    let interpretation_base = interpretation_temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_with_behaviour(
            interpretation_base.clone(),
            interpretation_outputs.clone(),
            deferred_chat_port(
                interpretation_resolutions.clone(),
                interpretation_calls.clone(),
                false,
            ),
            3,
            target(),
            false,
            true,
            estimator(),
        ))
        .await
        .expect("interpretation failures retain their successful completions");
    let interpretation_map =
        stage_envelopes(&latest_run_dir(&interpretation_base), "digest__map").await;
    let interpretation_failures = chunk_failures(&interpretation_map);
    assert_eq!(interpretation_failures.len(), MAP_CHUNKS);
    assert!(interpretation_failures.iter().all(|failure| matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Logic {
            logic: AiRoleLogicFailure::Parse { .. }
        }
    )));
    assert_eq!(interpretation_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(interpretation_calls.load(Ordering::SeqCst), MAP_CHUNKS);
    assert_completion_contract(
        &interpretation_map,
        MAP_CHUNKS,
        "ai_map_reduce.map.chat_completion",
    );
    assert!(interpretation_outputs
        .lock()
        .expect("interpretation outputs lock")
        .is_empty());

    let provider_temp = tempfile::tempdir().expect("temporary provider-failure journal root");
    let provider_calls = Arc::new(AtomicUsize::new(0));
    let provider_outputs = Arc::new(Mutex::new(Vec::new()));
    let provider_base = provider_temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_with_behaviour(
            provider_base.clone(),
            provider_outputs.clone(),
            error_chat_port(
                provider_calls.clone(),
                AiClientError::InvalidRequest {
                    message: "fixture rejected request".to_string(),
                },
            ),
            3,
            target(),
            false,
            false,
            estimator(),
        ))
        .await
        .expect("ordinary provider failures are domain terminals, not stage fatals");
    let provider_map = stage_envelopes(&latest_run_dir(&provider_base), "digest__map").await;
    let provider_failures = chunk_failures(&provider_map);
    assert_eq!(provider_failures.len(), MAP_CHUNKS);
    assert!(provider_failures.iter().all(|failure| matches!(
        &failure.cause,
        AiMapReduceRoleFailure::Provider {
            provider_kind: AiProviderFailureKind::InvalidRequest,
            ..
        }
    )));
    assert_eq!(provider_calls.load(Ordering::SeqCst), MAP_CHUNKS);
    assert_eq!(
        provider_map
            .iter()
            .filter(|envelope| {
                EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            })
            .count(),
        MAP_CHUNKS
    );
    assert_eq!(
        provider_map
            .iter()
            .filter(|envelope| envelope.event.event_type() == EFFECT_RECORD_EVENT_TYPE)
            .count(),
        MAP_CHUNKS
    );
    assert!(provider_map.iter().all(|envelope| {
        !ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
    }));
    assert!(provider_outputs
        .lock()
        .expect("provider outputs lock")
        .is_empty());
}

#[tokio::test]
async fn live_history_replays_without_resolving_or_invoking_chat() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live_resolutions = Arc::new(AtomicUsize::new(0));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_outputs.clone(),
            deferred_chat_port(live_resolutions.clone(), live_calls.clone(), false),
            3,
            target(),
            false,
        ))
        .await
        .expect("live generated map-reduce run succeeds");

    assert_eq!(live_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(
        *live_outputs.lock().expect("live output lock"),
        vec![DigestOut { total: 15 }]
    );

    let live_archive = latest_run_dir(&journal_base);
    let live_chunk = stage_envelopes(&live_archive, "digest__chunk").await;
    let live_map = stage_envelopes(&live_archive, "digest__map").await;
    let live_finalise = stage_envelopes(&live_archive, "digest__finalize").await;
    let manifests = live_chunk
        .iter()
        .filter_map(|envelope| AiMapReducePlanningManifest::from_event(&envelope.event))
        .collect::<Vec<_>>();
    let [manifest] = manifests.as_slice() else {
        panic!(
            "the deterministic witness must publish exactly one planning manifest, found {}",
            manifests.len()
        );
    };
    let expected_map_calls = manifest.chunk_count;
    assert_eq!(
        live_calls.load(Ordering::SeqCst),
        expected_map_calls + 1,
        "N map completions plus one finalise completion"
    );
    assert_eq!(
        live_map
            .iter()
            .filter(|envelope| {
                EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            })
            .count(),
        expected_map_calls
    );
    assert_eq!(
        live_finalise
            .iter()
            .filter(|envelope| {
                EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            })
            .count(),
        1
    );
    assert_atomic_completion_groups(&live_map, expected_map_calls);
    assert_atomic_completion_groups(&live_finalise, 1);
    assert_completion_contract(
        &live_map,
        expected_map_calls,
        "ai_map_reduce.map.chat_completion",
    );
    assert_completion_contract(&live_finalise, 1, "ai_map_reduce.finalise.chat_completion");
    let live_map_ids = effect_evidence_ids(&live_map);
    let live_finalise_ids = effect_evidence_ids(&live_finalise);

    let manifest_path = live_archive.join("run_manifest.json");
    let original_manifest =
        std::fs::read_to_string(&manifest_path).expect("live run manifest is readable");
    let parsed_manifest: serde_json::Value =
        serde_json::from_str(&original_manifest).expect("live run manifest is valid JSON");
    let mut incompatible_archives = Vec::new();

    let mut missing_history_capability = parsed_manifest.clone();
    missing_history_capability["capabilities"]
        .as_object_mut()
        .expect("capability map")
        .remove("effect_attempt_history");
    incompatible_archives.push((
        "missing attempt-history capability",
        missing_history_capability,
        "effect_attempt_history",
    ));

    let mut missing_admission_capability = parsed_manifest.clone();
    missing_admission_capability["capabilities"]
        .as_object_mut()
        .expect("capability map")
        .remove("bounded_direct_fact_admission");
    incompatible_archives.push((
        "missing bounded-admission capability",
        missing_admission_capability,
        "bounded_direct_fact_admission",
    ));

    let mut wrong_admission_capability = parsed_manifest.clone();
    wrong_admission_capability["capabilities"]["bounded_direct_fact_admission"] = json!(2);
    incompatible_archives.push((
        "wrong bounded-admission capability version",
        wrong_admission_capability,
        "bounded_direct_fact_admission",
    ));

    let mut missing_plan_row = parsed_manifest.clone();
    missing_plan_row["bounded_direct_fact_admission"]
        .as_array_mut()
        .expect("bounded admission rows")
        .pop()
        .expect("the witness has generated map and finalise rows");
    incompatible_archives.push((
        "missing generated plan row",
        missing_plan_row,
        "bounded direct-fact admission metadata",
    ));

    let mut duplicate_plan_row = parsed_manifest.clone();
    let duplicate = duplicate_plan_row["bounded_direct_fact_admission"]
        .as_array()
        .and_then(|rows| rows.first())
        .cloned()
        .expect("the witness has a generated admission row");
    duplicate_plan_row["bounded_direct_fact_admission"]
        .as_array_mut()
        .expect("bounded admission rows")
        .push(duplicate);
    incompatible_archives.push((
        "duplicate generated plan row",
        duplicate_plan_row,
        "bounded direct-fact admission metadata",
    ));

    let mut wrong_plan_input = parsed_manifest.clone();
    wrong_plan_input["bounded_direct_fact_admission"][0]["input_event_type"] =
        json!("flowip_128g.wrong_input:v1");
    incompatible_archives.push((
        "wrong generated plan input type",
        wrong_plan_input,
        "bounded direct-fact admission metadata",
    ));

    let mut wrong_plan_bound = parsed_manifest.clone();
    wrong_plan_bound["bounded_direct_fact_admission"][0]["max_live_data_rows"] = json!(4);
    incompatible_archives.push((
        "wrong generated plan bound",
        wrong_plan_bound,
        "bounded direct-fact admission metadata",
    ));

    for (case, incompatible_manifest, expected_detail) in incompatible_archives {
        std::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&incompatible_manifest)
                .expect("incompatible manifest serialises"),
        )
        .unwrap_or_else(|error| panic!("{case}: temporary manifest write failed: {error}"));
        assert_archive_contract_rejected_before_port_resolution(
            &journal_base,
            &live_archive,
            expected_detail,
        )
        .await;
    }
    std::fs::write(&manifest_path, original_manifest)
        .expect("compatible live manifest is restored");

    let divergent_resolutions = Arc::new(AtomicUsize::new(0));
    let divergent_calls = Arc::new(AtomicUsize::new(0));
    let divergent_result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            deferred_chat_port(divergent_resolutions.clone(), divergent_calls.clone(), true),
            3,
            target(),
            true,
        ))
        .await;
    assert!(
        divergent_result.is_err(),
        "a new preparation failure cannot replace archived effect history"
    );
    assert_eq!(divergent_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(divergent_calls.load(Ordering::SeqCst), 0);

    let replay_resolutions = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
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
            deferred_chat_port(replay_resolutions.clone(), replay_calls.clone(), true),
            3,
            target(),
            false,
        ))
        .await
        .expect("strict replay rematerialises the generated effect history");

    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        *replay_outputs.lock().expect("replay output lock"),
        vec![DigestOut { total: 15 }]
    );

    let replay_archive = latest_run_dir(&journal_base);
    let replay_map = stage_envelopes(&replay_archive, "digest__map").await;
    let replay_finalise = stage_envelopes(&replay_archive, "digest__finalize").await;
    assert_eq!(effect_evidence_ids(&replay_map), live_map_ids);
    assert_eq!(effect_evidence_ids(&replay_finalise), live_finalise_ids);
    assert_atomic_completion_groups(&replay_map, expected_map_calls);
    assert_atomic_completion_groups(&replay_finalise, 1);

    let verification = verify_run_dirs(
        &live_archive,
        &replay_archive,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .expect("run-directory verification executes");
    let verification_details = match &verification {
        VerifyOutcome::Completed { report, .. } => {
            serde_json::to_string_pretty(report).expect("verification report serialises")
        }
        VerifyOutcome::Refused(reason) => format!("verification refused: {reason}"),
    };
    assert_eq!(
        verification.exit_code(),
        0,
        "{}\n{verification_details}",
        obzenflow_infra::verify::render_verdict(&verification),
    );

    let empty_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            empty_outputs.clone(),
            EffectPortRegistry::new(),
            3,
            target(),
            false,
        ))
        .await
        .expect("strict replay accepts an empty effect-port registry");
    assert_eq!(
        *empty_outputs.lock().expect("empty-registry output lock"),
        vec![DigestOut { total: 15 }]
    );

    let eager_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base,
            Arc::new(Mutex::new(Vec::new())),
            eager_chat_port(eager_calls.clone(), true),
            3,
            target(),
            false,
        ))
        .await
        .expect("strict replay never invokes an eager panic client");
    assert_eq!(eager_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn descriptor_bound_rejects_one_and_two_credit_windows_before_port_resolution() {
    for window in [1_u64, 2] {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let resolutions = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let result = build_flow(
            temp.path().join("journals"),
            Arc::new(Mutex::new(Vec::new())),
            deferred_chat_port(resolutions.clone(), calls.clone(), true),
            window,
            target(),
            false,
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await;

        let error = match result {
            Ok(_) => {
                panic!("a {window}-credit window cannot admit the three-row generated continuation")
            }
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(
            message.contains("requires 3 live physical Data credits")
                && message.contains(&format!("resolved enforced window {window}")),
            "build error must expose the descriptor/window mismatch: {message}"
        );
        assert_eq!(resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn one_attempt_ordinal_does_not_claim_downstream_retry_cardinality() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let calls = Arc::new(AtomicUsize::new(0));
    let downstream_attempts = Arc::new(AtomicUsize::new(0));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            internally_retrying_chat_port(calls.clone(), downstream_attempts.clone()),
            3,
            target(),
            false,
        ))
        .await
        .expect("the internally retrying fake should complete");

    let archive = latest_run_dir(&journal_base);
    let chunks = stage_envelopes(&archive, "digest__chunk").await;
    let map = stage_envelopes(&archive, "digest__map").await;
    let finalise = stage_envelopes(&archive, "digest__finalize").await;
    let manifest = chunks
        .iter()
        .find_map(|envelope| AiMapReducePlanningManifest::from_event(&envelope.event))
        .expect("the generated plan should publish its manifest");
    let port_invocations =
        usize::try_from(manifest.chunk_count + 1).expect("fixture count fits usize");

    assert_eq!(calls.load(Ordering::SeqCst), port_invocations);
    assert_eq!(
        downstream_attempts.load(Ordering::SeqCst),
        port_invocations * 2,
        "two fake downstream attempts remain opaque inside each chat invocation"
    );

    let generated = [&map, &finalise];
    let starts = generated
        .iter()
        .flat_map(|stage| stage.iter())
        .filter(|envelope| EffectAttemptStarted::event_type_matches(&envelope.event.event_type()))
        .count();
    let completions = generated
        .iter()
        .flat_map(|stage| stage.iter())
        .filter(|envelope| {
            ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
        })
        .count();
    let settlements = generated
        .iter()
        .map(|stage| {
            circuit_breaker_event_count(stage, |event| {
                matches!(event, CircuitBreakerEvent::AttemptSettled { .. })
            })
        })
        .sum::<usize>();
    let recoveries = generated
        .iter()
        .map(|stage| {
            circuit_breaker_event_count(stage, |event| {
                matches!(event, CircuitBreakerEvent::RecoveryCompleted { .. })
            })
        })
        .sum::<usize>();
    let direct_data_rows = generated
        .iter()
        .flat_map(|stage| stage.iter())
        .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Data { .. }))
        .count();
    let direct_data_types = generated
        .iter()
        .flat_map(|stage| stage.iter())
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data { event_type, .. } => Some(event_type.clone()),
            _ => None,
        })
        .fold(
            std::collections::BTreeMap::<String, usize>::new(),
            |mut counts, event_type| {
                *counts.entry(event_type).or_default() += 1;
                counts
            },
        );

    assert_eq!(starts, port_invocations);
    assert_eq!(completions, port_invocations);
    assert_eq!(settlements, port_invocations);
    assert_eq!(recoveries, port_invocations);
    assert_eq!(direct_data_rows, port_invocations * 3 + 1);
    assert_eq!(
        direct_data_types,
        std::collections::BTreeMap::from([
            (AiMapReducePlanningManifest::versioned_event_type(), 1_usize,),
            (
                AiMapReduceTaggedPartial::<DigestPartial>::versioned_event_type(),
                usize::try_from(manifest.chunk_count).expect("fixture count fits usize"),
            ),
            (
                ChatCompletionCompleted::versioned_event_type(),
                port_invocations,
            ),
            (DigestOut::versioned_event_type(), 1),
            (
                EffectAttemptStarted::versioned_event_type(),
                port_invocations,
            ),
        ]),
        "the one protocol manifest plus three rows per port invocation are the complete Data set; \
         internal downstream retries allocate no ordinal, resilience settlement, or durable row"
    );
}

#[tokio::test]
async fn prepared_request_target_mismatch_is_fatal_before_history_or_port_resolution() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let resolutions = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            deferred_chat_port(resolutions.clone(), calls.clone(), true),
            3,
            ChatTarget::new("fixture", "wrong-model"),
            false,
        ))
        .await;

    assert!(
        result.is_err(),
        "request/descriptor target mismatch must terminate the stage"
    );
    assert_eq!(resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    assert!(
        !map.iter().any(|envelope| {
            EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
                || ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
        }),
        "target assertion must precede cursor history and physical execution"
    );
}

#[tokio::test]
async fn resolved_client_target_mismatch_is_fatal_before_start_or_chat() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            eager_chat_port_for_target(
                calls.clone(),
                false,
                ChatTarget::new("fixture", "wrong-client-model"),
            ),
            3,
            target(),
            false,
        ))
        .await;

    assert!(
        result.is_err(),
        "a resolved client for another target must terminate the stage"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "target validation must precede ChatClient::chat"
    );

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    assert!(
        !map.iter().any(|envelope| {
            EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
                || ChatCompletionCompleted::event_type_matches(&envelope.event.event_type())
                || envelope.event.event_type() == EFFECT_RECORD_EVENT_TYPE
        }),
        "client target validation must precede the attempt boundary"
    );
    assert!(
        chunk_failures(&map).is_empty(),
        "configuration fatals are not generated domain failures"
    );
}

#[tokio::test]
async fn post_start_target_invariant_commits_a_failed_attempt_terminal() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let resolutions = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            post_start_mismatch_port(resolutions.clone(), calls.clone()),
            3,
            target(),
            false,
        ))
        .await;

    assert!(
        result.is_err(),
        "a post-Start target invariant must terminate the stage"
    );
    assert_eq!(resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "digest__map").await;
    let starts = map
        .iter()
        .filter(|envelope| EffectAttemptStarted::event_type_matches(&envelope.event.event_type()))
        .collect::<Vec<_>>();
    assert_eq!(
        starts.len(),
        1,
        "one physical chat invocation has one Start"
    );

    let failed = map
        .iter()
        .find(|envelope| envelope.event.event_type() == EFFECT_RECORD_EVENT_TYPE)
        .expect("post-Start invariant commits a generic failed outcome");
    let ChainEventContent::Data { payload, .. } = &failed.event.content else {
        panic!("effect failure is a data fact");
    };
    let record: EffectRecord =
        serde_json::from_value(payload.clone()).expect("effect failure record decodes");
    assert!(matches!(
        record.outcome,
        EffectOutcomePayload::Failed {
            detail: Some(EffectFailureDetail::PortBindingInvariantViolation { .. }),
            ..
        }
    ));
    assert_eq!(
        failed
            .event
            .effect_provenance
            .as_ref()
            .and_then(|provenance| provenance.attempt),
        Some(obzenflow_core::event::EffectAttemptOrdinal::new(1))
    );
    assert!(failed
        .journal_group_id
        .as_deref()
        .is_some_and(|group| group.starts_with("effect-outcome:v1:")));
    assert_eq!(
        failed
            .journal_group_member
            .expect("failed terminal has atomic membership")
            .index,
        0
    );

    let mut saw_ignored_settlement = false;
    let mut saw_completed_recovery = false;
    for envelope in &map {
        let ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::CircuitBreaker(event),
        )) = &envelope.event.content
        else {
            continue;
        };
        match event {
            CircuitBreakerEvent::AttemptSettled {
                attempt,
                health_classification,
                ..
            } => {
                assert_eq!(*attempt, 1);
                assert!(matches!(
                    health_classification,
                    CircuitBreakerHealthClassification::Ignored
                ));
                assert_eq!(
                    envelope.journal_group_id, failed.journal_group_id,
                    "attempt settlement belongs to the same atomic terminal"
                );
                saw_ignored_settlement = true;
            }
            CircuitBreakerEvent::RecoveryCompleted { total_attempts, .. } => {
                assert_eq!(*total_attempts, 1);
                assert_eq!(
                    envelope.journal_group_id, failed.journal_group_id,
                    "recovery completion belongs to the same atomic terminal"
                );
                saw_completed_recovery = true;
            }
            _ => {}
        }
    }
    assert!(saw_ignored_settlement);
    assert!(saw_completed_recovery);
}
