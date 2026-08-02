// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128g checked fixture for the generated AI map-reduce effect path.
//!
//! A live disk-journal run executes six chat port invocations, then strict
//! replay uses the recorded effect history with a resolver that must never be
//! polled. The fixture compares the framework evidence and domain effect fact
//! identities emitted by both runs.

#[path = "../examples/hn_ai_digest_demo/config.rs"]
mod config;
#[path = "../examples/hn_ai_digest_demo/decoder.rs"]
mod decoder;
#[path = "../examples/hn_ai_digest_demo/domain.rs"]
mod domain;
#[path = "../examples/hn_ai_digest_demo/flow.rs"]
mod hn_demo_flow;
#[path = "../examples/hn_ai_digest_demo/mock_server.rs"]
mod mock_server;
#[path = "../examples/hn_ai_digest_demo/util.rs"]
mod util;

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::{
    ai_recovery_rejecting_resilience_for_test, ai_resilience,
};
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::ai::{
    chat_binding_fingerprint, AiClientError, AiFinaliseRole, AiMapReduceChunkFailed,
    AiMapReduceJobFailed, AiMapReducePlanningManifest, AiMapReduceRoleFailure,
    AiMapReduceTaggedPartial, AiMapRole, AiProvider, AiProviderFailureKind, AiRoleLogicFailure,
    ChatBindingContract, ChatClient, ChatCompletionReply, ChatMessage, ChatParams, ChatRequest,
    ChatRequestSpec, ChatResponse, ChatTarget, EstimateSource, HeuristicTokenEstimator, Many,
    ResolvedTokenEstimator, TokenCount, TokenEstimate, TokenEstimator,
    TokenEstimatorFallbackReason, TokenEstimatorResolutionInfo, CHAT_CLIENT_PORT,
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
    EffectRecord, EffectRecoveryAbandoned, PipelineLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{id::StageId, EventId, SystemId, TypedPayload, WriterId};
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
use obzenflow_runtime::testing::BackpressureAckGate;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

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
    fail_prepare: bool,
    fail_interpret: bool,
    prepare_calls: Option<Arc<AtomicUsize>>,
}

impl AiMapRole<DigestItem, DigestPartial> for MapRole {
    fn prepare(
        &self,
        items: &[DigestItem],
        _chunk: &obzenflow_core::ai::ChunkInfo,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        if let Some(calls) = &self.prepare_calls {
            calls.fetch_add(1, Ordering::SeqCst);
        }
        if self.fail_prepare {
            return Err(AiRoleLogicFailure::Prompt {
                message: "fixture preparation failure".to_string(),
            });
        }
        Ok(ChatRequestSpec {
            messages: vec![ChatMessage::user(format!("{} items", items.len()))],
            params: ChatParams::default(),
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        items: Vec<DigestItem>,
        _chunk: obzenflow_core::ai::ChunkInfo,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
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

struct FinaliseRole;

impl AiFinaliseRole<DigestSeed, Many<DigestPartial>, DigestOut> for FinaliseRole {
    fn prepare(
        &self,
        _seed: &DigestSeed,
        collected: &Many<DigestPartial>,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        Ok(ChatRequestSpec {
            messages: vec![ChatMessage::user(format!(
                "{} partials",
                collected.items.len()
            ))],
            params: ChatParams::default(),
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        _seed: DigestSeed,
        collected: Many<DigestPartial>,
        _request: ChatRequestSpec,
        _reply: ChatCompletionReply,
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

struct HangingChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
}

#[async_trait]
impl ChatClient for HangingChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, _request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        std::future::pending().await
    }
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

fn bound_fixture_target(endpoint: &str) -> ChatTarget {
    let provider = AiProvider::new("fixture");
    ChatTarget::with_binding_fingerprint(
        provider.clone(),
        "deterministic",
        chat_binding_fingerprint(&provider, "deterministic", endpoint),
    )
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

#[derive(Debug)]
struct ObservabilityOnlyEstimator {
    calls: Arc<AtomicUsize>,
}

impl TokenEstimator for ObservabilityOnlyEstimator {
    fn estimate_text(&self, text: &str) -> TokenEstimate {
        self.calls.fetch_add(1, Ordering::SeqCst);
        HeuristicTokenEstimator::default().estimate_text(text)
    }

    fn estimate_chat_request(&self, request: &ChatRequest) -> TokenEstimate {
        self.calls.fetch_add(1, Ordering::SeqCst);
        HeuristicTokenEstimator::default().estimate_chat_request(request)
    }

    fn source(&self) -> EstimateSource {
        EstimateSource::Heuristic
    }
}

fn observability_only_estimator(calls: Arc<AtomicUsize>) -> ResolvedTokenEstimator {
    ResolvedTokenEstimator::new(
        Arc::new(ObservabilityOnlyEstimator { calls }),
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
        .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, resolver)
        .expect("one chat resolver is registered")
}

fn counting_chat_resolver(
    client_target: ChatTarget,
    resolutions: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
    forbidden: bool,
) -> EffectPortResolver<dyn ChatClient> {
    Arc::new(move || {
        let target = client_target.clone();
        let resolutions = resolutions.clone();
        let calls = calls.clone();
        Box::pin(async move {
            resolutions.fetch_add(1, Ordering::SeqCst);
            assert!(!forbidden, "strict replay resolved the real-flow chat port");
            Ok(Arc::new(CountingChatClient {
                target,
                calls,
                forbidden,
                response_error: None,
            }) as Arc<dyn ChatClient>)
        })
    })
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
                response_error: Some(AiClientError::target_mismatch(
                    ChatTarget::new("fixture", "mutated-after-start"),
                    target(),
                )),
            }) as Arc<dyn ChatClient>)
        })
    });
    EffectPortRegistry::new()
        .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, resolver)
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
            CHAT_CLIENT_PORT,
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
            CHAT_CLIENT_PORT,
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
            CHAT_CLIENT_PORT,
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
    count: u64,
    writer: WriterId,
}

impl OneSeed {
    fn new() -> Self {
        Self::with_count(5)
    }

    fn with_count(count: u64) -> Self {
        Self {
            emitted: false,
            count,
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
            json!(DigestSeed { n: self.count }),
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
    build_flow_with_behaviour(FlowBehaviour {
        journal_base,
        outputs,
        effect_ports,
        backpressure_window,
        map_request_target,
        map_prepare_failure,
        map_interpret_failure: false,
        chat_estimator: estimator(),
        chat_target: target(),
        map_prepare_calls: None,
    })
}

struct FlowBehaviour {
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    effect_ports: EffectPortRegistry,
    backpressure_window: u64,
    map_request_target: ChatTarget,
    map_prepare_failure: bool,
    map_interpret_failure: bool,
    chat_estimator: ResolvedTokenEstimator,
    chat_target: ChatTarget,
    map_prepare_calls: Option<Arc<AtomicUsize>>,
}

fn build_flow_with_behaviour(behaviour: FlowBehaviour) -> FlowDefinition {
    let FlowBehaviour {
        journal_base,
        outputs,
        effect_ports,
        backpressure_window,
        map_request_target,
        map_prepare_failure,
        map_interpret_failure,
        chat_estimator,
        chat_target: _chat_target,
        map_prepare_calls,
    } = behaviour;
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = ChatBindingContract::from_resolved(map_request_target, chat_estimator)
            .expect("test chat target and estimator models agree");
        let seed = OneSeed::new();
        let map_role = MapRole {
            fail_prepare: map_prepare_failure,
            fail_interpret: map_interpret_failure,
            prepare_calls: map_prepare_calls,
        };
        let finalise_role = FinaliseRole;
        let collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_effect_replay_journal",
            journals: disk_journals(journal_base),
            middleware: [],
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(backpressure_window)
                .stall_timeout_ms(5_000),
            effect_ports,

            stages: {
                seed = source!(DigestSeed => seed);
                digest = ai_map_reduce!(
                    DigestSeed -> DigestOut => {
                        map: [DigestItem] ->{
                            at_least_once(ChatCompletion)
                                via chat
                                with { ai_resilience() }
                        } DigestPartial => map_role,
                        reduce: (DigestSeed, [DigestPartial]) ->{
                            at_least_once(ChatCompletion)
                                via chat
                                with { ai_resilience() }
                        } DigestOut => finalise_role,
                    },
                    chunking: by_budget {
                        items: |seed: &DigestSeed| {
                            (1..=seed.n)
                                .map(|value| DigestItem { value })
                                .collect::<Vec<_>>()
                        },
                        render: |item: &DigestItem, _ctx| item.value.to_string(),
                        budget: TokenCount::new(100),
                        max_items: Some(1),
                        oversize: error,
                    }
                );
                collected = sink!(DigestOut => collected);
            },

            topology: {
                seed |> digest;
                digest |> collected;
            }
        })
    })
}

fn build_recovery_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    effect_ports: EffectPortRegistry,
    map_policy: Box<dyn MiddlewareFactory>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = ChatBindingContract::from_resolved(target(), estimator())
            .expect("test chat target and estimator models agree");
        let recovery_seed = OneSeed::with_count(1);
        let map_role = MapRole {
            fail_prepare: false,
            fail_interpret: false,
            prepare_calls: None,
        };
        let finalise_role = FinaliseRole;
        let recovery_collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_recovery_composition",
            journals: disk_journals(journal_base),
            middleware: [],
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(3)
                .stall_timeout_ms(5_000),
            effect_ports,

            stages: {
                recovery_seed = source!(DigestSeed => recovery_seed);
                recovery_digest = ai_map_reduce!(
                    DigestSeed -> DigestOut => {
                        map: [DigestItem] ->{
                            at_least_once(ChatCompletion)
                                via chat
                                with { map_policy }
                        } DigestPartial => map_role,
                        reduce: (DigestSeed, [DigestPartial]) ->{
                            at_least_once(ChatCompletion)
                                via chat
                                with { ai_resilience() }
                        } DigestOut => finalise_role,
                    },
                    chunking: by_budget {
                        items: |seed: &DigestSeed| {
                            (1..=seed.n)
                                .map(|value| DigestItem { value })
                                .collect::<Vec<_>>()
                        },
                        render: |item: &DigestItem, _ctx| item.value.to_string(),
                        budget: TokenCount::new(100),
                        max_items: Some(1),
                        oversize: error,
                    }
                );
                recovery_collected = sink!(DigestOut => recovery_collected);
            },

            topology: {
                recovery_seed |> recovery_digest;
                recovery_digest |> recovery_collected;
            }
        })
    })
}

fn build_credit_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DigestOut>>>,
    effect_ports: EffectPortRegistry,
    prepare_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = ChatBindingContract::from_resolved(target(), estimator())
            .expect("test chat target and estimator models agree");
        let credit_seed = OneSeed::with_count(2);
        let map_role = MapRole {
            fail_prepare: false,
            fail_interpret: false,
            prepare_calls: Some(prepare_calls),
        };
        let finalise_role = FinaliseRole;
        let credit_collected = CollectOut { outputs };

        Ok(flow! {
            name: "hn_ai_digest_credit_composition",
            journals: disk_journals(journal_base),
            middleware: [],
            backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(3)
                .stall_timeout_ms(5_000),
            effect_ports,

            stages: {
                credit_seed = source!(DigestSeed => credit_seed);
                credit_digest = ai_map_reduce!(
                    DigestSeed -> DigestOut => {
                        map: [DigestItem] ->{
                            at_least_once(ChatCompletion)
                                via chat
                                with { ai_resilience() }
                        } DigestPartial => map_role,
                        reduce: (DigestSeed, [DigestPartial]) ->{
                            at_least_once(ChatCompletion)
                                via chat
                                with { ai_resilience() }
                        } DigestOut => finalise_role,
                    },
                    chunking: by_budget {
                        items: |seed: &DigestSeed| {
                            (1..=seed.n)
                                .map(|value| DigestItem { value })
                                .collect::<Vec<_>>()
                        },
                        render: |item: &DigestItem, _ctx| item.value.to_string(),
                        budget: TokenCount::new(100),
                        max_items: Some(1),
                        oversize: error,
                    }
                );
                credit_collected = sink!(DigestOut => credit_collected);
            },

            topology: {
                credit_seed |> credit_digest;
                credit_digest |> credit_collected;
            }
        })
    })
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

async fn system_events(run_dir: &Path) -> Vec<SystemEvent> {
    let manifest = archive_manifest(run_dir);
    let relative = manifest["system_journal_file"]
        .as_str()
        .expect("system journal path");
    let journal = DiskJournal::<SystemEvent>::with_owner(
        run_dir.join(relative),
        JournalOwner::system(SystemId::new()),
    )
    .expect("system journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("system journal is readable")
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
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
                || chat_completion_reply(&envelope.event).is_some()
        })
        .map(|envelope| envelope.event.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

const CHAT_COMPLETION_EFFECT_TYPE: &str = "obzenflow.ai.chat_completion";

fn chat_completion_reply(event: &ChainEvent) -> Option<ChatCompletionReply> {
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
    if record.descriptor.effect_type.as_str() != CHAT_COMPLETION_EFFECT_TYPE {
        return None;
    }
    match record.outcome {
        EffectOutcomePayload::Succeeded { output } => serde_json::from_value(output).ok(),
        EffectOutcomePayload::SucceededFact { .. } | EffectOutcomePayload::Failed { .. } => None,
    }
}

fn assert_atomic_completion_groups(envelopes: &[EventEnvelope<ChainEvent>], expected: usize) {
    let completions = envelopes
        .iter()
        .filter(|envelope| chat_completion_reply(&envelope.event).is_some())
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
    expected_target: &ChatTarget,
) {
    let completions = envelopes
        .iter()
        .filter_map(|envelope| {
            chat_completion_reply(&envelope.event).map(|reply| (envelope, reply))
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
        assert_eq!(
            provenance.descriptor.schema_version, 3,
            "recorded-reply storage is the ChatCompletion v3 schema"
        );
        assert_eq!(completion.observability.provider, expected_target.provider);
        assert_eq!(completion.observability.model, expected_target.model);
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
            expected_target.model
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
        "120j keeps LLM observation in framework reply evidence and does not copy custom[\"llm\"] onto generated facts"
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

async fn wait_for_counter(counter: &AtomicUsize, minimum: usize) {
    while counter.load(Ordering::SeqCst) < minimum {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[test]
fn hn_witness_uses_materialization_and_deferred_port_contract() {
    let _ = config::DEFAULT_HN_MAX_STORIES;
    let _ = config::DEFAULT_HN_SOURCE_RATE_LIMIT;
    let _ = config::PreparedHnRun::from_env;
    let _ = config::HnRunInputs::group_max_stories_label;
    let _ = hn_demo_flow::run_example;
    let _ = hn_demo_flow::run_demo_blocking;
    let _ = hn_demo_flow::build_presentation;

    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let flow_source = std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/flow.rs"))
        .expect("HN witness flow source is readable");
    let checked_config =
        std::fs::read_to_string(root.join("examples/hn_ai_digest_demo/obzenflow.toml"))
            .expect("HN witness config is readable");

    for required in [
        "FlowDefinition::materialize(move |runtime_config| {",
        "let HnRunInputs {",
        "let ai_models = runtime_config.ai_models();",
        "ChatEffectBinding::from_config(&ai_models)",
        "let (chat, chat_registration) =",
        ".into_parts();",
        "chat_registration.install_into(EffectPortRegistry::new())",
        "effect_ports,",
        "let hn_source = HttpPullSource::new(decoder, http_source_config);",
        "map: [FormattedStory] -> {",
        "reduce: (HnTopStories, [HnDigestGroupSummary]) -> {",
        "via chat",
        "with { ai_resilience() }",
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
        "std::env",
        "env_var(",
        "read_to_string(",
        "bindings:",
        "effect_ports: effect_ports,",
        "DemoConfig",
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
        .run_async(build_flow_with_behaviour(FlowBehaviour {
            journal_base: prepare_base.clone(),
            outputs: prepare_outputs.clone(),
            effect_ports: deferred_chat_port(
                prepare_resolutions.clone(),
                prepare_calls.clone(),
                false,
            ),
            backpressure_window: 3,
            map_request_target: target(),
            map_prepare_failure: true,
            map_interpret_failure: false,
            chat_estimator: estimator(),
            chat_target: target(),
            map_prepare_calls: None,
        }))
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
            && chat_completion_reply(&envelope.event).is_none()
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
        .run_async(build_flow_with_behaviour(FlowBehaviour {
            journal_base: interpretation_base.clone(),
            outputs: interpretation_outputs.clone(),
            effect_ports: deferred_chat_port(
                interpretation_resolutions.clone(),
                interpretation_calls.clone(),
                false,
            ),
            backpressure_window: 3,
            map_request_target: target(),
            map_prepare_failure: false,
            map_interpret_failure: true,
            chat_estimator: estimator(),
            chat_target: target(),
            map_prepare_calls: None,
        }))
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
        &target(),
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
        .run_async(build_flow_with_behaviour(FlowBehaviour {
            journal_base: provider_base.clone(),
            outputs: provider_outputs.clone(),
            effect_ports: error_chat_port(
                provider_calls.clone(),
                AiClientError::InvalidRequest {
                    message: "fixture rejected request".to_string(),
                },
            ),
            backpressure_window: 3,
            map_request_target: target(),
            map_prepare_failure: false,
            map_interpret_failure: false,
            chat_estimator: estimator(),
            chat_target: target(),
            map_prepare_calls: None,
        }))
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
    assert!(provider_map
        .iter()
        .all(|envelope| { chat_completion_reply(&envelope.event).is_none() }));
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
        &target(),
    );
    assert_completion_contract(
        &live_finalise,
        1,
        "ai_map_reduce.finalise.chat_completion",
        &target(),
    );
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
    let replay_estimator_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow_with_behaviour(FlowBehaviour {
            journal_base: journal_base.clone(),
            outputs: replay_outputs.clone(),
            effect_ports: deferred_chat_port(
                replay_resolutions.clone(),
                replay_calls.clone(),
                true,
            ),
            backpressure_window: 3,
            map_request_target: target(),
            map_prepare_failure: false,
            map_interpret_failure: false,
            chat_estimator: observability_only_estimator(replay_estimator_calls.clone()),
            chat_target: target(),
            map_prepare_calls: None,
        }))
        .await
        .expect("strict replay rematerialises the generated effect history");

    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_estimator_calls.load(Ordering::SeqCst),
        expected_map_calls,
        "strict replay only consults the binding estimator for deterministic chunk planning; \
         completion history hits retain their archived observations"
    );
    let replay_archive = latest_run_dir(&journal_base);
    let replay_map = stage_envelopes(&replay_archive, "digest__map").await;
    let replay_finalise = stage_envelopes(&replay_archive, "digest__finalize").await;
    assert_eq!(
        *replay_outputs.lock().expect("replay output lock"),
        vec![DigestOut { total: 15 }],
        "map event types: {:?}; finalise event types: {:?}",
        replay_map
            .iter()
            .map(|envelope| envelope.event.event_type())
            .collect::<Vec<_>>(),
        replay_finalise
            .iter()
            .map(|envelope| envelope.event.event_type())
            .collect::<Vec<_>>()
    );

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
async fn generated_recovery_abandonment_closes_the_real_composite_without_start_two() {
    let temp = tempfile::tempdir().expect("temporary recovery-composition journal root");
    let journal_base = temp.path().join("journals");
    let hanging_calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let hanging_ports = EffectPortRegistry::new()
        .with_port::<dyn ChatClient>(
            CHAT_CLIENT_PORT,
            Arc::new(HangingChatClient {
                target: target(),
                calls: hanging_calls.clone(),
                entered: entered.clone(),
            }),
        )
        .expect("hanging chat port is registered");

    let live_task = tokio::spawn({
        let journal_base = journal_base.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(build_recovery_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                    hanging_ports,
                    ai_resilience(),
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the live map call reaches the hanging client after Start(1)");
    assert_eq!(hanging_calls.load(Ordering::SeqCst), 1);
    live_task.abort();
    let _ = live_task.await;

    let in_doubt_archive = latest_run_dir(&journal_base);
    let in_doubt_map = stage_envelopes(&in_doubt_archive, "recovery_digest__map").await;
    assert_eq!(
        in_doubt_map
            .iter()
            .filter(|envelope| {
                EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            })
            .count(),
        1
    );
    assert!(in_doubt_map.iter().all(|envelope| {
        envelope.event.event_type() != EFFECT_RECORD_EVENT_TYPE
            && chat_completion_reply(&envelope.event).is_none()
    }));

    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            in_doubt_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_recovery_flow(
            journal_base.clone(),
            resume_outputs.clone(),
            eager_chat_port(resume_calls.clone(), false),
            ai_recovery_rejecting_resilience_for_test(),
        ))
        .await
        .expect("recovery rejection becomes a generated domain terminal");

    assert_eq!(
        resume_calls.load(Ordering::SeqCst),
        0,
        "the rejecting recovery boundary prevents a second physical chat call"
    );
    assert!(resume_outputs
        .lock()
        .expect("resume outputs lock")
        .is_empty());

    let abandonment_archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&abandonment_archive, "recovery_digest__map").await;
    let starts = map
        .iter()
        .filter_map(|envelope| EffectAttemptStarted::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(
        starts
            .iter()
            .map(|started| started.attempt.get())
            .collect::<Vec<_>>(),
        vec![1]
    );
    let abandonments = map
        .iter()
        .filter_map(|envelope| EffectRecoveryAbandoned::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(abandonments.len(), 1);
    assert_eq!(abandonments[0].highest_started_attempt.get(), 1);
    let failures = chunk_failures(&map);
    assert_eq!(failures.len(), 1);
    assert!(matches!(
        &failures[0].cause,
        AiMapReduceRoleFailure::RecoveryAbandoned {
            last_started_attempt: 1,
            ..
        }
    ));
    assert!(!map
        .iter()
        .any(|envelope| chat_completion_reply(&envelope.event).is_some()));

    let collect = stage_envelopes(&abandonment_archive, "recovery_digest__collect").await;
    assert_eq!(
        collect
            .iter()
            .filter_map(|envelope| AiMapReduceJobFailed::from_event(&envelope.event))
            .count(),
        1
    );
    let finalise = stage_envelopes(&abandonment_archive, "recovery_digest__finalize").await;
    assert!(!finalise.iter().any(|envelope| {
        EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            || chat_completion_reply(&envelope.event).is_some()
    }));

    let strict_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            abandonment_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_recovery_flow(
            journal_base.clone(),
            strict_outputs,
            EffectPortRegistry::new(),
            ai_resilience(),
        ))
        .await
        .expect("strict replay rematerialises abandonment without a dependency");
    let strict_archive = latest_run_dir(&journal_base);

    let replay_resolutions = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            abandonment_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_recovery_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            deferred_chat_port(replay_resolutions.clone(), replay_calls.clone(), true),
            ai_recovery_rejecting_resilience_for_test(),
        ))
        .await
        .expect("resume-of-resume treats abandonment as the settled cursor terminal");
    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    let resumed_again_archive = latest_run_dir(&journal_base);

    for replay_archive in [&strict_archive, &resumed_again_archive] {
        let replay_map = stage_envelopes(replay_archive, "recovery_digest__map").await;
        assert_eq!(effect_evidence_ids(&replay_map), effect_evidence_ids(&map));
        let verification = verify_run_dirs(
            &abandonment_archive,
            replay_archive,
            &VerifyOptions {
                write_report: false,
                ..VerifyOptions::default()
            },
        )
        .expect("recovery-composition run verification executes");
        assert_eq!(
            verification.exit_code(),
            0,
            "{}",
            obzenflow_infra::verify::render_verdict(&verification)
        );
    }
}

#[tokio::test]
async fn generated_map_waits_for_all_three_real_edge_credits_before_second_role_call() {
    let temp = tempfile::tempdir().expect("temporary credit-composition journal root");
    let journal_base = temp.path().join("journals");
    let gate = BackpressureAckGate::install("credit_digest__map", "credit_digest__collect", 0)
        .expect("the generated map-to-collector edge freezes before its first acknowledgement");
    let calls = Arc::new(AtomicUsize::new(0));
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let calls = calls.clone();
        let prepare_calls = prepare_calls.clone();
        let outputs = outputs.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(build_credit_flow(
                    journal_base,
                    outputs,
                    eager_chat_port(calls, false),
                    prepare_calls,
                ))
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(10), gate.wait_for_withheld(3))
        .await
        .expect("the first generated continuation fills the three-credit edge");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    let active_archive = latest_run_dir(&journal_base);
    let active_map = stage_envelopes(&active_archive, "credit_digest__map").await;
    assert_eq!(
        active_map
            .iter()
            .filter(|envelope| {
                EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            })
            .count(),
        1
    );

    gate.release(1).expect("first credit returns");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), wait_for_counter(&calls, 2))
            .await
            .is_err(),
        "one returned credit cannot admit the second three-row continuation"
    );
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    gate.release(1).expect("second credit returns");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), wait_for_counter(&calls, 2))
            .await
            .is_err(),
        "two returned credits still cannot admit the second continuation"
    );
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    gate.release(1).expect("third credit returns");
    tokio::time::timeout(Duration::from_secs(5), wait_for_counter(&calls, 2))
        .await
        .expect("all three returned credits admit the second map continuation");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 2);

    gate.open();
    tokio::time::timeout(Duration::from_secs(10), run_task)
        .await
        .expect("the unfrozen real flow terminates")
        .expect("flow task joins")
        .expect("credit-composition flow succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        *outputs.lock().expect("credit outputs lock"),
        vec![DigestOut { total: 3 }]
    );

    let archive = latest_run_dir(&journal_base);
    let map = stage_envelopes(&archive, "credit_digest__map").await;
    assert_eq!(
        map.iter()
            .filter(|envelope| {
                EffectAttemptStarted::event_type_matches(&envelope.event.event_type())
            })
            .count(),
        2
    );
    assert_eq!(
        map.iter()
            .filter(|envelope| chat_completion_reply(&envelope.event).is_some())
            .count(),
        2
    );
}

#[tokio::test]
async fn checked_gate_executes_the_shared_production_hn_flow_live_and_replay() {
    let temp = tempfile::tempdir().expect("temporary real HN-flow journal root");
    let journal_base = temp.path().join("journals");
    let server = mock_server::spawn_mock_hn_server()
        .await
        .expect("deterministic HN server starts");
    let demo_inputs = config::HnRunInputs {
        max_stories: 5,
        poll_timeout_secs: 10,
        source_rate_limit: 1_000.0,
        budget_per_group_override: Some(TokenCount::new(10_000)),
        max_stories_per_group: Some(5),
        interests: Some("runtime protocols".to_string()),
        mode_label: "mock".to_string(),
        base_url: server.base_url(),
    };
    let config_path =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/hn_ai_digest_demo/obzenflow.toml");
    let production_provider = AiProvider::new("ollama");
    let production_target = ChatTarget::with_binding_fingerprint(
        production_provider.clone(),
        "llama3.1:8b",
        chat_binding_fingerprint(
            &production_provider,
            "llama3.1:8b",
            "http://localhost:11434",
        ),
    );
    let live_resolutions = Arc::new(AtomicUsize::new(0));
    let live_calls = Arc::new(AtomicUsize::new(0));

    let live_flow = hn_demo_flow::build_flow_definition(
        demo_inputs.clone(),
        hn_demo_flow::HnFlowOptions {
            journal_base: journal_base.clone(),
            chat_resolver_override: Some(counting_chat_resolver(
                production_target.clone(),
                live_resolutions.clone(),
                live_calls.clone(),
                false,
            )),
        },
    );
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--config"),
            config_path.as_os_str().to_os_string(),
        ])
        .run_async(live_flow)
        .await
        .expect("the shared production HN flow runs live");
    let live_source_requests = server.request_count();
    assert!(
        live_source_requests > 0,
        "the live shared flow must poll the prepared HN fixture"
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
            "shared production HN flow must publish one manifest, found {}",
            manifests.len()
        );
    };
    let map_calls = manifest.chunk_count;
    assert!(map_calls > 0);
    assert_eq!(live_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(live_calls.load(Ordering::SeqCst), map_calls + 1);
    assert_completion_contract(
        &live_map,
        map_calls,
        "ai_map_reduce.map.chat_completion",
        &production_target,
    );
    assert_completion_contract(
        &live_finalise,
        1,
        "ai_map_reduce.finalise.chat_completion",
        &production_target,
    );
    let live_map_ids = effect_evidence_ids(&live_map);
    let live_finalise_ids = effect_evidence_ids(&live_finalise);

    // Replay prepares a fresh host fixture with a different physical endpoint.
    // The live guard can be retired; neither endpoint is durable flow identity.
    drop(server);
    let replay_server = mock_server::spawn_mock_hn_server()
        .await
        .expect("strict replay HN server starts");
    let replay_inputs = config::HnRunInputs {
        base_url: replay_server.base_url(),
        ..demo_inputs
    };
    let replay_resolutions = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_flow = hn_demo_flow::build_flow_definition(
        replay_inputs,
        hn_demo_flow::HnFlowOptions {
            journal_base: journal_base.clone(),
            chat_resolver_override: Some(counting_chat_resolver(
                production_target,
                replay_resolutions.clone(),
                replay_calls.clone(),
                true,
            )),
        },
    );
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--config"),
            config_path.as_os_str().to_os_string(),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(replay_flow)
        .await
        .expect("strict replay rematerialises the shared production HN flow");

    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        replay_server.request_count(),
        0,
        "strict replay may start a fresh host fixture but must not poll it"
    );
    let replay_archive = latest_run_dir(&journal_base);
    let replay_map = stage_envelopes(&replay_archive, "digest__map").await;
    let replay_finalise = stage_envelopes(&replay_archive, "digest__finalize").await;
    assert_eq!(effect_evidence_ids(&replay_map), live_map_ids);
    assert_eq!(effect_evidence_ids(&replay_finalise), live_finalise_ids);

    let verification = verify_run_dirs(
        &live_archive,
        &replay_archive,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .expect("shared production run-directory verification executes");
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
    let port_invocations = manifest.chunk_count + 1;

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
        .filter(|envelope| chat_completion_reply(&envelope.event).is_some())
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
                manifest.chunk_count,
            ),
            (EFFECT_RECORD_EVENT_TYPE.to_string(), port_invocations,),
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
                || chat_completion_reply(&envelope.event).is_some()
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
async fn endpoint_fingerprint_drift_is_rejected_by_effect_history_before_port_resolution() {
    let temp = tempfile::tempdir().expect("temporary endpoint-drift journal root");
    let journal_base = temp.path().join("journals");
    let endpoint_a = bound_fixture_target("http://fixture-a.invalid/v1");
    let endpoint_b = bound_fixture_target("http://fixture-b.invalid/v1");
    assert!(endpoint_a.logically_matches(&endpoint_b));
    assert_ne!(endpoint_a, endpoint_b);

    let live_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_with_behaviour(FlowBehaviour {
            journal_base: journal_base.clone(),
            outputs: Arc::new(Mutex::new(Vec::new())),
            effect_ports: eager_chat_port_for_target(live_calls.clone(), false, endpoint_a.clone()),
            backpressure_window: 3,
            map_request_target: endpoint_a.clone(),
            map_prepare_failure: false,
            map_interpret_failure: false,
            chat_estimator: estimator(),
            chat_target: endpoint_a,
            map_prepare_calls: None,
        }))
        .await
        .expect("the first endpoint-bound run succeeds");
    assert_eq!(live_calls.load(Ordering::SeqCst), 6);
    let live_archive = latest_run_dir(&journal_base);

    let replay_resolutions = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow_with_behaviour(FlowBehaviour {
            journal_base: journal_base.clone(),
            outputs: Arc::new(Mutex::new(Vec::new())),
            effect_ports: {
                let resolver = counting_chat_resolver(
                    endpoint_b.clone(),
                    replay_resolutions.clone(),
                    replay_calls.clone(),
                    true,
                );
                EffectPortRegistry::new()
                    .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, resolver)
                    .expect("endpoint-B replay resolver is registered")
            },
            backpressure_window: 3,
            map_request_target: endpoint_b.clone(),
            map_prepare_failure: false,
            map_interpret_failure: false,
            chat_estimator: estimator(),
            chat_target: endpoint_b,
            map_prepare_calls: None,
        }))
        .await;

    let error = replay_result.expect_err("endpoint drift must invalidate the archived descriptor");
    let failed_archive = latest_run_dir(&journal_base);
    let failure_reason = system_events(&failed_archive)
        .await
        .into_iter()
        .find_map(|event| match event.event {
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Failed {
                reason, ..
            }) => Some(reason),
            _ => None,
        })
        .expect("the failed replay records its pipeline failure reason");
    assert!(
        failure_reason.contains("effect descriptor mismatch"),
        "endpoint drift should surface as replay descriptor divergence: {failure_reason}; application error: {error}"
    );
    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
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
