// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120j checked journal witness for scalar `inference!`.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_core::ai::{
    AiClientError, AiInferenceRole, AiRoleLogicFailure, ChatBindingContract, ChatClient,
    ChatCompletionReply, ChatMessage, ChatParams, ChatRequest, ChatRequestSpec, ChatResponse,
    ChatTarget, HeuristicTokenEstimator, ResolvedTokenEstimator, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo, CHAT_CLIENT_PORT,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, EffectAttemptStarted, EffectFactOwner, EffectOutcomePayload,
    EffectRecord, PipelineLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{id::StageId, EventId, SystemId, TypedPayload};
use obzenflow_dsl::{flow, inference, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::{
    EffectPortRegistry, EffectPortResolver, SinkDeliverySafety, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::SinkHandler;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const CHAT_EFFECT_TYPE: &str = "obzenflow.ai.chat_completion";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ReducedEvidence {
    value: u64,
}

impl TypedPayload for ReducedEvidence {
    const EVENT_TYPE: &'static str = "flowip_120j.reduced_evidence";
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DecisionBrief {
    value: u64,
    answer: String,
}

impl TypedPayload for DecisionBrief {
    const EVENT_TYPE: &'static str = "flowip_120j.decision_brief";
}

struct BriefRole {
    prepare_calls: Arc<AtomicUsize>,
    interpret_calls: Arc<AtomicUsize>,
    prompt_suffix: &'static str,
}

impl AiInferenceRole<ReducedEvidence, DecisionBrief> for BriefRole {
    fn prepare(
        &self,
        input: &ReducedEvidence,
    ) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ChatRequestSpec {
            messages: vec![ChatMessage::user(format!(
                "decide {}{}",
                input.value, self.prompt_suffix
            ))],
            params: ChatParams::default(),
            tools: Vec::new(),
            response_format: None,
        })
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<DecisionBrief, AiRoleLogicFailure> {
        self.interpret_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request.messages,
            vec![ChatMessage::user(format!(
                "decide {}{}",
                input.value, self.prompt_suffix
            ))],
            "interpretation receives the exact retained target-free request"
        );
        Ok(DecisionBrief {
            value: input.value,
            answer: reply.response.text,
        })
    }
}

#[derive(Debug)]
struct CountingChatClient {
    target: ChatTarget,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl ChatClient for CountingChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.target(), self.target);
        Ok(ChatResponse {
            text: "ship the scalar path".to_string(),
            tool_calls: Vec::new(),
            usage: None,
            raw: None,
        })
    }
}

#[derive(Debug, Clone)]
struct CollectBrief {
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
}

#[async_trait]
impl SinkHandler for CollectBrief {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(output) = DecisionBrief::from_event(&event) {
            self.outputs
                .lock()
                .expect("brief output lock")
                .push(output);
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("CollectBrief".to_string()),
            Some(1),
        ))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
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

fn contract() -> ChatBindingContract {
    ChatBindingContract::from_resolved(target(), estimator())
}

fn live_chat_registry(
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
            }) as Arc<dyn ChatClient>)
        })
    });
    EffectPortRegistry::new()
        .with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, resolver)
        .expect("one chat resolver")
}

fn build_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    effect_ports: EffectPortRegistry,
    backpressure_window: u64,
    prepare_calls: Arc<AtomicUsize>,
    interpret_calls: Arc<AtomicUsize>,
    prompt_suffix: &'static str,
) -> FlowDefinition {
    let chat = contract();
    let brief_role = BriefRole {
        prepare_calls,
        interpret_calls,
        prompt_suffix,
    };
    flow! {
        name: "one_shot_inference_effect_journal",
        journals: disk_journals(journal_base),
        middleware: [],
        backpressure: obzenflow_dsl::dsl::backpressure_clause::enforced(backpressure_window)
            .stall_timeout_ms(5_000),
        effect_ports: effect_ports,

        stages: {
            evidence = source!(
                ReducedEvidence => obzenflow::typed::sources::finite([
                    ReducedEvidence { value: 7 },
                ])
            );
            brief = inference!(
                ReducedEvidence ->{
                    at_least_once(ChatCompletion)
                        via chat
                        with { ai_resilience() }
                } DecisionBrief => brief_role
            );
            collected = sink!(DecisionBrief => CollectBrief { outputs });
        },

        topology: {
            evidence |> brief;
            brief |> collected;
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

async fn stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
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
        .into_iter()
        .map(|envelope| envelope.event)
        .collect()
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

fn successful_chat_record(event: &ChainEvent) -> Option<(EffectRecord, ChatCompletionReply)> {
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
    if record.descriptor.effect_type.as_str() != CHAT_EFFECT_TYPE {
        return None;
    }
    let EffectOutcomePayload::Succeeded { output } = &record.outcome else {
        return None;
    };
    let reply = serde_json::from_value(output.clone()).ok()?;
    Some((record, reply))
}

fn durable_inference_ids(events: &[ChainEvent]) -> Vec<EventId> {
    let mut ids = events
        .iter()
        .filter(|event| {
            EffectAttemptStarted::event_type_matches(&event.event_type())
                || successful_chat_record(event).is_some()
                || DecisionBrief::event_type_matches(&event.event_type())
        })
        .map(|event| event.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

#[tokio::test]
async fn one_shot_inference_live_and_strict_replay_use_three_rows_and_no_live_replay_authority() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    let live_resolutions = Arc::new(AtomicUsize::new(0));
    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_prepare_calls = Arc::new(AtomicUsize::new(0));
    let live_interpret_calls = Arc::new(AtomicUsize::new(0));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_outputs.clone(),
            live_chat_registry(live_resolutions.clone(), live_calls.clone()),
            3,
            live_prepare_calls.clone(),
            live_interpret_calls.clone(),
            "",
        ))
        .await
        .expect("live one-shot inference succeeds");

    let expected = vec![DecisionBrief {
        value: 7,
        answer: "ship the scalar path".to_string(),
    }];
    assert_eq!(*live_outputs.lock().expect("live outputs"), expected);
    assert_eq!(live_prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(live_interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(live_resolutions.load(Ordering::SeqCst), 1);
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);

    let live_archive = latest_run_dir(&journal_base);
    let manifest = archive_manifest(&live_archive);
    assert_eq!(
        manifest["bounded_direct_fact_admission"],
        serde_json::json!([{
            "stage_key": "brief",
            "input_event_type": ReducedEvidence::versioned_event_type(),
            "max_live_data_rows": 3,
        }])
    );

    let live_events = stage_events(&live_archive, "brief").await;
    let live_data = live_events
        .iter()
        .filter(|event| matches!(event.content, ChainEventContent::Data { .. }))
        .collect::<Vec<_>>();
    assert_eq!(
        live_data.len(),
        3,
        "one successful scalar invocation has exactly three physical Data rows"
    );
    assert_eq!(
        live_data
            .iter()
            .filter(|event| EffectAttemptStarted::event_type_matches(&event.event_type()))
            .count(),
        1
    );
    let replies = live_data
        .iter()
        .filter_map(|event| successful_chat_record(event))
        .collect::<Vec<_>>();
    let [(reply_record, reply)] = replies.as_slice() else {
        panic!("one framework-owned recorded reply is required");
    };
    assert_eq!(
        reply_record.descriptor.label.as_str(),
        "inference.chat_completion"
    );
    assert_eq!(reply.response.text, "ship the scalar path");
    assert!(matches!(
        reply_record.outcome,
        EffectOutcomePayload::Succeeded { .. }
    ));
    let reply_event = live_data
        .iter()
        .find(|event| successful_chat_record(event).is_some())
        .expect("reply event");
    assert_eq!(
        reply_event
            .effect_provenance
            .as_ref()
            .map(|provenance| provenance.fact_owner),
        Some(EffectFactOwner::Framework)
    );
    assert_eq!(
        live_data
            .iter()
            .filter(|event| DecisionBrief::event_type_matches(&event.event_type()))
            .count(),
        1
    );
    assert!(
        live_events.iter().all(|event| {
            !matches!(
                event.event_type().as_str(),
                "ai.chat_completion.completed" | "ai.chat_completion.completed.v1"
            )
        }),
        "new runs never write the historical domain-fact reply type"
    );
    let live_ids = durable_inference_ids(&live_events);

    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let replay_prepare_calls = Arc::new(AtomicUsize::new(0));
    let replay_interpret_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            replay_outputs.clone(),
            EffectPortRegistry::new(),
            3,
            replay_prepare_calls.clone(),
            replay_interpret_calls.clone(),
            "",
        ))
        .await
        .expect("strict replay succeeds with an empty executable registry");

    assert_eq!(*replay_outputs.lock().expect("replay outputs"), expected);
    assert_eq!(replay_prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(replay_interpret_calls.load(Ordering::SeqCst), 1);

    let replay_archive = latest_run_dir(&journal_base);
    let replay_events = stage_events(&replay_archive, "brief").await;
    assert_eq!(durable_inference_ids(&replay_events), live_ids);

    let verification = verify_run_dirs(
        &live_archive,
        &replay_archive,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .expect("run-directory verification executes");
    assert_eq!(
        verification.exit_code(),
        0,
        "{}",
        obzenflow_infra::verify::render_verdict(&verification)
    );
}

#[tokio::test]
async fn inference_windows_one_and_two_fail_before_role_or_port_resolution() {
    for window in [1_u64, 2] {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let prepare_calls = Arc::new(AtomicUsize::new(0));
        let interpret_calls = Arc::new(AtomicUsize::new(0));
        let resolutions = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let result = FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_flow(
                temp.path().join("journals"),
                Arc::new(Mutex::new(Vec::new())),
                live_chat_registry(resolutions.clone(), calls.clone()),
                window,
                prepare_calls.clone(),
                interpret_calls.clone(),
                "",
            ))
            .await;

        let error = result.expect_err("an undersized inference window must fail materialisation");
        let detail = error.to_string();
        assert!(detail.contains("inference!"), "{detail}");
        assert!(detail.contains("generated stage 'brief'"), "{detail}");
        assert!(
            detail.contains("requires 3 live physical Data credits"),
            "{detail}"
        );
        assert!(
            detail.contains(&format!("resolved enforced window {window}")),
            "{detail}"
        );
        assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
        assert_eq!(interpret_calls.load(Ordering::SeqCst), 0);
        assert_eq!(resolutions.load(Ordering::SeqCst), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn changed_inference_request_is_replay_divergence_before_port_resolution() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            live_chat_registry(Arc::new(AtomicUsize::new(0)), live_calls.clone()),
            3,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("live archive");
    assert_eq!(live_calls.load(Ordering::SeqCst), 1);
    let live_archive = latest_run_dir(&journal_base);

    let replay_resolutions = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            live_chat_registry(replay_resolutions.clone(), replay_calls.clone()),
            3,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            " changed",
        ))
        .await;

    let error = result.expect_err("changed canonical request must fail strict replay");
    let failed_archive = latest_run_dir(&journal_base);
    let detail = system_events(&failed_archive)
        .await
        .into_iter()
        .find_map(|event| match event.event {
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Failed {
                reason, ..
            })
            | SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Cancelled {
                reason,
                ..
            }) => Some(reason),
            _ => None,
        })
        .expect("failed replay records its typed failure reason");
    assert!(
        detail.contains("descriptor mismatch"),
        "typed replay failure should expose descriptor drift: {detail}; application error: {error}"
    );
    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
}
