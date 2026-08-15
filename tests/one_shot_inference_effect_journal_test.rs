// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120j checked journal witness for scalar `inference!`.

use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_adapters::middleware::{MiddlewareFactory, RateLimiterFactory};
use obzenflow_core::ai::{
    AiClientError, AiInferenceRole, AiRoleLogicFailure, ChatBindingContract, ChatClient,
    ChatCompletionReply, ChatMessage, ChatParams, ChatRequest, ChatRequestSpec, ChatResponse,
    ChatTarget, HeuristicTokenEstimator, ResolvedTokenEstimator, TokenEstimatorFallbackReason,
    TokenEstimatorResolutionInfo, CHAT_CLIENT_PORT,
};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, EffectAttemptStarted, EffectFactOwner, EffectOutcomePayload,
    EffectRecord, PipelineLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{id::StageId, EventId, SystemId, TypedPayload};
use obzenflow_dsl::dsl::backpressure_clause::{
    enforced as enforced_backpressure, off as backpressure_off, track_only as track_backpressure,
    BackpressureClause,
};
use obzenflow_dsl::{flow, inference, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::{
    EffectPortRegistry, EffectPortResolver, SinkRedeliverySafety, EFFECT_RECORD_EVENT_TYPE,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
};
#[cfg(feature = "test-support")]
use obzenflow_runtime::testing::BackpressureAckGate;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const CHAT_EFFECT_TYPE: &str = "obzenflow.ai.chat_completion";
const REPLY_CUT_JOURNAL_ENV: &str = "OBZENFLOW_120J_REPLY_CUT_JOURNAL";

#[test]
fn one_shot_witness_uses_the_locked_materializer_surface() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(root.join("examples/one_shot_inference_demo/main.rs"))
        .expect("one-shot witness source is readable");

    for required in [
        "FlowDefinition::materialize(move |runtime_config| {",
        "let ai_models = runtime_config.ai_models();",
        "ChatEffectBinding::from_config(&ai_models)",
        "let evidence_source = sources::finite([input]);",
        "let brief_role = BriefRole;",
        "effect_ports,",
    ] {
        assert!(
            source.contains(required),
            "one-shot witness must retain the locked source clause: {required}"
        );
    }

    for forbidden in ["bindings:", "effect_ports: effect_ports,", "std::env"] {
        assert!(
            !source.contains(forbidden),
            "one-shot builder must not regain the retired source spelling: {forbidden}"
        );
    }
}

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
    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
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

struct BriefRoleV2 {
    inner: BriefRole,
}

impl AiInferenceRole<ReducedEvidence, DecisionBrief> for BriefRoleV2 {
    const LOGIC_VERSION: &'static str = "2";

    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        self.inner.prepare(input)
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<DecisionBrief, AiRoleLogicFailure> {
        self.inner.interpret(input, request, reply)
    }
}

#[derive(Clone)]
struct InterpretationCutGate {
    released: Arc<(Mutex<bool>, Condvar)>,
}

impl InterpretationCutGate {
    fn new() -> Self {
        Self {
            released: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    fn park_after_reply(&self) {
        let (released, wake) = &*self.released;
        let mut released = released.lock().expect("interpretation gate lock");
        while !*released {
            released = wake.wait(released).expect("interpretation gate wait");
        }
    }
}

struct GatedBriefRole {
    inner: BriefRole,
    gate: InterpretationCutGate,
}

impl AiInferenceRole<ReducedEvidence, DecisionBrief> for GatedBriefRole {
    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
        self.inner.prepare(input)
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        request: ChatRequestSpec,
        reply: ChatCompletionReply,
    ) -> Result<DecisionBrief, AiRoleLogicFailure> {
        self.gate.park_after_reply();
        self.inner.interpret(input, request, reply)
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

#[derive(Debug)]
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

    async fn chat(&self, request: ChatRequest) -> Result<ChatResponse, AiClientError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(request.target(), self.target);
        self.entered.notify_one();
        std::future::pending().await
    }
}

#[derive(Debug, Clone)]
struct CollectBrief {
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
}

#[async_trait]
impl InlineSink for CollectBrief {
    type Input = DecisionBrief;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified().with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(
        &mut self,
        output: DecisionBrief,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.outputs.lock().expect("brief output lock").push(output);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("CollectBrief".to_string()),
            Some(1),
        )))
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
        .expect("test chat target and estimator models agree")
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
    let brief_role = BriefRole {
        prepare_calls,
        interpret_calls,
        prompt_suffix,
    };
    build_flow_for_role(
        journal_base,
        outputs,
        effect_ports,
        enforced_backpressure(backpressure_window).stall_timeout_ms(5_000),
        vec![ReducedEvidence { value: 7 }],
        brief_role,
        ai_resilience(),
    )
}

fn build_flow_for_role<Role>(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    effect_ports: EffectPortRegistry,
    backpressure: BackpressureClause,
    evidence_inputs: Vec<ReducedEvidence>,
    brief_role: Role,
    brief_policy: Box<dyn MiddlewareFactory>,
) -> FlowDefinition
where
    Role: AiInferenceRole<ReducedEvidence, DecisionBrief>,
{
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = contract();
        let evidence_handler = obzenflow::typed::sources::finite(evidence_inputs);
        let collected_handler = CollectBrief { outputs };

        Ok(flow! {
            name: "one_shot_inference_effect_journal",
            journals: disk_journals(journal_base),
            backpressure: backpressure,
            effect_ports,

            stages: {
                evidence = source!(ReducedEvidence => evidence_handler);
                brief = inference!(
                    ReducedEvidence ->{
                        at_least_once(ChatCompletion)
                            via chat
                            with brief_policy
                    } DecisionBrief => brief_role
                );
                collected = sink!(DecisionBrief => collected_handler);
            },

            topology: {
                evidence |> brief;
                brief |> collected;
            }
        })
    })
}

#[cfg(feature = "test-support")]
fn build_credit_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    effect_ports: EffectPortRegistry,
    prepare_calls: Arc<AtomicUsize>,
    interpret_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = contract();
        let brief_role = BriefRole {
            prepare_calls,
            interpret_calls,
            prompt_suffix: "",
        };
        let credit_evidence = obzenflow::typed::sources::finite([
            ReducedEvidence { value: 7 },
            ReducedEvidence { value: 8 },
        ]);
        let credit_collected = CollectBrief { outputs };

        Ok(flow! {
            name: "one_shot_inference_credit_retirement",
            journals: disk_journals(journal_base),
            backpressure: enforced_backpressure(3).stall_timeout_ms(5_000),
            effect_ports,

            stages: {
                credit_evidence = source!(ReducedEvidence => credit_evidence);
                credit_brief = inference!(
                    ReducedEvidence ->{
                        at_least_once(ChatCompletion)
                            via chat
                            with ai_resilience()
                    } DecisionBrief => brief_role
                );
                credit_collected = sink!(DecisionBrief => credit_collected);
            },

            topology: {
                credit_evidence |> credit_brief;
                credit_brief |> credit_collected;
            }
        })
    })
}

#[cfg(feature = "test-support")]
fn build_fan_out_flow(
    journal_base: PathBuf,
    fast_outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    slow_outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    effect_ports: EffectPortRegistry,
    prepare_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let chat = contract();
        let brief_role = BriefRole {
            prepare_calls,
            interpret_calls: Arc::new(AtomicUsize::new(0)),
            prompt_suffix: "",
        };
        let fan_out_evidence = obzenflow::typed::sources::finite([
            ReducedEvidence { value: 7 },
            ReducedEvidence { value: 8 },
        ]);
        let fast_collected = CollectBrief {
            outputs: fast_outputs,
        };
        let slow_collected = CollectBrief {
            outputs: slow_outputs,
        };

        Ok(flow! {
            name: "one_shot_inference_fan_out",
            journals: disk_journals(journal_base),
            backpressure: enforced_backpressure(3).stall_timeout_ms(5_000),
            effect_ports,

            stages: {
                fan_out_evidence = source!(ReducedEvidence => fan_out_evidence);
                fan_out_brief = inference!(
                    ReducedEvidence ->{
                        at_least_once(ChatCompletion)
                            via chat
                            with ai_resilience()
                    } DecisionBrief => brief_role
                );
                fast_collected = sink!(DecisionBrief => fast_collected);
                slow_collected = sink!(DecisionBrief => slow_collected);
            },

            topology: {
                fan_out_evidence |> fan_out_brief;
                fan_out_brief |> fast_collected;
                fan_out_brief |> slow_collected;
            }
        })
    })
}

fn try_latest_run_dir(base: &Path) -> Option<PathBuf> {
    let mut entries = std::fs::read_dir(base.join("flows"))
        .ok()?
        .map(|entry| entry.expect("flow archive entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    entries.sort();
    entries.pop()
}

fn latest_run_dir(base: &Path) -> PathBuf {
    try_latest_run_dir(base).expect("completed flow archive")
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

#[cfg(feature = "test-support")]
async fn wait_for_counter(counter: &AtomicUsize, minimum: usize) {
    while counter.load(Ordering::SeqCst) < minimum {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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
    assert_eq!(
        reply_record.descriptor.schema_version, 3,
        "recorded-reply storage is the ChatCompletion v3 schema"
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
#[cfg(feature = "test-support")]
async fn inference_waits_at_zero_credit_until_all_three_rows_are_retired() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let gate = BackpressureAckGate::install("credit_brief", "credit_collected", 0)
        .expect("the inference output edge freezes before its first acknowledgement");
    let calls = Arc::new(AtomicUsize::new(0));
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let calls = calls.clone();
        let prepare_calls = prepare_calls.clone();
        let interpret_calls = interpret_calls.clone();
        let outputs = outputs.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(build_credit_flow(
                    journal_base,
                    outputs,
                    live_chat_registry(Arc::new(AtomicUsize::new(0)), calls),
                    prepare_calls,
                    interpret_calls,
                ))
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(10), gate.wait_for_withheld(3))
        .await
        .expect("the first inference fills the three-credit edge");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(interpret_calls.load(Ordering::SeqCst), 1);

    for returned in 1..=2 {
        gate.release(1).expect("one physical credit returns");
        assert!(
            tokio::time::timeout(
                Duration::from_millis(150),
                wait_for_counter(&prepare_calls, 2),
            )
            .await
            .is_err(),
            "{returned} returned credits cannot admit another three-row inference"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    gate.release(1).expect("the third physical credit returns");
    tokio::time::timeout(Duration::from_secs(5), wait_for_counter(&prepare_calls, 2))
        .await
        .expect("all three returned credits admit the second inference");
    tokio::time::timeout(Duration::from_secs(5), wait_for_counter(&calls, 2))
        .await
        .expect("the second inference reaches its chat effect");

    gate.open();
    tokio::time::timeout(Duration::from_secs(10), run_task)
        .await
        .expect("the unfrozen scalar flow terminates")
        .expect("flow task joins")
        .expect("credit-retirement flow succeeds");

    assert_eq!(interpret_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *outputs.lock().expect("credit outputs lock"),
        vec![
            DecisionBrief {
                value: 7,
                answer: "ship the scalar path".to_string(),
            },
            DecisionBrief {
                value: 8,
                answer: "ship the scalar path".to_string(),
            },
        ]
    );
}

#[tokio::test]
#[cfg(feature = "test-support")]
async fn inference_fan_out_acquires_once_against_the_slowest_edge() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let slow_gate = BackpressureAckGate::install("fan_out_brief", "slow_collected", 0)
        .expect("the slow fan-out edge freezes before its first acknowledgement");
    let calls = Arc::new(AtomicUsize::new(0));
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let fast_outputs = Arc::new(Mutex::new(Vec::new()));
    let slow_outputs = Arc::new(Mutex::new(Vec::new()));

    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let calls = calls.clone();
        let prepare_calls = prepare_calls.clone();
        let fast_outputs = fast_outputs.clone();
        let slow_outputs = slow_outputs.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(build_fan_out_flow(
                    journal_base,
                    fast_outputs,
                    slow_outputs,
                    live_chat_registry(Arc::new(AtomicUsize::new(0)), calls),
                    prepare_calls,
                ))
                .await
        }
    });

    tokio::time::timeout(Duration::from_secs(10), slow_gate.wait_for_withheld(3))
        .await
        .expect("the first inference fills the slow edge");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);

    slow_gate.release(2).expect("two slow-edge credits return");
    assert!(
        tokio::time::timeout(
            Duration::from_millis(150),
            wait_for_counter(&prepare_calls, 2),
        )
        .await
        .is_err(),
        "a fully acknowledged fast edge cannot bypass the slow edge's remaining debt"
    );

    slow_gate
        .release(1)
        .expect("the slow edge's third credit returns");
    tokio::time::timeout(Duration::from_secs(5), wait_for_counter(&prepare_calls, 2))
        .await
        .expect("the minimum participating edge admits the second inference");

    slow_gate.open();
    tokio::time::timeout(Duration::from_secs(10), run_task)
        .await
        .expect("the fan-out flow terminates")
        .expect("flow task joins")
        .expect("fan-out inference succeeds");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(fast_outputs.lock().expect("fast output lock").len(), 2);
    assert_eq!(slow_outputs.lock().expect("slow output lock").len(), 2);
}

#[tokio::test]
async fn inference_rejects_a_non_resilience_policy_before_role_or_port_resolution() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let resolutions = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));

    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow_for_role(
            temp.path().join("journals"),
            Arc::new(Mutex::new(Vec::new())),
            live_chat_registry(resolutions.clone(), calls.clone()),
            enforced_backpressure(3).stall_timeout_ms(5_000),
            vec![ReducedEvidence { value: 7 }],
            BriefRole {
                prepare_calls: prepare_calls.clone(),
                interpret_calls: interpret_calls.clone(),
                prompt_suffix: "",
            },
            Box::new(RateLimiterFactory::new(1.0)),
        ))
        .await;

    let detail = result
        .expect_err("inference! requires its one EffectResilience policy")
        .to_string();
    assert!(detail.contains("inference!"), "{detail}");
    assert!(detail.contains("generated stage 'brief'"), "{detail}");
    assert!(
        detail.contains("requires exactly one EffectResilience"),
        "{detail}"
    );
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
    assert_eq!(interpret_calls.load(Ordering::SeqCst), 0);
    assert_eq!(resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn inference_track_and_off_modes_remain_nonblocking_and_locally_bounded() {
    for (mode, backpressure) in [("track", track_backpressure()), ("off", backpressure_off())] {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let journal_base = temp.path().join("journals");
        let calls = Arc::new(AtomicUsize::new(0));
        let outputs = Arc::new(Mutex::new(Vec::new()));

        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_flow_for_role(
                journal_base.clone(),
                outputs.clone(),
                live_chat_registry(Arc::new(AtomicUsize::new(0)), calls.clone()),
                backpressure,
                vec![ReducedEvidence { value: 7 }],
                BriefRole {
                    prepare_calls: Arc::new(AtomicUsize::new(0)),
                    interpret_calls: Arc::new(AtomicUsize::new(0)),
                    prompt_suffix: "",
                },
                ai_resilience(),
            ))
            .await
            .unwrap_or_else(|error| panic!("{mode} inference should be nonblocking: {error}"));

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(outputs.lock().expect("mode outputs lock").len(), 1);
        let events = stage_events(&latest_run_dir(&journal_base), "brief").await;
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event.content, ChainEventContent::Data { .. }))
                .count(),
            3,
            "{mode} retains the descriptor-proved local three-row bound"
        );
    }
}

#[tokio::test]
async fn cancelling_an_active_inference_leaves_only_committed_physical_debt() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let effect_ports = EffectPortRegistry::new()
        .with_port::<dyn ChatClient>(
            CHAT_CLIENT_PORT,
            Arc::new(HangingChatClient {
                target: target(),
                calls: calls.clone(),
                entered: entered.clone(),
            }),
        )
        .expect("one hanging chat client");

    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(build_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                    effect_ports,
                    3,
                    Arc::new(AtomicUsize::new(0)),
                    Arc::new(AtomicUsize::new(0)),
                    "",
                ))
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), entered.notified())
        .await
        .expect("the inference reaches its hanging provider after Start(1)");
    run_task.abort();
    let _ = run_task.await;

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let events = stage_events(&latest_run_dir(&journal_base), "brief").await;
    assert_eq!(
        events
            .iter()
            .filter(|event| EffectAttemptStarted::event_type_matches(&event.event_type()))
            .count(),
        1,
        "the durable attempt start remains physical debt"
    );
    assert!(
        events
            .iter()
            .all(|event| successful_chat_record(event).is_none()),
        "cancellation before provider return commits no reply"
    );
    assert!(
        events
            .iter()
            .all(|event| !DecisionBrief::event_type_matches(&event.event_type())),
        "cancellation commits no domain output"
    );
}

#[test]
#[ignore = "subprocess fixture; run by resume_after_a_durable_reply_reinterprets"]
fn reply_cut_child_process() {
    let journal_base = std::env::var_os(REPLY_CUT_JOURNAL_ENV)
        .map(PathBuf::from)
        .expect("reply-cut child receives its journal root");
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("reply-cut child runtime");
    runtime
        .block_on(
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(build_flow_for_role(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                    live_chat_registry(
                        Arc::new(AtomicUsize::new(0)),
                        Arc::new(AtomicUsize::new(0)),
                    ),
                    enforced_backpressure(3).stall_timeout_ms(5_000),
                    vec![ReducedEvidence { value: 7 }],
                    GatedBriefRole {
                        inner: BriefRole {
                            prepare_calls: Arc::new(AtomicUsize::new(0)),
                            interpret_calls: Arc::new(AtomicUsize::new(0)),
                            prompt_suffix: "",
                        },
                        gate: InterpretationCutGate::new(),
                    },
                    ai_resilience(),
                )),
        )
        .expect("the parent process terminates this parked fixture");
}

#[tokio::test]
async fn resume_after_a_durable_reply_reinterprets_without_another_chat_call() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--ignored")
        .arg("--exact")
        .arg("reply_cut_child_process")
        .arg("--test-threads=1")
        .env(REPLY_CUT_JOURNAL_ENV, &journal_base)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("reply-cut child starts");

    let observed = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(archive) = try_latest_run_dir(&journal_base) {
                let events = stage_events(&archive, "brief").await;
                if let Some(reply) = events
                    .iter()
                    .find(|event| successful_chat_record(event).is_some())
                {
                    return (archive, reply.id);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let (cut_archive, reply_id) = match observed {
        Ok(observed) => observed,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            panic!("reply-cut child did not commit its durable reply: {error}");
        }
    };
    child.kill().expect("the process cut terminates the child");
    child.wait().expect("the reply-cut child is reaped");

    let cut_events = stage_events(&cut_archive, "brief").await;
    assert_eq!(
        cut_events
            .iter()
            .filter(|event| EffectAttemptStarted::event_type_matches(&event.event_type()))
            .count(),
        1
    );
    assert_eq!(
        cut_events
            .iter()
            .filter(|event| successful_chat_record(event).is_some())
            .count(),
        1
    );
    assert!(
        cut_events
            .iter()
            .all(|event| !DecisionBrief::event_type_matches(&event.event_type())),
        "the process cut occurs after reply durability and before domain emission"
    );

    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    let resume_resolutions = Arc::new(AtomicUsize::new(0));
    let resume_calls = Arc::new(AtomicUsize::new(0));
    let resume_prepare_calls = Arc::new(AtomicUsize::new(0));
    let resume_interpret_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            cut_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            journal_base.clone(),
            resume_outputs.clone(),
            live_chat_registry(resume_resolutions.clone(), resume_calls.clone()),
            3,
            resume_prepare_calls.clone(),
            resume_interpret_calls.clone(),
            "",
        ))
        .await
        .expect("resume derives the domain fact from the retained reply");

    assert_eq!(resume_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(resume_calls.load(Ordering::SeqCst), 0);
    assert_eq!(resume_prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(resume_interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *resume_outputs.lock().expect("resume outputs lock"),
        vec![DecisionBrief {
            value: 7,
            answer: "ship the scalar path".to_string(),
        }]
    );
    let resumed_events = stage_events(&latest_run_dir(&journal_base), "brief").await;
    assert!(
        resumed_events
            .iter()
            .any(|event| event.id == reply_id && successful_chat_record(event).is_some()),
        "resume preserves the physical identity of the retained reply"
    );
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

#[tokio::test]
async fn changed_inference_logic_version_is_replay_divergence_with_identical_request_bytes() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            live_chat_registry(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))),
            3,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("live version-one archive");
    let live_archive = latest_run_dir(&journal_base);

    let replay_resolutions = Arc::new(AtomicUsize::new(0));
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_prepare_calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .run_async(build_flow_for_role(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            live_chat_registry(replay_resolutions.clone(), replay_calls.clone()),
            enforced_backpressure(3).stall_timeout_ms(5_000),
            vec![ReducedEvidence { value: 7 }],
            BriefRoleV2 {
                inner: BriefRole {
                    prepare_calls: replay_prepare_calls.clone(),
                    interpret_calls: Arc::new(AtomicUsize::new(0)),
                    prompt_suffix: "",
                },
            },
            ai_resilience(),
        ))
        .await;

    let error = result
        .expect_err("a logic-version-only change must invalidate the archived effect descriptor");
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
        "logic version drift should surface as descriptor divergence: {detail}; application error: {error}"
    );
    assert_eq!(
        replay_prepare_calls.load(Ordering::SeqCst),
        1,
        "the version-two role prepares the same request bytes before descriptor comparison"
    );
    assert_eq!(replay_resolutions.load(Ordering::SeqCst), 0);
    assert_eq!(replay_calls.load(Ordering::SeqCst), 0);
}
