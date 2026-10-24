// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Checked journal witness for a one-shot `ai!` stage built with `ai::infer`.

mod replay_testkit;

#[path = "support/fake_ai.rs"]
pub mod fake_ai;

use fake_ai::FakeChatModels;
use obzenflow::ai::{self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec};
use obzenflow_adapters::middleware::{circuit_breaker, rate_limit, MiddlewareFactory};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{
    ChainEvent, ChainPayload, EffectAttemptStarted, EffectFactOwner, EffectOutcomePayload,
    EffectRecord, PipelineLifecycleEvent, SystemEvent, SystemPayload,
};
use obzenflow_core::{EventId, TypedPayload};
use obzenflow_dsl::{ai, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
};
#[cfg(feature = "test-support")]
use obzenflow_runtime::testing::BackpressureAckGate;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

const CHAT_EFFECT_TYPE: &str = "obzenflow.ai.chat_completion";
const REPLY_CUT_JOURNAL_ENV: &str = "OBZENFLOW_120J_REPLY_CUT_JOURNAL";
const ANSWER: &str = "ship the scalar path";

#[test]
fn one_shot_witness_uses_the_locked_materializer_surface() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(root.join("examples/one_shot_inference_demo/main.rs"))
        .expect("one-shot witness source is readable");

    for required in [
        "FlowDefinition::materialize(move |_runtime_config| {",
        "use obzenflow::stages::sources;",
        "let evidence = sources::ValuesSource::new([input]);",
        "impl AiRole<ReducedEvidence> for GenerateBrief {",
        "let generate_brief = ai::infer(GenerateBrief);",
        "brief = ai!(ReducedEvidence -> DecisionBrief with ai_breaker => generate_brief);",
    ] {
        assert!(
            source.contains(required),
            "one-shot witness must retain the locked source clause: {required}"
        );
    }

    for forbidden in [
        "bindings:",
        "effect_ports",
        "inference!",
        "InferenceHandler",
        "ChatEffectBinding",
        "ai_models()",
        "ai_resilience",
        "via chat",
        "uses at_least_once",
        "std::env",
        "EffectPortResolver",
        "EffectRegistrationBuilder",
        "LogicalEffectBindingName",
        "ResolvedEffectPort",
        "CHAT_CLIENT",
        "bind_deferred_with_metadata",
    ] {
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

#[derive(Clone, Debug)]
struct BriefRole {
    prepare_calls: Arc<AtomicUsize>,
    interpret_calls: Arc<AtomicUsize>,
    prompt_suffix: &'static str,
}

impl BriefRole {
    fn counted(prepare_calls: Arc<AtomicUsize>, interpret_calls: Arc<AtomicUsize>) -> Self {
        Self {
            prepare_calls,
            interpret_calls,
            prompt_suffix: "",
        }
    }

    fn uncounted() -> Self {
        Self::counted(Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)))
    }

    fn user_prompt(&self, input: &ReducedEvidence) -> String {
        format!("decide {}{}", input.value, self.prompt_suffix)
    }
}

impl AiRole<ReducedEvidence> for BriefRole {
    type Effect = ChatCompletion;
    type Out = DecisionBrief;

    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, HandlerError> {
        self.prepare_calls.fetch_add(1, Ordering::SeqCst);
        Ok(ChatRequestSpec::new("Decide.", self.user_prompt(input)))
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        self.interpret_calls.fetch_add(1, Ordering::SeqCst);
        let reply = outcome.replied()?;
        assert_eq!(
            reply.request().user_prompt(),
            Some(self.user_prompt(&input).as_str()),
            "interpretation receives the exact retained request"
        );
        Ok(DecisionBrief {
            value: input.value,
            answer: reply.text().to_string(),
        })
    }
}

#[derive(Clone, Debug)]
struct BriefRoleV2 {
    inner: BriefRole,
}

impl AiRole<ReducedEvidence> for BriefRoleV2 {
    type Effect = ChatCompletion;
    type Out = DecisionBrief;

    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, HandlerError> {
        self.inner.prepare(input)
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        self.inner.interpret(input, outcome)
    }

    fn stage_logic_version(&self) -> &str {
        "2"
    }
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
struct GatedBriefRole {
    inner: BriefRole,
    gate: InterpretationCutGate,
}

impl AiRole<ReducedEvidence> for GatedBriefRole {
    type Effect = ChatCompletion;
    type Out = DecisionBrief;

    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, HandlerError> {
        self.inner.prepare(input)
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        self.gate.park_after_reply();
        self.inner.interpret(input, outcome)
    }
}

#[derive(Clone, Copy, Debug)]
struct ReviewRole;

impl AiRole<DecisionBrief> for ReviewRole {
    type Effect = ChatCompletion;
    type Out = DecisionBrief;

    fn prepare(&self, input: &DecisionBrief) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Review.",
            format!("review decision {}: {}", input.value, input.answer),
        ))
    }

    fn interpret(
        &self,
        input: DecisionBrief,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        let reply = outcome.replied()?;
        Ok(DecisionBrief {
            value: input.value,
            answer: format!("{} / {}", input.answer, reply.text()),
        })
    }
}

#[derive(Debug, Clone)]
struct CollectBrief {
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
}

#[async_trait::async_trait]
impl obzenflow_runtime::stages::sink::SinkConnector for CollectBrief {
    type Input = DecisionBrief;
    type Writer = Self;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("CollectBrief".to_string()))
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
impl obzenflow_runtime::stages::sink::SinkWriter for CollectBrief {
    type Input = DecisionBrief;

    async fn write(
        &mut self,
        output: DecisionBrief,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        self.outputs.lock().expect("brief output lock").push(output);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("CollectBrief".to_string()),
            Some(1),
        )))
    }
}

fn answering() -> FakeChatModels {
    FakeChatModels::replying(|_| ANSWER.to_string())
}

fn expected_brief(value: u64) -> DecisionBrief {
    DecisionBrief {
        value,
        answer: ANSWER.to_string(),
    }
}

fn backpressure_config(journal_base: &Path, mode: &str, window: Option<u64>) -> PathBuf {
    let window = window
        .map(|window| format!("window = {window}\nstall_timeout_ms = 5000\n"))
        .unwrap_or_default();
    let path = journal_base.with_extension("toml");
    std::fs::write(
        &path,
        format!(
            "[runtime.backpressure.flow]\nmode = \"{mode}\"\n{window}\n\
             [ai.models]\nmodel = \"deterministic\"\n"
        ),
    )
    .expect("write the run configuration");
    path
}

fn enforced_config(journal_base: &Path, window: u64) -> PathBuf {
    backpressure_config(journal_base, "enforce", Some(window))
}

fn build_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    prepare_calls: Arc<AtomicUsize>,
    interpret_calls: Arc<AtomicUsize>,
    prompt_suffix: &'static str,
) -> FlowDefinition {
    build_flow_for_role(
        journal_base,
        outputs,
        vec![ReducedEvidence { value: 7 }],
        BriefRole {
            prepare_calls,
            interpret_calls,
            prompt_suffix,
        },
        circuit_breaker().consecutive_failures(5),
    )
}

fn build_flow_for_role<R>(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    evidence_inputs: Vec<ReducedEvidence>,
    role: R,
    brief_policy: impl MiddlewareFactory + 'static,
) -> FlowDefinition
where
    R: AiRole<ReducedEvidence, Effect = ChatCompletion, Out = DecisionBrief>,
{
    FlowDefinition::materialize(move |_runtime_config| {
        let evidence_handler = obzenflow::stages::sources::ValuesSource::new(evidence_inputs);
        let generate_brief = ai::infer(role);
        let collected_handler = CollectBrief { outputs };

        Ok(flow! {
            name: "one_shot_inference_effect_journal",
            journals: disk_journals(journal_base),
            stages: {
                evidence = source!(ReducedEvidence => evidence_handler);
                brief = ai!(ReducedEvidence -> DecisionBrief with brief_policy => generate_brief);
                collected = sink!(DecisionBrief => collected_handler);
            },

            topology: {
                evidence |> brief;
                brief |> collected;
            }
        })
    })
}

fn build_two_stage_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let evidence =
            obzenflow::stages::sources::ValuesSource::new([ReducedEvidence { value: 7 }]);
        let generate_brief = ai::infer(BriefRole::uncounted());
        let review = ai::infer(ReviewRole);
        let collected = CollectBrief { outputs };

        Ok(flow! {
            name: "two_ai_stages",
            journals: disk_journals(journal_base),
            stages: {
                evidence = source!(ReducedEvidence => evidence);
                brief = ai!(ReducedEvidence -> DecisionBrief => generate_brief);
                reviewed = ai!(DecisionBrief -> DecisionBrief => review);
                collected = sink!(DecisionBrief => collected);
            },
            topology: {
                evidence |> brief;
                brief |> reviewed;
                reviewed |> collected;
            }
        })
    })
}

#[cfg(feature = "test-support")]
fn build_credit_flow(
    journal_base: PathBuf,
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
    prepare_calls: Arc<AtomicUsize>,
    interpret_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let generate_brief = ai::infer(BriefRole::counted(prepare_calls, interpret_calls));
        let credit_evidence = obzenflow::stages::sources::ValuesSource::new([
            ReducedEvidence { value: 7 },
            ReducedEvidence { value: 8 },
        ]);
        let credit_collected = CollectBrief { outputs };

        Ok(flow! {
            name: "one_shot_inference_credit_retirement",
            journals: disk_journals(journal_base),
            stages: {
                credit_evidence = source!(ReducedEvidence => credit_evidence);
                credit_brief = ai!(ReducedEvidence -> DecisionBrief => generate_brief);
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
    prepare_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let generate_brief = ai::infer(BriefRole::counted(
            prepare_calls,
            Arc::new(AtomicUsize::new(0)),
        ));
        let fan_out_evidence = obzenflow::stages::sources::ValuesSource::new([
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
            stages: {
                fan_out_evidence = source!(ReducedEvidence => fan_out_evidence);
                fan_out_brief = ai!(ReducedEvidence -> DecisionBrief => generate_brief);
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
    replay_testkit::read_journal_envelopes::<ChainEvent>(&run_dir.join(relative))
        .await
        .into_iter()
        .map(|envelope| envelope.authored())
        .collect()
}

async fn assert_each_stage_journal_has_distinct_event_ids(archive_label: &str, run_dir: &Path) {
    let manifest = archive_manifest(run_dir);
    let stage_keys = manifest["stages"]
        .as_object()
        .expect("manifest stages are an object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    for stage_key in stage_keys {
        let mut seen = BTreeMap::new();
        for event in stage_events(run_dir, &stage_key).await {
            if let Some(first_type) = seen.insert(event.id, event.event_type()) {
                panic!(
                    "{archive_label} commits event {} twice in '{stage_key}' ({first_type}, {})",
                    event.id,
                    event.event_type()
                );
            }
        }
    }
}

async fn system_events(run_dir: &Path) -> Vec<SystemEvent> {
    let manifest = archive_manifest(run_dir);
    let relative = manifest["system_journal_file"]
        .as_str()
        .expect("system journal path");
    replay_testkit::read_journal_envelopes::<SystemEvent>(&run_dir.join(relative))
        .await
        .into_iter()
        .map(|envelope| envelope.authored())
        .collect()
}

fn successful_chat_record(event: &ChainEvent) -> Option<(EffectRecord, serde_json::Value)> {
    let ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) = &event.payload else {
        return None;
    };
    let record = record.clone();
    if record.descriptor.effect_type.as_str() != CHAT_EFFECT_TYPE {
        return None;
    }
    let EffectOutcomePayload::Succeeded { output } = &record.outcome else {
        return None;
    };
    let output = output.clone();
    Some((record, output))
}

fn durable_inference_ids(events: &[ChainEvent]) -> Vec<EventId> {
    let mut ids = events
        .iter()
        .filter(|event| {
            EffectAttemptStarted::matches_event_type(&event.event_type())
                || successful_chat_record(event).is_some()
                || DecisionBrief::matches_event_type(&event.event_type())
        })
        .map(|event| event.id)
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

async fn replay_failure_reason(run_dir: &Path) -> String {
    system_events(run_dir)
        .await
        .into_iter()
        .find_map(|event| match event.payload {
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Failed { reason, .. })
            | SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Cancelled {
                reason, ..
            }) => Some(reason),
            _ => None,
        })
        .expect("failed replay records its typed failure reason")
}

#[cfg(feature = "test-support")]
async fn wait_for_counter(counter: &AtomicUsize, minimum: usize) {
    while counter.load(Ordering::SeqCst) < minimum {
        tokio::task::yield_now().await;
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn two_ai_stages_run_live_and_strict_replay_without_connecting() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    let live = answering();

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(backpressure_config(&journal_base, "off", None))
        .with_ai_models(live.providers())
        .run_async(build_two_stage_flow(journal_base.clone(), live_outputs.clone()))
        .await
        .expect("two ai! stages run live");

    let expected = vec![DecisionBrief {
        value: 7,
        answer: format!("{ANSWER} / {ANSWER}"),
    }];
    assert_eq!(*live_outputs.lock().expect("live output lock"), expected);
    assert_eq!(live.connect_count(), 2, "each stage connects its own binding");
    assert_eq!(live.call_count(), 2);

    let archive = latest_run_dir(&journal_base);
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let replay = FakeChatModels::forbidden();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive.as_os_str().to_os_string(),
            OsString::from("--verify"),
        ])
        .with_config_file(backpressure_config(&journal_base, "off", None))
        .with_ai_models(replay.providers())
        .run_async(build_two_stage_flow(journal_base, replay_outputs.clone()))
        .await
        .expect("strict replay serves both recorded replies");

    assert_eq!(*replay_outputs.lock().expect("replay output lock"), expected);
    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);
}

#[tokio::test]
async fn one_shot_inference_live_and_replay_closure_use_three_rows_without_live_replay_authority() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    let live = answering();
    let live_prepare_calls = Arc::new(AtomicUsize::new(0));
    let live_interpret_calls = Arc::new(AtomicUsize::new(0));

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(live.providers())
        .run_async(build_flow(
            journal_base.clone(),
            live_outputs.clone(),
            live_prepare_calls.clone(),
            live_interpret_calls.clone(),
            "",
        ))
        .await
        .expect("live one-shot inference succeeds");

    let expected = vec![expected_brief(7)];
    assert_eq!(*live_outputs.lock().expect("live outputs"), expected);
    assert_eq!(live_prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(live_interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(live.connect_count(), 1);
    assert_eq!(live.call_count(), 1);

    let live_archive = latest_run_dir(&journal_base);
    assert_each_stage_journal_has_distinct_event_ids("live archive", &live_archive).await;
    let manifest = archive_manifest(&live_archive);
    assert_eq!(
        manifest["bounded_direct_fact_admission"],
        serde_json::json!([{
            "stage_key": "brief",
            "input_event_type": ReducedEvidence::event_type_name(),
            "max_live_data_rows": 3,
        }])
    );
    assert_eq!(manifest["capabilities"]["ai_model_specs"], 1);
    let specs = &manifest["stages"]["brief"]["ai_models"];
    assert_eq!(specs[0]["effect_type"], CHAT_EFFECT_TYPE);
    assert_eq!(specs[0]["endpoint"], "ollama");
    assert_eq!(specs[0]["model"], "deterministic");

    let live_events = stage_events(&live_archive, "brief").await;
    let live_data = live_events
        .iter()
        .filter(|event| event.consumes_data_credit())
        .collect::<Vec<_>>();
    assert_eq!(
        live_data.len(),
        3,
        "one successful scalar invocation has exactly three physical Data rows"
    );
    assert_eq!(
        live_data
            .iter()
            .filter(|event| EffectAttemptStarted::matches_event_type(&event.event_type()))
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
    assert_eq!(reply_record.descriptor.label.as_str(), "ai.infer");
    assert_eq!(
        reply_record.descriptor.schema_version, 3,
        "recorded-reply storage is the ChatCompletion v3 schema"
    );
    assert_eq!(reply["response"]["text"], ANSWER);
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
            .filter(|event| DecisionBrief::matches_event_type(&event.event_type()))
            .count(),
        1
    );
    let live_ids = durable_inference_ids(&live_events);

    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    let replay_prepare_calls = Arc::new(AtomicUsize::new(0));
    let replay_interpret_calls = Arc::new(AtomicUsize::new(0));
    let replay = FakeChatModels::forbidden();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(replay.providers())
        .run_async(build_flow(
            journal_base.clone(),
            replay_outputs.clone(),
            replay_prepare_calls.clone(),
            replay_interpret_calls.clone(),
            "",
        ))
        .await
        .expect("strict replay succeeds without connecting a client");

    assert_eq!(*replay_outputs.lock().expect("replay outputs"), expected);
    assert_eq!(replay_prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(replay_interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);

    let replay_archive = latest_run_dir(&journal_base);
    let replay_events = stage_events(&replay_archive, "brief").await;
    assert_eq!(durable_inference_ids(&replay_events), live_ids);
    assert_each_stage_journal_has_distinct_event_ids("strict replay archive", &replay_archive)
        .await;

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

    let replay_of_replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            replay_archive.as_os_str().to_os_string(),
        ])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(FakeChatModels::forbidden().providers())
        .run_async(build_flow(
            journal_base.clone(),
            replay_of_replay_outputs.clone(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("strict replay of a replay succeeds with no live authority");

    assert_eq!(
        *replay_of_replay_outputs
            .lock()
            .expect("replay-of-replay outputs"),
        expected
    );
    let replay_of_replay_archive = latest_run_dir(&journal_base);
    assert_each_stage_journal_has_distinct_event_ids(
        "replay-of-replay archive",
        &replay_of_replay_archive,
    )
    .await;
}

#[tokio::test]
async fn strict_replay_reads_the_recorded_model_and_never_the_configured_one() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(answering().providers())
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("live archive");
    let live_archive = latest_run_dir(&journal_base);

    let unconfigured = temp.path().join("unconfigured.toml");
    std::fs::write(
        &unconfigured,
        "[runtime.backpressure.flow]\nmode = \"enforce\"\nwindow = 3\nstall_timeout_ms = 5000\n\n\
         [ai.models]\nendpoint = \"nowhere\"\n",
    )
    .expect("write a configuration with no usable model");
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let replay = FakeChatModels::forbidden();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .with_config_file(unconfigured)
        .with_ai_models(replay.providers())
        .run_async(build_flow(
            journal_base,
            outputs.clone(),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("strict replay ignores [ai.*] and uses the recorded model");
    assert_eq!(*outputs.lock().unwrap(), vec![expected_brief(7)]);
    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);
}

#[tokio::test]
async fn inference_windows_one_and_two_fail_before_handler_or_port_resolution() {
    for window in [1_u64, 2] {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let prepare_calls = Arc::new(AtomicUsize::new(0));
        let interpret_calls = Arc::new(AtomicUsize::new(0));
        let models = answering();
        let journal_base = temp.path().join("journals");
        let result = FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_config_file(enforced_config(&journal_base, window))
            .with_ai_models(models.providers())
            .run_async(build_flow(
                journal_base,
                Arc::new(Mutex::new(Vec::new())),
                prepare_calls.clone(),
                interpret_calls.clone(),
                "",
            ))
            .await;

        let error = result.expect_err("an undersized inference window must fail materialisation");
        let detail = error.to_string();
        assert!(detail.contains("ai!"), "{detail}");
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
        assert_eq!(models.connect_count(), 0);
        assert_eq!(models.call_count(), 0);
    }
}

#[tokio::test]
#[cfg(feature = "test-support")]
async fn inference_waits_at_zero_credit_until_all_three_rows_are_retired() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let gate = BackpressureAckGate::install("credit_brief", "credit_collected", 0)
        .expect("the inference output edge freezes before its first acknowledgement");
    let models = answering();
    let calls = models.calls.clone();
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let outputs = Arc::new(Mutex::new(Vec::new()));

    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let prepare_calls = prepare_calls.clone();
        let interpret_calls = interpret_calls.clone();
        let outputs = outputs.clone();
        let providers = models.providers();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .with_config_file(enforced_config(&journal_base, 3))
                .with_ai_models(providers)
                .run_async(build_credit_flow(
                    journal_base,
                    outputs,
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
        vec![expected_brief(7), expected_brief(8)]
    );
}

#[tokio::test]
#[cfg(feature = "test-support")]
async fn inference_fan_out_acquires_once_against_the_slowest_edge() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let slow_gate = BackpressureAckGate::install("fan_out_brief", "slow_collected", 0)
        .expect("the slow fan-out edge freezes before its first acknowledgement");
    let models = answering();
    let calls = models.calls.clone();
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let fast_outputs = Arc::new(Mutex::new(Vec::new()));
    let slow_outputs = Arc::new(Mutex::new(Vec::new()));

    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let prepare_calls = prepare_calls.clone();
        let fast_outputs = fast_outputs.clone();
        let slow_outputs = slow_outputs.clone();
        let providers = models.providers();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .with_config_file(enforced_config(&journal_base, 3))
                .with_ai_models(providers)
                .run_async(build_fan_out_flow(
                    journal_base,
                    fast_outputs,
                    slow_outputs,
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
async fn a_retry_policy_is_refused_before_any_role_or_client_runs() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let models = answering();

    let invalid_journals = temp.path().join("invalid-journals");
    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&invalid_journals, 3))
        .with_ai_models(models.providers())
        .run_async(build_flow_for_role(
            invalid_journals,
            Arc::new(Mutex::new(Vec::new())),
            vec![ReducedEvidence { value: 7 }],
            BriefRole::counted(prepare_calls.clone(), interpret_calls.clone()),
            obzenflow_adapters::middleware::retry(),
        ))
        .await;

    let detail = result
        .expect_err("a non-idempotent AI effect refuses retry")
        .to_string();
    assert!(detail.contains("retry"), "{detail}");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 0);
    assert_eq!(interpret_calls.load(Ordering::SeqCst), 0);
    assert_eq!(models.connect_count(), 0);
    assert_eq!(models.call_count(), 0);

    let corrected_outputs = Arc::new(Mutex::new(Vec::new()));
    let corrected_journals = temp.path().join("corrected-journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&corrected_journals, 3))
        .with_ai_models(models.providers())
        .run_async(build_flow_for_role(
            corrected_journals,
            corrected_outputs.clone(),
            vec![ReducedEvidence { value: 7 }],
            BriefRole::counted(prepare_calls.clone(), interpret_calls.clone()),
            circuit_breaker().consecutive_failures(5),
        ))
        .await
        .expect("the same provider serves a corrected build");

    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(models.connect_count(), 1);
    assert_eq!(models.call_count(), 1);
    assert_eq!(
        *corrected_outputs.lock().expect("corrected output lock"),
        vec![expected_brief(7)]
    );
}

#[tokio::test]
async fn inference_track_and_off_modes_remain_nonblocking_and_locally_bounded() {
    for mode in ["track", "off"] {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let journal_base = temp.path().join("journals");
        let models = answering();
        let outputs = Arc::new(Mutex::new(Vec::new()));

        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .with_config_file(backpressure_config(&journal_base, mode, None))
            .with_ai_models(models.providers())
            .run_async(build_flow_for_role(
                journal_base.clone(),
                outputs.clone(),
                vec![ReducedEvidence { value: 7 }],
                BriefRole::uncounted(),
                circuit_breaker().consecutive_failures(5),
            ))
            .await
            .unwrap_or_else(|error| panic!("{mode} inference should be nonblocking: {error}"));

        assert_eq!(models.call_count(), 1);
        assert_eq!(outputs.lock().expect("mode outputs lock").len(), 1);
        let events = stage_events(&latest_run_dir(&journal_base), "brief").await;
        assert_eq!(
            events
                .iter()
                .filter(|event| event.consumes_data_credit())
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
    let entered = Arc::new(Notify::new());
    let models = FakeChatModels::hanging(entered.clone());

    let run_task = tokio::spawn({
        let journal_base = journal_base.clone();
        let providers = models.providers();
        async move {
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .with_config_file(enforced_config(&journal_base, 3))
                .with_ai_models(providers)
                .run_async(build_flow(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
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

    assert_eq!(models.call_count(), 1);
    let events = stage_events(&latest_run_dir(&journal_base), "brief").await;
    assert_eq!(
        events
            .iter()
            .filter(|event| EffectAttemptStarted::matches_event_type(&event.event_type()))
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
            .all(|event| !DecisionBrief::matches_event_type(&event.event_type())),
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
                .with_config_file(enforced_config(&journal_base, 3))
                .with_ai_models(answering().providers())
                .run_async(build_flow_for_role(
                    journal_base,
                    Arc::new(Mutex::new(Vec::new())),
                    vec![ReducedEvidence { value: 7 }],
                    GatedBriefRole {
                        inner: BriefRole::uncounted(),
                        gate: InterpretationCutGate::new(),
                    },
                    circuit_breaker().consecutive_failures(5),
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
            .filter(|event| EffectAttemptStarted::matches_event_type(&event.event_type()))
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
            .all(|event| !DecisionBrief::matches_event_type(&event.event_type())),
        "the process cut occurs after reply durability and before domain emission"
    );

    let resume_outputs = Arc::new(Mutex::new(Vec::new()));
    let resume = answering();
    let resume_prepare_calls = Arc::new(AtomicUsize::new(0));
    let resume_interpret_calls = Arc::new(AtomicUsize::new(0));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--resume-from"),
            cut_archive.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(resume.providers())
        .run_async(build_flow(
            journal_base.clone(),
            resume_outputs.clone(),
            resume_prepare_calls.clone(),
            resume_interpret_calls.clone(),
            "",
        ))
        .await
        .expect("resume derives the domain fact from the retained reply");

    assert_eq!(resume.connect_count(), 0);
    assert_eq!(resume.call_count(), 0);
    assert_eq!(resume_prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(resume_interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        *resume_outputs.lock().expect("resume outputs lock"),
        vec![expected_brief(7)]
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
    let live = answering();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(live.providers())
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("live archive");
    assert_eq!(live.call_count(), 1);
    let live_archive = latest_run_dir(&journal_base);

    let replay = FakeChatModels::forbidden();
    let result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(replay.providers())
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            " changed",
        ))
        .await;

    let error = result.expect_err("changed canonical request must fail strict replay");
    let detail = replay_failure_reason(&latest_run_dir(&journal_base)).await;
    assert!(
        detail.contains("descriptor mismatch"),
        "typed replay failure should expose descriptor drift: {detail}; application error: {error}"
    );
    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);
}

#[tokio::test]
async fn changed_inference_logic_version_is_replay_divergence_with_identical_request_bytes() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(answering().providers())
        .run_async(build_flow(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            "",
        ))
        .await
        .expect("live version-one archive");
    let live_archive = latest_run_dir(&journal_base);

    let replay = FakeChatModels::forbidden();
    let replay_prepare_calls = Arc::new(AtomicUsize::new(0));
    let result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_archive.as_os_str().to_os_string(),
        ])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(replay.providers())
        .run_async(build_flow_for_role(
            journal_base.clone(),
            Arc::new(Mutex::new(Vec::new())),
            vec![ReducedEvidence { value: 7 }],
            BriefRoleV2 {
                inner: BriefRole::counted(
                    replay_prepare_calls.clone(),
                    Arc::new(AtomicUsize::new(0)),
                ),
            },
            circuit_breaker().consecutive_failures(5),
        ))
        .await;

    let error = result
        .expect_err("a logic-version-only change must invalidate the archived effect descriptor");
    let detail = replay_failure_reason(&latest_run_dir(&journal_base)).await;
    assert!(
        detail.contains("descriptor mismatch"),
        "logic version drift should surface as descriptor divergence: {detail}; application error: {error}"
    );
    assert_eq!(
        replay_prepare_calls.load(Ordering::SeqCst),
        1,
        "the version-two role prepares the same request bytes before descriptor comparison"
    );
    assert_eq!(replay.connect_count(), 0);
    assert_eq!(replay.call_count(), 0);
}

#[tokio::test]
async fn inference_accepts_limiter_only_effect_control() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let prepare_calls = Arc::new(AtomicUsize::new(0));
    let interpret_calls = Arc::new(AtomicUsize::new(0));
    let models = answering();
    let outputs = Arc::new(Mutex::new(Vec::new()));
    let journal_base = temp.path().join("journals");
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(enforced_config(&journal_base, 3))
        .with_ai_models(models.providers())
        .run_async(build_flow_for_role(
            journal_base,
            outputs.clone(),
            vec![ReducedEvidence { value: 7 }],
            BriefRole::counted(prepare_calls.clone(), interpret_calls.clone()),
            rate_limit(100.0),
        ))
        .await
        .expect("a limiter-only plan is valid for an ai! stage");
    assert_eq!(prepare_calls.load(Ordering::SeqCst), 1);
    assert_eq!(interpret_calls.load(Ordering::SeqCst), 1);
    assert_eq!(models.connect_count(), 1);
    assert_eq!(models.call_count(), 1);
    assert_eq!(outputs.lock().expect("output lock").len(), 1);
}
