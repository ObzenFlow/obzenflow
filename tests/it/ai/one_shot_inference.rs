// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::fake_ai;

use fake_ai::FakeChatModels;
use obzenflow::ai::{
    self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec,
};
use obzenflow_adapters::middleware::{circuit_breaker, MiddlewareFactory};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{ai, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{perform, EffectError, Port};
use obzenflow_runtime::runtime_config::JournalSync;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const ANSWER: &str = "ship the scalar path";

#[test]
fn one_shot_witness_uses_the_locked_materializer_surface() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = std::fs::read_to_string(root.join("examples/one_shot_inference_demo/main.rs"))
        .expect("one-shot witness source is readable");

    for required in [
        "FlowDefinition::materialize(move |_| {",
        "use obzenflow::stages::sources;",
        "let evidence = Port::owned(sources::ValuesSource::new([input]));",
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
        "CHAT_MODEL",
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
        call: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        self.interpret_calls.fetch_add(1, Ordering::SeqCst);
        let reply = call.replied()?;
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
        call: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        let reply = call.replied()?;
        Ok(DecisionBrief {
            value: input.value,
            answer: format!("{} / {}", input.answer, reply.text()),
        })
    }
}

#[obzenflow_runtime::effects::port]
trait CollectBriefPort: Send + Sync {
    #[effect(type = "one_shot_fixture.collect", at_least_once, records = reply)]
    fn collect(&self, brief: &DecisionBrief) -> Result<(), EffectError>;
}

struct CollectBrief {
    outputs: Arc<Mutex<Vec<DecisionBrief>>>,
}

#[obzenflow_runtime::effects::system("brief-collector")]
impl CollectBriefPort for CollectBrief {
    fn collect(&self, brief: &DecisionBrief) -> Result<(), EffectError> {
        self.outputs
            .lock()
            .expect("brief output lock")
            .push(brief.clone());
        Ok(())
    }
}

fn collect_into(outputs: Arc<Mutex<Vec<DecisionBrief>>>) -> Port<dyn CollectBriefPort> {
    Port::to(CollectBrief { outputs })
}

fn answering() -> FakeChatModels {
    FakeChatModels::replying(|_| ANSWER.to_string())
}

fn backpressure_config(
    journal_base: &Path,
    mode: &str,
    window: Option<u64>,
    sync: JournalSync,
) -> PathBuf {
    let window = window
        .map(|window| format!("window = {window}\nstall_timeout_ms = 5000\n"))
        .unwrap_or_default();
    let path = journal_base.with_extension("toml");
    std::fs::write(
        &path,
        format!(
            "[journal]\nsync = \"{}\"\n\n\
             [runtime.backpressure.flow]\nmode = \"{mode}\"\n{window}\n\
             [ai.models]\nmodel = \"deterministic\"\n",
            sync.as_token()
        ),
    )
    .expect("write the run configuration");
    path
}

fn enforced_config(journal_base: &Path, window: u64) -> PathBuf {
    backpressure_config(journal_base, "enforce", Some(window), JournalSync::Off)
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
        let evidence_handler = Port::owned(obzenflow::stages::sources::ValuesSource::new(
            evidence_inputs,
        ));
        let generate_brief = ai::infer(role);
        let collected_handler =
            perform(|brief: &DecisionBrief| brief.clone()).through(collect_into(outputs));

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
            obzenflow::effects::Port::owned(obzenflow::stages::sources::ValuesSource::new([
                ReducedEvidence { value: 7 },
            ]));
        let generate_brief = ai::infer(BriefRole::uncounted());
        let review = ai::infer(ReviewRole);
        let collected =
            perform(|brief: &DecisionBrief| brief.clone()).through(collect_into(outputs));

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

fn ai_config(journal_base: &Path, body: &str) -> PathBuf {
    let path = journal_base.with_extension("toml");
    std::fs::write(&path, format!("[journal]\nsync = \"off\"\n\n{body}"))
        .expect("write the run configuration");
    path
}

#[tokio::test]
async fn an_embeddings_table_for_a_chat_stage_fails_the_build() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");
    let live = answering();

    let error = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .with_config_file(ai_config(
            &journal_base,
            "[ai.models]\nmodel = \"deterministic\"\n\n\
             [ai.embeddings.stages.brief]\nmodel = \"embedding-model\"\n",
        ))
        .with_ai_models(live.resolvers())
        .run_async(build_two_stage_flow(
            journal_base,
            Arc::new(Mutex::new(Vec::new())),
        ))
        .await
        .expect_err("`brief` generates no embeddings");
    assert!(
        error
            .to_string()
            .contains("config: [ai.embeddings.stages.brief] names no AI stage in this flow"),
        "{error}"
    );
    assert_eq!(live.connect_count() + live.call_count(), 0);
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
            .with_ai_models(models.resolvers())
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
