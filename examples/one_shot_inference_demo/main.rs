// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One-shot AI inference with a replay-safe, target-free role.
//!
//! Run:
//! `cargo run -p obzenflow --example one_shot_inference_demo --features ai -- \
//!   --config examples/one_shot_inference_demo/obzenflow.toml`
//!
//! The first run calls the configured model once. Re-run with
//! `--replay-from <live-run-dir> --verify` to reconstruct the recorded reply
//! without resolving credentials or contacting the provider.

use anyhow::Result;
use obzenflow::ai::ChatEffectBinding;
use obzenflow::typed::{ai, sinks, sources};
use obzenflow_adapters::middleware::control::ai_resilience;
use obzenflow_core::ai::{
    AiRoleLogicFailure, ChatCompletionReply, ChatMessage, ChatParams, ChatRequestSpec,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, inference, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReducedEvidence {
    question: String,
    evidence: Vec<String>,
}

impl TypedPayload for ReducedEvidence {
    const EVENT_TYPE: &'static str = "demo.one_shot.reduced_evidence";
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DecisionBrief {
    question: String,
    recommendation: String,
}

impl TypedPayload for DecisionBrief {
    const EVENT_TYPE: &'static str = "demo.one_shot.decision_brief";
}

fn prepare_brief(input: &ReducedEvidence) -> Result<ChatRequestSpec, AiRoleLogicFailure> {
    Ok(ChatRequestSpec {
        messages: vec![
            ChatMessage::system(
                "Produce one concise recommendation using only the supplied evidence.",
            ),
            ChatMessage::user(format!(
                "Question: {}\nEvidence:\n- {}",
                input.question,
                input.evidence.join("\n- ")
            )),
        ],
        params: ChatParams {
            temperature: Some(0.1),
            max_tokens: Some(240),
            ..ChatParams::default()
        },
        tools: Vec::new(),
        response_format: None,
    })
}

fn interpret_brief(
    input: ReducedEvidence,
    _request: ChatRequestSpec,
    reply: ChatCompletionReply,
) -> Result<DecisionBrief, AiRoleLogicFailure> {
    Ok(DecisionBrief {
        question: input.question,
        recommendation: reply.response.text,
    })
}

fn build_flow_definition(input: ReducedEvidence, journal_path: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |runtime_config| {
        let chat = ChatEffectBinding::from_config(&runtime_config.ai_models())?;
        let evidence = sources::once(input);
        let generate_brief = ai::inference_handler(prepare_brief, interpret_brief);
        let display = sinks::console(|brief: &DecisionBrief| {
            format!("{}\n\n{}", brief.question, brief.recommendation.trim())
        });

        Ok(flow! {
            name: "one_shot_inference_demo",
            journals: disk_journals(journal_path),
            stages: {
                evidence = source!(ReducedEvidence => evidence);
                brief = inference!(
                    ReducedEvidence -> DecisionBrief
                    uses at_least_once(ChatCompletion)
                        via chat
                        with ai_resilience()
                    => generate_brief
                );
                display = sink!(DecisionBrief => display);
            },

            topology: {
                evidence |> brief;
                brief |> display;
            }
        })
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let input = ReducedEvidence {
        question: "Should the release use one-shot inference or map-reduce?".to_string(),
        evidence: vec![
            "The input is already reduced and bounded.".to_string(),
            "Exactly one model decision is required.".to_string(),
            "No fan-out or fan-in is needed.".to_string(),
        ],
    };

    FlowApplication::builder()
        .run_async(build_flow_definition(
            input,
            PathBuf::from("target/one_shot_inference_demo_journal"),
        ))
        .await?;

    Ok(())
}
