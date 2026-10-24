// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One-shot AI inference with a replay-safe AI role.
//!
//! Run:
//! `cargo run -p obzenflow --example one_shot_inference_demo --features ai -- \
//!   --config examples/one_shot_inference_demo/obzenflow.toml`
//!
//! The first run calls the configured model once. Re-run with
//! `--replay-from <live-run-dir> --verify` to reconstruct the recorded reply
//! without resolving credentials or contacting the provider.

use anyhow::Result;
use obzenflow::ai::{self, AiOutcome, AiRole, ChatCompletion, ChatCompletionReply, ChatRequestSpec};
use obzenflow::application::FlowApplication;
use obzenflow::error::HandlerError;
use obzenflow::flow::{ai, flow, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::circuit_breaker;
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks;
use obzenflow::stages::sources;
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

#[derive(Clone, Debug)]
struct GenerateBrief;

impl AiRole<ReducedEvidence> for GenerateBrief {
    type Effect = ChatCompletion;
    type Out = DecisionBrief;

    fn prepare(&self, input: &ReducedEvidence) -> Result<ChatRequestSpec, HandlerError> {
        Ok(ChatRequestSpec::new(
            "Produce one concise recommendation using only the supplied evidence.",
            format!(
                "Question: {}\nEvidence:\n- {}",
                input.question,
                input.evidence.join("\n- ")
            ),
        )
        .temperature(0.1)
        .max_tokens(240))
    }

    fn interpret(
        &self,
        input: ReducedEvidence,
        outcome: AiOutcome<ChatCompletionReply>,
    ) -> Result<DecisionBrief, HandlerError> {
        let reply = outcome.replied()?;
        Ok(DecisionBrief {
            question: input.question,
            recommendation: reply.text().to_string(),
        })
    }
}

fn build_flow_definition(input: ReducedEvidence, journal_path: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let evidence = sources::ValuesSource::new([input]);
        let generate_brief = ai::infer(GenerateBrief);
        let ai_breaker = circuit_breaker().consecutive_failures(5);
        let display = sinks::ConsoleSink::new(|brief: &DecisionBrief| {
            format!("{}\n\n{}", brief.question, brief.recommendation.trim())
        });

        Ok(flow! {
            name: "one_shot_inference_demo",
            journals: disk_journals(journal_path),
            stages: {
                evidence = source!(ReducedEvidence => evidence);
                brief = ai!(ReducedEvidence -> DecisionBrief with ai_breaker => generate_brief);
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
