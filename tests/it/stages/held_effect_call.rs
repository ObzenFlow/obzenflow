// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow::effects::{EffectError, Effects, Port, StageCompletion};
use obzenflow::error::HandlerError;
use obzenflow::flow::{effectful_transform, flow, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::stages::sinks::{
    DeliveryMethod, InlineSink, SinkDescription, SinkRedeliverySafety, SinkWriteFailure,
};
use obzenflow::stages::sources::ValuesSource;
use obzenflow::stages::transforms::EffectfulTransformHandler;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

const HELD_VALUE: u64 = 2;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Number {
    value: u64,
}

impl TypedPayload for Number {
    const EVENT_TYPE: &'static str = "held_effect_call.number";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Answered {
    value: u64,
}

impl TypedPayload for Answered {
    const EVENT_TYPE: &'static str = "held_effect_call.answered";
}

#[derive(Clone, Debug, Serialize)]
struct Ask {
    value: u64,
}

#[obzenflow::port]
#[async_trait]
trait AnswerPort: Send + Sync {
    #[effect(type = "held_effect_call.answer", exactly_once)]
    async fn answer(&self, request: &Ask) -> Result<Answered, EffectError>;
}

struct HeldAnswers {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

#[obzenflow::system("held-answers")]
#[async_trait]
impl AnswerPort for HeldAnswers {
    async fn answer(&self, request: &Ask) -> Result<Answered, EffectError> {
        if request.value == HELD_VALUE {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(Answered {
            value: request.value,
        })
    }
}

#[obzenflow::command]
#[derive(Clone, Debug)]
struct AnswerNumbers {
    #[port]
    answers: Port<dyn AnswerPort>,
}

#[async_trait]
impl EffectfulTransformHandler for AnswerNumbers {
    type Input = Number;
    type Output = obzenflow::schema::stage_fact_set![Answered];

    async fn process(
        &self,
        number: Number,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        fx.call(Ask {
            value: number.value,
        })
        .await
        .map_err(|error| HandlerError::Other(error.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "held-effect-call-v1"
    }
}

#[derive(Clone, Debug, Default)]
struct Collect {
    arrived: Arc<Notify>,
}

#[async_trait]
impl InlineSink for Collect {
    type Input = Answered;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _answered: Answered) -> Result<(), SinkWriteFailure> {
        self.arrived.notify_one();
        Ok(())
    }
}

fn build(journal_base: PathBuf, answers: Port<dyn AnswerPort>, sink: Collect) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let numbers_source =
            ValuesSource::new((1..=3).map(|value| Number { value }).collect::<Vec<_>>());
        let answer_numbers = AnswerNumbers {
            answers: answers.clone(),
        };
        let collector = sink.clone();
        let journals = journal_base.clone();

        Ok(flow! {
            name: "held_effect_call_test",
            journals: disk_journals(journals),

            stages: {
                numbers = source!(Number => numbers_source);
                answer = effectful_transform!(Number -> { Answered } => answer_numbers);
                collector = sink!(Answered => collector);
            },

            topology: {
                numbers |> answer;
                answer |> collector;
            }
        })
    })
}

fn journal_bytes(dir: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            total += journal_bytes(&path)?;
        } else if path.extension().is_some_and(|extension| extension == "log") {
            total += std::fs::metadata(&path)?.len();
        }
    }
    Ok(total)
}

#[tokio::test]
async fn flow_effect_call_held_appends_nothing() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let sink = Collect::default();
    let answers = Port::to(HeldAnswers {
        entered: entered.clone(),
        release: release.clone(),
    });
    let handle = build(journal_base.clone(), answers, sink.clone())
        .build(replay_testkit::unsynced_context())
        .await
        .map_err(|error| anyhow!("{error:?}"))?;
    handle.start().await?;

    tokio::time::timeout(Duration::from_secs(10), entered.notified()).await?;
    tokio::time::timeout(Duration::from_secs(10), sink.arrived.notified()).await?;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let run = replay_testkit::latest_run_dir(&journal_base);
    let before = journal_bytes(&run)?;
    tokio::time::sleep(Duration::from_millis(2_500)).await;
    let after = journal_bytes(&run)?;

    release.notify_one();
    tokio::time::timeout(Duration::from_secs(15), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the pipeline to finish"))??;
    assert_eq!(after, before);
    Ok(())
}
