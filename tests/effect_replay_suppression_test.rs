// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl::{effectful_async_transform, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulAsyncTransformHandler, FiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReplayInput {
    value: u64,
}

impl TypedPayload for ReplayInput {
    const EVENT_TYPE: &'static str = "effect_replay.input";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReplayOutput {
    value: u64,
    effect_value: u64,
}

impl TypedPayload for ReplayOutput {
    const EVENT_TYPE: &'static str = "effect_replay.output";
}

#[derive(Clone, Debug)]
struct ReplaySource {
    next_value: u64,
    writer_id: WriterId,
}

impl ReplaySource {
    fn new() -> Self {
        Self {
            next_value: 1,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for ReplaySource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next_value > 3 {
            return Ok(None);
        }

        let value = self.next_value;
        self.next_value += 1;

        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            ReplayInput::EVENT_TYPE,
            json!(ReplayInput { value }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "effect_replay.counting";
    const SCHEMA_VERSION: u32 = 1;

    type Output = u64;

    fn label(&self) -> Cow<'static, str> {
        Cow::Borrowed("counting")
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Output, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.value + 100)
    }

    fn safety(&self) -> EffectSafety {
        EffectSafety::NonIdempotentRequiresKey
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("counting:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct ReplayTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulAsyncTransformHandler for ReplayTransform {
    type Input = ReplayInput;
    type Output = ReplayOutput;

    async fn process(
        &self,
        input: ReplayInput,
        fx: &mut Effects,
    ) -> Result<ReplayOutput, HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;

        Ok(ReplayOutput {
            value: input.value,
            effect_value,
        })
    }

    fn stage_logic_version(&self) -> Cow<'static, str> {
        Cow::Borrowed("effect-replay-v1")
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(output) = ReplayOutput::from_event(&event) {
            self.outputs
                .lock()
                .expect("outputs lock poisoned")
                .push(output);
        }

        Ok(DeliveryPayload::success(
            "collector",
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }
}

fn build_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    outputs: Arc<Mutex<Vec<ReplayOutput>>>,
) -> FlowDefinition {
    flow! {
        name: "effect_replay_suppression",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            inputs = source!(ReplayInput => ReplaySource::new());
            effectful = effectful_async_transform!(ReplayInput -> ReplayOutput => ReplayTransform { calls });
            collector = sink!(ReplayOutput => CollectSink { outputs });
        },

        topology: {
            inputs |> effectful;
            effectful |> collector;
        }
    }
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries
        .pop()
        .expect("live run should have produced a replay archive")
}

#[tokio::test]
async fn effectful_transform_replay_suppresses_effect_execution() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    let live_calls = Arc::new(AtomicUsize::new(0));
    let live_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journal_base.clone(),
            live_calls.clone(),
            live_outputs.clone(),
        ))
        .await
        .expect("live flow should complete");

    assert_eq!(live_calls.load(Ordering::SeqCst), 3);
    let live_domain_outputs = live_outputs.lock().expect("outputs lock poisoned").clone();
    assert_eq!(
        live_domain_outputs,
        vec![
            ReplayOutput {
                value: 1,
                effect_value: 101
            },
            ReplayOutput {
                value: 2,
                effect_value: 102
            },
            ReplayOutput {
                value: 3,
                effect_value: 103
            },
        ]
    );

    let archive_dir = latest_run_dir(&journal_base);
    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_outputs = Arc::new(Mutex::new(Vec::new()));
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            archive_dir.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(
            journal_base,
            replay_calls.clone(),
            replay_outputs.clone(),
        ))
        .await
        .expect("replay flow should complete");

    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "replay must return recorded effect results without executing"
    );
    assert_eq!(
        replay_outputs
            .lock()
            .expect("outputs lock poisoned")
            .clone(),
        live_domain_outputs,
        "replay should emit the same domain payloads"
    );
}
