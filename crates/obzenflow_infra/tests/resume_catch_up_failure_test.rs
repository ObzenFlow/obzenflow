// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: catch-up failure aborts the resume loudly.
//!
//! Records a two-source fan-in flow, corrupts one source's archived journal
//! mid-file, and resumes. The corrupted source's catch-up fails, it never
//! authors its watermark, and the resume aborts through normal stage-failure
//! propagation (FLOWIP-120n F5). No live input is admitted through the
//! fan-in: the merge blocks on the failed source's recorded generation, so
//! its journal shows no live-range outputs and no authored watermark, the
//! effect boundary executes nothing, and the sink delivers nothing. The
//! healthy source's own journal is deliberately not asserted live-free: the
//! per-stage flip is local by design and the fan-in is where admission is
//! blocked.

use crate::replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::sinks::CountingSink;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::{EffectError, Effects};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, TypedTransformHandler,
};
use obzenflow_runtime::systems::Port;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChannelTick {
    channel: String,
    value: u64,
}

impl TypedPayload for ChannelTick {
    const EVENT_TYPE: &'static str = "resume_abort.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Merged {
    channel: String,
    value: u64,
}

impl TypedPayload for Merged {
    const EVENT_TYPE: &'static str = "resume_abort.merged";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_abort.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct AbortOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for AbortOutput {
    const EVENT_TYPE: &'static str = "resume_abort.output";
}

#[derive(Clone, Debug)]
struct MergeTransform;

impl MergeTransform {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for MergeTransform {
    type Input = ChannelTick;
    type Output = Merged;

    fn process(&self, tick: ChannelTick) -> Result<Merged, HandlerError> {
        Ok(Merged {
            channel: tick.channel,
            value: tick.value,
        })
    }
}

#[obzenflow_runtime::effects::system("call-counter")]
struct CallCounter {
    calls: Arc<AtomicUsize>,
}

#[derive(Clone, Debug, Serialize)]
struct Counting {
    value: u64,
}

#[obzenflow_runtime::effects::port]
impl CallCounter {
    #[effect(type = "resume_abort.counting", exactly_once)]
    fn count(&self, request: &Counting) -> Result<EffectValue, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EffectValue {
            effect_value: request.value + 1000,
        })
    }
}

#[obzenflow_runtime::effects::command]
#[derive(Clone, Debug)]
struct EffectfulTail {
    #[port]
    counter: Port<CallCounter>,
}

#[async_trait]
impl EffectfulTransformHandler for EffectfulTail {
    type Input = Merged;
    type Output = obzenflow_core::outcome![AbortOutput | EffectValue];

    async fn process(
        &self,
        input: Merged,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .call(Counting { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(AbortOutput {
            channel: input.channel,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "resume-abort-effectful-v1"
    }
}

struct SourceRanges {
    a_first: u64,
    a_count: u64,
    b_first: u64,
    b_count: u64,
}

fn build_flow(
    journal_base: PathBuf,
    ranges: &SourceRanges,
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    let a_first = ranges.a_first;
    let a_count = ranges.a_count;
    let b_first = ranges.b_first;
    let b_count = ranges.b_count;
    FlowDefinition::materialize(move |_runtime_config| {
        let channel_a =
            replay_testkit::preloaded_input((a_first..a_first + a_count).map(|value| {
                ChannelTick {
                    channel: "a".to_string(),
                    value,
                }
            }));
        let channel_b =
            replay_testkit::preloaded_input((b_first..b_first + b_count).map(|value| {
                ChannelTick {
                    channel: "b".to_string(),
                    value,
                }
            }));
        let merge_transform = MergeTransform::new();
        let effectful_tail = EffectfulTail {
            counter: Port::to(CallCounter { calls }),
        };
        let counting_sink = CountingSink::<AbortOutput>::new(delivered);

        Ok(flow! {
            name: "resume_abort",
            journals: disk_journals(journal_base),

            stages: {
                src_a = source!(ChannelTick => channel_a);
                src_b = source!(ChannelTick => channel_b);
                merge = transform!(ChannelTick -> Merged => merge_transform);
                effectful = effectful_transform!(
                    Merged -> { AbortOutput, EffectValue } => effectful_tail);
                collect = sink!(AbortOutput => counting_sink);
            },

            topology: {
                src_a |> merge;
                src_b |> merge;
                merge |> effectful;
                effectful |> collect;
            }
        })
    })
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))?
}

async fn wait_for_count(counter: &Arc<AtomicU64>, target: u64) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| {
        anyhow!(
            "timeout waiting for the sink to consume {target} events (saw {})",
            counter.load(Ordering::SeqCst)
        )
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn corrupted_source_archive_aborts_the_resume_before_any_live_admission() -> Result<()> {
    // Recorded: a emits 1..=4, b emits 101..=104; the Kahn merge delivers 7
    // of the 8 before blocking on a quiet input, the recording is cancelled.
    const RECORDED_DELIVERED: u64 = 7;
    let recorded_ranges = SourceRanges {
        a_first: 1,
        a_count: 4,
        b_first: 101,
        b_count: 4,
    };
    let live_ranges = SourceRanges {
        a_first: 5,
        a_count: 2,
        b_first: 105,
        b_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Record and cancel.
    {
        let calls = Arc::new(AtomicUsize::new(0));
        let delivered = Arc::new(AtomicU64::new(0));
        let handle = build_flow(
            journal_base.clone(),
            &recorded_ranges,
            calls.clone(),
            delivered.clone(),
        )
        .build(replay_testkit::unsynced_context())
        .await
        .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        wait_for_count(&delivered, RECORDED_DELIVERED).await?;
        handle.stop().await?;
        tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    }
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // Corrupt src_b's archived journal mid-file: mangle the frame carrying
    // b:102 while later frames stay intact, so the failure is unambiguous
    // corruption rather than a tolerated torn tail (FLOWIP-120q).
    let manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        recorded_run.join("run_manifest.json"),
    )?)?;
    let src_b_journal = manifest["stages"]["src_b"]["data_journal_file"]
        .as_str()
        .expect("manifest names src_b's data journal");
    let journal_path = recorded_run.join(src_b_journal);
    obzenflow_infra::testing::journal::corrupt_chain_frame(
        &recorded_run,
        &journal_path,
        |records| {
            records
                .iter()
                .any(|record| record["payload"]["value"] == 102)
        },
    )
    .map_err(anyhow::Error::from_boxed)?;

    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let failure = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        build_flow(
            journal_base.clone(),
            &live_ranges,
            calls.clone(),
            delivered.clone(),
        )
        .build(replay_testkit::unsynced_context())
        .await
        .err()
        .ok_or_else(|| anyhow!("a corrupted source archive must refuse the resume"))?
    };
    let failure_reason = format!("{:?}", failure.error);
    assert!(
        failure_reason.contains("src_b"),
        "the failure must name the corrupted source's stage: {failure_reason}"
    );
    assert!(
        failure_reason.contains("checksum mismatch"),
        "the failure must name the corruption: {failure_reason}"
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no effect executed for the refused resume"
    );
    assert_eq!(
        delivered.load(Ordering::SeqCst),
        0,
        "nothing reached the sink for the refused resume"
    );

    Ok(())
}
