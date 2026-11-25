// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: mixed finite and infinite sources under resume.
//!
//! One finite source (authors EOF at natural exhaustion) and one infinite
//! source feed a deterministic fan-in. Under resume the finite source behaves
//! as replay: it re-admits its rows, re-authors EOF, and authors no catch-up
//! watermark. The infinite source authors its watermark and continues live.
//! The fan-in must still author its own watermark, counting the EOF-exhausted
//! input as vacuously crossed (the F17 disjunction), and go live. All
//! assertions read the event-sourced journals.

use crate::replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::rate_limit;
use obzenflow_adapters::sinks::CountingSink;
use obzenflow_adapters::sources::{ChannelSource, ValuesSource};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, ReattachGuarantee, SourceReattachedFact,
};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{
    ChainEvent, ChainPayload, JournalRecord, PipelineLifecycleEvent, SourceDiagnosticReason,
    SystemEvent, SystemPayload,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_infra::systems::file::{CsvDecodeError, CsvDecoder, CsvRecord, CsvSource};
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::{EffectError, Effects};
use obzenflow_runtime::events::{source_channel, SourceReceiver};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::queries::FiniteRead;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::SourceRead;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::source::ResumeRefusal;
use obzenflow_runtime::systems::Port;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChannelTick {
    channel: String,
    value: u64,
}

impl TypedPayload for ChannelTick {
    const EVENT_TYPE: &'static str = "resume_mixed.tick";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Merged {
    channel: String,
    value: u64,
}

impl TypedPayload for Merged {
    const EVENT_TYPE: &'static str = "resume_mixed.merged";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_mixed.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct MixedOutput {
    channel: String,
    value: u64,
    effect_value: u64,
}

impl TypedPayload for MixedOutput {
    const EVENT_TYPE: &'static str = "resume_mixed.output";
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

struct CallCounter {
    calls: Arc<AtomicUsize>,
}

/// The effectful descendant that makes the fan-in a deterministic orderer.
#[derive(Clone, Debug, Serialize)]
struct Counting {
    value: u64,
}

#[obzenflow_runtime::effects::port]
impl CallCounter {
    #[effect(type = "resume_mixed.counting", exactly_once)]
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
    type Output = obzenflow_core::outcome![MixedOutput | EffectValue];

    async fn process(
        &self,
        input: Merged,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<obzenflow_runtime::effects::StageCompletion<Self::Output>, HandlerError> {
        let effect_value = fx
            .call(Counting { value: input.value })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(MixedOutput {
            channel: input.channel,
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(fx.complete()?)
    }

    fn stage_logic_version(&self) -> &str {
        "resume-mixed-effectful-v1"
    }
}

/// Ranges for one run: the finite channel f and the infinite channel i.
struct SourceRanges {
    fin_first: u64,
    fin_count: u64,
    inf_first: u64,
    inf_count: u64,
}

fn build_flow(
    journal_base: PathBuf,
    ranges: &SourceRanges,
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    let fin_first = ranges.fin_first;
    let fin_count = ranges.fin_count;
    let inf_first = ranges.inf_first;
    let inf_count = ranges.inf_count;
    FlowDefinition::materialize(move |_runtime_config| {
        let finite_handler = ValuesSource::new(
            (fin_first..fin_first + fin_count)
                .map(|value| ChannelTick {
                    channel: "f".to_string(),
                    value,
                })
                .collect::<Vec<_>>(),
        );
        let infinite_handler =
            replay_testkit::preloaded_input((inf_first..inf_first + inf_count).map(|value| {
                ChannelTick {
                    channel: "i".to_string(),
                    value,
                }
            }));
        let merge_handler = MergeTransform::new();
        let effectful_handler = EffectfulTail {
            counter: Port::to(CallCounter { calls }),
        };
        let collect_handler = CountingSink::<MixedOutput>::new(delivered);

        Ok(flow! {
            name: "resume_mixed",
            journals: disk_journals(journal_base),

            stages: {
                fin = source!(ChannelTick => finite_handler);
                inf = source!(ChannelTick => infinite_handler);
                merge = transform!(ChannelTick -> Merged => merge_handler);
                effectful = effectful_transform!(
                    Merged -> { MixedOutput, EffectValue } => effectful_handler);
                collect = sink!(MixedOutput => collect_handler);
            },

            topology: {
                fin |> merge;
                inf |> merge;
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

async fn run_until_delivered(
    journal_base: &Path,
    ranges: &SourceRanges,
    expected: u64,
) -> Result<usize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(
        journal_base.to_path_buf(),
        ranges,
        calls.clone(),
        delivered.clone(),
    )
    .build(replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, expected).await?;
    handle.stop().await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(calls.load(Ordering::SeqCst))
}

/// A journal reduced to data payloads, catch-up watermarks, reattachment
/// evidence and authored EOF, in physical append order.
#[derive(Debug, Clone, PartialEq)]
enum MixedRow {
    Data(serde_json::Value),
    CatchUp { stage_key: String, generation: u64 },
    Reattached(SourceReattachedFact),
    Eof,
}

fn mixed_rows(envelopes: &[JournalRecord<ChainPayload>]) -> Vec<MixedRow> {
    envelopes
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Fact(_) | ChainPayload::CompositeData(_) => {
                Some(MixedRow::Data(envelope.payload().clone()))
            }
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
                generation,
                stage_key,
            }) => Some(MixedRow::CatchUp {
                stage_key: stage_key.to_string(),
                generation: generation.0,
            }),
            ChainPayload::Execution(ExecutionPayload::SourceReattached(fact)) => {
                Some(MixedRow::Reattached(fact.clone()))
            }
            ChainPayload::FlowControl(FlowControlPayload::Eof { .. }) => Some(MixedRow::Eof),
            _ => None,
        })
        .collect()
}

fn tick_rows(channel: &str, values: std::ops::RangeInclusive<u64>) -> Vec<MixedRow> {
    values
        .map(|value| {
            MixedRow::Data(json!(ChannelTick {
                channel: channel.to_string(),
                value,
            }))
        })
        .collect()
}

fn merged_payload_set(channel: &str, values: std::ops::RangeInclusive<u64>) -> BTreeSet<String> {
    values
        .map(|value| {
            json!(Merged {
                channel: channel.to_string(),
                value,
            })
            .to_string()
        })
        .collect()
}

fn payload_set(rows: &[MixedRow]) -> BTreeSet<String> {
    rows.iter()
        .filter_map(|row| match row {
            MixedRow::Data(payload) => Some(payload.to_string()),
            _ => None,
        })
        .collect()
}

/// The F17 vacuous-EOF crossing on the path where the finite source's EOF is
/// delivered at the fan-in before the infinite source's watermark: the finite
/// channel is shorter, so its authored EOF takes a lower merge ordinal than
/// the watermark and crosses first.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_sources_resume_flips_the_fan_in_across_a_vacuous_eof_crossing() -> Result<()> {
    // Recorded: f emits 1..=3 then EOF, i emits 101..=104 then idles. All 7
    // recorded inputs deliver (seq-ordered fan-in, FLOWIP-120n F18). Live:
    // i emits 105..=106.
    const RECORDED: u64 = 7;
    const LIVE: u64 = 2;
    let recorded_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 3,
        inf_first: 101,
        inf_count: 4,
    };
    let live_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 3,
        inf_first: 105,
        inf_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let recorded_calls = run_until_delivered(&journal_base, &recorded_ranges, RECORDED).await?;
    assert_eq!(recorded_calls, RECORDED as usize);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    let resumed_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        run_until_delivered(&journal_base, &live_ranges, LIVE).await?
    };
    assert_eq!(
        resumed_calls, LIVE as usize,
        "catch-up re-executes zero effects; only the live tail runs"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // The finite source behaves as replay: re-admitted rows, re-authored EOF,
    // no watermark.
    let fin_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "fin").await);
    let mut expected_fin = tick_rows("f", 1..=3);
    expected_fin.push(MixedRow::Eof);
    assert_eq!(
        fin_rows, expected_fin,
        "the finite source re-admits its rows and re-authors EOF, no watermark"
    );

    // The infinite source authors its watermark, records its declared
    // guarantee before its first live record (FLOWIP-122i), and continues live.
    let inf_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "inf").await);
    let mut expected_inf = tick_rows("i", 101..=104);
    expected_inf.push(MixedRow::CatchUp {
        stage_key: "inf".to_string(),
        generation: 1,
    });
    expected_inf.push(MixedRow::Reattached(SourceReattachedFact {
        generation: 1,
        guarantee: ReattachGuarantee::AtMostOnce,
    }));
    expected_inf.extend(tick_rows("i", 105..=106));
    assert_eq!(
        inf_rows, expected_inf,
        "the infinite source authors its watermark and guarantee between prefix and live tail"
    );

    let merge_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "merge").await);
    let boundary = merge_rows
        .iter()
        .position(|row| {
            matches!(
                row,
                MixedRow::CatchUp { stage_key, generation } if stage_key == "merge" && *generation == 1
            )
        })
        .unwrap_or_else(|| panic!("the fan-in must author its watermark: {merge_rows:#?}"));
    assert_eq!(
        merge_rows
            .iter()
            .filter(|row| matches!(row, MixedRow::CatchUp { .. }))
            .count(),
        1,
        "the fan-in authors exactly one watermark"
    );
    let data_before = merge_rows[..boundary]
        .iter()
        .filter(|row| matches!(row, MixedRow::Data(_)))
        .count();
    assert_eq!(
        data_before as u64, RECORDED,
        "every recorded input is processed before the fan-in's boundary: {merge_rows:#?}"
    );
    assert!(
        merge_rows[..boundary]
            .iter()
            .any(|row| matches!(row, MixedRow::Eof)),
        "the finite EOF crossed the fan-in before its boundary: {merge_rows:#?}"
    );
    let mut expected_recorded = merged_payload_set("f", 1..=3);
    expected_recorded.extend(merged_payload_set("i", 101..=104));
    assert_eq!(
        payload_set(&merge_rows[..boundary]),
        expected_recorded,
        "the pre-boundary outputs are exactly the recorded inputs"
    );
    assert_eq!(
        payload_set(&merge_rows[boundary + 1..]),
        merged_payload_set("i", 105..=106),
        "the post-boundary outputs are exactly the live tail"
    );

    // Catch-up recomputed the recorded delivered order exactly.
    replay_testkit::assert_prefix_stable(&recorded_run, &resumed_run, "merge", &["fin", "inf"])
        .await;

    // The manifest records the entered generation.
    let manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        resumed_run.join("run_manifest.json"),
    )?)?;
    assert_eq!(manifest["resume"]["resume_generation"], 1);

    Ok(())
}

/// The F17 crossing with the finite channel longer than the infinite one.
///
/// Under the seq-ordered merge (FLOWIP-120n F18) the recording no longer
/// blocks on the shorter quiet input, so all 8 recorded inputs deliver, and
/// on resume the fin EOF always precedes the inf watermark at the fan-in (a
/// re-authored EOF orders by inherited journal position, at most the recorded
/// maximum, while the watermark draws a seeded live sequence above it). The
/// original arranged order, watermark consumed while the finite reader is
/// still mid-prefix, is no longer constructible at a source-fed fan-in; this
/// variant keeps the longer-finite shape and the F17 flip coverage.
#[tokio::test(flavor = "multi_thread")]
async fn mixed_sources_resume_flips_when_the_eof_arrives_after_the_watermark() -> Result<()> {
    // Recorded: f emits 1..=5 then EOF, i emits 101..=103 then idles; all 8
    // deliver (no Kahn wait at a source-fed fan-in). Live: i emits 104..=105.
    const RECORDED_DELIVERED: u64 = 8;
    const RECORDED_INPUTS: u64 = 8;
    const LIVE: u64 = 2;
    let recorded_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 5,
        inf_first: 101,
        inf_count: 3,
    };
    let live_ranges = SourceRanges {
        fin_first: 1,
        fin_count: 5,
        inf_first: 104,
        inf_count: 2,
    };

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let recorded_calls =
        run_until_delivered(&journal_base, &recorded_ranges, RECORDED_DELIVERED).await?;
    assert_eq!(recorded_calls, RECORDED_DELIVERED as usize);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    let resumed_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        run_until_delivered(&journal_base, &live_ranges, LIVE).await?
    };
    assert_eq!(
        resumed_calls, LIVE as usize,
        "recorded outcomes are suppressed; only the live tail executes"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // The fan-in must author its watermark: the finite input crossed
    // vacuously at its EOF (F17), the infinite one at its watermark.
    let merge_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "merge").await);
    let boundary = merge_rows
        .iter()
        .position(|row| {
            matches!(
                row,
                MixedRow::CatchUp { stage_key, generation } if stage_key == "merge" && *generation == 1
            )
        })
        .unwrap_or_else(|| panic!("the fan-in must author its watermark: {merge_rows:#?}"));
    let data_before = merge_rows[..boundary]
        .iter()
        .filter(|row| matches!(row, MixedRow::Data(_)))
        .count();
    assert_eq!(
        data_before as u64, RECORDED_INPUTS,
        "every recorded input is processed before the fan-in's boundary: {merge_rows:#?}"
    );
    assert_eq!(
        payload_set(&merge_rows[boundary + 1..]),
        merged_payload_set("i", 104..=105),
        "the post-boundary outputs are exactly the live tail"
    );

    replay_testkit::assert_prefix_stable(&recorded_run, &resumed_run, "merge", &["fin", "inf"])
        .await;

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TickRead(u64);

#[obzenflow_runtime::effects::port]
#[async_trait::async_trait]
trait TickInput {
    #[query(type = "resume_mixed.ticks.read")]
    async fn read(
        &self,
        from: &TickRead,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<ChannelTick, TickRead>, obzenflow_runtime::queries::QueryError>;
}

struct StallingTicks {
    polls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl TickInput for StallingTicks {
    async fn read(
        &self,
        from: &TickRead,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<ChannelTick, TickRead>, obzenflow_runtime::queries::QueryError> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if from.0 < 3 {
            return Ok(FiniteRead::Batch {
                records: vec![ChannelTick {
                    channel: "f".to_string(),
                    value: from.0 + 1,
                }],
                next: TickRead(from.0 + 1),
            });
        }
        std::future::pending().await
    }
}

struct StallingFiniteSource(Port<dyn TickInput>);

impl TypedFiniteSourceHandler for StallingFiniteSource {
    type Output = ChannelTick;

    fn read(&self) -> SourceRead<ChannelTick> {
        SourceRead::finite(self.0.clone(), TickRead(0), Ok).in_process()
    }
}

fn build_stalling_flow(
    journal_base: PathBuf,
    polls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let finite_handler = StallingFiniteSource(Port::to(StallingTicks { polls }));
        let infinite_handler =
            replay_testkit::preloaded_input((101..103).map(|value| ChannelTick {
                channel: "i".to_string(),
                value,
            }));
        let merge_handler = MergeTransform::new();
        let collect_handler = CountingSink::<Merged>::new(delivered);

        Ok(flow! {
            name: "resume_mixed_refusal",
            journals: disk_journals(journal_base),

            stages: {
                fin = source!(ChannelTick => finite_handler);
                inf = source!(ChannelTick => infinite_handler);
                merge = transform!(ChannelTick -> Merged => merge_handler);
                collect = sink!(Merged => collect_handler);
            },

            topology: {
                fin |> merge;
                inf |> merge;
                merge |> collect;
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unfinished_refuse_source_refuses_the_resume_at_admission() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_stalling_flow(
        journal_base.clone(),
        Arc::new(AtomicUsize::new(0)),
        delivered.clone(),
    )
    .build(replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_count(&delivered, 5).await?;
    handle.stop_graceful(Duration::from_secs(5)).await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    // The interrupted process ends, releasing its journal writers (B7).
    drop(handle);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // The operator stop cut the finite input short (B5).
    let fin_eofs: Vec<EofKind> =
        replay_testkit::read_stage_envelopes_appended(&recorded_run, "fin")
            .await
            .iter()
            .filter_map(|envelope| match &envelope.payload {
                ChainPayload::FlowControl(control) => control.eof_kind(),
                _ => None,
            })
            .collect();
    assert_eq!(fin_eofs, vec![EofKind::Truncated]);

    let runs_before = std::fs::read_dir(journal_base.join("flows"))?.count();
    let resumed_polls = Arc::new(AtomicUsize::new(0));
    let failure = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        build_stalling_flow(
            journal_base.clone(),
            resumed_polls.clone(),
            Arc::new(AtomicU64::new(0)),
        )
        .build(replay_testkit::unsynced_context())
        .await
        .err()
        .ok_or_else(|| anyhow!("an unfinished Refuse source must refuse the resume"))?
    };
    match &failure.error {
        obzenflow_dsl::dsl::FlowBuildError::ArchiveVerbRefused(
            obzenflow_core::journal::archive_actions::ArchiveRefusal::Sources {
                run: archive,
                refusals,
            },
        ) => {
            assert_eq!(archive, &recorded_run);
            assert_eq!(
                refusals,
                &vec![("fin".to_string(), ResumeRefusal::UnfinishedInput)],
                "the reattaching source is not named"
            );
        }
        other => panic!("expected the unfinished-source refusal, got: {other}"),
    }
    assert!(failure
        .error
        .to_string()
        .contains("source 'fin' stopped with its input unfinished"));
    assert_eq!(
        std::fs::read_dir(journal_base.join("flows"))?.count(),
        runs_before,
        "the refusal leaves no run directory"
    );
    assert_eq!(
        resumed_polls.load(Ordering::SeqCst),
        0,
        "the refused resume never polls a source"
    );
    assert!(
        !recorded_run.join("run_continuation.json").exists(),
        "the refused resume claims nothing (B12)"
    );

    Ok(())
}

fn build_channel_flow(
    journal_base: PathBuf,
    ticks: SourceReceiver<ChannelTick>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let channel_source = ChannelSource::new(ticks);
        let merge_handler = MergeTransform::new();
        let collect_handler = CountingSink::<Merged>::new(delivered);

        Ok(flow! {
            name: "resume_channel_source",
            journals: disk_journals(journal_base),

            stages: {
                chan = source!(ChannelTick => channel_source);
                merge = transform!(ChannelTick -> Merged => merge_handler);
                collect = sink!(Merged => collect_handler);
            },

            topology: {
                chan |> merge;
                merge |> collect;
            }
        })
    })
}

fn tick(value: u64) -> ChannelTick {
    ChannelTick {
        channel: "c".to_string(),
        value,
    }
}

/// FLOWIP-122i: `ChannelSource` is a connector that reattaches live with
/// `AtMostOnce`, recorded after its watermark and before its first live tick.
#[tokio::test(flavor = "multi_thread")]
async fn a_channel_source_reattaches_and_records_at_most_once() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    let (sender, ticks) = source_channel(8);
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_channel_flow(journal_base.clone(), ticks, delivered.clone())
        .build(replay_testkit::unsynced_context())
        .await
        .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    for value in 1..=2 {
        sender.send(tick(value)).await?;
    }
    wait_for_count(&delivered, 2).await?;
    handle.stop_graceful(Duration::from_secs(5)).await?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    drop(handle);
    drop(sender);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    let (sender, ticks) = source_channel(8);
    let delivered = Arc::new(AtomicU64::new(0));
    {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(resume_bootstrap(&recorded_run)).await,
        );
        let handle = build_channel_flow(journal_base.clone(), ticks, delivered.clone())
            .build(replay_testkit::unsynced_context())
            .await
            .map_err(|e| anyhow!("resume flow failed to build: {e:?}"))?;
        wait_for_running(&handle).await?;
        sender.send(tick(3)).await?;
        wait_for_count(&delivered, 1).await?;
        handle.stop_graceful(Duration::from_secs(5)).await?;
        tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
            .await
            .map_err(|_| anyhow!("timeout waiting for the resumed run to stop"))??;
    }
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    let chan_rows =
        mixed_rows(&replay_testkit::read_stage_envelopes_appended(&resumed_run, "chan").await);
    let data = |value: u64| MixedRow::Data(json!(tick(value)));
    assert_eq!(
        chan_rows,
        [
            data(1),
            data(2),
            MixedRow::CatchUp {
                stage_key: "chan".to_string(),
                generation: 1,
            },
            MixedRow::Reattached(SourceReattachedFact {
                generation: 1,
                guarantee: ReattachGuarantee::AtMostOnce,
            }),
            data(3),
            MixedRow::Eof,
        ]
    );

    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    id: u64,
}

impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "resume_mixed.order";
}

#[derive(Clone)]
struct OrderCsv;

impl CsvDecoder for OrderCsv {
    type Output = Order;
}

const ORDERS: u64 = 8;

fn write_orders(path: &Path) -> Result<()> {
    let rows: String = (1..=ORDERS).map(|id| format!("{id}\n")).collect();
    std::fs::write(path, format!("id\n{rows}"))?;
    Ok(())
}

/// Orders whose ids a decoder value scales. The value is application logic,
/// outside the input identity (B9).
#[derive(Clone)]
struct ScaledOrderCsv {
    factor: u64,
}

impl CsvDecoder for ScaledOrderCsv {
    type Output = Order;

    fn decode(&self, record: CsvRecord<'_>) -> Result<Order, CsvDecodeError> {
        let order: Order = record.deserialize()?;
        Ok(Order {
            id: order.id * self.factor,
        })
    }
}

/// Orders read one row per poll, paced by a limiter that admits `burst` rows
/// at once and then `rate` rows a second.
fn build_csv_flow(
    journal_base: PathBuf,
    input: PathBuf,
    decoder: impl CsvDecoder<Output = Order>,
    rate: f64,
    burst: f64,
    delivered: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let orders_source = CsvSource::builder(decoder)
            .path(input)
            .chunk_size(1)
            .build()
            .expect("valid CSV configuration");
        let collect_handler = replay_testkit::CountDeliveries::<Order>::new(delivered);

        Ok(flow! {
            name: "resume_csv",
            journals: disk_journals(journal_base),

            stages: {
                orders = source!(Order with {
                    rate_limit(rate).burst_capacity(burst)
                } => orders_source);
                collect = sink!(Order => collect_handler);
            },

            topology: {
                orders |> collect;
            }
        })
    })
}

async fn wait_for_deliveries(counter: &Arc<AtomicUsize>, target: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        while counter.load(Ordering::SeqCst) < target {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {target} deliveries"))
}

/// Record a run stopped after the limiter's burst, before the end of its input.
async fn record_interrupted_csv_run(journal_base: &Path, input: &Path) -> Result<PathBuf> {
    let delivered = Arc::new(AtomicUsize::new(0));
    let handle = build_csv_flow(
        journal_base.to_path_buf(),
        input.to_path_buf(),
        OrderCsv,
        0.5,
        3.0,
        delivered.clone(),
    )
    .build(replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    wait_for_running(&handle).await?;
    wait_for_deliveries(&delivered, 3).await?;
    handle.stop_graceful(Duration::from_secs(10)).await?;
    tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;
    Ok(replay_testkit::latest_run_dir(journal_base))
}

/// Resume `archive` without pacing; the outcome and the resumed run.
async fn resume_csv_run(
    journal_base: &Path,
    input: &Path,
    archive: &Path,
    decoder: impl CsvDecoder<Output = Order>,
) -> Result<(Result<(), obzenflow_runtime::errors::FlowError>, PathBuf)> {
    let _bootstrap = install_bootstrap_config(
        replay_testkit::bootstrap_with_archive(ReplayBootstrap {
            archive_path: archive.to_path_buf(),
            allow_incomplete_archive: true,
            verb: ReplayVerb::Resume,
        })
        .await,
    );
    let handle = build_csv_flow(
        journal_base.to_path_buf(),
        input.to_path_buf(),
        decoder,
        1000.0,
        100.0,
        Arc::new(AtomicUsize::new(0)),
    )
    .build(replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow!("resumed flow failed to build: {e:?}"))?;
    let outcome = tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the resumed run to finish"))?;
    let resumed = replay_testkit::latest_run_dir(journal_base);
    assert_ne!(resumed, archive, "the resume writes a new run");
    Ok((outcome, resumed))
}

#[derive(Clone, Debug, PartialEq)]
enum SourceRow {
    Read(u64),
    CatchUp,
    Order(u64),
    Eof(EofKind),
}

async fn source_rows(run: &Path) -> Vec<SourceRow> {
    replay_testkit::read_stage_envelopes_appended(run, "orders")
        .await
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            ChainPayload::Fact(order) => {
                Some(SourceRow::Order(order["id"].as_u64().expect("order id")))
            }
            ChainPayload::Execution(ExecutionPayload::EffectRecord(record))
                if record.is_query() =>
            {
                Some(SourceRow::Read(record.cursor.input_seq.get()))
            }
            ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete { .. }) => {
                Some(SourceRow::CatchUp)
            }
            ChainPayload::FlowControl(control) => control.eof_kind().map(SourceRow::Eof),
            _ => None,
        })
        .collect()
}

async fn source_errors(run: &Path) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = replay_testkit::archive_manifest(run);
    let file = manifest["stages"]["orders"]["error_journal_file"]
        .as_str()
        .expect("the source has an error journal");
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run.join(file)).await
}

fn orders(ids: std::ops::RangeInclusive<u64>) -> impl Iterator<Item = SourceRow> {
    ids.flat_map(|id| [SourceRow::Read(id), SourceRow::Order(id)])
}

/// The generations whose reconstruction a run recorded as complete (B6).
async fn reconstruction_completions(run: &Path) -> Vec<u64> {
    let manifest = replay_testkit::archive_manifest(run);
    let file = manifest["system_journal_file"]
        .as_str()
        .expect("the run has a system journal");
    replay_testkit::read_journal_envelopes_appended::<SystemEvent>(&run.join(file))
        .await
        .into_iter()
        .filter_map(|envelope| match envelope.into_parts().1 {
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::ReconstructionCompleted {
                generation,
            }) => Some(generation),
            _ => None,
        })
        .collect()
}

/// The interrupted run's committed orders, 1..=k, after checking its shape.
async fn recorded_prefix(run: &Path) -> u64 {
    let rows = source_rows(run).await;
    let committed = rows
        .iter()
        .filter(|row| matches!(row, SourceRow::Order(_)))
        .count() as u64;
    assert!(
        (1..ORDERS).contains(&committed),
        "the stop must land inside the input: {rows:?}"
    );
    let mut expected = orders(1..=committed).collect::<Vec<_>>();
    expected.push(SourceRow::Eof(EofKind::Truncated));
    assert_eq!(
        rows, expected,
        "a truncated EOF follows the committed orders"
    );
    committed
}

#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_csv_source_continues_after_its_last_committed_row() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");
    write_orders(&input)?;

    let recorded_run = record_interrupted_csv_run(&journal_base, &input).await?;
    let committed = recorded_prefix(&recorded_run).await;

    let (outcome, resumed_run) =
        resume_csv_run(&journal_base, &input, &recorded_run, OrderCsv).await?;
    outcome?;
    let mut expected = orders(1..=committed).collect::<Vec<_>>();
    expected.push(SourceRow::CatchUp);
    expected.extend(orders(committed + 1..=ORDERS));
    expected.extend([
        SourceRow::Read(ORDERS + 1),
        SourceRow::Eof(EofKind::Natural),
    ]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    assert!(source_errors(&resumed_run).await.is_empty());
    // B6: every stage crossed, so the resumed run alone is resume authority
    // and a resume of it opens no ancestor.
    assert!(reconstruction_completions(&recorded_run).await.is_empty());
    assert_eq!(reconstruction_completions(&resumed_run).await, [1]);
    let reopened =
        obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive::open_for_resume(
            resumed_run.clone(),
            true,
        )
        .await?;
    assert!(obzenflow_core::journal::archive::ReplayArchive::ancestors(&reopened).is_empty());
    Ok(())
}

fn resume_bootstrap(archive: &Path) -> ReplayBootstrap {
    ReplayBootstrap {
        archive_path: archive.to_path_buf(),
        allow_incomplete_archive: true,
        verb: ReplayVerb::Resume,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn changed_decoder_value_decodes_resumed_suffix() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");
    write_orders(&input)?;
    let recorded_run = record_interrupted_csv_run(&journal_base, &input).await?;
    let committed = recorded_prefix(&recorded_run).await;

    let (outcome, resumed_run) = resume_csv_run(
        &journal_base,
        &input,
        &recorded_run,
        ScaledOrderCsv { factor: 100 },
    )
    .await?;
    outcome?;
    let mut expected = orders(1..=committed).collect::<Vec<_>>();
    expected.push(SourceRow::CatchUp);
    expected.extend(
        (committed + 1..=ORDERS).flat_map(|id| [SourceRow::Read(id), SourceRow::Order(id * 100)]),
    );
    expected.extend([
        SourceRow::Read(ORDERS + 1),
        SourceRow::Eof(EofKind::Natural),
    ]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderV2 {
    id: u64,
}

impl TypedPayload for OrderV2 {
    const EVENT_TYPE: &'static str = "resume_mixed.order_v2";
}

#[derive(Clone)]
struct OrderV2Csv;

impl CsvDecoder for OrderV2Csv {
    type Output = OrderV2;
}

/// FLOWIP-134i B9 (AC6): a decoder with another output type cannot re-admit
/// the recorded facts, so the resume fails at re-admission before any live row.
#[tokio::test(flavor = "multi_thread")]
async fn a_changed_decoder_output_type_fails_at_re_admission() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");
    write_orders(&input)?;
    let recorded_run = record_interrupted_csv_run(&journal_base, &input).await?;
    recorded_prefix(&recorded_run).await;

    let _bootstrap = install_bootstrap_config(
        replay_testkit::bootstrap_with_archive(ReplayBootstrap {
            archive_path: recorded_run.clone(),
            allow_incomplete_archive: true,
            verb: ReplayVerb::Resume,
        })
        .await,
    );
    let orders_source = CsvSource::builder(OrderV2Csv)
        .path(input.clone())
        .chunk_size(1)
        .build()
        .expect("valid CSV configuration");
    let collect_handler =
        replay_testkit::CountDeliveries::<OrderV2>::new(Arc::new(AtomicUsize::new(0)));
    let journals = journal_base.clone();
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "resume_csv",
            journals: disk_journals(journals),

            stages: {
                orders = source!(OrderV2 => orders_source);
                collect = sink!(OrderV2 => collect_handler);
            },

            topology: {
                orders |> collect;
            }
        })
    })
    .build(replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow!("resumed flow failed to build: {e:?}"))?;
    tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the resumed run to fail"))?
        .expect_err("recorded orders cannot re-admit as another type");
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    let mut snapshot = obzenflow_infra::journal::read::open_disk_run(&resumed_run).await?;
    while snapshot.next().await?.is_some() {}
    match snapshot
        .into_tail()
        .progress()
        .outcome
        .as_ref()
        .map(|recorded| &recorded.outcome)
    {
        Some(obzenflow_core::journal::read::RunOutcome::Failed { reason }) => assert!(
            reason.contains("emitted OrderV2 at input 1, but its recording's next action is"),
            "{reason}"
        ),
        other => panic!("the resumed run must record its failure: {other:?}"),
    }
    assert!(
        !source_rows(&resumed_run)
            .await
            .iter()
            .any(|row| matches!(row, SourceRow::Order(_) | SourceRow::CatchUp)),
        "nothing is admitted or crossed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn changed_csv_input_resume_refused() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");
    write_orders(&input)?;

    let recorded_run = record_interrupted_csv_run(&journal_base, &input).await?;
    let committed = recorded_prefix(&recorded_run).await;
    let mut appended = std::fs::OpenOptions::new().append(true).open(&input)?;
    std::io::Write::write_all(&mut appended, b"9\n")?;

    let (outcome, resumed_run) =
        resume_csv_run(&journal_base, &input, &recorded_run, OrderCsv).await?;
    assert!(outcome.is_err(), "a refused source fails the resumed run");
    let mut expected = orders(1..=committed).collect::<Vec<_>>();
    expected.extend([SourceRow::CatchUp, SourceRow::Read(committed + 1)]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    let refusals: Vec<_> = source_errors(&resumed_run)
        .await
        .into_iter()
        .filter_map(|envelope| match envelope.into_parts().1 {
            ChainPayload::Execution(ExecutionPayload::SourcePollFailed(fact)) => {
                Some(fact.diagnostic.reason())
            }
            _ => None,
        })
        .collect();
    assert_eq!(refusals, [SourceDiagnosticReason::InputChanged]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn unread_csv_source_resume_reads_input() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");
    let input = temp.path().join("orders.csv");

    let handle = build_csv_flow(
        journal_base.clone(),
        input.clone(),
        OrderCsv,
        1000.0,
        100.0,
        Arc::new(AtomicUsize::new(0)),
    )
    .build(replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow!("flow failed to build: {e:?}"))?;
    let failed = tokio::time::timeout(Duration::from_secs(20), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for the failed open"))?;
    assert!(
        failed.is_err(),
        "a missing input fails the source's first read"
    );
    // The failed process ends, releasing its journal writers (B7).
    drop(handle);
    let failed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_eq!(source_rows(&failed_run).await, [SourceRow::Read(1)]);

    write_orders(&input)?;
    let (outcome, resumed_run) =
        resume_csv_run(&journal_base, &input, &failed_run, OrderCsv).await?;
    outcome?;
    let mut expected = vec![SourceRow::Read(1), SourceRow::CatchUp];
    expected.extend(orders(1..=ORDERS));
    expected.extend([
        SourceRow::Read(ORDERS + 1),
        SourceRow::Eof(EofKind::Natural),
    ]);
    assert_eq!(source_rows(&resumed_run).await, expected);
    assert_eq!(reconstruction_completions(&resumed_run).await, [1]);
    Ok(())
}
