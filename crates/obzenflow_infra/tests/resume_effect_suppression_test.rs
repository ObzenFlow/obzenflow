// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-120n PR-J: effect suppression across resume.
//!
//! Records an infinite-source flow whose effectful transform counts real
//! executions, then resumes it with two live inputs. Catch-up must re-execute
//! zero effects (outcomes read from recorded facts), and the live inputs must
//! execute exactly twice, recording outcome facts under the SAME root flow id
//! at `input_seq` beyond the recorded range. Assertions read the effect
//! provenance rows of the event-sourced journals.

#[path = "../../../tests/replay_testkit/mod.rs"]
mod replay_testkit;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventContent, EventEnvelope};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{effectful_transform, flow, infinite_source, sink, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::bootstrap::{install_bootstrap_config, ReplayBootstrap, ReplayVerb};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectSafety, Effects, IdempotencyKey, SinkDeliverySafety,
};
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    EffectfulTransformHandler, InfiniteSourceHandler, SinkHandler,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reading {
    value: u64,
}

impl TypedPayload for Reading {
    const EVENT_TYPE: &'static str = "resume_effects.reading";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct EffectValue {
    effect_value: u64,
}

impl TypedPayload for EffectValue {
    const EVENT_TYPE: &'static str = "resume_effects.effect_value";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Enriched {
    value: u64,
    effect_value: u64,
}

impl TypedPayload for Enriched {
    const EVENT_TYPE: &'static str = "resume_effects.enriched";
}

/// Emits `count` readings starting at `first_value`, then idles forever.
#[derive(Clone, Debug)]
struct BoundedReader {
    writer_id: WriterId,
    next_value: u64,
    remaining: u64,
}

impl BoundedReader {
    fn new(first_value: u64, count: u64) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            next_value: first_value,
            remaining: count,
        }
    }
}

impl InfiniteSourceHandler for BoundedReader {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        if self.remaining == 0 {
            return Ok(Vec::new());
        }
        self.remaining -= 1;
        let value = self.next_value;
        self.next_value += 1;
        Ok(vec![Reading { value }.to_event(self.writer_id)])
    }
}

/// Counts real executions via the shared counter; a suppressed catch-up
/// replay never reaches `execute`.
#[derive(Clone, Debug)]
struct CountingEffect {
    value: u64,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Effect for CountingEffect {
    const EFFECT_TYPE: &'static str = "resume_effects.counting";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentRequiresKey;

    type Outcome = EffectValue;

    fn label(&self) -> &str {
        "counting"
    }

    fn canonical_input(&self) -> serde_json::Value {
        json!({ "value": self.value })
    }

    async fn execute(&self, _ctx: &mut EffectContext) -> Result<Self::Outcome, EffectError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(EffectValue {
            effect_value: self.value + 100,
        })
    }

    fn idempotency_key(&self) -> Option<IdempotencyKey> {
        Some(IdempotencyKey(format!("resume-effects:{}", self.value)))
    }
}

#[derive(Clone, Debug)]
struct EnrichTransform {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl EffectfulTransformHandler for EnrichTransform {
    type Input = Reading;

    async fn process(&self, input: Reading, fx: &mut Effects) -> Result<(), HandlerError> {
        let effect_value = fx
            .perform(CountingEffect {
                value: input.value,
                calls: self.calls.clone(),
            })
            .await
            .map_err(|e| HandlerError::Other(e.to_string()))?;
        fx.emit(Enriched {
            value: input.value,
            effect_value: effect_value.effect_value,
        })
        .await
        .map_err(|e| HandlerError::Other(e.to_string()))?;
        Ok(())
    }

    fn stage_logic_version(&self) -> &str {
        "resume-effects-v1"
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    delivered: Arc<AtomicU64>,
}

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if Enriched::from_event(&event).is_some() {
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }

    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }
}

fn build_flow(
    journal_base: PathBuf,
    first_value: u64,
    count: u64,
    calls: Arc<AtomicUsize>,
    delivered: Arc<AtomicU64>,
) -> FlowDefinition {
    flow! {
        name: "resume_effects",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            src = infinite_source!(Reading => BoundedReader::new(first_value, count));
            enrich = effectful_transform!(
                Reading -> { Enriched, EffectValue } => EnrichTransform { calls },
                effects: [CountingEffect],
                middleware: []
            );
            snk = sink!(Enriched => CountingSink { delivered });
        },

        topology: {
            src |> enrich;
            enrich |> snk;
        }
    }
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

/// Run one flow instance until the sink consumed `expected` outputs, then
/// stop. Returns the effect executions this run's handlers performed.
async fn run_until_delivered(
    journal_base: &Path,
    first_value: u64,
    count: u64,
    expected: u64,
) -> Result<usize> {
    let calls = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicU64::new(0));
    let handle = build_flow(
        journal_base.to_path_buf(),
        first_value,
        count,
        calls.clone(),
        delivered.clone(),
    )
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

/// The user-owned effect outcome facts of a stage journal, in physical append
/// order: `(recorded_flow_id, input_seq)` per fact.
fn outcome_fact_cursors(envelopes: &[EventEnvelope<ChainEvent>]) -> Vec<(String, u64)> {
    let outcome_type = EffectValue::versioned_event_type();
    envelopes
        .iter()
        .filter_map(|envelope| {
            let ChainEventContent::Data { event_type, .. } = &envelope.event.content else {
                return None;
            };
            if event_type != outcome_type.as_str() {
                return None;
            }
            let provenance = envelope
                .event
                .effect_provenance
                .as_ref()
                .expect("an effect outcome fact must carry effect provenance");
            assert!(
                provenance.fact_owner.is_user(),
                "typed effect outcomes are user-owned domain facts"
            );
            Some((
                provenance.cursor.recorded_flow_id.as_str().to_string(),
                provenance.cursor.input_seq.get(),
            ))
        })
        .collect()
}

/// Index of the stage's authored catch-up watermark in append order.
fn watermark_index(envelopes: &[EventEnvelope<ChainEvent>], stage_key: &str) -> usize {
    envelopes
        .iter()
        .position(|envelope| {
            matches!(
                &envelope.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::CatchUpComplete {
                    stage_key: key,
                    ..
                }) if key.as_str() == stage_key
            )
        })
        .expect("the effectful stage must author its catch-up watermark")
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_suppresses_recorded_effects_and_executes_the_live_tail_once() -> Result<()> {
    const RECORDED: u64 = 3; // readings 1..=3
    const LIVE: u64 = 2; // readings 4..=5

    let temp = tempfile::tempdir()?;
    let journal_base = temp.path().join("journals");

    // Record: every effect executes live, once per input.
    let recorded_calls = run_until_delivered(&journal_base, 1, RECORDED, RECORDED).await?;
    assert_eq!(recorded_calls, RECORDED as usize);
    let recorded_run = replay_testkit::latest_run_dir(&journal_base);

    // The recording's root flow id anchors every effect cursor.
    let recorded_manifest: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        recorded_run.join("run_manifest.json"),
    )?)?;
    let root_flow_id = recorded_manifest["flow_id"]
        .as_str()
        .expect("the recorded manifest names its flow id")
        .to_string();

    let recorded_facts = outcome_fact_cursors(
        &replay_testkit::read_stage_envelopes_appended(&recorded_run, "enrich").await,
    );
    assert_eq!(
        recorded_facts,
        (1..=RECORDED)
            .map(|seq| (root_flow_id.clone(), seq))
            .collect::<Vec<_>>(),
        "the recording anchors outcome facts at input_seq 1..=K under its own flow id"
    );

    // Resume with two live inputs. The counter is fresh, so any catch-up
    // re-execution would be visible in the final count.
    let resumed_calls = {
        let _bootstrap = install_bootstrap_config(
            replay_testkit::bootstrap_with_archive(ReplayBootstrap {
                archive_path: recorded_run.clone(),
                allow_incomplete_archive: true,
                allow_duplicate_sink_delivery: false,
                verb: ReplayVerb::Resume,
            })
            .await,
        );
        // Wait for prefix re-deliveries + live (the sink re-consumes the
        // recorded prefix during catch-up, F14).
        run_until_delivered(&journal_base, RECORDED + 1, LIVE, RECORDED + LIVE).await?
    };
    assert_eq!(
        resumed_calls, LIVE as usize,
        "catch-up re-executes ZERO effects; only the live inputs execute"
    );
    let resumed_run = replay_testkit::latest_run_dir(&journal_base);
    assert_ne!(recorded_run, resumed_run);

    // The resumed run's effect ledger: the K recorded outcome facts are
    // re-committed from history during catch-up, then the stage's watermark,
    // then the two live outcome facts, all under the SAME root flow id, with
    // the live facts at input_seq strictly beyond the recorded range.
    let resumed_envelopes =
        replay_testkit::read_stage_envelopes_appended(&resumed_run, "enrich").await;
    let resumed_facts = outcome_fact_cursors(&resumed_envelopes);
    assert_eq!(
        resumed_facts,
        (1..=RECORDED + LIVE)
            .map(|seq| (root_flow_id.clone(), seq))
            .collect::<Vec<_>>(),
        "every outcome fact carries the root flow id; live facts sit at \
         input_seq {}..={}",
        RECORDED + 1,
        RECORDED + LIVE
    );

    // The live facts land after the stage's authored catch-up watermark, the
    // structural witness that they were recorded in the live phase.
    let boundary = watermark_index(&resumed_envelopes, "enrich");
    let outcome_type = EffectValue::versioned_event_type();
    let outcome_positions: Vec<usize> = resumed_envelopes
        .iter()
        .enumerate()
        .filter_map(|(index, envelope)| match &envelope.event.content {
            ChainEventContent::Data { event_type, .. } if event_type == outcome_type.as_str() => {
                Some(index)
            }
            _ => None,
        })
        .collect();
    let (catch_up_facts, live_facts): (Vec<usize>, Vec<usize>) = outcome_positions
        .into_iter()
        .partition(|index| *index < boundary);
    assert_eq!(
        catch_up_facts.len() as u64,
        RECORDED,
        "the recorded outcome facts are re-committed during catch-up, before the boundary"
    );
    assert_eq!(
        live_facts.len() as u64,
        LIVE,
        "the live outcome facts are recorded after the boundary"
    );

    Ok(())
}
