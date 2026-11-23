// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use obzenflow::effects::Port;
use obzenflow::stages::sinks::DiscardSink;
use obzenflow::stages::sources::ValuesSource;
use obzenflow::stages::stateful::{self, emit};
use obzenflow::testing::RecordingSink;
use obzenflow_core::event::chain_event::{ChainEvent, ChainPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventId, StageId, TypedPayload, WriterId};
use obzenflow_dsl::{sink, source, stateful, test_flow, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::EffectContext;
use obzenflow_runtime::effects::EffectError;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::{FiniteRead, SourceRead};
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedTransformHandler,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

fn sequence(max: u64) -> impl TypedFiniteSourceHandler<Output = WindowInput> {
    ValuesSource::new(
        (0..max)
            .map(|index| WindowInput { index })
            .collect::<Vec<_>>(),
    )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WindowRead(u64);

#[obzenflow_runtime::effects::port]
#[async_trait::async_trait]
trait WindowFeed {
    #[query(type = "emit_per_window.input.read")]
    async fn read(
        &self,
        from: &WindowRead,
        ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<WindowInput, WindowRead>, obzenflow::queries::QueryError>;
}

struct DelayedSequence {
    max: u64,
    per_event_delay: Duration,
}

#[obzenflow_runtime::effects::system("delayed-sequence")]
#[async_trait::async_trait]
impl WindowFeed for DelayedSequence {
    async fn read(
        &self,
        from: &WindowRead,
        _ctx: &obzenflow::queries::QueryContext,
    ) -> Result<FiniteRead<WindowInput, WindowRead>, obzenflow::queries::QueryError> {
        if from.0 >= self.max {
            return Ok(FiniteRead::EndOfInput);
        }
        tokio::time::sleep(self.per_event_delay).await;
        Ok(FiniteRead::Batch {
            records: vec![WindowInput { index: from.0 }],
            next: WindowRead(from.0 + 1),
        })
    }
}

struct DelayedSequenceSource(obzenflow::queries::QueryPort<dyn WindowFeed>);

impl DelayedSequenceSource {
    fn new(max: u64, per_event_delay: Duration) -> Self {
        Self(obzenflow::queries::QueryPort::to(DelayedSequence {
            max,
            per_event_delay,
        }))
    }
}

impl TypedFiniteSourceHandler for DelayedSequenceSource {
    type Output = WindowInput;

    fn read(&self) -> SourceRead<WindowInput> {
        SourceRead::finite(self.0.clone(), WindowRead(0), Ok)
    }
}

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WindowInput {
    index: u64,
}

impl TypedPayload for WindowInput {
    const EVENT_TYPE: &'static str = "stateful.emit_per_window.window.input";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct WindowAgg {
    event_count: u64,
}

impl TypedPayload for WindowAgg {
    const EVENT_TYPE: &'static str = "stateful.emit_per_window.window.aggregate";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GroupInput {
    group: String,
    index: u64,
}

impl TypedPayload for GroupInput {
    const EVENT_TYPE: &'static str = "stateful.emit_per_window.group_by.input";
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
struct GroupAgg {
    event_count: u64,
}

impl TypedPayload for GroupAgg {
    const EVENT_TYPE: &'static str = "stateful.emit_per_window.group_by.aggregate";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct GroupAggOutput {
    key: String,
    result: GroupAgg,
}

impl TypedPayload for GroupAggOutput {
    const EVENT_TYPE: &'static str = "stateful.emit_per_window.group_by.output";
}

type AggregateSink = RecordingSink<WindowAgg>;

type AckSink = DiscardSink<GroupAggOutput>;

#[derive(Clone, Debug)]
struct IdentityTransform;

impl TypedTransformHandler for IdentityTransform {
    type Input = WindowAgg;
    type Output = WindowAgg;

    fn process(&self, event: WindowAgg) -> std::result::Result<WindowAgg, HandlerError> {
        Ok(event)
    }
}

async fn authored_eof_position(
    stage_id: StageId,
    journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<Option<(usize, FlowControlPayload)>> {
    let ordered = journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read stage journal: {e}"))?;

    for (idx, env) in ordered.iter().enumerate() {
        let ChainPayload::FlowControl(FlowControlPayload::Eof { writer_id, .. }) = &env.payload
        else {
            continue;
        };

        let authored_by_stage = match writer_id {
            Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
            Some(_) => false,
            None => true,
        };

        if authored_by_stage {
            if let ChainPayload::FlowControl(payload) = &env.payload {
                return Ok(Some((idx, payload.clone())));
            }
        }
    }

    Ok(None)
}

async fn data_event_ids_of_type(
    journal: &Arc<dyn Journal<ChainEvent>>,
    event_type: &str,
) -> Result<Vec<EventId>> {
    let ordered = journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read stage journal: {e}"))?;

    let mut ids = Vec::new();
    for env in ordered {
        if env.consumes_data_credit() && env.event_type() == event_type {
            ids.push(env.envelope.provenance.event.id);
        }
    }

    Ok(ids)
}

async fn last_window_aggregate_event(
    journal: &Arc<dyn Journal<ChainEvent>>,
) -> Result<Option<ChainEvent>> {
    let ordered = journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read stage journal: {e}"))?;

    let mut last = None;
    for env in ordered {
        if WindowAgg::from_event(&env.authored()).is_some() {
            last = Some(env.authored().clone());
        }
    }
    Ok(last)
}

fn payload_writer_stage(payload: &FlowControlPayload) -> Option<StageId> {
    match payload {
        FlowControlPayload::Eof {
            writer_id: Some(WriterId::Stage(stage)),
            ..
        } => Some(*stage),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emit_per_window_flushes_final_partial_window_before_authored_eof() -> Result<()> {
    let (sink, seen) = AggregateSink::new();
    let source = sequence(3);
    let window = stateful::reduce(
        WindowAgg::default(),
        |acc: &mut WindowAgg, _in: &WindowInput| acc.event_count += 1,
    )
    .emit(emit::per_window(Duration::from_secs(60)));
    let harness = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "emit_per_window_final_flush",
        journals: disk_journals(unique_journal_dir("emit_per_window_final_flush")),

        stages: {
            src = source!(WindowInput => source);
            win = stateful!(WindowInput -> WindowAgg => window);
            snk = sink!(WindowAgg => sink);
        },

        topology: {
            src |> win;
            win |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    let (win_stage_id, win_journal) = harness
        .stage_journal_for_test("win")
        .map_err(|e| anyhow!("failed to resolve win stage journal: {e}"))?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let aggregates = seen.items();
    assert_eq!(aggregates.len(), 1, "expected exactly one final aggregate");
    assert_eq!(aggregates[0].event_count, 3);

    let ordered = win_journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read win stage journal: {e}"))?;

    let mut last_agg_idx: Option<usize> = None;
    let mut last_agg_event_id: Option<EventId> = None;
    for (idx, env) in ordered.iter().enumerate() {
        if WindowAgg::from_event(&env.authored()).is_some() {
            last_agg_idx = Some(idx);
            last_agg_event_id = Some(env.envelope.provenance.event.id);
        }
    }

    let (eof_idx, eof_payload) = authored_eof_position(win_stage_id, &win_journal)
        .await?
        .ok_or_else(|| anyhow!("expected win stage to author a terminal EOF"))?;

    let agg_idx = last_agg_idx.ok_or_else(|| anyhow!("expected win stage to emit an aggregate"))?;
    assert!(
        agg_idx < eof_idx,
        "expected aggregate to be journalled before authored EOF (agg_idx={agg_idx}, eof_idx={eof_idx})"
    );

    if let FlowControlPayload::Eof { last_event_id, .. } = eof_payload {
        assert_eq!(
            last_event_id, last_agg_event_id,
            "expected authored EOF last_event_id to prefer the final aggregate"
        );
    } else {
        unreachable!("authored_eof_position returned non-EOF payload")
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emit_per_window_final_aggregate_preserves_buffered_input_lineage() -> Result<()> {
    let (sink, _seen) = AggregateSink::new();
    let source = sequence(3);
    let window = stateful::reduce(
        WindowAgg::default(),
        |acc: &mut WindowAgg, _in: &WindowInput| acc.event_count += 1,
    )
    .emit(emit::per_window(Duration::from_secs(60)));
    let harness = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "emit_per_window_lineage",
        journals: disk_journals(unique_journal_dir("emit_per_window_lineage")),

        stages: {
            src = source!(WindowInput => source);
            win = stateful!(WindowInput -> WindowAgg => window);
            snk = sink!(WindowAgg => sink);
        },

        topology: {
            src |> win;
            win |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    let (win_stage_id, win_journal) = harness.stage_journal_for_test("win")?;
    let (_src_stage_id, src_journal) = harness.stage_journal_for_test("src")?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let input_ids = data_event_ids_of_type(&src_journal, &WindowInput::event_type_name()).await?;
    assert_eq!(input_ids.len(), 3, "expected three input events");

    let aggregate = last_window_aggregate_event(&win_journal)
        .await?
        .ok_or_else(|| anyhow!("expected win stage to emit an aggregate"))?;

    assert_eq!(
        aggregate.writer_id,
        WriterId::Stage(win_stage_id),
        "expected aggregate to be authored by the windowing stage"
    );
    assert_eq!(
        aggregate.causality.parent_ids, input_ids,
        "expected aggregate causality parents to be the buffered input event IDs"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emit_per_window_final_aggregate_folds_runtime_minted_source_correlations() -> Result<()> {
    let (sink, seen) = AggregateSink::new();
    let source = sequence(2);
    let window = stateful::reduce(
        WindowAgg::default(),
        |acc: &mut WindowAgg, _in: &WindowInput| acc.event_count += 1,
    )
    .emit(emit::per_window(Duration::from_secs(60)));
    let harness = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "emit_per_window_runtime_source_correlation",
        journals: disk_journals(unique_journal_dir("emit_per_window_runtime_source_correlation")),

        stages: {
            src = source!(WindowInput => source);
            win = stateful!(WindowInput -> WindowAgg => window);
            snk = sink!(WindowAgg => sink);
        },

        topology: {
            src |> win;
            win |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    let (_win_stage_id, win_journal) = harness.stage_journal_for_test("win")?;
    let (_src_stage_id, src_journal) = harness.stage_journal_for_test("src")?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let aggregates = seen.items();
    assert_eq!(aggregates.len(), 1);
    assert_eq!(aggregates[0].event_count, 2);

    let aggregate_event = last_window_aggregate_event(&win_journal)
        .await?
        .ok_or_else(|| anyhow!("expected win stage to emit an aggregate"))?;

    assert!(
        aggregate_event.correlation_id().is_none(),
        "a window with multiple source roots must not expose one scalar correlation"
    );
    let source_events = src_journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read source journal: {e}"))?;
    let mut expected = source_events
        .iter()
        .filter(|envelope| WindowInput::from_event(&envelope.authored()).is_some())
        .map(|envelope| {
            envelope
                .correlation_id()
                .expect("the runtime commit seam mints one correlation per source fact")
        })
        .collect::<Vec<_>>();
    expected.sort();
    assert_eq!(expected.len(), 2);

    let recorded = aggregate_event
        .correlation_ids()
        .expect("the aggregate retains the complete source correlation set");
    assert_eq!(recorded, expected.as_slice());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emit_per_window_emits_more_than_one_window_aggregate_per_run_and_resets_state(
) -> Result<()> {
    let window = Duration::from_millis(50);
    let per_event_delay = Duration::from_millis(120);
    let total = 3u64;

    let (sink, seen) = AggregateSink::new();
    let source = DelayedSequenceSource::new(total, per_event_delay);
    let window_handler = stateful::reduce(
        WindowAgg::default(),
        |acc: &mut WindowAgg, _in: &WindowInput| acc.event_count += 1,
    )
    .emit(emit::per_window(window));
    let harness = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "emit_per_window_multi_window",
        journals: disk_journals(unique_journal_dir("emit_per_window_multi_window")),

        stages: {
            src = source!(WindowInput => source);
            win = stateful!(WindowInput -> WindowAgg => window_handler);
            snk = sink!(WindowAgg => sink);
        },

        topology: {
            src |> win;
            win |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let aggregates = seen.items();
    assert!(
        aggregates.len() >= 2,
        "expected more than one mid-stream window aggregate per run, got {}",
        aggregates.len()
    );

    let sum: u64 = aggregates.iter().map(|a| a.event_count).sum();
    let max: u64 = aggregates.iter().map(|a| a.event_count).max().unwrap_or(0);
    assert_eq!(
        sum, total,
        "expected windows to partition inputs without duplication"
    );
    assert!(
        max < total,
        "expected accumulator state to reset between windows (max={max}, total={total})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn forwarded_inbound_eof_does_not_complete_downstream_reader() -> Result<()> {
    let (sink, _seen) = AggregateSink::new();
    let source = sequence(3);
    let window = stateful::reduce(
        WindowAgg::default(),
        |acc: &mut WindowAgg, _in: &WindowInput| acc.event_count += 1,
    )
    .emit(emit::per_window(Duration::from_secs(60)));
    let identity = IdentityTransform;
    let harness = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "emit_per_window_forwarded_eof_invariant",
        journals: disk_journals(unique_journal_dir("emit_per_window_forwarded_eof_invariant")),

        stages: {
            src = source!(WindowInput => source);
            win = stateful!(WindowInput -> WindowAgg => window);
            tr = transform!(WindowAgg -> WindowAgg => identity);
            snk = sink!(WindowAgg => sink);
        },

        topology: {
            src |> win;
            win |> tr;
            tr |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    let (src_stage_id, _src_journal) = harness.stage_journal_for_test("src")?;
    let (_win_stage_id, _win_journal) = harness.stage_journal_for_test("win")?;
    let (_tr_stage_id, tr_journal) = harness.stage_journal_for_test("tr")?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let ordered = tr_journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read tr stage journal: {e}"))?;

    let mut forwarded_src_eof_idx: Option<usize> = None;
    let mut agg_idx: Option<usize> = None;

    for (idx, env) in ordered.iter().enumerate() {
        if agg_idx.is_none() && WindowAgg::from_event(&env.authored()).is_some() {
            agg_idx = Some(idx);
        }

        let ChainPayload::FlowControl(payload) = &env.payload else {
            continue;
        };
        let FlowControlPayload::Eof { .. } = payload else {
            continue;
        };

        if payload_writer_stage(payload) == Some(src_stage_id) && forwarded_src_eof_idx.is_none() {
            forwarded_src_eof_idx = Some(idx);
        }
    }

    let forwarded_src_eof_idx = forwarded_src_eof_idx
        .ok_or_else(|| anyhow!("expected tr stage to see forwarded source EOF"))?;
    let agg_idx =
        agg_idx.ok_or_else(|| anyhow!("expected tr stage to consume a window aggregate event"))?;

    assert!(
        forwarded_src_eof_idx < agg_idx,
        "expected downstream stage to consume aggregate after forwarded source EOF (eof_idx={forwarded_src_eof_idx}, agg_idx={agg_idx})"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_by_emit_per_window_parents_each_group_to_its_own_inputs() -> Result<()> {
    let items = [
        ("a".to_string(), 0u64),
        ("b".to_string(), 1u64),
        ("a".to_string(), 2u64),
        ("b".to_string(), 3u64),
        ("c".to_string(), 4u64),
    ];

    let source = ValuesSource::new(
        items
            .into_iter()
            .map(|(group, index)| GroupInput { group, index })
            .collect::<Vec<_>>(),
    );
    let grouped = stateful::group_by(
        |input: &GroupInput| input.group.clone(),
        |acc: &mut GroupAgg, _input: &GroupInput| acc.event_count += 1,
        |key: &String, result: &GroupAgg| GroupAggOutput {
            key: key.clone(),
            result: result.clone(),
        },
    )
    .emit(emit::per_window(Duration::from_secs(60)))
    .updated_groups();
    let sink = AckSink::new();
    let harness = test_flow! {
        build_context: crate::replay_testkit::unsynced_context(),
        name: "emit_per_window_group_by_lineage",
        journals: disk_journals(unique_journal_dir("emit_per_window_group_by_lineage")),

        stages: {
            src = source!(GroupInput => source);
            grp = stateful!(GroupInput -> GroupAggOutput => grouped);
            snk = sink!(GroupAggOutput => sink);
        },

        topology: {
            src |> grp;
            grp |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    let (_grp_stage_id, grp_journal) = harness.stage_journal_for_test("grp")?;
    let (_src_stage_id, src_journal) = harness.stage_journal_for_test("src")?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let ordered_src = src_journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read src stage journal: {e}"))?;

    let mut expected: BTreeMap<String, Vec<EventId>> = BTreeMap::new();
    for env in ordered_src {
        let event_type = env.event_type();
        let ChainPayload::Fact(payload) = &env.payload else {
            continue;
        };
        if !GroupInput::matches_event_type(&event_type) {
            continue;
        }
        let group = payload
            .get("group")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("expected group field on input payload"))?
            .to_string();
        expected
            .entry(group)
            .or_default()
            .push(env.envelope.provenance.event.id);
    }

    let ordered_grp = grp_journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read grp stage journal: {e}"))?;

    let mut seen: BTreeMap<String, Vec<EventId>> = BTreeMap::new();
    for env in ordered_grp {
        let event_type = env.event_type();
        let ChainPayload::Fact(payload) = &env.payload else {
            continue;
        };
        if !GroupAggOutput::matches_event_type(&event_type) {
            continue;
        }
        let output: GroupAggOutput = serde_json::from_value(payload.clone())
            .map_err(|error| anyhow!("expected named group_by output: {error}"))?;

        seen.insert(
            output.key,
            env.envelope.provenance.event.causality.parent_ids.clone(),
        );
    }

    assert_eq!(
        seen, expected,
        "expected each group aggregate to be parented only to that group's input events"
    );

    Ok(())
}
