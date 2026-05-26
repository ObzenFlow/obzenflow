// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::CorrelationId;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventId, StageId, TypedPayload, WriterId};
use obzenflow_dsl::{sink, source, stateful, test_flow};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::stateful::{
    ProcessingTimeTumblingWindow, ProcessingTimeWindowAggregate,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

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
    const EVENT_TYPE: &'static str = "stateful.processing_time_window.input";
}

#[derive(Clone, Debug)]
struct SequenceSource {
    writer_id: WriterId,
    next: u64,
    max: u64,
}

impl SequenceSource {
    fn new(max: u64) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            next: 0,
            max,
        }
    }
}

impl FiniteSourceHandler for SequenceSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next >= self.max {
            return Ok(None);
        }

        let idx = self.next;
        self.next += 1;
        Ok(Some(vec![obzenflow_core::event::chain_event::ChainEventFactory::data_event(
            self.writer_id,
            WindowInput::EVENT_TYPE,
            json!({ "index": idx }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct CorrelatedSequenceSource {
    writer_id: WriterId,
    correlation_ids: Vec<CorrelationId>,
    next: usize,
}

impl CorrelatedSequenceSource {
    fn new(correlation_ids: Vec<CorrelationId>) -> Self {
        Self {
            writer_id: WriterId::from(StageId::new()),
            correlation_ids,
            next: 0,
        }
    }
}

impl FiniteSourceHandler for CorrelatedSequenceSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.next >= self.correlation_ids.len() {
            return Ok(None);
        }

        let correlation_id = self.correlation_ids[self.next];
        self.next += 1;

        let mut event = obzenflow_core::event::chain_event::ChainEventFactory::data_event(
            self.writer_id,
            WindowInput::EVENT_TYPE,
            json!({ "index": self.next as u64 - 1 }),
        );
        event.correlation_id = Some(correlation_id);
        Ok(Some(vec![event]))
    }
}

#[derive(Clone, Debug)]
struct AggregateSink {
    seen: Arc<Mutex<Vec<ProcessingTimeWindowAggregate>>>,
}

impl AggregateSink {
    fn new() -> (Self, Arc<Mutex<Vec<ProcessingTimeWindowAggregate>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (Self { seen: seen.clone() }, seen)
    }
}

#[async_trait]
impl SinkHandler for AggregateSink {
    async fn consume(&mut self, event: ChainEvent) -> std::result::Result<DeliveryPayload, HandlerError> {
        if let Some(agg) = ProcessingTimeWindowAggregate::from_event(&event) {
            let mut guard = self.seen.lock().unwrap();
            guard.push(agg);
        }
        Ok(DeliveryPayload::success(
            "aggregate_sink",
            DeliveryMethod::Custom("Collect".to_string()),
            None,
        ))
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
        let ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) =
            &env.event.content
        else {
            continue;
        };

        let authored_by_stage = match writer_id {
            Some(WriterId::Stage(eof_stage)) => *eof_stage == stage_id,
            Some(_) => false,
            None => true,
        };

        if authored_by_stage {
            if let ChainEventContent::FlowControl(payload) = &env.event.content {
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
        let ChainEventContent::Data { event_type: ty, .. } = &env.event.content else {
            continue;
        };
        if ty == event_type {
            ids.push(env.event.id);
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
        let ChainEventContent::Data { event_type, .. } = &env.event.content else {
            continue;
        };
        if ProcessingTimeWindowAggregate::event_type_matches(event_type.as_str()) {
            last = Some(env.event.clone());
        }
    }
    Ok(last)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processing_time_window_flushes_final_partial_window_before_authored_eof() -> Result<()> {
    let (sink, seen) = AggregateSink::new();
    let harness = test_flow! {
        name: "processing_time_window_final_flush",
        journals: disk_journals(unique_journal_dir("processing_time_window_final_flush")),
        middleware: [],

        stages: {
            src = source!(WindowInput => SequenceSource::new(3));
            win = stateful!(WindowInput -> ProcessingTimeWindowAggregate =>
                ProcessingTimeTumblingWindow::<WindowInput>::count(Duration::from_secs(60)),
                emit_interval = Duration::from_secs(60)
            );
            snk = sink!(ProcessingTimeWindowAggregate => sink);
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

    let aggregates = seen.lock().unwrap().clone();
    assert_eq!(
        aggregates.len(),
        1,
        "expected exactly one final aggregate (no time window completion before EOF)"
    );
    assert_eq!(aggregates[0].event_count, 3);

    let ordered = win_journal
        .read_causally_ordered()
        .await
        .map_err(|e| anyhow!("failed to read win stage journal: {e}"))?;

    let mut last_agg_idx: Option<usize> = None;
    let mut last_agg_event_id: Option<obzenflow_core::EventId> = None;
    for (idx, env) in ordered.iter().enumerate() {
        if let ChainEventContent::Data { event_type, .. } = &env.event.content {
            if ProcessingTimeWindowAggregate::event_type_matches(event_type.as_str()) {
                last_agg_idx = Some(idx);
                last_agg_event_id = Some(env.event.id);
            }
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
            last_event_id,
            last_agg_event_id,
            "expected authored EOF last_event_id to prefer the final aggregate"
        );
    } else {
        unreachable!("authored_eof_position returned non-EOF payload")
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processing_time_window_final_aggregate_preserves_buffered_input_lineage() -> Result<()> {
    let (sink, _seen) = AggregateSink::new();
    let harness = test_flow! {
        name: "processing_time_window_lineage",
        journals: disk_journals(unique_journal_dir("processing_time_window_lineage")),
        middleware: [],

        stages: {
            src = source!(WindowInput => SequenceSource::new(3));
            win = stateful!(WindowInput -> ProcessingTimeWindowAggregate =>
                ProcessingTimeTumblingWindow::<WindowInput>::count(Duration::from_secs(60)),
                emit_interval = Duration::from_secs(60)
            );
            snk = sink!(ProcessingTimeWindowAggregate => sink);
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

    let input_ids = data_event_ids_of_type(&src_journal, WindowInput::EVENT_TYPE).await?;
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
        aggregate.causality.parent_ids,
        input_ids,
        "expected aggregate causality parents to be the buffered input event IDs"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processing_time_window_final_aggregate_records_mixed_correlation_ids() -> Result<()> {
    let correlation_a = CorrelationId::new();
    let correlation_b = CorrelationId::new();

    let (sink, seen) = AggregateSink::new();
    let harness = test_flow! {
        name: "processing_time_window_mixed_correlation",
        journals: disk_journals(unique_journal_dir("processing_time_window_mixed_correlation")),
        middleware: [],

        stages: {
            src = source!(WindowInput => CorrelatedSequenceSource::new(vec![correlation_a, correlation_b]));
            win = stateful!(WindowInput -> ProcessingTimeWindowAggregate =>
                ProcessingTimeTumblingWindow::<WindowInput>::count(Duration::from_secs(60)),
                emit_interval = Duration::from_secs(60)
            );
            snk = sink!(ProcessingTimeWindowAggregate => sink);
        },

        topology: {
            src |> win;
            win |> snk;
        }
    }
    .await
    .map_err(|e| anyhow!("failed to build test flow: {e}"))?;

    let (_win_stage_id, win_journal) = harness.stage_journal_for_test("win")?;

    harness
        .into_inner()
        .run()
        .await
        .map_err(|e| anyhow!("flow run failed: {e}"))?;

    let aggregates = seen.lock().unwrap().clone();
    assert_eq!(aggregates.len(), 1);
    let recorded = aggregates[0]
        .correlation_ids
        .clone()
        .expect("expected correlation_ids to be present for mixed windows");
    assert_eq!(recorded.len(), 2);
    assert!(recorded.contains(&correlation_a));
    assert!(recorded.contains(&correlation_b));
    assert!(recorded.windows(2).all(|w| w[0] <= w[1]), "expected deterministic sort");

    let aggregate_event = last_window_aggregate_event(&win_journal)
        .await?
        .ok_or_else(|| anyhow!("expected win stage to emit an aggregate"))?;
    assert!(
        aggregate_event.correlation_id.is_none(),
        "mixed windows must not propagate a scalar correlation_id on the aggregate event"
    );

    Ok(())
}
