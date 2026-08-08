// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134b journal oracle for scalar and dynamic synchronous transforms.

#[path = "../examples/csv_demo_support_sla/domain.rs"]
#[allow(dead_code)]
mod support_domain;

use obzenflow_core::ai::{ChunkEnvelope, TokenCount};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{FiniteSourceHandler, TypedTransformHandler};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::transform::{ChunkByBudgetBuilder, FilterTyped, TryMapTyped};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use support_domain::{triage_ticket, Ticket, TriagedTicket};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ChunkSeed {
    items: Vec<String>,
}

impl TypedPayload for ChunkSeed {
    const EVENT_TYPE: &'static str = "flowip_134b.chunk_seed";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct TryMapRecord {
    index: u64,
}

impl TypedPayload for TryMapRecord {
    const EVENT_TYPE: &'static str = "flowip_134b.try_map_record";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct FilterRecord {
    id: String,
    keep: bool,
}

impl TypedPayload for FilterRecord {
    const EVENT_TYPE: &'static str = "flowip_134b.filter_record";
}

#[derive(Clone, Debug)]
struct TypedValuesSource<T> {
    values: Vec<T>,
    next: usize,
    writer_id: WriterId,
}

impl<T> TypedValuesSource<T> {
    fn new(values: Vec<T>) -> Self {
        Self {
            values,
            next: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl<T> FiniteSourceHandler for TypedValuesSource<T>
where
    T: TypedPayload + Clone + Debug + Send + Sync + 'static,
{
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::SourceError,
    > {
        let Some(value) = self.values.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(vec![value.to_event(self.writer_id)]))
    }
}

#[derive(Clone, Debug)]
struct TicketTriageWitness;

impl TypedTransformHandler for TicketTriageWitness {
    type Input = Ticket;
    type Output = TriagedTicket;

    fn process(&self, ticket: Ticket) -> Result<TriagedTicket, HandlerError> {
        Ok(triage_ticket(ticket))
    }
}

fn ticket_fixtures() -> Vec<Ticket> {
    vec![
        Ticket {
            ticket_id: "T-100".to_string(),
            customer_id: "C-1".to_string(),
            created_at: "2026-08-06T10:00:00Z".to_string(),
            priority: "P0".to_string(),
            category: "outage".to_string(),
        },
        Ticket {
            ticket_id: "T-101".to_string(),
            customer_id: "C-2".to_string(),
            created_at: "2026-08-06T11:00:00Z".to_string(),
            priority: "P2".to_string(),
            category: "billing".to_string(),
        },
    ]
}

fn chunk_fixtures() -> Vec<ChunkSeed> {
    vec![
        ChunkSeed { items: Vec::new() },
        ChunkSeed {
            items: vec!["one".to_string()],
        },
        ChunkSeed {
            items: vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()],
        },
    ]
}

fn try_map_fixtures() -> Vec<TryMapRecord> {
    vec![TryMapRecord { index: 0 }, TryMapRecord { index: 1 }]
}

fn left_filter_fixtures() -> Vec<FilterRecord> {
    vec![
        FilterRecord {
            id: "left-drop".to_string(),
            keep: false,
        },
        FilterRecord {
            id: "left-keep".to_string(),
            keep: true,
        },
    ]
}

fn right_filter_fixtures() -> Vec<FilterRecord> {
    vec![
        FilterRecord {
            id: "right-keep".to_string(),
            keep: true,
        },
        FilterRecord {
            id: "right-drop".to_string(),
            keep: false,
        },
    ]
}

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticket_source = TypedValuesSource::new(ticket_fixtures());
        let seed_source = TypedValuesSource::new(chunk_fixtures());
        let try_map_source = TypedValuesSource::new(try_map_fixtures());
        let left_filter_source = TypedValuesSource::new(left_filter_fixtures());
        let right_filter_source = TypedValuesSource::new(right_filter_fixtures());
        let triage = TicketTriageWitness;
        let chunker = ChunkByBudgetBuilder::new()
            .items(|seed: &ChunkSeed| seed.items.clone())
            .render(|item: &String, _context| item.clone())
            .budget(TokenCount::new(64))
            .max_items_per_chunk(Some(1))
            .build();
        let try_map = TryMapTyped::new(
            |record: TryMapRecord| -> Result<TryMapRecord, &'static str> {
                if record.index == 0 {
                    Err("index zero is invalid")
                } else {
                    Ok(record)
                }
            },
        );
        let left_filter = FilterTyped::new(|record: &FilterRecord| record.keep);
        let right_filter = FilterTyped::new(|record: &FilterRecord| record.keep);
        let ticket_sink = SinkTyped::new(|_ticket: TriagedTicket| async move {}).idempotent();
        let chunk_sink = SinkTyped::new(|_chunk: ChunkEnvelope<String>| async move {}).idempotent();
        let try_map_sink = SinkTyped::new(|_record: TryMapRecord| async move {}).idempotent();
        let merged_filter_sink = SinkTyped::new(|_record: FilterRecord| async move {}).idempotent();
        let fan_out_filter_sink =
            SinkTyped::new(|_record: FilterRecord| async move {}).idempotent();

        Ok(flow! {
            name: "typed_sync_transform_journal_parity",
            journals: disk_journals(journal_base),
            middleware: [],

            stages: {
                tickets = source!(Ticket => ticket_source);
                triage = transform!(Ticket -> TriagedTicket => triage);
                ticket_sink = sink!(TriagedTicket => ticket_sink);

                seeds = source!(ChunkSeed => seed_source);
                chunks = transform!(ChunkSeed -> ChunkEnvelope<String> => chunker);
                chunk_sink = sink!(ChunkEnvelope<String> => chunk_sink);

                try_map_inputs = source!(TryMapRecord => try_map_source);
                try_map = transform!(TryMapRecord -> TryMapRecord => try_map);
                try_map_sink = sink!(TryMapRecord => try_map_sink);

                left_filter_inputs = source!(FilterRecord => left_filter_source);
                left_filter = transform!(FilterRecord -> FilterRecord => left_filter);
                right_filter_inputs = source!(FilterRecord => right_filter_source);
                right_filter = transform!(FilterRecord -> FilterRecord => right_filter);
                merged_filter_sink = sink!(FilterRecord => merged_filter_sink);
                fan_out_filter_sink = sink!(FilterRecord => fan_out_filter_sink);
            },

            topology: {
                tickets |> triage;
                triage |> ticket_sink;
                seeds |> chunks;
                chunks |> chunk_sink;
                try_map_inputs |> try_map;
                try_map |> try_map_sink;
                left_filter_inputs |> left_filter;
                right_filter_inputs |> right_filter;
                left_filter |> merged_filter_sink;
                right_filter |> merged_filter_sink;
                left_filter |> fan_out_filter_sink;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory exists")
        .map(|entry| entry.expect("flow directory entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("flow produced a replay archive")
}

fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest is readable"),
    )
    .expect("manifest parses")
}

fn stage_writer(run_dir: &Path, stage_name: &str) -> WriterId {
    let manifest = archive_manifest(run_dir);
    let stage_id = manifest["stages"][stage_name]["stage_id"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names stage ID for {stage_name}"))
        .parse::<StageId>()
        .unwrap_or_else(|error| panic!("stage ID for {stage_name} parses: {error}"));
    WriterId::from(stage_id)
}

async fn read_stage_journal(
    run_dir: &Path,
    stage_name: &str,
    manifest_field: &str,
) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name][manifest_field]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names {manifest_field} for stage {stage_name}"));
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("stage journal reads")
}

async fn read_stage(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    read_stage_journal(run_dir, stage_name, "data_journal_file").await
}

async fn read_stage_errors(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    read_stage_journal(run_dir, stage_name, "error_journal_file").await
}

fn triage_projection(events: &[EventEnvelope<ChainEvent>]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } if TriagedTicket::event_type_matches(event_type) => Some(payload.clone()),
            _ => None,
        })
        .collect()
}

fn chunk_projection(events: &[EventEnvelope<ChainEvent>]) -> Vec<ChunkEnvelope<String>> {
    events
        .iter()
        .filter_map(|envelope| ChunkEnvelope::<String>::from_event(&envelope.event))
        .collect()
}

fn chunk_journal_sequence(events: &[EventEnvelope<ChainEvent>]) -> Vec<&'static str> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::Custom { name, .. },
            )) if name == "ai_chunking.snapshot" => Some("snapshot"),
            ChainEventContent::Data { event_type, .. }
                if ChunkEnvelope::<String>::event_type_matches(event_type) =>
            {
                Some("chunk")
            }
            _ => None,
        })
        .collect()
}

fn snapshot_projection(events: &[EventEnvelope<ChainEvent>]) -> Vec<serde_json::Value> {
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Observability(ObservabilityPayload::Metrics(
                MetricsLifecycle::Custom { name, value, .. },
            )) if name == "ai_chunking.snapshot" => Some(value.clone()),
            _ => None,
        })
        .collect()
}

fn try_map_success_projection(events: &[EventEnvelope<ChainEvent>]) -> Vec<TryMapRecord> {
    events
        .iter()
        .filter_map(|envelope| {
            matches!(
                envelope.event.processing_info.status,
                ProcessingStatus::Success
            )
            .then(|| TryMapRecord::from_event(&envelope.event))
            .flatten()
        })
        .collect()
}

fn try_map_error_projection(events: &[EventEnvelope<ChainEvent>]) -> Vec<(u64, ErrorKind)> {
    events
        .iter()
        .filter_map(|envelope| {
            let record = TryMapRecord::from_event(&envelope.event)?;
            let ProcessingStatus::Error {
                kind: Some(kind), ..
            } = &envelope.event.processing_info.status
            else {
                return None;
            };
            Some((record.index, kind.clone()))
        })
        .collect()
}

fn filter_projection(events: &[EventEnvelope<ChainEvent>]) -> Vec<FilterRecord> {
    events
        .iter()
        .filter_map(|envelope| FilterRecord::from_event(&envelope.event))
        .collect()
}

fn delivery_count(events: &[EventEnvelope<ChainEvent>]) -> usize {
    events
        .iter()
        .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Delivery(_)))
        .count()
}

fn assert_canonical_event_type_and_eof<T: TypedPayload>(
    events: &[EventEnvelope<ChainEvent>],
    expected_data_rows: usize,
) {
    let canonical = T::versioned_event_type();
    let rows = events
        .iter()
        .filter(|envelope| T::event_type_matches(&envelope.event.event_type()))
        .collect::<Vec<_>>();
    assert_eq!(rows.len(), expected_data_rows);
    assert!(rows
        .iter()
        .all(|envelope| envelope.event.event_type() == canonical));

    let eof_keys = events
        .iter()
        .rev()
        .find_map(|envelope| match &envelope.event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof {
                writer_seq_by_event_type,
                ..
            }) => Some(writer_seq_by_event_type),
            _ => None,
        })
        .expect("typed transform journal contains EOF evidence");
    if expected_data_rows == 0 {
        assert!(!eof_keys.keys().any(|key| key.as_str() == canonical));
    } else {
        let matching = eof_keys
            .iter()
            .filter(|(key, _)| T::event_type_matches(key.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            matching.len(),
            1,
            "one canonical event-type bucket appears in EOF evidence: {eof_keys:?}"
        );
        assert_eq!(matching[0].0.as_str(), canonical);
        assert_eq!(matching[0].1 .0, expected_data_rows as u64);
    }
    assert!(
        eof_keys.keys().all(|key| key.as_str() != T::EVENT_TYPE),
        "legacy semantic keys must not appear in migrated EOF maps"
    );
}

fn assert_derived_stage_authorship<T: TypedPayload>(
    run_dir: &Path,
    stage_name: &str,
    parent_events: &[EventEnvelope<ChainEvent>],
    output_events: &[EventEnvelope<ChainEvent>],
) {
    let writer = stage_writer(run_dir, stage_name);
    let writer_clock = writer.to_string();
    let parents = parent_events
        .iter()
        .filter(|envelope| envelope.event.is_data())
        .map(|envelope| (envelope.event.id, envelope.event.writer_id))
        .collect::<std::collections::HashMap<_, _>>();
    for output in output_events
        .iter()
        .filter(|envelope| T::event_type_matches(&envelope.event.event_type()))
    {
        assert_eq!(output.event.writer_id, writer);
        let parent_id = output
            .event
            .causality
            .parent_ids
            .first()
            .expect("derived transform fact names its direct parent");
        let parent_writer = parents
            .get(parent_id)
            .unwrap_or_else(|| panic!("parent {parent_id} exists in the upstream journal"));
        assert!(output.vector_clock.get(&writer_clock) > 0);
        assert!(output.vector_clock.get(&parent_writer.to_string()) > 0);
    }
}

async fn run(journal_base: &Path, replay_from: Option<&Path>) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(journal_base.to_path_buf()))
        .await
        .expect("typed transform parity flow completes");
}

#[tokio::test(flavor = "multi_thread")]
async fn scalar_and_dynamic_typed_outputs_have_live_replay_journal_parity() {
    let temp = tempfile::tempdir().expect("temporary journal root");
    let journal_base = temp.path().join("journals");

    run(&journal_base, None).await;
    let live = latest_run_dir(&journal_base);
    let live_tickets = read_stage(&live, "tickets").await;
    let live_triage = read_stage(&live, "triage").await;
    let live_seeds = read_stage(&live, "seeds").await;
    let live_chunks = read_stage(&live, "chunks").await;
    let live_try_map_inputs = read_stage(&live, "try_map_inputs").await;
    let live_try_map = read_stage(&live, "try_map").await;
    let live_try_map_errors = read_stage_errors(&live, "try_map").await;
    let live_left_filter_inputs = read_stage(&live, "left_filter_inputs").await;
    let live_left_filter = read_stage(&live, "left_filter").await;
    let live_right_filter_inputs = read_stage(&live, "right_filter_inputs").await;
    let live_right_filter = read_stage(&live, "right_filter").await;
    let live_merged_filter_sink = read_stage(&live, "merged_filter_sink").await;
    let live_fan_out_filter_sink = read_stage(&live, "fan_out_filter_sink").await;

    assert_eq!(
        triage_projection(&live_triage),
        vec![
            serde_json::json!({
                "ticket_id": "T-100",
                "customer_id": "C-1",
                "created_at": "2026-08-06T10:00:00Z",
                "priority": "P0",
                "category": "outage",
                "priority_sla_hours": 4
            }),
            serde_json::json!({
                "ticket_id": "T-101",
                "customer_id": "C-2",
                "created_at": "2026-08-06T11:00:00Z",
                "priority": "P2",
                "category": "billing",
                "priority_sla_hours": 72
            }),
        ]
    );

    assert_eq!(
        chunk_journal_sequence(&live_chunks),
        vec!["snapshot", "snapshot", "chunk", "snapshot", "chunk", "chunk", "chunk"],
        "each invocation commits its snapshot before zero, one, or many flat chunks"
    );
    let live_chunk_projection = chunk_projection(&live_chunks);
    assert_eq!(live_chunk_projection.len(), 4);
    assert!(live_chunk_projection
        .iter()
        .all(|chunk| chunk.items.len() == 1));
    let live_snapshots = snapshot_projection(&live_chunks);
    assert_eq!(live_snapshots.len(), 3);
    assert_eq!(live_snapshots[0]["chunk_count"], 0);
    assert_eq!(live_snapshots[1]["chunk_count"], 1);
    assert_eq!(live_snapshots[2]["chunk_count"], 3);
    assert!(live_chunks.iter().all(|envelope| {
        !matches!(
            &envelope.event.content,
            ChainEventContent::Data { event_type, .. }
                if event_type.as_str().contains("StageOutputs")
        )
    }));
    assert_eq!(
        try_map_success_projection(&live_try_map),
        vec![TryMapRecord { index: 1 }]
    );
    assert_eq!(
        try_map_error_projection(&live_try_map_errors),
        vec![(0, ErrorKind::Unknown)]
    );
    assert_eq!(
        filter_projection(&live_left_filter),
        vec![FilterRecord {
            id: "left-keep".to_string(),
            keep: true,
        }]
    );
    assert_eq!(
        filter_projection(&live_right_filter),
        vec![FilterRecord {
            id: "right-keep".to_string(),
            keep: true,
        }]
    );
    assert_eq!(
        delivery_count(&live_merged_filter_sink),
        2,
        "the converged sink receives both selected filter facts"
    );
    assert_eq!(
        delivery_count(&live_fan_out_filter_sink),
        1,
        "the sibling fan-out sink receives the left selected fact once"
    );

    assert_canonical_event_type_and_eof::<TriagedTicket>(&live_triage, 2);
    assert_canonical_event_type_and_eof::<ChunkEnvelope<String>>(&live_chunks, 4);
    assert_canonical_event_type_and_eof::<TryMapRecord>(&live_try_map, 1);
    assert_canonical_event_type_and_eof::<FilterRecord>(&live_left_filter, 1);
    assert_canonical_event_type_and_eof::<FilterRecord>(&live_right_filter, 1);
    assert_derived_stage_authorship::<TriagedTicket>(&live, "triage", &live_tickets, &live_triage);
    assert_derived_stage_authorship::<ChunkEnvelope<String>>(
        &live,
        "chunks",
        &live_seeds,
        &live_chunks,
    );
    assert_derived_stage_authorship::<TryMapRecord>(
        &live,
        "try_map",
        &live_try_map_inputs,
        &live_try_map,
    );
    assert_derived_stage_authorship::<FilterRecord>(
        &live,
        "left_filter",
        &live_left_filter_inputs,
        &live_left_filter,
    );
    assert_derived_stage_authorship::<FilterRecord>(
        &live,
        "right_filter",
        &live_right_filter_inputs,
        &live_right_filter,
    );

    let rejected_input = live_try_map_inputs
        .iter()
        .find(|envelope| {
            TryMapRecord::from_event(&envelope.event).is_some_and(|record| record.index == 0)
        })
        .expect("source journal contains rejected try-map input");
    let error_parent = live_try_map_errors
        .iter()
        .find(|envelope| TryMapRecord::event_type_matches(&envelope.event.event_type()))
        .expect("try-map error journal contains rejected parent");
    assert_eq!(error_parent.event.id, rejected_input.event.id);
    assert_eq!(error_parent.event.writer_id, rejected_input.event.writer_id);
    assert_eq!(
        error_parent.event.causality.parent_ids,
        rejected_input.event.causality.parent_ids
    );

    run(&journal_base, Some(&live)).await;
    let replay = latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    let replay_tickets = read_stage(&replay, "tickets").await;
    let replay_triage = read_stage(&replay, "triage").await;
    let replay_seeds = read_stage(&replay, "seeds").await;
    let replay_chunks = read_stage(&replay, "chunks").await;
    let replay_try_map_inputs = read_stage(&replay, "try_map_inputs").await;
    let replay_try_map = read_stage(&replay, "try_map").await;
    let replay_try_map_errors = read_stage_errors(&replay, "try_map").await;
    let replay_left_filter_inputs = read_stage(&replay, "left_filter_inputs").await;
    let replay_left_filter = read_stage(&replay, "left_filter").await;
    let replay_right_filter_inputs = read_stage(&replay, "right_filter_inputs").await;
    let replay_right_filter = read_stage(&replay, "right_filter").await;
    let replay_merged_filter_sink = read_stage(&replay, "merged_filter_sink").await;
    let replay_fan_out_filter_sink = read_stage(&replay, "fan_out_filter_sink").await;

    assert_eq!(
        triage_projection(&replay_triage),
        triage_projection(&live_triage)
    );
    assert_eq!(chunk_projection(&replay_chunks), live_chunk_projection);
    assert_eq!(snapshot_projection(&replay_chunks), live_snapshots);
    assert_eq!(
        chunk_journal_sequence(&replay_chunks),
        chunk_journal_sequence(&live_chunks)
    );
    assert_eq!(
        try_map_success_projection(&replay_try_map),
        try_map_success_projection(&live_try_map)
    );
    assert_eq!(
        try_map_error_projection(&replay_try_map_errors),
        try_map_error_projection(&live_try_map_errors)
    );
    assert_eq!(
        filter_projection(&replay_left_filter),
        filter_projection(&live_left_filter)
    );
    assert_eq!(
        filter_projection(&replay_right_filter),
        filter_projection(&live_right_filter)
    );
    assert_eq!(
        delivery_count(&replay_merged_filter_sink),
        delivery_count(&live_merged_filter_sink)
    );
    assert_eq!(
        delivery_count(&replay_fan_out_filter_sink),
        delivery_count(&live_fan_out_filter_sink)
    );

    assert_canonical_event_type_and_eof::<TriagedTicket>(&replay_triage, 2);
    assert_canonical_event_type_and_eof::<ChunkEnvelope<String>>(&replay_chunks, 4);
    assert_canonical_event_type_and_eof::<TryMapRecord>(&replay_try_map, 1);
    assert_canonical_event_type_and_eof::<FilterRecord>(&replay_left_filter, 1);
    assert_canonical_event_type_and_eof::<FilterRecord>(&replay_right_filter, 1);
    assert_derived_stage_authorship::<TriagedTicket>(
        &replay,
        "triage",
        &replay_tickets,
        &replay_triage,
    );
    assert_derived_stage_authorship::<ChunkEnvelope<String>>(
        &replay,
        "chunks",
        &replay_seeds,
        &replay_chunks,
    );
    assert_derived_stage_authorship::<TryMapRecord>(
        &replay,
        "try_map",
        &replay_try_map_inputs,
        &replay_try_map,
    );
    assert_derived_stage_authorship::<FilterRecord>(
        &replay,
        "left_filter",
        &replay_left_filter_inputs,
        &replay_left_filter,
    );
    assert_derived_stage_authorship::<FilterRecord>(
        &replay,
        "right_filter",
        &replay_right_filter_inputs,
        &replay_right_filter,
    );
}
