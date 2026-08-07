// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134b journal oracle for scalar and dynamic synchronous transforms.

#[path = "../examples/csv_demo_support_sla/domain.rs"]
#[allow(dead_code)]
mod support_domain;

use obzenflow_core::ai::{ChunkEnvelope, TokenCount};
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
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
use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;
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

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticket_source = TypedValuesSource::new(ticket_fixtures());
        let seed_source = TypedValuesSource::new(chunk_fixtures());
        let triage = TicketTriageWitness;
        let chunker = ChunkByBudgetBuilder::new()
            .items(|seed: &ChunkSeed| seed.items.clone())
            .render(|item: &String, _context| item.clone())
            .budget(TokenCount::new(64))
            .max_items_per_chunk(Some(1))
            .build();
        let ticket_sink = SinkTyped::new(|_ticket: TriagedTicket| async move {}).idempotent();
        let chunk_sink = SinkTyped::new(|_chunk: ChunkEnvelope<String>| async move {}).idempotent();

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
            },

            topology: {
                tickets |> triage;
                triage |> ticket_sink;
                seeds |> chunks;
                chunks |> chunk_sink;
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

async fn read_stage(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json")).expect("manifest is readable"),
    )
    .expect("manifest parses");
    let journal_file = manifest["stages"][stage_name]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest names stage {stage_name}"));
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
    let live_triage = read_stage(&live, "triage").await;
    let live_chunks = read_stage(&live, "chunks").await;

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

    run(&journal_base, Some(&live)).await;
    let replay = latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    let replay_triage = read_stage(&replay, "triage").await;
    let replay_chunks = read_stage(&replay, "chunks").await;

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
}
