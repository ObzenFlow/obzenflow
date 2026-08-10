// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134f journal oracle for typed joins, contribution evidence, and replay.

use obzenflow_core::event::context::CompositeActivationContext;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::id::CompositeId;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, JoinReferenceView, TypedJoinHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::sink::SinkTyped;
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ReferenceItem {
    key: String,
    version: String,
}

impl TypedPayload for ReferenceItem {
    const EVENT_TYPE: &'static str = "flowip_134f.reference";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct StreamItem {
    key: String,
    reject: bool,
}

impl TypedPayload for StreamItem {
    const EVENT_TYPE: &'static str = "flowip_134f.stream";
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct JoinedFact {
    phase: String,
    key: String,
    reference_version: String,
    ordinal: usize,
}

impl TypedPayload for JoinedFact {
    const EVENT_TYPE: &'static str = "flowip_134f.joined";
}

#[derive(Clone, Debug)]
struct ReferenceSource {
    rows: Vec<ReferenceItem>,
    next: usize,
    writer_id: WriterId,
    reads: Arc<AtomicUsize>,
}

impl ReferenceSource {
    fn new(reads: Arc<AtomicUsize>) -> Self {
        Self {
            rows: vec![
                ReferenceItem {
                    key: "k1".to_string(),
                    version: "old".to_string(),
                },
                ReferenceItem {
                    key: "k1".to_string(),
                    version: "new".to_string(),
                },
                ReferenceItem {
                    key: "k2".to_string(),
                    version: "terminal".to_string(),
                },
            ],
            next: 0,
            writer_id: WriterId::from(StageId::new()),
            reads,
        }
    }
}

impl FiniteSourceHandler for ReferenceSource {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let Some(row) = self.rows.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        let label = format!("reference:{}:{}", row.key, row.version);
        let event = row.to_event(self.writer_id);
        let activation = CompositeActivationContext::new(
            CompositeId::new("flowip-134f:reference"),
            event.id,
            label,
            self.next as u64,
        );
        Ok(Some(vec![event
            .try_with_composite_activations(vec![activation])
            .expect("reference activation")]))
    }
}

#[derive(Clone, Debug)]
struct StreamSource {
    rows: Vec<StreamItem>,
    next: usize,
    writer_id: WriterId,
    reads: Arc<AtomicUsize>,
}

impl StreamSource {
    fn new(reads: Arc<AtomicUsize>) -> Self {
        Self {
            rows: vec![
                StreamItem {
                    key: "k1".to_string(),
                    reject: false,
                },
                StreamItem {
                    key: "foreign-error".to_string(),
                    reject: true,
                },
                StreamItem {
                    key: "k2".to_string(),
                    reject: false,
                },
            ],
            next: 0,
            writer_id: WriterId::from(StageId::new()),
            reads,
        }
    }
}

impl FiniteSourceHandler for StreamSource {
    fn bind_writer_id(&mut self, writer_id: WriterId) {
        self.writer_id = writer_id;
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let Some(row) = self.rows.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        let label = format!("stream:{}", row.key);
        let event = row.to_event(self.writer_id);
        let activation = CompositeActivationContext::new(
            CompositeId::new("flowip-134f:stream"),
            event.id,
            label,
            self.next as u64,
        );
        Ok(Some(vec![event
            .try_with_composite_activations(vec![activation])
            .expect("stream activation")]))
    }
}

#[derive(Clone, Debug)]
struct IdentityReference;

impl TypedTransformHandler for IdentityReference {
    type Input = ReferenceItem;
    type Output = ReferenceItem;

    fn process(&self, reference: ReferenceItem) -> Result<ReferenceItem, HandlerError> {
        Ok(reference)
    }
}

#[derive(Clone, Debug)]
struct RejectMarkedStream;

impl TypedTransformHandler for RejectMarkedStream {
    type Input = StreamItem;
    type Output = StreamItem;

    fn process(&self, stream: StreamItem) -> Result<StreamItem, HandlerError> {
        if stream.reject {
            Err(HandlerError::Domain(
                "intentional foreign-family row".to_string(),
            ))
        } else {
            Ok(stream)
        }
    }
}

#[derive(Clone, Debug)]
struct ExactJoin {
    calls: Arc<AtomicUsize>,
}

impl TypedJoinHandler for ExactJoin {
    type State = ();
    type ReferenceKey = String;
    type Reference = ReferenceItem;
    type Stream = StreamItem;
    type Output = JoinedFact;

    fn initial_state(&self) -> Self::State {}

    fn admit_reference(&self, reference: &Self::Reference) -> Result<String, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(reference.key.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, ReferenceItem>,
        stream: StreamItem,
    ) -> Result<Vec<JoinedFact>, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let Some(reference) = references.select(&stream.key) else {
            return Ok(Vec::new());
        };
        let count = if stream.key == "k1" { 2 } else { 1 };
        Ok((0..count)
            .map(|ordinal| JoinedFact {
                phase: "stream".to_string(),
                key: stream.key.clone(),
                reference_version: reference.version.clone(),
                ordinal,
            })
            .collect())
    }

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, ReferenceItem>,
    ) -> Result<Vec<JoinedFact>, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let reference = references
            .select(&"k2".to_string())
            .expect("terminal reference exists");
        Ok(vec![JoinedFact {
            phase: "hook".to_string(),
            key: "k2".to_string(),
            reference_version: reference.version,
            ordinal: 0,
        }])
    }

    fn drain(
        &self,
        _state: &Self::State,
        references: &mut JoinReferenceView<'_, String, ReferenceItem>,
    ) -> Result<Vec<JoinedFact>, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let reference = references
            .select(&"k1".to_string())
            .expect("drain reference exists");
        Ok(vec![JoinedFact {
            phase: "drain".to_string(),
            key: "k1".to_string(),
            reference_version: reference.version,
            ordinal: 0,
        }])
    }
}

fn build_flow(
    journal_base: PathBuf,
    reference_reads: Arc<AtomicUsize>,
    stream_reads: Arc<AtomicUsize>,
    join_calls: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let references = ReferenceSource::new(reference_reads.clone());
        let streams = StreamSource::new(stream_reads.clone());
        let reference_validate = IdentityReference;
        let stream_validate = RejectMarkedStream;
        let joined = ExactJoin {
            calls: join_calls.clone(),
        };
        let output = SinkTyped::new(|_fact: JoinedFact| async move {}).idempotent();

        Ok(flow! {
            name: "typed_join_journal_parity",
            journals: disk_journals(journal_base),

            stages: {
                references = source!(ReferenceItem => references);
                reference_validate = transform!(ReferenceItem -> ReferenceItem => reference_validate);
                streams = source!(StreamItem => streams);
                stream_validate = transform!(StreamItem -> StreamItem => stream_validate);
                joined = join!(catalog reference_validate: ReferenceItem, StreamItem -> JoinedFact => joined);
                output = sink!(JoinedFact => output);
            },

            topology: {
                references |> reference_validate;
                streams |> stream_validate;
                stream_validate |> joined;
                joined |> output;
            }
        })
    })
}

async fn run(
    journal_base: &Path,
    replay_from: Option<&Path>,
    reference_reads: Arc<AtomicUsize>,
    stream_reads: Arc<AtomicUsize>,
    join_calls: Arc<AtomicUsize>,
) {
    let mut args = vec![OsString::from("obzenflow")];
    if let Some(archive) = replay_from {
        args.push(OsString::from("--replay-from"));
        args.push(archive.as_os_str().to_os_string());
    }
    FlowApplication::builder()
        .with_cli_args(args)
        .run_async(build_flow(
            journal_base.to_path_buf(),
            reference_reads,
            stream_reads,
            join_calls,
        ))
        .await
        .expect("typed join parity flow completes");
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory")
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

async fn read_stage_appended(run_dir: &Path, stage_name: &str) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest data journal for {stage_name}"));
    let journal = DiskJournal::<ChainEvent>::with_owner(
        run_dir.join(journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .expect("stage journal opens");
    let mut reader = journal.reader().await.expect("stage journal reader");
    let mut events = Vec::new();
    while let Some(event) = reader.next().await.expect("stage journal read") {
        events.push(event);
    }
    events
}

fn stage_writer(run_dir: &Path, stage_name: &str) -> WriterId {
    let manifest = archive_manifest(run_dir);
    let stage_id = manifest["stages"][stage_name]["stage_id"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest stage id for {stage_name}"))
        .parse::<StageId>()
        .unwrap_or_else(|error| panic!("stage id for {stage_name}: {error}"));
    WriterId::from(stage_id)
}

fn transport_signature(run_dir: &Path, events: &[EventEnvelope<ChainEvent>]) -> Vec<String> {
    let reference_writer = stage_writer(run_dir, "reference_validate");
    let stream_writer = stage_writer(run_dir, "stream_validate");
    let join_writer = stage_writer(run_dir, "joined");
    events
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data { .. } => {
                let status = if matches!(
                    envelope.event.processing_info.status,
                    ProcessingStatus::Error { .. }
                ) {
                    "error"
                } else {
                    "success"
                };
                Some(format!("data:{}:{status}", envelope.event.event_type()))
            }
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                let writer = writer_id.unwrap_or(envelope.event.writer_id);
                let role = if writer == reference_writer {
                    "reference"
                } else if writer == stream_writer {
                    "stream"
                } else if writer == join_writer {
                    "local"
                } else {
                    "foreign"
                };
                Some(format!("eof:{role}"))
            }
            _ => None,
        })
        .collect()
}

fn facts(events: &[EventEnvelope<ChainEvent>]) -> Vec<JoinedFact> {
    events
        .iter()
        .filter_map(|envelope| JoinedFact::from_event(&envelope.event))
        .collect()
}

fn has_activation(event: &ChainEvent, entry_port: &str) -> bool {
    event
        .composite_activations()
        .iter()
        .any(|activation| activation.entry_port == entry_port)
}

fn assert_journal_contract(run_dir: &Path, events: &[EventEnvelope<ChainEvent>]) {
    let expected = vec![
        JoinedFact {
            phase: "stream".to_string(),
            key: "k1".to_string(),
            reference_version: "new".to_string(),
            ordinal: 0,
        },
        JoinedFact {
            phase: "stream".to_string(),
            key: "k1".to_string(),
            reference_version: "new".to_string(),
            ordinal: 1,
        },
        JoinedFact {
            phase: "stream".to_string(),
            key: "k2".to_string(),
            reference_version: "terminal".to_string(),
            ordinal: 0,
        },
        JoinedFact {
            phase: "hook".to_string(),
            key: "k2".to_string(),
            reference_version: "terminal".to_string(),
            ordinal: 0,
        },
        JoinedFact {
            phase: "drain".to_string(),
            key: "k1".to_string(),
            reference_version: "new".to_string(),
            ordinal: 0,
        },
    ];
    assert_eq!(facts(events), expected);

    let writer = stage_writer(run_dir, "joined");
    let authored = events
        .iter()
        .filter(|envelope| JoinedFact::from_event(&envelope.event).is_some())
        .collect::<Vec<_>>();
    assert!(authored.iter().all(|envelope| {
        envelope.event.writer_id == writer
            && envelope.event.event_type() == JoinedFact::versioned_event_type()
    }));

    let k1 = authored
        .iter()
        .filter(|envelope| {
            JoinedFact::from_event(&envelope.event).is_some_and(|fact| fact.key == "k1")
        })
        .collect::<Vec<_>>();
    assert!(k1.iter().all(|envelope| {
        has_activation(&envelope.event, "reference:k1:new")
            && !has_activation(&envelope.event, "reference:k1:old")
    }));
    assert!(authored
        .iter()
        .filter(|envelope| {
            JoinedFact::from_event(&envelope.event).is_some_and(|fact| fact.key == "k2")
        })
        .all(|envelope| has_activation(&envelope.event, "reference:k2:terminal")));

    let local_eof = events
        .iter()
        .rev()
        .find(|envelope| envelope.event.writer_id == writer && envelope.event.is_eof())
        .expect("join-authored EOF");
    let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_seq_by_event_type,
        ..
    }) = &local_eof.event.content
    else {
        unreachable!("local EOF shape")
    };
    let compatible = writer_seq_by_event_type
        .iter()
        .filter(|(key, _)| JoinedFact::event_type_matches(key.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(compatible.len(), 1);
    assert_eq!(compatible[0].0.as_str(), JoinedFact::versioned_event_type());
    assert_eq!(compatible[0].1 .0, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_join_has_live_replay_journal_parity_and_zero_replay_reads() {
    let temp = tempfile::tempdir().expect("parity tempdir");
    let journal_base = temp.path().join("journals");

    let live_reference_reads = Arc::new(AtomicUsize::new(0));
    let live_stream_reads = Arc::new(AtomicUsize::new(0));
    let live_join_calls = Arc::new(AtomicUsize::new(0));
    run(
        &journal_base,
        None,
        live_reference_reads.clone(),
        live_stream_reads.clone(),
        live_join_calls.clone(),
    )
    .await;
    assert!(live_reference_reads.load(Ordering::SeqCst) > 0);
    assert!(live_stream_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(live_join_calls.load(Ordering::SeqCst), 7);

    let live = latest_run_dir(&journal_base);
    let live_join = read_stage_appended(&live, "joined").await;
    assert_journal_contract(&live, &live_join);

    let validator_rows = read_stage_appended(&live, "stream_validate").await;
    let foreign = validator_rows
        .iter()
        .find(|envelope| {
            envelope.event.is_data()
                && matches!(
                    envelope.event.processing_info.status,
                    ProcessingStatus::Error { .. }
                )
        })
        .expect("validator foreign-family error row");
    let forwarded = live_join
        .iter()
        .find(|envelope| envelope.event.id == foreign.event.id)
        .expect("join forwards the same error envelope");
    assert_eq!(forwarded.event.writer_id, foreign.event.writer_id);
    assert_eq!(forwarded.event.event_type(), foreign.event.event_type());

    let replay_reference_reads = Arc::new(AtomicUsize::new(0));
    let replay_stream_reads = Arc::new(AtomicUsize::new(0));
    let replay_join_calls = Arc::new(AtomicUsize::new(0));
    run(
        &journal_base,
        Some(&live),
        replay_reference_reads.clone(),
        replay_stream_reads.clone(),
        replay_join_calls.clone(),
    )
    .await;
    assert_eq!(replay_reference_reads.load(Ordering::SeqCst), 0);
    assert_eq!(replay_stream_reads.load(Ordering::SeqCst), 0);
    assert_eq!(replay_join_calls.load(Ordering::SeqCst), 7);

    let replay = latest_run_dir(&journal_base);
    assert_ne!(live, replay);
    let replay_join = read_stage_appended(&replay, "joined").await;
    assert_journal_contract(&replay, &replay_join);
    assert_eq!(facts(&replay_join), facts(&live_join));
    assert_eq!(
        transport_signature(&live, &live_join),
        transport_signature(&replay, &replay_join),
        "fan-in transport order is replay-stable"
    );
}
