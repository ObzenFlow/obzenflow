// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-134f journal oracle for typed joins, selected reference facts, and replay.
//!
//! Composite-activation selection is covered at the typed join adapter boundary,
//! where tests can supply genuine runtime-owned activation evidence. This
//! end-to-end oracle deliberately does not manufacture envelope provenance from
//! ordinary middleware.

mod replay_testkit;

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEvent, ChainPayload, JournalRecord};
use obzenflow_core::{StageId, StageOutputFacts, TypedPayload, WriterId};
use obzenflow_dsl::{flow, join, sink, source, transform, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    JoinReferenceView, TypedFiniteSourceHandler, TypedJoinHandler, TypedTransformHandler,
};
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
            reads,
        }
    }
}

impl TypedFiniteSourceHandler for ReferenceSource {
    type Output = ReferenceItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let Some(row) = self.rows.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(vec![row]))
    }
}

#[derive(Clone, Debug)]
struct StreamSource {
    rows: Vec<StreamItem>,
    next: usize,
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
            reads,
        }
    }
}

impl TypedFiniteSourceHandler for StreamSource {
    type Output = StreamItem;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let Some(row) = self.rows.get(self.next).cloned() else {
            return Ok(None);
        };
        self.next += 1;
        Ok(Some(vec![row]))
    }
}

#[derive(Clone, Debug, StageOutputFacts)]
enum MixedFact {
    Reference(ReferenceItem),
    Stream(StreamItem),
}

#[derive(Clone, Debug)]
struct MixedSource {
    rows: std::collections::VecDeque<MixedFact>,
    reads: Arc<AtomicUsize>,
}

impl TypedFiniteSourceHandler for MixedSource {
    type Output = MixedFact;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self.rows.pop_front().map(|row| vec![row]))
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
        let output = replay_testkit::Discard::<JoinedFact>::default();

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
                (reference_validate, stream_validate) |> joined;
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

async fn read_stage_appended(run_dir: &Path, stage_name: &str) -> Vec<JournalRecord<ChainPayload>> {
    let manifest = archive_manifest(run_dir);
    let journal_file = manifest["stages"][stage_name]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest data journal for {stage_name}"));
    replay_testkit::read_journal_envelopes_appended::<ChainEvent>(&run_dir.join(journal_file)).await
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

fn transport_signature(run_dir: &Path, events: &[JournalRecord<ChainPayload>]) -> Vec<String> {
    let reference_writer = stage_writer(run_dir, "reference_validate");
    let stream_writer = stage_writer(run_dir, "stream_validate");
    let join_writer = stage_writer(run_dir, "joined");
    events
        .iter()
        .filter_map(|envelope| match &envelope.payload {
            payload if payload.consumes_data_credit() => {
                let status = if matches!(
                    envelope.envelope.provenance.event.processing.status,
                    ProcessingStatus::Error { .. }
                ) {
                    "error"
                } else {
                    "success"
                };
                Some(format!("data:{}:{status}", envelope.event_type()))
            }
            ChainPayload::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                let writer = writer_id.unwrap_or(envelope.envelope.provenance.event.writer_id);
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

fn facts(events: &[JournalRecord<ChainPayload>]) -> Vec<JoinedFact> {
    events
        .iter()
        .filter_map(|envelope| JoinedFact::from_event(&envelope.authored()))
        .collect()
}

fn assert_journal_contract(run_dir: &Path, events: &[JournalRecord<ChainPayload>]) {
    let manifest = archive_manifest(run_dir);
    let join_writer = stage_writer(run_dir, "joined");
    let mut finals = 0;
    let mut progress_paths = std::collections::HashSet::new();
    for envelope in events {
        if envelope.envelope.provenance.event.writer_id != join_writer {
            continue;
        }
        let (is_contract, reader_path) = match &envelope.payload {
            ChainPayload::FlowControl(FlowControlPayload::ConsumptionProgress {
                reader_path,
                ..
            }) => (true, Some(reader_path.0.clone())),
            ChainPayload::FlowControl(FlowControlPayload::ConsumptionFinal { .. }) => {
                finals += 1;
                (true, None)
            }
            _ => (false, None),
        };
        if !is_contract {
            continue;
        }
        if let Some(path) = reader_path {
            progress_paths.insert(path);
        }
        let context = &envelope.envelope.provenance.event.flow_context;
        assert_eq!(context.stage_name, "joined");
        assert_eq!(WriterId::from(context.stage_id), join_writer);
        assert_eq!(
            context.stage_type,
            obzenflow_core::event::context::StageType::Join
        );
        assert_eq!(context.flow_name, manifest["flow_name"].as_str().unwrap());
        assert_eq!(context.flow_id, manifest["flow_id"].as_str().unwrap());
    }
    assert_eq!(
        finals, 2,
        "both join subscriptions must author final contracts"
    );
    for upstream in ["reference_validate", "stream_validate"] {
        assert!(
            progress_paths.contains(manifest["stages"][upstream]["stage_id"].as_str().unwrap()),
            "missing {upstream} progress from the join subscription"
        );
    }
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
        .filter(|envelope| JoinedFact::from_event(&envelope.authored()).is_some())
        .collect::<Vec<_>>();
    assert!(authored.iter().all(|envelope| {
        envelope.envelope.provenance.event.writer_id == writer
            && envelope.event_type() == JoinedFact::event_type_name()
    }));

    let local_eof = events
        .iter()
        .rev()
        .find(|envelope| {
            envelope.envelope.provenance.event.writer_id == writer && envelope.is_eof()
        })
        .expect("join-authored EOF");
    let ChainPayload::FlowControl(FlowControlPayload::Eof {
        writer_seq,
        writer_seq_by_event_type,
        last_event_id,
        ..
    }) = &local_eof.payload
    else {
        unreachable!("local EOF shape")
    };
    let compatible = writer_seq_by_event_type
        .iter()
        .filter(|(key, _)| **key == JoinedFact::descriptor())
        .collect::<Vec<_>>();
    assert_eq!(compatible.len(), 1);
    assert_eq!(*compatible[0].0, JoinedFact::descriptor());
    assert_eq!(compatible[0].1 .0, 5);
    assert_eq!(writer_seq.map(|seq| seq.0), Some(5));
    assert_eq!(
        *last_event_id,
        authored
            .last()
            .map(|envelope| envelope.envelope.provenance.event.id),
        "the terminal points at the last locally authored fact, not a forwarded row"
    );
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
    let manifest = archive_manifest(&live);
    let archived_join_path = live.join(
        manifest["stages"]["joined"]["data_journal_file"]
            .as_str()
            .unwrap(),
    );
    let archived_join_bytes = std::fs::read(&archived_join_path).unwrap();

    let validator_rows = read_stage_appended(&live, "stream_validate").await;
    let validator_writer = stage_writer(&live, "stream_validate");
    let foreign = validator_rows
        .iter()
        .find(|envelope| {
            envelope.consumes_data_credit()
                && matches!(
                    envelope.envelope.provenance.event.processing.status,
                    ProcessingStatus::Error { .. }
                )
        })
        .expect("validator foreign-family error row");
    let forwarded = live_join
        .iter()
        .find(|envelope| {
            envelope.envelope.provenance.event.id == foreign.envelope.provenance.event.id
        })
        .expect("join forwards the same error envelope");
    assert_eq!(
        forwarded.envelope.provenance.event.writer_id,
        foreign.envelope.provenance.event.writer_id
    );
    assert_eq!(forwarded.event_type(), foreign.event_type());

    let validator_authored = validator_rows
        .iter()
        .filter(|envelope| {
            envelope.envelope.provenance.event.writer_id == validator_writer
                && envelope.consumes_data_credit()
        })
        .collect::<Vec<_>>();
    assert_eq!(validator_authored.len(), 2);
    let validator_eof = validator_rows
        .iter()
        .find(|envelope| {
            envelope.envelope.provenance.event.writer_id == validator_writer && envelope.is_eof()
        })
        .expect("validator-authored EOF");
    let ChainPayload::FlowControl(FlowControlPayload::Eof {
        writer_seq,
        writer_seq_by_event_type,
        last_event_id,
        ..
    }) = &validator_eof.payload
    else {
        unreachable!("validator EOF shape")
    };
    assert_eq!(writer_seq.map(|seq| seq.0), Some(2));
    assert_eq!(
        writer_seq_by_event_type
            .get(&StreamItem::descriptor())
            .map(|seq| seq.0),
        Some(2)
    );
    assert_eq!(
        *last_event_id,
        validator_authored
            .last()
            .map(|envelope| envelope.envelope.provenance.event.id)
    );

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
    // Reopening a DiskJournal creates a fresh journal_writer_id on its reader
    // envelopes. Compare the persisted bytes for immutability; decoded records
    // above remain the oracle for context, causal identity, and replay behavior.
    assert!(
        std::fs::read(&archived_join_path).unwrap() == archived_join_bytes,
        "replay must not rewrite archived records or contexts"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explicit_multi_stream_join_has_one_catalog_and_releases_enforced_backpressure() {
    use obzenflow_core::StageKey;
    use obzenflow_dsl::dsl::backpressure_clause::enforced;
    use obzenflow_runtime::run_context::FlowBuildContext;
    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("multi-stream");
    let handle = FlowDefinition::materialize(move |_| {
        let references = ReferenceSource::new(Arc::new(AtomicUsize::new(0)));
        let stream_a = StreamSource::new(Arc::new(AtomicUsize::new(0)));
        let stream_b = StreamSource::new(Arc::new(AtomicUsize::new(0)));
        let joined = ExactJoin { calls: Arc::new(AtomicUsize::new(0)) };
        let output = replay_testkit::Discard::<JoinedFact>::default();
        Ok(flow! {
            name: "explicit_multi_stream_join",
            journals: disk_journals(journal_base),
            backpressure: enforced(2).stall_timeout_ms(3_000),
            stages: {
                references = source!(ReferenceItem => references , backpressure: enforced(1));
                stream_a = source!(StreamItem => stream_a);
                stream_b = source!(StreamItem => stream_b);
                joined = join!(catalog references: ReferenceItem, StreamItem -> JoinedFact => joined);
                output = sink!(JoinedFact => output);
            },
            topology: {
                (references, stream_a) |> joined;
                (references, stream_b) |> joined;
                joined |> output;
            }
        })
    }).build(FlowBuildContext::for_tests()).await.unwrap();
    let topology = handle.topology().unwrap();
    let join = topology
        .stages()
        .find(|stage| stage.name == "joined")
        .unwrap();
    let metadata = join.join_metadata.as_ref().unwrap();
    assert_eq!(metadata.catalog_source_ids.len(), 1);
    assert_eq!(metadata.stream_source_ids.len(), 2);
    assert_eq!(
        topology.edges().len(),
        4,
        "one catalog edge, two stream edges, one output edge"
    );
    let config = handle.flow_effective_config().unwrap();
    for producer in ["references", "stream_a", "stream_b"] {
        assert_eq!(
            config
                .backpressure_window_for(&StageKey::from(producer), &StageKey::from("joined"))
                .unwrap()
                .value
                .as_u64(),
            Some(if producer == "references" { 1 } else { 2 })
        );
    }
    assert_eq!(
        config
            .backpressure_window_for(&StageKey::from("joined"), &StageKey::from("output"))
            .unwrap()
            .value
            .as_u64(),
        Some(2)
    );
    let archive = handle
        .run_substrate()
        .locator()
        .unwrap()
        .path()
        .to_path_buf();
    tokio::time::timeout(std::time::Duration::from_secs(30), handle.run())
        .await
        .unwrap()
        .unwrap();
    let records = read_stage_appended(&archive, "joined").await;
    let authored = facts(&records);
    assert_eq!(
        authored
            .iter()
            .filter(|fact| fact.phase == "stream")
            .count(),
        6
    );
    assert_eq!(
        authored.iter().filter(|fact| fact.phase == "hook").count(),
        1
    );
    assert_eq!(
        authored.iter().filter(|fact| fact.phase == "drain").count(),
        1
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn mixed_join_feeds_filter_both_roles_with_backpressure_and_replay() {
    use obzenflow_dsl::dsl::backpressure_clause::enforced;

    let temp = tempfile::tempdir().unwrap();
    let mut live_archive = None;
    let mut live_facts = Vec::new();
    for mode in ["live", "replay"] {
        let base = temp.path().join(mode);
        let journal_base = base.clone();
        let reads = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let source_reads = reads.clone();
        let join_calls = calls.clone();
        let definition = FlowDefinition::materialize(move |_| {
            // Each journal includes the other role's payload before, between,
            // and after its selected rows. A one-row credit window also proves
            // that filtered rows release backpressure on both subscriptions.
            let reference = |key: &str, version: &str| {
                MixedFact::Reference(ReferenceItem {
                    key: key.into(),
                    version: version.into(),
                })
            };
            let stream = |key: &str| {
                MixedFact::Stream(StreamItem {
                    key: key.into(),
                    reject: false,
                })
            };
            let references = MixedSource {
                rows: [
                    stream("unselected"),
                    reference("k1", "old"),
                    stream("unselected"),
                    reference("k1", "new"),
                    reference("k2", "terminal"),
                    stream("unselected"),
                ]
                .into(),
                reads: source_reads.clone(),
            };
            let stream_a = MixedSource {
                rows: [
                    reference("k1", "unselected"),
                    stream("k1"),
                    reference("k1", "unselected"),
                    stream("k2"),
                    reference("k1", "unselected"),
                ]
                .into(),
                reads: source_reads,
            };
            let stream_b = stream_a.clone();
            let joined = ExactJoin { calls: join_calls };
            let output = replay_testkit::Discard::<JoinedFact>::default();
            Ok(flow! {
                name: "mixed_join_feeds",
                journals: disk_journals(journal_base),
                backpressure: enforced(1).stall_timeout_ms(3_000),
                stages: {
                    references = source!({ ReferenceItem, StreamItem } => references);
                    stream_a = source!({ ReferenceItem, StreamItem } => stream_a);
                    stream_b = source!({ ReferenceItem, StreamItem } => stream_b);
                    joined = join!(catalog references: ReferenceItem, StreamItem -> JoinedFact => joined);
                    output = sink!(JoinedFact => output);
                },
                topology: {
                    (references, stream_a) |> joined;
                    (references, stream_b) |> joined;
                    joined |> output;
                }
            })
        });
        let mut args = vec![OsString::from("obzenflow")];
        if let Some(archive) = &live_archive {
            args.push(OsString::from("--replay-from"));
            args.push(OsString::from(archive));
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            FlowApplication::builder()
                .with_cli_args(args)
                .run_async(definition),
        )
        .await
        .expect("mixed feeds release enforced backpressure")
        .expect("only selected payloads reach the join handler");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            9,
            "{mode}: selected rows and terminal hooks"
        );
        assert_eq!(
            reads.load(Ordering::SeqCst),
            if mode == "live" { 19 } else { 0 }
        );

        let archive = latest_run_dir(&base);
        let records = read_stage_appended(&archive, "joined").await;
        let join_writer = stage_writer(&archive, "joined");
        let mut consumed = Vec::new();
        for record in &records {
            if record.envelope.provenance.event.writer_id != join_writer {
                continue;
            }
            if let ChainPayload::FlowControl(FlowControlPayload::ConsumptionFinal {
                pass,
                consumed_count,
                reader_seq,
                advertised_writer_seq,
                eof_seen,
                ..
            }) = &record.payload
            {
                assert!(*pass && *eof_seen, "{mode}: selected-feed contract passes");
                assert_eq!(Some(*reader_seq), *advertised_writer_seq);
                assert_eq!(consumed_count.0, reader_seq.0);
                consumed.push(consumed_count.0);
            }
        }
        consumed.sort();
        assert_eq!(
            consumed,
            [2, 2, 3],
            "{mode}: contracts count selected facts"
        );
        let authored = facts(&records);
        assert_eq!(authored.len(), 8, "six stream facts and two terminal facts");
        assert!(authored.iter().all(
            |fact| fact.reference_version == if fact.key == "k1" { "new" } else { "terminal" }
        ));
        if mode == "live" {
            live_archive = Some(archive);
            live_facts = authored;
        } else {
            assert_eq!(authored, live_facts, "selected-feed order is replay-stable");
        }
    }
}

#[tokio::test]
async fn invalid_authored_join_forms_fail_before_journal_provider_evaluation() {
    use obzenflow_runtime::run_context::FlowBuildContext;
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("must-not-exist");
    let providers = Arc::new(AtomicUsize::new(0));
    let reads = Arc::new(AtomicUsize::new(0));
    macro_rules! invalid {
        ($($wiring:tt)*) => {{
            let root = root.clone();
            let providers = providers.clone();
            let reads = reads.clone();
            FlowDefinition::materialize(move |_| {
                let catalog = ReferenceSource::new(reads.clone());
                let stream = ReferenceSource::new(reads.clone());
                let other = ReferenceSource::new(reads);
                let joiner = obzenflow::stages::joins::inner(
                    |row: &ReferenceItem| row.key.clone(),
                    |row: &ReferenceItem| row.key.clone(),
                    |reference: ReferenceItem, _stream: ReferenceItem| reference,
                );
                Ok(flow! {
                    journals: {
                        providers.fetch_add(1, Ordering::SeqCst);
                        disk_journals(root)
                    },
                    stages: {
                        catalog = source!(ReferenceItem => catalog);
                        stream = source!(ReferenceItem => stream);
                        other = source!(ReferenceItem => other);
                        joined = join!(name: "visible-name", catalog catalog: ReferenceItem, ReferenceItem -> ReferenceItem => joiner);
                    },
                    topology: { $($wiring)* }
                })
            })
        }};
    }
    for (definition, diagnostic) in [
        (invalid!(), "requires explicit"),
        (invalid!(stream |> joined;), "plain edge"),
        (invalid!(catalog |> joined; stream |> joined;), "plain edge"),
        (
            invalid!((catalog, stream) |> joined; other |> joined;),
            "plain edge",
        ),
        (
            invalid!((stream, catalog) |> joined;),
            "declares catalog 'catalog'",
        ),
        (
            invalid!((other, stream) |> joined;),
            "declares catalog 'catalog'",
        ),
        (
            invalid!((catalog, stream) |> joined; (catalog, stream) |> joined;),
            "duplicate join tuple",
        ),
        (
            invalid!((catalog, catalog) |> joined;),
            "both catalog and stream",
        ),
        (
            invalid!((catalog, missing) |> joined;),
            "unknown binding 'missing'",
        ),
        (invalid!((catalog, stream) |> other;), "not a declared join"),
        (invalid!(joined <| catalog;), "requires explicit"),
    ] {
        let error = definition
            .build(FlowBuildContext::for_tests())
            .await
            .err()
            .unwrap();
        assert!(error.to_string().contains(diagnostic), "{error}");
        assert_eq!(providers.load(Ordering::SeqCst), 0);
        assert_eq!(reads.load(Ordering::SeqCst), 0);
        assert!(!root.exists());
    }
}

#[cfg(feature = "test-support")]
#[tokio::test]
async fn test_flow_rejects_plain_join_input_before_journal_provider_evaluation() {
    let providers = Arc::new(AtomicUsize::new(0));
    let reference = ReferenceSource::new(Arc::new(AtomicUsize::new(0)));
    let stream = StreamSource::new(Arc::new(AtomicUsize::new(0)));
    let joiner = ExactJoin {
        calls: Arc::new(AtomicUsize::new(0)),
    };
    let provider_counter = providers.clone();
    let result = obzenflow_dsl::test_flow! {
        journals: {
            provider_counter.fetch_add(1, Ordering::SeqCst);
            obzenflow_infra::journal::memory_journals()
        },
        stages: {
            reference = source!(ReferenceItem => reference);
            stream = source!(StreamItem => stream);
            joined = join!(catalog reference: ReferenceItem, StreamItem -> JoinedFact => joiner);
        },
        topology: { stream |> joined; }
    }
    .await;
    let error = result.err().unwrap();
    assert!(error.to_string().contains("plain edge"), "{error}");
    assert_eq!(providers.load(Ordering::SeqCst), 0);
}

#[derive(Clone, Debug)]
struct HookCounts {
    mode: obzenflow_runtime::stages::join::JoinReferenceMode,
    stream_eofs: Arc<AtomicUsize>,
    drains: Arc<AtomicUsize>,
}

fn hook_fact(phase: &str, ordinal: usize) -> JoinedFact {
    JoinedFact {
        phase: phase.to_string(),
        key: "k1".to_string(),
        reference_version: String::new(),
        ordinal,
    }
}

impl TypedJoinHandler for HookCounts {
    type State = ();
    type ReferenceKey = String;
    type Reference = ReferenceItem;
    type Stream = StreamItem;
    type Output = JoinedFact;

    fn initial_state(&self) -> Self::State {}

    fn reference_mode(&self) -> obzenflow_runtime::stages::join::JoinReferenceMode {
        self.mode
    }

    fn admit_reference(&self, reference: &Self::Reference) -> Result<String, HandlerError> {
        Ok(reference.key.clone())
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        references: &mut JoinReferenceView<'_, String, ReferenceItem>,
        stream: StreamItem,
    ) -> Result<Vec<JoinedFact>, HandlerError> {
        Ok(references
            .select(&stream.key)
            .map(|reference| JoinedFact {
                phase: "stream".to_string(),
                key: stream.key,
                reference_version: reference.version,
                ordinal: 0,
            })
            .into_iter()
            .collect())
    }

    fn on_stream_eof(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, String, ReferenceItem>,
    ) -> Result<Vec<JoinedFact>, HandlerError> {
        self.stream_eofs.fetch_add(1, Ordering::SeqCst);
        Ok(vec![hook_fact("hook", 0)])
    }

    fn drain(
        &self,
        _state: &Self::State,
        _references: &mut JoinReferenceView<'_, String, ReferenceItem>,
    ) -> Result<Vec<JoinedFact>, HandlerError> {
        self.drains.fetch_add(1, Ordering::SeqCst);
        Ok((0..3).map(|ordinal| hook_fact("drain", ordinal)).collect())
    }
}

fn completed(records: &[JournalRecord<ChainPayload>]) -> bool {
    use obzenflow_core::event::payloads::execution_payload::{
        ExecutionPayload, StageLifecycleFact,
    };
    records.iter().any(|record| {
        matches!(
            record.payload,
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                StageLifecycleFact::Completed { .. }
            ))
        )
    })
}

async fn hooks_under_enforce(
    mode: obzenflow_runtime::stages::join::JoinReferenceMode,
    batch_only: bool,
) -> (HookCounts, Vec<JoinedFact>) {
    use obzenflow::stages::merges;
    use obzenflow::stages::sources::ChannelSource;
    use obzenflow_dsl::dsl::backpressure_clause::enforced;
    use obzenflow_dsl::{async_infinite_source, merge};
    use obzenflow_runtime::run_context::FlowBuildContext;

    let temp = tempfile::tempdir().unwrap();
    let journal_base = temp.path().join("hooks");
    let handler = HookCounts {
        mode,
        stream_eofs: Arc::default(),
        drains: Arc::default(),
    };
    let joined = handler.clone();
    let (_live_tx, live_references) = tokio::sync::mpsc::channel::<ReferenceItem>(1);
    let live_references = std::sync::Mutex::new(Some(live_references));
    let handle = FlowDefinition::materialize(move |_| {
        let references = ReferenceSource::new(Arc::new(AtomicUsize::new(0)));
        let streams = StreamSource::new(Arc::new(AtomicUsize::new(0)));
        let output = replay_testkit::Discard::<JoinedFact>::default();
        if batch_only {
            let live_references = ChannelSource::new(
                live_references
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take()
                    .expect("the flow is built once"),
            );
            return Ok(flow! {
                name: "join_hooks_under_enforce",
                journals: disk_journals(journal_base),
                backpressure: enforced(1).stall_timeout_ms(10_000),
                stages: {
                    batch_references = source!(ReferenceItem => references);
                    live_references = async_infinite_source!(ReferenceItem => live_references);
                    catalog = merge!(ReferenceItem => merges::then_live);
                    streams = source!(StreamItem => streams);
                    joined = join!(catalog catalog: ReferenceItem, StreamItem -> JoinedFact => joined);
                    output = sink!(JoinedFact => output);
                },
                topology: {
                    batch_references |> catalog;
                    live_references |> catalog;
                    (catalog, streams) |> joined;
                    joined |> output;
                }
            });
        }
        Ok(flow! {
            name: "join_hooks_under_enforce",
            journals: disk_journals(journal_base),
            backpressure: enforced(1).stall_timeout_ms(10_000),
            stages: {
                references = source!(ReferenceItem => references);
                streams = source!(StreamItem => streams);
                joined = join!(catalog references: ReferenceItem, StreamItem -> JoinedFact => joined);
                output = sink!(JoinedFact => output);
            },
            topology: {
                (references, streams) |> joined;
                joined |> output;
            }
        })
    })
    .build(FlowBuildContext::for_tests())
    .await
    .unwrap();
    let archive = handle
        .run_substrate()
        .locator()
        .unwrap()
        .path()
        .to_path_buf();
    if batch_only {
        handle.start().await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while !completed(&read_stage_appended(&archive, "joined").await) {
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("the join drains before any stop");
        handle
            .stop_graceful(std::time::Duration::from_secs(10))
            .await
            .unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(15),
            handle.wait_for_completion(),
        )
        .await
        .unwrap()
        .unwrap();
    } else {
        tokio::time::timeout(std::time::Duration::from_secs(30), handle.run())
            .await
            .unwrap()
            .unwrap();
    }
    let records = read_stage_appended(&archive, "joined").await;
    (handler, facts(&records))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_join_runs_each_end_of_input_hook_once_when_its_drain_backs_off() {
    use obzenflow_runtime::stages::join::JoinReferenceMode;
    for (mode, batch_only) in [
        (JoinReferenceMode::FiniteEof, false),
        (JoinReferenceMode::Live, false),
        (JoinReferenceMode::FiniteThenLive, true),
    ] {
        let (handler, authored) = hooks_under_enforce(mode, batch_only).await;
        assert_eq!(handler.stream_eofs.load(Ordering::SeqCst), 1, "{mode:?}");
        assert_eq!(handler.drains.load(Ordering::SeqCst), 1, "{mode:?}");
        let phases: Vec<(&str, usize)> = authored
            .iter()
            .filter(|fact| fact.phase != "stream")
            .map(|fact| (fact.phase.as_str(), fact.ordinal))
            .collect();
        assert_eq!(
            phases,
            [("hook", 0), ("drain", 0), ("drain", 1), ("drain", 2)],
            "{mode:?}"
        );
    }
}
