// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-080n first measurements, using the supported retained-run projection.
//! Journal wall-clock stamps are preliminary evidence, not monotonic publication
//! completion timings. This scanner never starts a flow or deducts breaker waits.

use anyhow::{bail, ensure, Context, Result};
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryMethod, DeliveryPayload, DeliveryResult,
};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourcePollErrorKind, SourcePollKind,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{ChainPayload, JournalCommitRef};
use obzenflow_core::journal::read::{
    RunJournalKind, RunOutcome, RunRecord, RunRecordData, TailRead,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;

const SOURCE: &str = "high_volume_source";
const TRANSFORM: &str = "error_processor";
const SOURCE_OUTAGE_INTERVAL: u64 = 20_000;

#[derive(Clone)]
struct BusinessStamp {
    commitment: JournalCommitRef,
    wall_ns: i64,
    parent_ids: Vec<obzenflow_core::EventId>,
}

#[derive(Default)]
struct Cohort {
    rows: BTreeMap<u64, BusinessStamp>,
}

impl Cohort {
    fn insert(&mut self, id: u64, stamp: BusinessStamp) -> Result<()> {
        ensure!(
            self.rows.insert(id, stamp).is_none(),
            "duplicate committed business input {id}"
        );
        Ok(())
    }

    fn verify(&self, expected: impl Iterator<Item = u64>, name: &str) -> Result<()> {
        ensure!(
            self.rows.keys().copied().eq(expected),
            "{name} contains missing or unexpected input identities"
        );
        Ok(())
    }

    fn span(&self) -> Result<Value> {
        let first = self.rows.values().next().context("empty business cohort")?;
        let last = self.rows.values().last().context("empty business cohort")?;
        let mut preceding = first.wall_ns;
        for stamp in self.rows.values() {
            ensure!(
                stamp.wall_ns >= preceding,
                "journal wall clock regressed within an ordered business cohort"
            );
            preceding = stamp.wall_ns;
        }
        wall_span(self.rows.len() as u64, first, last)
    }
}

#[derive(Default)]
struct ArchiveCohorts {
    sources: Cohort,
    successes: Cohort,
    errors: Cohort,
    source_poll_errors: u64,
    summaries: Vec<(JournalCommitRef, u64)>,
    receipts: Vec<DeliveryPayload>,
    projected_records: u64,
}

impl ArchiveCohorts {
    fn observe(&mut self, projected: &RunRecord) -> Result<()> {
        self.projected_records += 1;
        let RunRecordData::Chain(row) = &projected.record else {
            return Ok(());
        };
        let stage = projected
            .journal
            .stage
            .as_ref()
            .context("chain record lacks a physical stage journal")?;
        let physical = (stage.key.as_str(), projected.journal.kind);
        match &row.payload {
            ChainPayload::Fact(payload) => {
                let status = &row.envelope.provenance.event.processing.status;
                ensure!(
                    row.envelope.provenance.event.payload_schema_version.get() == 1,
                    "unexpected business payload schema version"
                );
                if physical == ("event_counter", RunJournalKind::Data) {
                    ensure!(status.is_success(), "summary is error-marked");
                    ensure!(
                        row.event_type_name() == "prometheus.event_count",
                        "unexpected summary schema"
                    );
                    self.summaries.push((
                        row.commitment(),
                        payload["event_count"].as_u64().context("summary count")?,
                    ));
                    return Ok(());
                }
                let id = payload["id"].as_u64().context("business input identity")?;
                let should_fail = id.is_multiple_of(100);
                ensure!(
                    payload["batch"].as_u64() == Some(id / 100)
                        && payload["should_fail"].as_bool() == Some(should_fail),
                    "input {id} has a changed deterministic payload"
                );
                let cohort = match physical {
                    (SOURCE, RunJournalKind::Data) => {
                        ensure!(
                            status.is_success(),
                            "source business record is error-marked"
                        );
                        ensure!(row.event_type_name() == "data.request", "source schema");
                        &mut self.sources
                    }
                    (TRANSFORM, RunJournalKind::Data) => {
                        ensure!(
                            status.is_success() && !should_fail,
                            "unexpected success {id}"
                        );
                        ensure!(
                            row.event_type_name() == "processed.event"
                                && payload["processed"].as_bool() == Some(true)
                                && payload["processing_stage"].as_str()
                                    == Some("error_prone_transform"),
                            "transform result for input {id} has changed"
                        );
                        &mut self.successes
                    }
                    (TRANSFORM, RunJournalKind::Error) => {
                        ensure!(
                            !status.is_success()
                                && should_fail
                                && status.kind() == Some(&ErrorKind::Unknown),
                            "unexpected transform error {id}"
                        );
                        ensure!(row.event_type_name() == "data.request", "error schema");
                        &mut self.errors
                    }
                    _ => bail!("business input {id} in unexpected physical journal {physical:?}"),
                };
                cohort.insert(
                    id,
                    BusinessStamp {
                        commitment: row.commitment(),
                        parent_ids: row.envelope.provenance.event.causality.parent_ids.clone(),
                        wall_ns: row
                            .envelope
                            .provenance
                            .journal
                            .timestamp
                            .timestamp_nanos_opt()
                            .context("journal timestamp exceeds nanosecond range")?,
                    },
                )?;
            }
            ChainPayload::Execution(ExecutionPayload::SourcePollError(failure)) => {
                ensure!(
                    physical == (SOURCE, RunJournalKind::Error)
                        && matches!(failure.source_type, SourcePollKind::Finite)
                        && failure.error_type == SourcePollErrorKind::Timeout,
                    "unexpected source poll failure or physical journal: {physical:?}"
                );
                self.source_poll_errors += 1;
            }
            ChainPayload::Delivery(receipt) => {
                ensure!(
                    physical == ("summary_sink", RunJournalKind::Data),
                    "delivery in unexpected physical journal {physical:?}"
                );
                self.receipts.push(receipt.clone());
            }
            _ if projected.journal.kind == RunJournalKind::Error => {
                bail!("unexpected non-business error in physical journal {physical:?}");
            }
            _ => {}
        }
        Ok(())
    }

    fn verify_business(&self, count: u64) -> Result<()> {
        self.sources.verify(0..count, "source")?;
        self.successes.verify(
            (0..count).filter(|id| !id.is_multiple_of(100)),
            "transform success",
        )?;
        self.errors
            .verify((0..count).step_by(100), "transform error")?;
        for (id, error) in &self.errors.rows {
            ensure!(
                error.commitment.event_id == self.sources.rows[id].commitment.event_id,
                "error {id} did not retain its source input identity"
            );
        }
        for (id, success) in &self.successes.rows {
            ensure!(
                success
                    .parent_ids
                    .contains(&self.sources.rows[id].commitment.event_id),
                "successful output {id} is not derived from its corresponding source input"
            );
        }
        Ok(())
    }

    fn verify_complete(&self, count: u64) -> Result<()> {
        self.verify_business(count)?;
        ensure!(
            self.source_poll_errors == 2 * ((count - 1) / SOURCE_OUTAGE_INTERVAL),
            "source poll error count differs from the shipped outage schedule"
        );
        let expected_successes = count - count.div_ceil(100);
        let [(summary_commitment, summary_count)] = self.summaries.as_slice() else {
            bail!("expected exactly one committed summary");
        };
        ensure!(
            *summary_count == expected_successes,
            "incorrect summary count"
        );
        let [receipt] = self.receipts.as_slice() else {
            bail!("expected exactly one delivery receipt");
        };
        let summary = serde_json::from_value(json!({"event_count": expected_successes}))?;
        let expected_bytes =
            format!("{}\n", super::prometheus_demo::format_summary(&summary)).len();
        ensure!(
            matches!(receipt.result, DeliveryResult::Success { .. })
                && receipt.delivery_method == DeliveryMethod::ConsoleStdout
                && receipt.items_delivered == Some(1)
                && receipt.bytes_processed == Some(expected_bytes as u64)
                && receipt.subject.input == *summary_commitment,
            "summary delivery did not successfully settle the recorded summary"
        );
        Ok(())
    }

    fn preliminary_timings(&self, count: u64) -> Result<Value> {
        let first_source = &self.sources.rows[&0];
        let final_outcome = self
            .successes
            .rows
            .values()
            .chain(self.errors.rows.values())
            .max_by_key(|stamp| stamp.wall_ns)
            .context("no resolved input")?;
        let mut latencies = Vec::with_capacity(self.sources.rows.len());
        for (id, source) in &self.sources.rows {
            let outcome = self
                .successes
                .rows
                .get(id)
                .or_else(|| self.errors.rows.get(id))
                .context("unresolved input")?;
            let elapsed = outcome.wall_ns - source.wall_ns;
            ensure!(
                elapsed >= 0,
                "wall-clock outcome predates source input {id}"
            );
            latencies.push(elapsed);
        }
        latencies.sort_unstable();
        let percentile = |p: usize| latencies[((latencies.len() - 1) * p) / 100];
        Ok(json!({
            "clock": "wall-clock journal provenance timestamp; not monotonic commit-completion timing",
            "rate_numerator": "all verified unique business records in the named cohort",
            "rate_denominator": "last minus first recorded publication stamp, including deliberate and incidental waits",
            "source_publication": self.sources.span()?,
            "transform_success_output": self.successes.span()?,
            "resolved_inputs": wall_span(count, first_source, final_outcome)?,
            "source_to_outcome_wall_clock_latency_ns": {
                "min": latencies[0], "p50": percentile(50), "p95": percentile(95),
                "p99": percentile(99), "max": latencies[latencies.len() - 1],
                "percentile_method": "floor((sample_count - 1) * percentile / 100)"
            },
            "deducted_wait_ns": 0,
            "active_rates": null,
            "limitations": [
                "Wall-clock stamps are assigned before physical append completes and may be adjusted by the operating system.",
                "These rates are preliminary diagnostics and cannot certify the FLOWIP-080n throughput target.",
                "First-to-last publication spans exclude startup, first-publication cost, final aggregate delivery and drain.",
                "No benchmark repetition, uncertainty or instrumentation-overhead qualification is established by this archive scan."
            ]
        }))
    }
}

fn wall_span(count: u64, first: &BusinessStamp, last: &BusinessStamp) -> Result<Value> {
    let elapsed_ns = last.wall_ns - first.wall_ns;
    ensure!(elapsed_ns >= 0, "negative journal wall-clock span");
    Ok(json!({
        "business_records": count,
        "first_commitment": first.commitment,
        "last_commitment": last.commitment,
        "first_wall_unix_ns": first.wall_ns,
        "last_wall_unix_ns": last.wall_ns,
        "elapsed_ns": elapsed_ns,
        "records_per_second": (elapsed_ns > 0).then(|| count as f64 * 1e9 / elapsed_ns as f64)
    }))
}

pub(super) async fn inspect_archive(archive: &Path, count: u64) -> Result<Value> {
    ensure!(
        count >= 2,
        "at least two explicit expected inputs are required"
    );
    let mut snapshot = obzenflow_infra::journal::read::open_disk_run(archive).await?;
    let identity = snapshot.identity().clone();
    let manifest = snapshot.manifest().clone();
    ensure!(
        manifest.flow_name == "prometheus_demo",
        "not the shipped demo"
    );
    ensure!(
        manifest.replay.is_none() && manifest.resume.is_none(),
        "expected an original live run"
    );
    let mut expected_stages = BTreeMap::from([
        (SOURCE, Vec::<&str>::new()),
        (TRANSFORM, vec![SOURCE]),
        ("event_counter", vec![TRANSFORM]),
        ("summary_sink", vec!["event_counter"]),
    ]);
    for (name, stage) in &manifest.stages {
        let inbound = expected_stages
            .remove(name.as_str())
            .context("unexpected demo stage")?;
        ensure!(stage.inbound == inbound, "changed inbound work for {name}");
    }
    ensure!(expected_stages.is_empty(), "missing demo stage");
    let mut cohorts = ArchiveCohorts::default();
    while let Some(record) = snapshot.next().await? {
        cohorts.observe(&record)?;
    }
    // Snapshot admission captures a finite prefix. One non-blocking tail pass
    // establishes the supported settled prefix after all admitted rows were read.
    // A new row means the archive changed during inspection, not a valid sample.
    let mut tail = snapshot.into_tail();
    ensure!(
        matches!(tail.read_next().await?, TailRead::Pending),
        "archive was still being appended during measurement"
    );
    let progress = tail.progress();
    ensure!(
        progress
            .outcome
            .as_ref()
            .is_some_and(|outcome| outcome.outcome == RunOutcome::Completed),
        "archive has no successful terminal outcome"
    );
    ensure!(
        progress.settled_prefix.is_some(),
        "archive is not fully settled"
    );
    cohorts.verify_complete(count)?;
    Ok(json!({
        "measurement_contract": "flowip-080n-retained-journal-preliminary-v1",
        "acceptance_grade": false,
        "archive": archive.canonicalize()?,
        "archive_identity": identity,
        "manifest": manifest,
        "verified_progress": progress,
        "counts": {
            "expected_inputs": count,
            "source_inputs": cohorts.sources.rows.len(),
            "transform_successes": cohorts.successes.rows.len(),
            "transform_intended_errors": cohorts.errors.rows.len(),
            "source_poll_errors": cohorts.source_poll_errors,
            "summaries": cohorts.summaries.len(),
            "successful_summary_receipts": cohorts.receipts.len(),
            "projected_records": cohorts.projected_records
        },
        "preliminary_timing": cohorts.preliminary_timings(count)?
    }))
}

/// Invoke explicitly after the actual hosted example has completed. The caller
/// pins the input count; archive contents cannot silently choose their own oracle.
#[tokio::test]
#[ignore = "manual FLOWIP-080n retained-archive measurement; requires explicit archive and input count"]
async fn measure_retained_prometheus_archive() -> Result<()> {
    let archive = std::env::var_os("PROMETHEUS_MEASUREMENT_ARCHIVE")
        .context("PROMETHEUS_MEASUREMENT_ARCHIVE is required")?;
    let count = std::env::var("PROMETHEUS_MEASUREMENT_INPUTS")
        .context("PROMETHEUS_MEASUREMENT_INPUTS is required")?
        .parse::<u64>()
        .context("PROMETHEUS_MEASUREMENT_INPUTS must be an unsigned input count")?;
    let report = inspect_archive(Path::new(&archive), count).await?;
    let directory = Path::new("target/flowip-080n");
    std::fs::create_dir_all(directory)?;
    let flow_id = report["manifest"]["flow_id"]
        .as_str()
        .context("flow identity")?;
    let output = directory.join(format!("{flow_id}-preliminary.json"));
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&output)
        .with_context(|| {
            format!(
                "preserving prior reports; cannot create {}",
                output.display()
            )
        })?;
    let mut writer = std::io::BufWriter::new(file);
    report.serialize(&mut serde_json::Serializer::pretty(&mut writer))?;
    use std::io::Write;
    writer.write_all(b"\n")?;
    writer.flush()?;
    println!(
        "FLOWIP-080n preliminary journal measurement: {}",
        output.display()
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&report["preliminary_timing"])?
    );
    Ok(())
}

#[test]
fn retained_measurement_rejects_duplicate_missing_and_wrong_journal_business_rows() -> Result<()> {
    use obzenflow_core::event::provenance::FlowContext;
    use obzenflow_core::event::{ChainEventFactory, JournalRecord};
    use obzenflow_core::journal::read::{
        JournalPosition, RunIdentity, RunJournal, RunRecordKind, RunStage, RUN_RECORD_VERSION,
    };
    use obzenflow_core::{FlowId, JournalId, StageId, SystemId};

    let source = StageId::new();
    let journal = JournalId::new();
    let event = ChainEventFactory::data_event(
        source.into(),
        "data.request",
        std::num::NonZeroU32::MIN,
        json!({"id": 0, "should_fail": true, "batch": 0}),
    )
    .with_flow_context(FlowContext::new(SOURCE, source));
    let row = RunRecord {
        version: RUN_RECORD_VERSION,
        run: RunIdentity {
            flow_id: FlowId::new(),
            pipeline_writer_id: SystemId::new().into(),
        },
        journal: RunJournal {
            id: journal,
            kind: RunJournalKind::Data,
            stage: Some(RunStage {
                key: SOURCE.into(),
                id: source,
                stage_type: obzenflow_core::event::context::StageType::FiniteSource,
                is_effectful: false,
            }),
        },
        position: JournalPosition(0),
        kind: RunRecordKind::SourceFact,
        record: JournalRecord::new(journal.into(), event).into(),
    };
    let mut source_one = row.clone();
    if let RunRecordData::Chain(record) = &mut source_one.record {
        record.envelope.provenance.event.id = obzenflow_core::EventId::new();
        record.payload = ChainPayload::Fact(json!({"id": 1, "should_fail": false, "batch": 0}));
    }
    let transform = StageId::new();
    let mut error_zero = row.clone();
    error_zero.journal.kind = RunJournalKind::Error;
    let stage = error_zero.journal.stage.as_mut().unwrap();
    stage.key = TRANSFORM.into();
    stage.id = transform;
    stage.stage_type = obzenflow_core::event::context::StageType::Transform;
    if let RunRecordData::Chain(record) = &mut error_zero.record {
        record.envelope.provenance.event.processing.status =
            obzenflow_core::event::status::processing_status::ProcessingStatus::error_with_kind(
                "Simulated processing error",
                Some(ErrorKind::Unknown),
            );
    }
    let mut success_one = source_one.clone();
    success_one.journal.stage = error_zero.journal.stage.clone();
    if let RunRecordData::Chain(record) = &mut success_one.record {
        record.envelope.provenance.event.causality.parent_ids = vec![*record.id()];
        record.envelope.provenance.event.id = obzenflow_core::EventId::new();
        record.envelope.provenance.event.event_type = "processed.event".into();
        record.payload = ChainPayload::Fact(json!({
            "id": 1, "should_fail": false, "batch": 0,
            "processed": true, "processing_stage": "error_prone_transform"
        }));
    }
    let rows = [row.clone(), source_one, error_zero, success_one];
    let observe = |rows: &[RunRecord]| -> Result<ArchiveCohorts> {
        let mut scan = ArchiveCohorts::default();
        for row in rows {
            scan.observe(row)?;
        }
        Ok(scan)
    };
    observe(&rows)?.verify_business(2)?;
    for index in 0..rows.len() {
        let mut missing = rows.to_vec();
        missing.remove(index);
        assert!(
            observe(&missing)?.verify_business(2).is_err(),
            "missing business row {index} was accepted"
        );
        let mut duplicate = rows[index].clone();
        if let RunRecordData::Chain(record) = &mut duplicate.record {
            record.envelope.provenance.event.id = obzenflow_core::EventId::new();
        }
        let mut duplicated = rows.to_vec();
        duplicated.push(duplicate);
        assert!(
            observe(&duplicated).is_err(),
            "distinct event IDs concealed duplicated business row {index}"
        );
    }
    let mut wrong_journal = row;
    wrong_journal.journal.kind = RunJournalKind::Error;
    assert!(ArchiveCohorts::default().observe(&wrong_journal).is_err());
    let mut wrong_parent = rows.to_vec();
    if let RunRecordData::Chain(record) = &mut wrong_parent[3].record {
        record.envelope.provenance.event.causality.parent_ids =
            vec![obzenflow_core::EventId::new()];
    }
    assert!(
        observe(&wrong_parent)?.verify_business(2).is_err(),
        "unrelated success cannot settle this input cohort"
    );
    Ok(())
}
