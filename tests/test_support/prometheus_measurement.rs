// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Complete demo outcomes through the supported retained-run projection.
//! Timing is external to this oracle; no performance logs or trace data are parsed.

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
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::Path;

const SOURCE: &str = "high_volume_source";
const TRANSFORM: &str = "error_processor";
const SOURCE_OUTAGE_INTERVAL: u64 = 20_000;

#[derive(Clone)]
struct BusinessStamp {
    commitment: JournalCommitRef,
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
}

fn verify_source_rate_limit(
    evidence: Option<&obzenflow_core::config::EffectiveConfigEvidence>,
    enabled: bool,
) -> Result<Value> {
    use obzenflow_core::config::{ConfigSubject, EVIDENCE_SCHEMA_VERSION};
    use obzenflow_runtime::runtime_config::{
        RATE_LIMITER_COST_PER_ATTEMPT_KEY, RATE_LIMITER_EVENTS_PER_SECOND_KEY,
    };

    let evidence = evidence.context("archive has no effective configuration evidence")?;
    ensure!(
        evidence.schema_version == EVIDENCE_SCHEMA_VERSION && !evidence.values.is_empty(),
        "expected complete current-schema effective configuration evidence"
    );
    let rows: Vec<_> = evidence
        .values
        .iter()
        .filter(|row| row.key_path.starts_with("middleware.rate_limiter."))
        .collect();
    // The shipped demo's only possible limiter consumer is its source. Flow
    // materialisation resolves StageOrEffect knobs only for attached factories'
    // declared consumption points. These optional knobs have no framework
    // defaults, so an absent factory emits no limiter rows. This is a config
    // attachment proof, not an inference from missing runtime observations.
    if enabled {
        ensure!(
            rows.len() == 2,
            "expected exactly the demo's two limiter settings"
        );
        for (key, expected) in [
            (RATE_LIMITER_EVENTS_PER_SECOND_KEY, 1000.0),
            (RATE_LIMITER_COST_PER_ATTEMPT_KEY, 1.0),
        ] {
            let row = rows
                .iter()
                .find(|row| row.key_path == key)
                .with_context(|| format!("missing source limiter setting {key}"))?;
            ensure!(
                row.scope == format!("stage:{SOURCE}")
                    && row.source == "dsl"
                    && row.resolved_for.is_none()
                    && row.winning_subject() == ConfigSubject::Unqualified
                    && !row.redacted
                    && row.value.as_f64() == Some(expected),
                "source limiter setting {key} differs from the shipped demo policy"
            );
        }
    } else {
        ensure!(
            rows.is_empty(),
            "archive still declares rate-limiter middleware"
        );
    }
    Ok(json!({
        "stage": SOURCE,
        "enabled": enabled,
        "events_per_second": enabled.then_some(1000.0),
        "cost_per_attempt": enabled.then_some(1.0),
        "evidence_schema_version": evidence.schema_version,
        "effective_config_rows": rows,
        "proof": "verified against the completed original demo archive's flow-materialised effective configuration; limiter knobs materialise only for declared middleware consumers",
        "limitation": "proves the captured configured policy, not runtime admission or wait counts"
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
        "measurement_contract": "complete-demo-outcomes-v1",
        "flow_id": identity.flow_id,
        "counts": {
            "expected_inputs": count,
            "source_inputs": cohorts.sources.rows.len(),
            "transform_successes": cohorts.successes.rows.len(),
            "transform_intended_errors": cohorts.errors.rows.len(),
            "source_poll_errors": cohorts.source_poll_errors,
            "summaries": cohorts.summaries.len(),
            "successful_summary_receipts": cohorts.receipts.len(),
            "projected_records": cohorts.projected_records
        }
    }))
}

#[tokio::test]
#[ignore = "manual retained-archive outcome check; requires explicit archive and input count"]
async fn measure_retained_prometheus_archive() -> Result<()> {
    let archive = std::env::var_os("PROMETHEUS_MEASUREMENT_ARCHIVE")
        .context("PROMETHEUS_MEASUREMENT_ARCHIVE is required")?;
    let count = std::env::var("PROMETHEUS_MEASUREMENT_INPUTS")?.parse()?;
    let report = inspect_archive(Path::new(&archive), count).await?;
    if let Ok(mode) = std::env::var("PROMETHEUS_MEASUREMENT_RATE_LIMIT") {
        let enabled = match mode.as_str() {
            "0" => false,
            "1" => true,
            _ => bail!("PROMETHEUS_MEASUREMENT_RATE_LIMIT must be 0 or 1"),
        };
        let snapshot = obzenflow_infra::journal::read::open_disk_run(Path::new(&archive)).await?;
        verify_source_rate_limit(snapshot.manifest().effective_config.as_ref(), enabled)?;
    }
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

/// Executes the shipped binary. Build it first; compilation and archive checking
/// are outside the monotonic launch-to-exit interval. Child lifetime is owned.
/// This is the proposal's separate integration check, not a Criterion benchmark.
#[tokio::test]
#[ignore = "manual external demo baseline; requires a built example and explicit result path"]
async fn measure_demo_process_completion() -> Result<()> {
    use std::process::Stdio;
    use std::time::{Duration, Instant};
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let binary = std::env::var_os("PROMETHEUS_DEMO_BINARY")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| root.join("target/debug/examples/prometheus_demo"));
    let binary = binary
        .canonicalize()
        .context("build the example before timing")?;
    let output = std::env::var_os("PROMETHEUS_COMPLETION_REPORT")
        .context("PROMETHEUS_COMPLETION_REPORT is required")?;
    let count: u64 = std::env::var("PROMETHEUS_MEASUREMENT_INPUTS")
        .unwrap_or_else(|_| "20000".into())
        .parse()?;
    ensure!(
        (2..=20000).contains(&count),
        "bounded demo cohort must be 2..=20000"
    );
    let console = std::env::var("PROMETHEUS_MEASUREMENT_CONSOLE").unwrap_or_else(|_| "0".into());
    ensure!(
        matches!(console.as_str(), "0" | "1"),
        "Console must be 0 or 1"
    );
    let directory = tempfile::tempdir()?;
    let log = std::fs::File::create(directory.path().join("demo.log"))?;
    let mut command = tokio::process::Command::new(binary);
    command
        .current_dir(directory.path())
        .args([
            "--config",
            root.join("examples/prometheus_demo/obzenflow.studio.toml")
                .to_str()
                .unwrap(),
            "--startup-mode",
            "auto",
            "--server-port",
            "0",
        ])
        .env("PROMETHEUS_EVENT_COUNT", count.to_string())
        .env("PROMETHEUS_RATE_LIMIT", "0")
        .env("PROMETHEUS_TOKIO_CONSOLE", &console)
        .env("TOKIO_CONSOLE_BIND", "127.0.0.1:0")
        .env("TOKIO_WORKER_THREADS", "2")
        .env("RUST_LOG", "info")
        .stdin(Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true);
    let start = Instant::now();
    let mut child = command.spawn()?;
    let status = match tokio::time::timeout(Duration::from_secs(180), child.wait()).await {
        Ok(result) => result?,
        Err(_) => {
            child.kill().await?;
            bail!("demo completion timed out");
        }
    };
    let elapsed = start.elapsed();
    ensure!(
        status.success(),
        "demo exited unsuccessfully: {status}; {}",
        std::fs::read_to_string(directory.path().join("demo.log"))?
    );
    let runs = std::fs::read_dir(
        directory
            .path()
            .join("target/prometheus_demo_journal/flows"),
    )?
    .collect::<std::io::Result<Vec<_>>>()?;
    ensure!(runs.len() == 1, "expected one demo archive");
    let archive = runs[0].path();
    let mut report = inspect_archive(&archive, count).await?;
    let snapshot = obzenflow_infra::journal::read::open_disk_run(&archive).await?;
    verify_source_rate_limit(snapshot.manifest().effective_config.as_ref(), false)?;
    report["elapsed_seconds"] = json!(elapsed.as_secs_f64());
    report["completed_inputs_per_second"] = json!(count as f64 / elapsed.as_secs_f64());
    report["boundary"] = json!("monotonic process launch through successful exit; includes startup and drain; verification excluded");
    report["workers"] = json!(2);
    report["console_enabled"] = json!(console == "1");
    // The bounded Console witness also replays its own current-schema archive.
    // This is outside the live completion interval. A zero-input live source
    // cannot reproduce the cohort if replay accidentally invokes it.
    if count == 5000 {
        let replay_log = std::fs::File::create(directory.path().join("replay.log"))?;
        command
            .arg("--replay-from")
            .arg(&archive)
            .arg("--verify")
            .env("PROMETHEUS_EVENT_COUNT", "0")
            .stdout(replay_log.try_clone()?)
            .stderr(replay_log);
        let mut replay = command.spawn()?;
        let status = match tokio::time::timeout(Duration::from_secs(180), replay.wait()).await {
            Ok(result) => result?,
            Err(_) => {
                replay.kill().await?;
                bail!("bounded demo replay timed out");
            }
        };
        ensure!(
            status.success(),
            "same-build replay verification failed: {status}; {}",
            std::fs::read_to_string(directory.path().join("replay.log"))?
        );
        report["replay"] = json!({"verified": true, "live_source_input_limit": 0});
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    serde_json::to_writer_pretty(&mut file, &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
#[test]
fn retained_measurement_verifies_declared_source_limiter_policy() -> Result<()> {
    use obzenflow_core::config::{EffectiveConfigEvidence, EVIDENCE_SCHEMA_VERSION};
    use obzenflow_runtime::runtime_config::{
        RATE_LIMITER_COST_PER_ATTEMPT_KEY, RATE_LIMITER_EVENTS_PER_SECOND_KEY,
    };

    let disabled: EffectiveConfigEvidence = serde_json::from_value(json!({
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "values": [{
            "key_path": "runtime.max_lineage_depth", "scope": "global",
            "source": "default", "value": 128
        }]
    }))?;
    let mut enabled = disabled.clone();
    for (key, value) in [
        (RATE_LIMITER_EVENTS_PER_SECOND_KEY, 1000.0),
        (RATE_LIMITER_COST_PER_ATTEMPT_KEY, 1.0),
    ] {
        enabled.values.push(serde_json::from_value(json!({
            "key_path": key, "scope": format!("stage:{SOURCE}"),
            "source": "dsl", "value": value
        }))?);
    }
    let report = verify_source_rate_limit(Some(&enabled), true)?;
    assert_eq!(report["events_per_second"], 1000.0);
    assert_eq!(report["effective_config_rows"].as_array().unwrap().len(), 2);
    let report = verify_source_rate_limit(Some(&disabled), false)?;
    assert_eq!(report["enabled"], false);
    assert!(report["effective_config_rows"]
        .as_array()
        .unwrap()
        .is_empty());

    assert!(verify_source_rate_limit(Some(&enabled), false).is_err());
    assert!(verify_source_rate_limit(Some(&disabled), true).is_err());
    assert!(verify_source_rate_limit(None, false).is_err());
    let mut incomplete = disabled.clone();
    incomplete.values.clear();
    assert!(verify_source_rate_limit(Some(&incomplete), false).is_err());
    incomplete = disabled.clone();
    incomplete.schema_version = 1;
    assert!(verify_source_rate_limit(Some(&incomplete), false).is_err());
    let mut altered = enabled.clone();
    altered.values[1].value = json!(2000.0);
    assert!(verify_source_rate_limit(Some(&altered), true).is_err());
    altered = enabled.clone();
    altered.values[1].scope = format!("stage:{TRANSFORM}");
    assert!(verify_source_rate_limit(Some(&altered), true).is_err());
    altered = enabled.clone();
    altered.values[1].redacted = true;
    assert!(verify_source_rate_limit(Some(&altered), true).is_err());
    // Repeated rows cannot stand in for the missing second required setting.
    altered = enabled.clone();
    altered.values[2] = altered.values[1].clone();
    assert!(verify_source_rate_limit(Some(&altered), true).is_err());
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
