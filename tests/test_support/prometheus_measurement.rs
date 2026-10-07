// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-080n first measurements, using the supported retained-run projection.
//! Journal wall-clock stamps are preliminary evidence, not monotonic publication
//! completion timings. This scanner never starts a flow or deducts breaker waits.

use anyhow::{bail, ensure, Context, Result};
use obzenflow_core::event::observability::{CaptureStamp, ObservationSource};
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
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
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
    observations: obzenflow_runtime::metrics::observations::LatestObservationMap,
}

impl ArchiveCohorts {
    fn observe(&mut self, projected: &RunRecord) -> Result<()> {
        self.projected_records += 1;
        let observation = match &projected.record {
            RunRecordData::Chain(row) => row.envelope.observability.as_ref(),
            RunRecordData::System(row) => row.envelope.observability.as_ref(),
        };
        if let Some(packet) = observation
            .filter(|packet| packet.capture.capture_scope.flow_id == projected.run.flow_id)
        {
            self.observations
                .select_recorded(packet.clone())
                .map_err(|_| anyhow::anyhow!("retained observation selection dropped a packet"))?;
        }
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

#[derive(Default)]
struct LoopCounters {
    total: Option<(CaptureStamp, u64)>,
    with_work: Option<(CaptureStamp, u64)>,
}

fn retained_loop_utilization(
    observations: &obzenflow_runtime::metrics::observations::LatestObservationMap,
    manifest: &obzenflow_core::journal::RunManifest,
) -> Value {
    let stage_by_id: BTreeMap<_, _> = manifest
        .stages
        .iter()
        .map(|(key, stage)| (stage.stage_id.as_str(), key.as_str()))
        .collect();
    let mut counters: BTreeMap<_, LoopCounters> = manifest
        .stages
        .keys()
        .map(|key| (key.clone(), LoopCounters::default()))
        .collect();
    // The existing selector orders each recorded measurement family by its
    // capture scope/sequence. Carrier order and physical rail do not win.
    for packet in observations.snapshot() {
        let Some(stage) = packet.capture.observer.as_stage() else {
            continue;
        };
        let stage_id = stage.to_string();
        let Some(key) = stage_by_id.get(stage_id.as_str()) else {
            continue;
        };
        let Some(runtime) = packet.runtime else {
            continue;
        };
        let counts = counters.get_mut(*key).unwrap();
        if let Some(total) = runtime.event_loops_total {
            counts.total = Some((packet.capture, total));
        }
        if let Some(with_work) = runtime.event_loops_with_work_total {
            counts.with_work = Some((packet.capture, with_work));
        }
    }
    let rows: Vec<_> = counters
        .into_iter()
        .map(|(stage, counts)| {
            let same_capture = counts
                .total
                .zip(counts.with_work)
                .is_some_and(|((total_stamp, _), (work_stamp, _))| total_stamp == work_stamp);
            let ratio = counts
                .total
                .zip(counts.with_work)
                .filter(|_| same_capture)
                .map(|((_, total), (_, work))| {
                    if total == 0 {
                        0.0
                    } else {
                        work as f64 * 100.0 / total as f64
                    }
                });
            json!({
                "stage": stage,
                "event_loops_total": counts.total.map(|(_, count)| count),
                "event_loops_with_work_total": counts.with_work.map(|(_, count)| count),
                "total_capture": counts.total.map(|(capture, _)| capture),
                "with_work_capture": counts.with_work.map(|(capture, _)| capture),
                "same_capture": same_capture, "work_ratio_percent": ratio
            })
        })
        .collect();
    json!({
        "source": "latest retained recorded loop-count families, selected by observer and capture scope/sequence",
        "denominator": "event_loops_total accumulated over the stage instrumentation lifetime through the recorded capture; not a supervisor state or time interval",
        "ratio": "100 * event_loops_with_work_total / event_loops_total; existing zero-denominator convention is 0%; missing or differently stamped pairs have no ratio",
        "limitation": "recorded optional captures can predate final in-memory totals; this count ratio is neither CPU utilisation nor a time-based busy fraction",
        "stages": rows
    })
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
        "measurement_contract": "flowip-080n-retained-journal-preliminary-v1",
        "acceptance_grade": false,
        "archive": archive.canonicalize()?,
        "archive_identity": identity,
        "manifest": manifest,
        "verified_progress": progress,
        "count_based_utilization": retained_loop_utilization(&cohorts.observations, &manifest),
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

const PERFORMANCE_SUPERVISORS: [&str; 6] = [
    SOURCE,
    TRANSFORM,
    "event_counter",
    "summary_sink",
    obzenflow_core::event::vocabulary::supervisor::PIPELINE_NAME,
    obzenflow_core::event::vocabulary::supervisor::METRICS_NAME,
];
const RUNNER_PHASES: [&str; 7] = [
    "inline_action",
    "pending_action",
    "pending_dispatch",
    "direct_dispatch",
    "transition",
    "yield",
    "residual",
];
const CYCLE_PHASES: [&str; 9] = [
    "read",
    "handler",
    "prepare",
    "publish",
    "credit_wait",
    "acknowledge",
    "control",
    "idle",
    "residual",
];

#[derive(Default)]
struct CycleTotals {
    dispatches: u64,
    inputs: u64,
    acknowledgements: u64,
    elapsed: u64,
    phases: [u64; 9],
    worst_residual: u64,
    worst_elapsed: u64,
    max_residual: u64,
    residual_over_five_percent: u64,
    outcomes: BTreeMap<String, u64>,
}

fn percent(part: u64, whole: u64) -> Option<f64> {
    (whole > 0).then(|| 100.0 * part as f64 / whole as f64)
}

fn inspect_cycle_summaries(
    capture: &Value,
    identities: &BTreeMap<String, (String, String)>,
    states: &BTreeSet<(String, String)>,
    expected_inputs: u64,
) -> Result<(Vec<Value>, Vec<Value>)> {
    let summaries = capture["cycle_summaries"]
        .as_array()
        .context("cycle summaries")?;
    ensure!(!summaries.is_empty(), "empty cycle summaries");
    let mut keys = BTreeSet::new();
    let mut by_state = BTreeMap::<(String, String), CycleTotals>::new();
    let mut input_counts = BTreeMap::<String, u64>::new();
    let mut acknowledgement_counts = BTreeMap::<String, u64>::new();
    let mut rows = Vec::new();
    for summary in summaries {
        let supervisor = text_field(summary, "supervisor")?;
        ensure!(
            [SOURCE, TRANSFORM].contains(&supervisor),
            "unexpected cycle supervisor"
        );
        ensure!(
            text_field(summary, "supervisor_kind")?
                == if supervisor == SOURCE {
                    "FiniteSource"
                } else {
                    "Transform"
                },
            "unexpected cycle supervisor kind"
        );
        let identity = identities
            .get(supervisor)
            .context("cycle supervisor lacks runner summary")?;
        ensure!(
            text_field(summary, "writer_id")? == identity.0
                && text_field(summary, "supervision_mode")? == identity.1,
            "cycle writer/mode does not match verified archive runner"
        );
        let state = text_field(summary, "state")?;
        ensure!(
            states.contains(&(supervisor.to_owned(), state.to_owned())),
            "cycle state lacks runner summary"
        );
        let outcome = text_field(summary, "outcome")?;
        ensure!(
            matches!(outcome, "completed" | "error"),
            "unfinished cycle outcome {outcome}"
        );
        ensure!(summary["interrupted"] == false, "interrupted cycle summary");
        ensure!(
            keys.insert((supervisor.to_owned(), state.to_owned(), outcome.to_owned())),
            "duplicate cycle summary"
        );
        let count = unsigned(summary, "dispatch_count")?;
        ensure!(count > 0, "empty cycle summary");
        let inputs = unsigned(summary, "business_input_count")?;
        let acknowledgements = unsigned(summary, "acknowledgement_count")?;
        let elapsed = unsigned(summary, "elapsed_ns")?;
        let mut phases = [0_u64; 9];
        let mut sum = 0_u64;
        let mut percentages = BTreeMap::new();
        for (index, phase) in CYCLE_PHASES.into_iter().enumerate() {
            phases[index] = unsigned(summary, &format!("{phase}_ns"))?;
            sum = sum
                .checked_add(phases[index])
                .context("cycle phase overflow")?;
            percentages.insert(phase, percent(phases[index], elapsed));
        }
        ensure!(
            sum == elapsed,
            "nonconserved cycle phases for {supervisor}/{state}/{outcome}"
        );
        ensure!(
            unsigned(summary, "conservation_failures")? == 0
                && unsigned(summary, "max_conservation_error_ns")? == 0,
            "per-cycle conservation failed"
        );
        let worst = unsigned(summary, "worst_residual_ns")?;
        let worst_elapsed = unsigned(summary, "worst_residual_elapsed_ns")?;
        let maximum = unsigned(summary, "max_residual_ns")?;
        let residual_over_five = unsigned(summary, "residual_over_five_percent_count")?;
        let residual = phases[8];
        ensure!(
            worst <= worst_elapsed
                && worst_elapsed <= elapsed
                && (elapsed == 0 || worst_elapsed > 0)
                && worst <= maximum
                && maximum <= residual
                && (residual == 0 || worst > 0)
                && residual_over_five <= count
                && (residual_over_five > 0) == (worst as u128 * 100 > worst_elapsed as u128 * 5)
                && (worst as u128 * elapsed as u128 >= residual as u128 * worst_elapsed as u128),
            "inconsistent worst cycle residual evidence"
        );
        let total_inputs = input_counts.entry(supervisor.to_owned()).or_default();
        *total_inputs = total_inputs
            .checked_add(inputs)
            .context("cycle input count overflow")?;
        let total_acknowledgements = acknowledgement_counts
            .entry(supervisor.to_owned())
            .or_default();
        *total_acknowledgements = total_acknowledgements
            .checked_add(acknowledgements)
            .context("cycle acknowledgement count overflow")?;
        let mut row = summary.clone();
        row["phase_percent"] = json!(percentages);
        row["worst_residual_percent"] = json!(percent(worst, worst_elapsed));
        rows.push(row);
        let total = by_state
            .entry((supervisor.to_owned(), state.to_owned()))
            .or_default();
        total.dispatches = total
            .dispatches
            .checked_add(count)
            .context("cycle count overflow")?;
        total.inputs = total
            .inputs
            .checked_add(inputs)
            .context("cycle input count overflow")?;
        total.acknowledgements = total
            .acknowledgements
            .checked_add(acknowledgements)
            .context("cycle acknowledgement count overflow")?;
        total.elapsed = total
            .elapsed
            .checked_add(elapsed)
            .context("cycle elapsed overflow")?;
        for (total, value) in total.phases.iter_mut().zip(phases) {
            *total = total.checked_add(value).context("cycle phase overflow")?;
        }
        if total.worst_elapsed == 0
            || worst as u128 * total.worst_elapsed as u128
                > total.worst_residual as u128 * worst_elapsed as u128
        {
            total.worst_residual = worst;
            total.worst_elapsed = worst_elapsed;
        }
        total.max_residual = total.max_residual.max(maximum);
        total.residual_over_five_percent = total
            .residual_over_five_percent
            .checked_add(residual_over_five)
            .context("residual count overflow")?;
        total.outcomes.insert(outcome.to_owned(), count);
    }
    for supervisor in [SOURCE, TRANSFORM] {
        ensure!(
            input_counts.get(supervisor) == Some(&expected_inputs),
            "cycle business inputs for {supervisor} do not match verified demo cohort"
        );
        // This demo has one source-to-transform data row per original input;
        // both successful and intended error outcomes release that input's
        // credit. Sources have no upstream credit to acknowledge.
        let expected_acknowledgements = if supervisor == TRANSFORM {
            expected_inputs
        } else {
            0
        };
        ensure!(
            acknowledgement_counts.get(supervisor) == Some(&expected_acknowledgements),
            "cycle acknowledgements for {supervisor} do not match verified demo cohort"
        );
    }
    rows.sort_by(|a, b| {
        (
            a["supervisor"].as_str(),
            a["state"].as_str(),
            a["outcome"].as_str(),
        )
            .cmp(&(
                b["supervisor"].as_str(),
                b["state"].as_str(),
                b["outcome"].as_str(),
            ))
    });
    let totals = by_state
        .into_iter()
        .map(|((supervisor, state), total)| {
            let phases: BTreeMap<_, _> = CYCLE_PHASES.into_iter().zip(total.phases).collect();
            let percentages: BTreeMap<_, _> = phases
                .iter()
                .map(|(phase, ns)| (*phase, percent(*ns, total.elapsed)))
                .collect();
            json!({
                "supervisor": supervisor, "state": state, "dispatch_count": total.dispatches,
                "business_input_count": total.inputs, "elapsed_ns": total.elapsed,
                "acknowledgement_count": total.acknowledgements,
                "phase_ns": phases, "phase_percent": percentages, "outcomes": total.outcomes,
                "worst_residual_ns": total.worst_residual,
                "worst_residual_elapsed_ns": total.worst_elapsed,
                "worst_residual_percent": percent(total.worst_residual, total.worst_elapsed),
                "max_residual_ns": total.max_residual,
                "residual_over_five_percent_count": total.residual_over_five_percent
            })
        })
        .collect();
    Ok((rows, totals))
}

fn verify_cycle_completeness(capture: &Value, cycles: &[Value]) -> Result<Vec<Value>> {
    let summaries = capture["supervisor_summaries"]
        .as_array()
        .context("runner summaries")?;
    let mut verified = Vec::new();
    for supervisor in [SOURCE, TRANSFORM] {
        let mut completions = BTreeMap::<String, u64>::new();
        let mut controls = 0_u64;
        for summary in summaries
            .iter()
            .filter(|row| row["supervisor"] == supervisor)
        {
            let state = text_field(summary, "state")?;
            let complete = unsigned(summary, "pending_dispatch_complete_wins")?;
            let control = unsigned(summary, "pending_dispatch_control_wins")?;
            let failure = unsigned(summary, "pending_dispatch_publication_failure_wins")?;
            ensure!(
                failure == 0,
                "cycle completeness is unqualified after publication failure"
            );
            if matches!(state, "Running" | "Draining") {
                completions.insert(state.to_owned(), complete);
                controls = controls
                    .checked_add(control)
                    .context("cycle control count overflow")?;
            } else {
                // Finite-source acquisition is owned dispatch but deliberately
                // outside the measured Running/Draining cycle scope. A control
                // migration through another state would make this subtraction
                // ambiguous, so this completed-demo oracle fails explicitly.
                ensure!(control == 0 && (complete == 0 || (supervisor == SOURCE && state == "AcquiringInput")),
                    "cycle completeness is unqualified across unmeasured dispatch state {supervisor}/{state}");
            }
        }
        let mut counts = BTreeMap::<String, u64>::new();
        for row in cycles.iter().filter(|row| row["supervisor"] == supervisor) {
            let state = text_field(row, "state")?;
            ensure!(
                matches!(state, "Running" | "Draining"),
                "unexpected measured cycle state"
            );
            counts.insert(state.to_owned(), unsigned(row, "dispatch_count")?);
        }
        let sum = |counts: &BTreeMap<String, u64>| -> Result<u64> {
            counts.values().try_fold(0_u64, |sum, count| {
                sum.checked_add(*count)
                    .context("cycle completion count overflow")
            })
        };
        let expected = sum(&completions)?;
        let actual = sum(&counts)?;
        ensure!(actual == expected, "missing or extra dispatch cycles for {supervisor}: {actual} != {expected} runner completions");
        if controls == 0 {
            for state in ["Running", "Draining"] {
                ensure!(
                    counts.get(state).copied().unwrap_or(0)
                        == completions.get(state).copied().unwrap_or(0),
                    "missing or extra dispatch cycles for {supervisor}/{state}"
                );
            }
        }
        verified.push(json!({
            "supervisor": supervisor, "dispatch_count": actual,
            "runner_complete_wins": expected, "runner_control_wins": controls,
            "comparison": if controls == 0 { "per state and combined Running/Draining" } else { "combined Running/Draining; cycle state is its start state, runner completion state can differ after control" }
        }));
    }
    Ok(verified)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct PerformanceFrame {
    name: String,
    #[serde(default)]
    identity: BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct PerformanceRow {
    path: Vec<PerformanceFrame>,
    calls: u64,
    elapsed_ns: u64,
    entered_ns: u64,
    enters: u64,
    max_elapsed_ns: u64,
    open_calls: u64,
}

#[derive(Default, Serialize)]
struct OperationTotals {
    calls: u64,
    elapsed_ns: u64,
    entered_ns: u64,
    enters: u64,
}

impl OperationTotals {
    fn add(&mut self, row: &PerformanceRow) -> Result<()> {
        self.calls = self
            .calls
            .checked_add(row.calls)
            .context("call count overflow")?;
        self.elapsed_ns = self
            .elapsed_ns
            .checked_add(row.elapsed_ns)
            .context("elapsed overflow")?;
        self.entered_ns = self
            .entered_ns
            .checked_add(row.entered_ns)
            .context("entered overflow")?;
        self.enters = self
            .enters
            .checked_add(row.enters)
            .context("enter count overflow")?;
        Ok(())
    }
}

fn unsigned(value: &Value, key: &str) -> Result<u64> {
    value[key]
        .as_u64()
        .with_context(|| format!("missing unsigned performance field {key}"))
}

fn text_field<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|value| !value.is_empty())
        .with_context(|| format!("missing performance identity {key}"))
}

fn strip_ansi(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for code in chars.by_ref() {
                if ('@'..='~').contains(&code) {
                    break;
                }
            }
        } else {
            output.push(ch);
        }
    }
    output
}

fn disk_boundary(name: &str) -> Option<&'static str> {
    match name {
        "disk_journal_read_next"
        | "disk_journal_read_all"
        | "disk_journal_read_event"
        | "disk_journal_read_tail"
        | "disk_journal_metrics_tail"
        | "disk_journal_observation_lookup"
        | "disk_journal_reader_advance"
        | "disk_journal_reader_admit_snapshot"
        | "disk_journal_archive_scan_maxima"
        | "disk_journal_archive_scan_status" => Some("read"),
        "disk_journal_append" | "disk_journal_append_record" | "disk_journal_append_group" => {
            Some("append")
        }
        _ => None,
    }
}

fn expected_performance_writers(
    manifest: &obzenflow_core::journal::RunManifest,
) -> Result<BTreeMap<String, String>> {
    let mut writers = BTreeMap::new();
    for name in [SOURCE, TRANSFORM, "event_counter", "summary_sink"] {
        let stage = manifest
            .stages
            .get(name)
            .context("missing demo stage writer")?;
        let id = stage
            .stage_id
            .parse::<obzenflow_core::StageId>()
            .with_context(|| format!("invalid archive stage identity for {name}"))?;
        writers.insert(
            name.to_owned(),
            obzenflow_core::WriterId::from(id).to_string(),
        );
    }
    writers.insert(
        obzenflow_core::event::vocabulary::supervisor::PIPELINE_NAME.to_owned(),
        manifest.pipeline_writer_id.to_string(),
    );
    writers.insert(
        obzenflow_core::event::vocabulary::supervisor::METRICS_NAME.to_owned(),
        manifest
            .metrics_journals
            .as_ref()
            .context("archive lacks metrics writer identity")?
            .writer_id
            .to_string(),
    );
    Ok(writers)
}

fn inspect_performance_log(
    log: &str,
    flow_id: &str,
    expected_writers: &BTreeMap<String, String>,
    expected_inputs: u64,
) -> Result<Value> {
    let log = strip_ansi(log);
    ensure!(
        log.contains(flow_id),
        "performance log does not identify archive flow {flow_id}"
    );
    let mut captures = log
        .lines()
        .filter(|line| line.contains("performance_capture"));
    let line = captures.next().context("missing performance_capture")?;
    ensure!(
        captures.next().is_none(),
        "expected exactly one performance_capture"
    );
    let (_, encoded) = line
        .split_once("report=")
        .context("performance_capture lacks report=")?;
    let capture: Value =
        serde_json::from_str(encoded.trim()).context("performance_capture JSON")?;
    ensure!(
        unsigned(&capture, "version")? == 2,
        "unsupported performance version"
    );
    let mode = text_field(&capture, "capture_mode")?;
    ensure!(
        matches!(mode, "coarse" | "deep"),
        "unsupported capture mode"
    );
    ensure!(
        capture["interrupted"] == false,
        "interrupted performance capture"
    );
    for key in ["open_spans", "dropped_spans", "dropped_summaries"] {
        ensure!(
            unsigned(&capture, key)? == 0,
            "incomplete performance capture: {key}"
        );
    }
    let collector_callbacks = json!({
        "elapsed_ns": unsigned(&capture, "collector_callback_ns")?,
        "calls": unsigned(&capture, "collector_callback_calls")?,
        "semantics": text_field(&capture, "collector_callback_semantics")?
    });
    let summaries = capture["supervisor_summaries"]
        .as_array()
        .context("supervisor summaries")?;
    ensure!(!summaries.is_empty(), "empty supervisor summaries");
    let mut identities = BTreeMap::<String, (String, String)>::new();
    let mut states = BTreeSet::new();
    let mut phase_rows = Vec::new();
    for summary in summaries {
        let supervisor = text_field(summary, "supervisor")?;
        ensure!(
            PERFORMANCE_SUPERVISORS.contains(&supervisor),
            "unexpected supervisor {supervisor}"
        );
        let writer = text_field(summary, "writer_id")?;
        ensure!(
            expected_writers.get(supervisor).map(String::as_str) == Some(writer),
            "performance writer for {supervisor} does not match the verified archive"
        );
        let mode = text_field(summary, "supervision_mode")?;
        let state = text_field(summary, "state")?;
        ensure!(
            summary["interrupted"] == false,
            "interrupted supervisor {supervisor}/{state}"
        );
        let identity = (writer.to_owned(), mode.to_owned());
        if let Some(prior) = identities.insert(supervisor.to_owned(), identity.clone()) {
            ensure!(
                prior == identity,
                "multiple writers or modes for supervisor {supervisor}"
            );
        }
        ensure!(
            states.insert((supervisor.to_owned(), state.to_owned())),
            "duplicate supervisor state summary"
        );
        let elapsed = unsigned(summary, "elapsed_ns")?;
        let turns = unsigned(summary, "outer_turns")?;
        let mut sum = 0u64;
        let mut percentages = BTreeMap::new();
        for phase in RUNNER_PHASES {
            let ns = unsigned(summary, &format!("{phase}_ns"))?;
            unsigned(summary, &format!("{phase}_entries"))?;
            sum = sum.checked_add(ns).context("phase total overflow")?;
            percentages.insert(
                phase,
                (elapsed > 0).then(|| ns as f64 * 100.0 / elapsed as f64),
            );
        }
        ensure!(
            sum == elapsed,
            "nonconserved runner phases for {supervisor}/{state}: {sum} != {elapsed}"
        );
        phase_rows.push(json!({
            "supervisor": supervisor, "writer_id": writer, "supervision_mode": mode,
            "state": state, "elapsed_ns": elapsed, "outer_turns": turns,
            "phase_percent": percentages
        }));
    }
    ensure!(
        identities.len() == PERFORMANCE_SUPERVISORS.len(),
        "missing expected supervisor summaries"
    );
    phase_rows.sort_by(|a, b| {
        (a["supervisor"].as_str(), a["state"].as_str())
            .cmp(&(b["supervisor"].as_str(), b["state"].as_str()))
    });
    let (cycle_rows, cycle_totals) =
        inspect_cycle_summaries(&capture, &identities, &states, expected_inputs)?;
    let cycle_completeness = verify_cycle_completeness(&capture, &cycle_totals)?;

    let spans: Vec<PerformanceRow> =
        serde_json::from_value(capture["spans"].clone()).context("performance span rows")?;
    ensure!(
        if mode == "coarse" {
            spans.is_empty()
        } else {
            !spans.is_empty()
        },
        "span capture does not match declared mode"
    );
    let mut paths = BTreeSet::new();
    let mut rooted_supervisors = BTreeSet::new();
    let mut disk = BTreeMap::<(String, String), OperationTotals>::new();
    let mut operations = BTreeMap::<(String, String, String), OperationTotals>::new();
    for row in &spans {
        let leaf = row.path.last().context("empty performance path")?;
        ensure!(
            row.open_calls == 0 && row.calls > 0,
            "open or empty span totals"
        );
        ensure!(
            row.max_elapsed_ns <= row.elapsed_ns,
            "span maximum exceeds total"
        );
        ensure!(
            paths.insert(row.path.clone()),
            "duplicate aggregate span path"
        );
        let root =
            row.path.iter().enumerate().rev().find(|(_, frame)| {
                matches!(frame.name.as_str(), "supervisor" | "metrics_tail_reader")
            });
        let background_reader = root.is_some_and(|(_, root)| root.name == "metrics_tail_reader");
        let supervisor = if let Some((root_index, root)) = root {
            let name = root
                .identity
                .get("supervisor")
                .context("root supervisor identity")?;
            if background_reader {
                ensure!(
                    root_index == 0
                        && name == obzenflow_core::event::vocabulary::supervisor::METRICS_NAME
                        && root.identity.get("supervisor_kind").map(String::as_str)
                            == Some("MetricsAggregator")
                        && matches!(
                            root.identity.get("journal_kind").map(String::as_str),
                            Some("data" | "error" | "system")
                        )
                        && !row
                            .path
                            .iter()
                            .any(|frame| frame.name == "supervisor_state"),
                    "metrics tail reader lacks independent metrics identity"
                );
            }
            let expected = identities
                .get(name)
                .context("span supervisor lacks summary")?;
            ensure!(
                root.identity.get("writer_id") == Some(&expected.0)
                    && root.identity.get("supervision_mode") == Some(&expected.1),
                "span/summary identity mismatch"
            );
            if !background_reader {
                rooted_supervisors.insert(name.clone());
            }
            name.as_str()
        } else {
            "unattributed"
        };
        let state = row
            .path
            .iter()
            .rev()
            .find(|frame| frame.name == "supervisor_state")
            .and_then(|frame| frame.identity.get("state"))
            .map(String::as_str)
            .unwrap_or("outside_state");
        if supervisor != "unattributed" && state != "outside_state" {
            ensure!(
                states.contains(&(supervisor.to_owned(), state.to_owned())),
                "span state lacks summary"
            );
        }
        if [
            "disk_journal_",
            "subscription_",
            "source_",
            "transform_",
            "backpressure_",
            "output_",
        ]
        .iter()
        .any(|prefix| leaf.name.starts_with(prefix))
        {
            operations
                .entry((supervisor.to_owned(), state.to_owned(), leaf.name.clone()))
                .or_default()
                .add(row)?;
        }
        if let Some(kind) = disk_boundary(&leaf.name) {
            // Only the outermost disk operation counts here: e.g. append_record
            // is already included in its append parent. Concurrency may still
            // overlap; these totals are not an exclusive elapsed-time ledger.
            if !row.path[..row.path.len() - 1]
                .iter()
                .any(|frame| disk_boundary(&frame.name).is_some())
            {
                disk.entry((supervisor.to_owned(), kind.to_owned()))
                    .or_default()
                    .add(row)?;
            }
        }
    }
    ensure!(
        mode == "coarse" || rooted_supervisors.len() == PERFORMANCE_SUPERVISORS.len(),
        "missing expected supervisor span ancestry"
    );
    let disk_rows: Vec<_> = disk
        .into_iter()
        .map(|((supervisor, operation), totals)| {
            json!({
                "supervisor": supervisor, "operation": operation, "totals": totals
            })
        })
        .collect();
    let mut operation_rows: Vec<_> = operations
        .into_iter()
        .map(|((supervisor, state, operation), totals)| {
            json!({
                "supervisor": supervisor, "state": state, "operation": operation, "totals": totals
            })
        })
        .collect();
    operation_rows
        .sort_by_key(|row| std::cmp::Reverse(row["totals"]["elapsed_ns"].as_u64().unwrap()));
    Ok(json!({
        "report_contract": "flowip-080n-performance-diagnostic-v2", "flow_id": flow_id,
        "capture_mode": mode,
        "verified_archive_writers": expected_writers,
        "acceptance_grade": false,
        "phase_percent_denominator": "the same supervisor and state elapsed_ns; seven disjoint runner intervals conserve it exactly",
        "disk_total_semantics": "outermost disk read/append spans only; nested children excluded, concurrent operations may overlap; not a flow wall-time denominator",
        "operation_total_semantics": "inclusive nested span totals; do not sum operation rows or add them to disk totals",
        "background_reader_semantics": "metrics_tail_reader is an independent metrics root; its operations are outside_state and can overlap the supervisor and other readers; reader lifetimes never enter the exclusive runner phase ledger",
        "collector_callbacks": collector_callbacks,
        "cycle_percent_denominator": "summed complete dispatch-cycle wall time in the same supervisor/state; nine exclusive phases conserve each cycle, including suspension and resume delay; outcome shares are weighted by elapsed nanoseconds, not averaged percentages",
        "cycle_rows": cycle_rows, "cycle_totals": cycle_totals,
        "cycle_completeness": cycle_completeness,
        "phase_rows": phase_rows, "disk_operations": disk_rows,
        "nested_operations_by_elapsed": operation_rows, "capture": capture
    }))
}

fn performance_markdown(report: &Value) -> Result<String> {
    use std::fmt::Write;
    let mut out = format!("# Initial performance breakdown\n\nFlow: `{}`. Diagnostic capture; instrumentation overhead is unqualified.\n\n", text_field(report, "flow_id")?);
    if let Some(callbacks) = report
        .get("collector_callbacks")
        .filter(|value| !value.is_null())
    {
        writeln!(out, "Collector bookkeeping: {} callbacks, {:.3} ms summed callback wall time, including lock waits. Concurrent callbacks overlap; this is neither CPU time nor additive flow wall time, and excludes report serialisation.\n", unsigned(callbacks, "calls")?, unsigned(callbacks, "elapsed_ns")? as f64 / 1e6)?;
    }
    if let Some(enabled) = report["source_rate_limit"]["enabled"].as_bool() {
        let policy = if enabled {
            "enabled at 1,000 events/sec with cost 1 per attempt"
        } else {
            "disabled, with no rate-limiter middleware configuration rows"
        };
        writeln!(out, "Source limiter: {policy}, verified from the archive's effective configuration. This establishes the configured policy, not runtime admission or wait counts.\n")?;
    }
    writeln!(out, "Capture mode: {}. The ±5 percentage point allocation target is not an established accuracy guarantee. Raw nanoseconds remain in the JSON.\n", text_field(report, "capture_mode")?)?;
    out.push_str("Dispatch cycles use nine exclusive wall-time phases, including suspension and resume delay. Percentages divide summed phase nanoseconds by summed cycle nanoseconds in the same supervisor/state, weighting outcomes by elapsed time. Business inputs count delivered inputs once; retries can add cycles without inputs. Acknowledgements count physical data-credit rows, including filtered rows. Means per input amortise cycles and are not individual event latency.\n\n| Supervisor | State | Cycles | Business inputs | Acknowledgements | Total cycle ms | Mean µs/cycle | Amortised µs/input |\n|---|---|---:|---:|---:|---:|---:|---:|\n");
    let cycles = report["cycle_totals"].as_array().context("cycle totals")?;
    for row in cycles {
        writeln!(
            out,
            "| {} | {} | {} | {} | {} | {:.3} | {} | {} |",
            text_field(row, "supervisor")?,
            text_field(row, "state")?,
            unsigned(row, "dispatch_count")?,
            unsigned(row, "business_input_count")?,
            unsigned(row, "acknowledgement_count")?,
            unsigned(row, "elapsed_ns")? as f64 / 1e6,
            mean_microseconds(
                unsigned(row, "elapsed_ns")?,
                unsigned(row, "dispatch_count")?
            ),
            mean_microseconds(
                unsigned(row, "elapsed_ns")?,
                unsigned(row, "business_input_count")?
            )
        )?;
    }
    out.push_str("\n| Supervisor | State | Read % | Handler % | Prepare % | Publish % | Credit wait % | Acknowledge % | Control % | Idle % | Residual % |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in cycles {
        write!(
            out,
            "| {} | {}",
            text_field(row, "supervisor")?,
            text_field(row, "state")?
        )?;
        for phase in CYCLE_PHASES {
            match row["phase_percent"][phase].as_f64() {
                Some(value) => write!(out, " | {value:.1}")?,
                None => out.push_str(" | n/a"),
            }
        }
        out.push_str(" |\n");
    }
    out.push_str("\nResidual maxima describe individual cycles. The worst fraction's numerator and denominator are retained together; the largest absolute residual can belong to another cycle. The count marks cycles with residual strictly above 5% of their own elapsed time. These checks expose unattributed elapsed, not total measurement error.\n\n| Supervisor | State | Total residual ms | Cycles above 5% | Worst residual fraction % | Residual µs at worst fraction | Cycle µs at worst fraction | Largest residual µs |\n|---|---|---:|---:|---:|---:|---:|---:|\n");
    for row in cycles {
        let fraction = row["worst_residual_percent"]
            .as_f64()
            .map(|value| format!("{value:.1}"))
            .unwrap_or_else(|| "n/a".into());
        writeln!(
            out,
            "| {} | {} | {:.3} | {} | {} | {:.3} | {:.3} | {:.3} |",
            text_field(row, "supervisor")?,
            text_field(row, "state")?,
            unsigned(&row["phase_ns"], "residual")? as f64 / 1e6,
            unsigned(row, "residual_over_five_percent_count")?,
            fraction,
            unsigned(row, "worst_residual_ns")? as f64 / 1e3,
            unsigned(row, "worst_residual_elapsed_ns")? as f64 / 1e3,
            unsigned(row, "max_residual_ns")? as f64 / 1e3
        )?;
    }
    out.push_str("\nCycle outcomes are dispatch return outcomes; handled business errors may still complete a dispatch.\n\n| Supervisor | State | Outcome | Cycles | Business inputs |\n|---|---|---|---:|---:|\n");
    for row in report["cycle_rows"].as_array().context("cycle rows")? {
        writeln!(
            out,
            "| {} | {} | {} | {} | {} |",
            text_field(row, "supervisor")?,
            text_field(row, "state")?,
            text_field(row, "outcome")?,
            unsigned(row, "dispatch_count")?,
            unsigned(row, "business_input_count")?
        )?;
    }
    out.push('\n');
    if let Some(rows) = report["count_based_utilization"]["stages"].as_array() {
        out.push_str("Existing count-based utilisation uses the latest retained loop counters over each stage instrumentation lifetime through its capture. It can predate final in-memory totals. It is separate from the state timing below and is neither CPU utilisation nor a time-based busy fraction. Missing or differently stamped counter pairs show n/a.\n\n| Stage | Total loops | Loops with work | Work ratio % |\n|---|---:|---:|---:|\n");
        for row in rows {
            let count = |key: &str| {
                row[key]
                    .as_u64()
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "n/a".into())
            };
            let ratio = row["work_ratio_percent"]
                .as_f64()
                .map(|value| format!("{value:.1}"))
                .unwrap_or_else(|| "n/a".into());
            writeln!(
                out,
                "| {} | {} | {} | {} |",
                text_field(row, "stage")?,
                count("event_loops_total"),
                count("event_loops_with_work_total"),
                ratio
            )?;
        }
        out.push('\n');
    }
    out.push_str("Runner phases are disjoint elapsed intervals, including suspension and resume delay. Percentages use each row's supervisor/state elapsed time; they are not CPU percentages. Outer runner turns include dispatch setup and pending-future selection turns: they are not business-event counts. Mean per turn amortises the entire state interval, including intervening bookkeeping.\n\n| Supervisor | State | Total elapsed ms | Outer turns | Mean µs/turn | Inline % | Pending action % | Pending dispatch % | Direct dispatch % | Transition % | Yield % | Residual % |\n|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
    for row in report["phase_rows"].as_array().context("phase rows")? {
        write!(
            out,
            "| {} | {} | {:.3} | {} | {}",
            text_field(row, "supervisor")?,
            text_field(row, "state")?,
            unsigned(row, "elapsed_ns")? as f64 / 1e6,
            unsigned(row, "outer_turns")?,
            mean_microseconds(unsigned(row, "elapsed_ns")?, unsigned(row, "outer_turns")?)
        )?;
        for phase in RUNNER_PHASES {
            match row["phase_percent"][phase].as_f64() {
                Some(percent) => write!(out, " | {percent:.1}")?,
                None => out.push_str(" | n/a"),
            }
        }
        out.push_str(" |\n");
    }
    if report["capture_mode"] == "coarse" {
        out.push_str("\nFine operation spans were disabled. Disk and nested operation timing was not measured in this capture.\n");
        return Ok(out);
    }
    out.push_str("\nDeep span totals are inclusive and non-additive. Disk totals count only outermost read/append boundaries. Nested children are excluded; concurrent operations may still overlap. Metrics background reader operations belong to metrics/outside_state and can overlap each other and the metrics supervisor. Their lifetimes are excluded from the runner phase ledger. Inclusive elapsed includes waits; entered wall time includes descheduling and nested work. Neither is CPU time. Unattributed work has no captured supervisor or metrics reader ancestor. Means are per operation call, not per unique business input, and do not provide latency percentiles.\n\n| Supervisor | Disk operation | Calls | Total inclusive ms | Mean inclusive µs/call | Total entered ms |\n|---|---|---:|---:|---:|---:|\n");
    for row in report["disk_operations"].as_array().context("disk rows")? {
        let totals = &row["totals"];
        writeln!(
            out,
            "| {} | {} | {} | {:.3} | {} | {:.3} |",
            text_field(row, "supervisor")?,
            text_field(row, "operation")?,
            unsigned(totals, "calls")?,
            unsigned(totals, "elapsed_ns")? as f64 / 1e6,
            mean_microseconds(unsigned(totals, "elapsed_ns")?, unsigned(totals, "calls")?),
            unsigned(totals, "entered_ns")? as f64 / 1e6
        )?;
    }
    out.push_str("\nLargest nested operations by inclusive elapsed time (first 30). Parent rows include child rows; do not sum this table. Full ancestor paths and all operation rows are retained in the JSON.\n\n| Supervisor | State | Operation | Calls | Total inclusive ms | Mean inclusive µs/call | Total entered ms |\n|---|---|---|---:|---:|---:|---:|\n");
    for row in report["nested_operations_by_elapsed"]
        .as_array()
        .context("operation rows")?
        .iter()
        .take(30)
    {
        let totals = &row["totals"];
        writeln!(
            out,
            "| {} | {} | {} | {} | {:.3} | {} | {:.3} |",
            text_field(row, "supervisor")?,
            text_field(row, "state")?,
            text_field(row, "operation")?,
            unsigned(totals, "calls")?,
            unsigned(totals, "elapsed_ns")? as f64 / 1e6,
            mean_microseconds(unsigned(totals, "elapsed_ns")?, unsigned(totals, "calls")?),
            unsigned(totals, "entered_ns")? as f64 / 1e6
        )?;
    }
    Ok(out)
}

fn mean_microseconds(elapsed_ns: u64, calls: u64) -> String {
    if calls == 0 {
        "n/a".into()
    } else {
        format!("{:.3}", elapsed_ns as f64 / calls as f64 / 1e3)
    }
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
    let mut report = inspect_archive(Path::new(&archive), count).await?;
    let manifest: obzenflow_core::journal::RunManifest =
        serde_json::from_value(report["manifest"].clone()).context("verified archive manifest")?;
    if let Some(mode) = std::env::var_os("PROMETHEUS_MEASUREMENT_RATE_LIMIT") {
        let enabled = match mode.to_str() {
            Some("0") => false,
            Some("1") => true,
            _ => bail!("PROMETHEUS_MEASUREMENT_RATE_LIMIT must be 0 or 1"),
        };
        report["source_rate_limit"] =
            verify_source_rate_limit(manifest.effective_config.as_ref(), enabled)?;
    }
    let flow_id = report["manifest"]["flow_id"]
        .as_str()
        .context("flow identity")?;
    let performance = std::env::var_os("PROMETHEUS_MEASUREMENT_LOG")
        .map(|path| -> Result<(Value, String)> {
            let log_path = Path::new(&path);
            let log = std::fs::read_to_string(log_path).context("read performance log")?;
            let expected_writers = expected_performance_writers(&manifest)?;
            let mut performance = inspect_performance_log(&log, flow_id, &expected_writers, count)?;
            performance["count_based_utilization"] = report["count_based_utilization"].clone();
            if let Some(policy) = report.get("source_rate_limit") {
                performance["source_rate_limit"] = policy.clone();
            }
            performance["log"] = json!(log_path.canonicalize()?);
            performance["archive"] = json!(Path::new(&archive).canonicalize()?);
            let markdown = performance_markdown(&performance)?;
            Ok((performance, markdown))
        })
        .transpose()?;
    let directory = Path::new("target/flowip-080n");
    std::fs::create_dir_all(directory)?;
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
    if let Some((performance, markdown)) = performance {
        let output = directory.join(format!("{flow_id}-performance.json"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&output)
            .with_context(|| format!("preserving prior report {}", output.display()))?;
        serde_json::to_writer_pretty(&mut file, &performance)?;
        file.write_all(b"\n")?;
        let markdown_path = directory.join(format!("{flow_id}-performance.md"));
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&markdown_path)
            .with_context(|| format!("preserving prior report {}", markdown_path.display()))?;
        file.write_all(markdown.as_bytes())?;
        println!("FLOWIP-080n performance report: {}", output.display());
        println!(
            "FLOWIP-080n readable breakdown: {}",
            markdown_path.display()
        );
    }
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

fn performance_fixture() -> Value {
    let mut summaries = Vec::new();
    let mut cycles = Vec::new();
    let mut spans = Vec::new();
    for supervisor in PERFORMANCE_SUPERVISORS {
        let identity = json!({
            "supervisor": supervisor, "writer_id": format!("writer_{supervisor}"),
            "supervision_mode": "self_supervised"
        });
        let mut summary = identity.clone();
        summary["state"] = json!("Running");
        summary["elapsed_ns"] = json!(100);
        summary["outer_turns"] = json!(2);
        summary["interrupted"] = json!(false);
        summary["pending_dispatch_complete_wins"] =
            json!(if [SOURCE, TRANSFORM].contains(&supervisor) {
                10
            } else {
                0
            });
        summary["pending_dispatch_control_wins"] = json!(0);
        summary["pending_dispatch_publication_failure_wins"] = json!(0);
        for phase in RUNNER_PHASES {
            summary[format!("{phase}_ns")] = json!(match phase {
                "direct_dispatch" => 60,
                "residual" => 40,
                _ => 0,
            });
            summary[format!("{phase}_entries")] = json!(1);
        }
        if [SOURCE, TRANSFORM].contains(&supervisor) {
            let mut cycle = identity.clone();
            cycle["supervisor_kind"] = json!(if supervisor == SOURCE {
                "FiniteSource"
            } else {
                "Transform"
            });
            cycle["state"] = json!("Running");
            cycle["outcome"] = json!("completed");
            cycle["dispatch_count"] = json!(10);
            cycle["business_input_count"] = json!(10);
            cycle["acknowledgement_count"] = json!(if supervisor == TRANSFORM { 10 } else { 0 });
            cycle["elapsed_ns"] = json!(100);
            cycle["interrupted"] = json!(false);
            cycle["worst_residual_ns"] = json!(5);
            cycle["worst_residual_elapsed_ns"] = json!(10);
            cycle["max_residual_ns"] = json!(5);
            cycle["residual_over_five_percent_count"] = json!(4);
            cycle["conservation_failures"] = json!(0);
            cycle["max_conservation_error_ns"] = json!(0);
            for phase in CYCLE_PHASES {
                cycle[format!("{phase}_ns")] = json!(match phase {
                    "read" => 60,
                    "handler" => 20,
                    "residual" => 20,
                    _ => 0,
                });
            }
            cycles.push(cycle);
        }
        summaries.push(summary);
        let root = json!({"name": "supervisor", "identity": identity});
        let row = |path: Value, elapsed: u64| {
            json!({
                "path": path, "calls": 1, "elapsed_ns": elapsed, "entered_ns": elapsed / 2,
                "enters": 2, "max_elapsed_ns": elapsed, "open_calls": 0
            })
        };
        spans.push(row(json!([root]), 1000));
        if supervisor == SOURCE {
            let state = json!({"name": "supervisor_state", "identity": {"state": "Running"}});
            spans.push(row(
                json!([root, state, {"name": "disk_journal_read_next"}]),
                100,
            ));
            spans.push(row(
                json!([root, state, {"name": "disk_journal_append"}]),
                200,
            ));
            spans.push(row(json!([root, state, {"name": "disk_journal_append"}, {"name": "disk_journal_append_record"}]), 150));
            spans.push(row(json!([root, state, {"name": "disk_journal_append"}, {"name": "disk_journal_append_record"}, {"name": "disk_journal_file_write"}]), 75));
        }
    }
    json!({"version": 2, "capture_mode": "deep", "interrupted": false, "open_spans": 0,
        "dropped_spans": 0, "dropped_summaries": 0,
        "collector_callback_ns": 0, "collector_callback_calls": 0,
        "collector_callback_semantics": "summed callback wall time; not additive flow wall time",
        "spans": spans, "supervisor_summaries": summaries, "cycle_summaries": cycles})
}

fn performance_fixture_writers() -> BTreeMap<String, String> {
    PERFORMANCE_SUPERVISORS
        .into_iter()
        .map(|name| (name.to_owned(), format!("writer_{name}")))
        .collect()
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
fn performance_report_accepts_conserved_nested_capture_without_double_counting() -> Result<()> {
    let log = format!(
        "INFO flow=fixture-flow\nDEBUG performance_capture \u{1b}[3mreport\u{1b}[0m={}\n",
        performance_fixture()
    );
    let report = inspect_performance_log(&log, "fixture-flow", &performance_fixture_writers(), 10)?;
    let operations = report["disk_operations"].as_array().unwrap();
    assert_eq!(operations.len(), 2);
    let append = operations
        .iter()
        .find(|row| row["operation"] == "append")
        .unwrap();
    assert_eq!(append["totals"]["elapsed_ns"], 200);
    assert_eq!(append["totals"]["calls"], 1);
    assert_eq!(
        report["phase_rows"][0]["phase_percent"]["direct_dispatch"],
        60.0
    );
    let markdown = performance_markdown(&report)?;
    assert!(markdown.contains("disk_journal_file_write"));
    assert!(markdown.contains("do not sum this table"));
    Ok(())
}

#[test]
fn performance_report_accepts_coarse_cycles_and_weights_outcomes_by_elapsed() -> Result<()> {
    let mut capture = performance_fixture();
    capture["capture_mode"] = json!("coarse");
    capture["spans"] = json!([]);
    let mut error = capture["cycle_summaries"][0].clone();
    error["outcome"] = json!("error");
    error["dispatch_count"] = json!(1);
    error["business_input_count"] = json!(0);
    error["elapsed_ns"] = json!(300);
    for phase in CYCLE_PHASES {
        error[format!("{phase}_ns")] = json!(if phase == "handler" { 300 } else { 0 });
    }
    error["worst_residual_ns"] = json!(0);
    error["worst_residual_elapsed_ns"] = json!(300);
    error["max_residual_ns"] = json!(0);
    error["residual_over_five_percent_count"] = json!(0);
    capture["cycle_summaries"]
        .as_array_mut()
        .unwrap()
        .push(error);
    capture["supervisor_summaries"][0]["pending_dispatch_complete_wins"] = json!(11);
    let report = inspect_performance_log(
        &format!("INFO fixture-flow\nDEBUG performance_capture report={capture}\n"),
        "fixture-flow",
        &performance_fixture_writers(),
        10,
    )?;
    let source = report["cycle_totals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["supervisor"] == SOURCE)
        .unwrap();
    assert_eq!(source["phase_percent"]["read"], 15.0);
    assert_eq!(source["phase_percent"]["handler"], 80.0);
    assert_eq!(source["phase_percent"]["residual"], 5.0);
    assert_eq!(source["dispatch_count"], 11);
    assert_eq!(source["business_input_count"], 10);
    assert_eq!(source["outcomes"]["error"], 1);
    assert_eq!(source["worst_residual_percent"], 50.0);
    assert_eq!(source["residual_over_five_percent_count"], 4);
    assert!(report["disk_operations"].as_array().unwrap().is_empty());
    let markdown = performance_markdown(&report)?;
    assert!(markdown.contains("Fine operation spans were disabled"));
    assert!(markdown.contains("not an established accuracy guarantee"));
    assert!(!markdown.contains("Largest nested operations"));
    Ok(())
}

#[test]
fn performance_report_rejects_incomplete_or_foreign_cycle_evidence() {
    let inspect = |capture: &Value| {
        inspect_performance_log(
            &format!("INFO fixture-flow\nDEBUG performance_capture report={capture}\n"),
            "fixture-flow",
            &performance_fixture_writers(),
            10,
        )
    };
    for (key, value) in [
        ("writer_id", json!("writer_foreign")),
        ("business_input_count", json!(9)),
        ("acknowledgement_count", json!("0")),
        ("publish_ns", json!(1)),
        ("conservation_failures", json!(1)),
        ("max_conservation_error_ns", json!(1)),
        ("worst_residual_elapsed_ns", json!(1)),
        ("residual_over_five_percent_count", json!(0)),
        ("interrupted", json!(true)),
        ("outcome", json!("cancelled")),
    ] {
        let mut capture = performance_fixture();
        capture["cycle_summaries"][0][key] = value;
        assert!(inspect(&capture).is_err(), "accepted malformed cycle {key}");
    }
    let mut capture = performance_fixture();
    capture["cycle_summaries"][0]
        .as_object_mut()
        .unwrap()
        .remove("read_ns");
    assert!(inspect(&capture).is_err());
    capture = performance_fixture();
    capture["cycle_summaries"][1]["acknowledgement_count"] = json!(9);
    assert!(
        inspect(&capture).is_err(),
        "accepted a missing demo acknowledgement"
    );
    capture = performance_fixture();
    capture["cycle_summaries"][1]
        .as_object_mut()
        .unwrap()
        .remove("acknowledgement_count");
    assert!(
        inspect(&capture).is_err(),
        "accepted absent acknowledgement evidence"
    );
    capture = performance_fixture();
    capture["cycle_summaries"].as_array_mut().unwrap().pop();
    assert!(inspect(&capture).is_err());
    capture = performance_fixture();
    let duplicate = capture["cycle_summaries"][0].clone();
    capture["cycle_summaries"]
        .as_array_mut()
        .unwrap()
        .push(duplicate);
    assert!(inspect(&capture).is_err());
    capture = performance_fixture();
    capture["capture_mode"] = json!("coarse");
    assert!(
        inspect(&capture).is_err(),
        "coarse capture retained fine spans"
    );
}

#[test]
fn performance_report_requires_zero_input_cycles_and_accounts_for_control_migration() -> Result<()>
{
    let inspect = |capture: &Value| {
        inspect_performance_log(
            &format!("INFO fixture-flow\nDEBUG performance_capture report={capture}\n"),
            "fixture-flow",
            &performance_fixture_writers(),
            10,
        )
    };
    let mut capture = performance_fixture();
    capture["capture_mode"] = json!("coarse");
    capture["spans"] = json!([]);
    let mut runner = capture["supervisor_summaries"][0].clone();
    runner["state"] = json!("Draining");
    runner["pending_dispatch_complete_wins"] = json!(1);
    capture["supervisor_summaries"]
        .as_array_mut()
        .unwrap()
        .push(runner);
    let mut drain = capture["cycle_summaries"][0].clone();
    drain["state"] = json!("Draining");
    drain["dispatch_count"] = json!(1);
    drain["business_input_count"] = json!(0);
    for phase in CYCLE_PHASES {
        drain[format!("{phase}_ns")] = json!(if phase == "control" { 100 } else { 0 });
    }
    drain["worst_residual_ns"] = json!(0);
    drain["worst_residual_elapsed_ns"] = json!(100);
    drain["max_residual_ns"] = json!(0);
    drain["residual_over_five_percent_count"] = json!(0);
    capture["cycle_summaries"]
        .as_array_mut()
        .unwrap()
        .push(drain);
    inspect(&capture)?;

    let mut missing = capture.clone();
    missing["cycle_summaries"].as_array_mut().unwrap().pop();
    assert!(
        inspect(&missing).is_err(),
        "accepted a missing zero-input cycle bucket"
    );

    // The retained cycle keeps its start state while its completion can be
    // observed by the runner after a control transition to Draining.
    capture["supervisor_summaries"][0]["pending_dispatch_complete_wins"] = json!(9);
    capture["supervisor_summaries"][0]["pending_dispatch_control_wins"] = json!(1);
    capture["supervisor_summaries"]
        .as_array_mut()
        .unwrap()
        .last_mut()
        .unwrap()["pending_dispatch_complete_wins"] = json!(2);
    let report = inspect(&capture)?;
    assert_eq!(report["cycle_completeness"][0]["dispatch_count"], 11);
    assert_eq!(report["cycle_completeness"][0]["runner_complete_wins"], 11);

    let mut acquisition = capture["supervisor_summaries"][0].clone();
    acquisition["state"] = json!("AcquiringInput");
    acquisition["pending_dispatch_complete_wins"] = json!(1);
    capture["supervisor_summaries"]
        .as_array_mut()
        .unwrap()
        .push(acquisition);
    assert!(
        inspect(&capture).is_err(),
        "accepted ambiguous unmeasured-state migration"
    );
    Ok(())
}

#[test]
fn performance_report_attributes_background_metrics_reads_outside_runner_phases() -> Result<()> {
    let inspect = |capture: &Value| {
        inspect_performance_log(
            &format!("INFO flow=fixture-flow\nDEBUG performance_capture report={capture}\n"),
            "fixture-flow",
            &performance_fixture_writers(),
            10,
        )
    };
    let mut capture = performance_fixture();
    let baseline_phases = inspect(&capture)?["phase_rows"].clone();
    let metrics = obzenflow_core::event::vocabulary::supervisor::METRICS_NAME;
    let row = |path: Value, elapsed: u64| {
        json!({
            "path": path, "calls": 1, "elapsed_ns": elapsed, "entered_ns": elapsed / 2,
            "enters": 2, "max_elapsed_ns": elapsed, "open_calls": 0
        })
    };
    for (journal_kind, elapsed) in [("data", 400), ("error", 250)] {
        let root = json!({
            "name": "metrics_tail_reader",
            "identity": {
                "supervisor": metrics, "supervisor_kind": "MetricsAggregator",
                "writer_id": format!("writer_{metrics}"),
                "supervision_mode": "self_supervised", "journal_kind": journal_kind
            }
        });
        let spans = capture["spans"].as_array_mut().unwrap();
        spans.push(row(json!([root]), 1000));
        spans.push(row(
            json!([root, {"name": "disk_journal_metrics_tail"}]),
            elapsed,
        ));
        spans.push(row(json!([root, {"name": "disk_journal_metrics_tail"}, {"name": "disk_journal_observation_file_read"}]), elapsed / 2));
    }
    capture["collector_callback_ns"] = json!(2500);
    capture["collector_callback_calls"] = json!(20);
    capture["collector_callback_semantics"] = json!("summed wall time, not additive flow time");
    let report = inspect(&capture)?;
    assert_eq!(report["phase_rows"], baseline_phases);
    let read = report["disk_operations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["supervisor"] == metrics && row["operation"] == "read")
        .context("missing attributed metrics read")?;
    assert_eq!(read["totals"]["calls"], 2);
    assert_eq!(read["totals"]["elapsed_ns"], 650);
    for row in report["nested_operations_by_elapsed"].as_array().unwrap() {
        if row["supervisor"] == metrics {
            assert_eq!(row["state"], "outside_state");
        }
    }
    let markdown = performance_markdown(&report)?;
    assert!(markdown.contains("20 callbacks"));
    assert!(markdown.contains("neither CPU time nor additive flow wall time"));
    assert!(markdown.contains("lifetimes are excluded from the runner phase ledger"));

    let mut altered = capture.clone();
    altered["spans"].as_array_mut().unwrap().last_mut().unwrap()["path"][0]["identity"]
        ["writer_id"] = json!("writer_another_run");
    assert!(inspect(&altered).is_err());
    altered = capture.clone();
    altered["spans"].as_array_mut().unwrap().last_mut().unwrap()["path"]
        .as_array_mut()
        .unwrap()
        .insert(
            1,
            json!({"name": "supervisor_state", "identity": {"state": "Running"}}),
        );
    assert!(inspect(&altered).is_err());
    altered = capture.clone();
    altered["collector_callback_calls"] = json!("20");
    assert!(inspect(&altered).is_err());
    capture
        .as_object_mut()
        .unwrap()
        .remove("collector_callback_ns");
    assert!(inspect(&capture).is_err());
    Ok(())
}

#[test]
fn performance_report_rejects_incomplete_nonconserved_and_cross_archive_capture() {
    let inspect = |capture: &Value, flow: &str| {
        inspect_performance_log(
            &format!("INFO flow=fixture-flow\nDEBUG performance_capture report={capture}\n"),
            flow,
            &performance_fixture_writers(),
            10,
        )
    };
    for key in ["open_spans", "dropped_spans", "dropped_summaries"] {
        let mut capture = performance_fixture();
        capture[key] = json!(1);
        assert!(inspect(&capture, "fixture-flow").is_err(), "accepted {key}");
    }
    let mut capture = performance_fixture();
    capture["interrupted"] = json!(true);
    assert!(inspect(&capture, "fixture-flow").is_err());
    let mut capture = performance_fixture();
    capture["supervisor_summaries"][0]["residual_ns"] = json!(41);
    assert!(inspect(&capture, "fixture-flow").is_err());
    let mut capture = performance_fixture();
    capture["supervisor_summaries"]
        .as_array_mut()
        .unwrap()
        .pop();
    assert!(inspect(&capture, "fixture-flow").is_err());
    assert!(inspect(&performance_fixture(), "another-flow").is_err());
    let duplicate = format!(
        "INFO fixture-flow\nperformance_capture report={}\nperformance_capture report={}\n",
        performance_fixture(),
        performance_fixture()
    );
    assert!(inspect_performance_log(
        &duplicate,
        "fixture-flow",
        &performance_fixture_writers(),
        10
    )
    .is_err());
    // A mixed log can contain the expected flow ID while its one internally
    // consistent capture belongs to another run. Both summary and span writer
    // substitutions must still fail against the independent manifest oracle.
    let mut foreign_capture = performance_fixture();
    for summary in foreign_capture["supervisor_summaries"]
        .as_array_mut()
        .unwrap()
    {
        if summary["supervisor"] == SOURCE {
            summary["writer_id"] = json!("writer_another_run");
        }
    }
    for row in foreign_capture["spans"].as_array_mut().unwrap() {
        for frame in row["path"].as_array_mut().unwrap() {
            if frame["name"] == "supervisor" && frame["identity"]["supervisor"] == SOURCE {
                frame["identity"]["writer_id"] = json!("writer_another_run");
            }
        }
    }
    assert!(inspect(&foreign_capture, "fixture-flow").is_err());
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
