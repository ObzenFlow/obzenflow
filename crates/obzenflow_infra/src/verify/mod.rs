// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay output verification (FLOWIP-095j): a read-only comparison of two
//! run directories over the verification projection, with a human verdict, a
//! versioned machine-readable report, and a CI exit-code contract.
//!
//! All verification logic lives in this module; there is no separate crate.
//! The comparator submodules (`projection`, `certification`, `walker`,
//! `lineage`, `report`, `verdict`) are I/O-free over `obzenflow_core` types;
//! `source` and `fs` touch disk, reusing infra's framed journal format and
//! archive status derivation. Both product surfaces (the `--verify` flag on
//! `FlowApplication` and the `obzenflow verify` subcommand) call
//! [`verify_run_dirs`], so both certify identically from manifest data.

pub mod certification;
pub mod error;
pub mod fs;
pub mod lineage;
pub mod projection;
pub mod report;
pub mod source;
pub mod validate;
pub mod verdict;
pub mod walker;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub use error::{RefusalReason, VerifyError};
pub use lineage::IdentityMode;
pub use report::{VerificationReport, REPORT_VERSION};
pub use source::{DiskRunSource, RunSource};
pub use verdict::{Verdict, VerifyOutcome, MATCHED_LINE};
pub use walker::WalkMode;

use certification::StageCertification;
use report::{delta_side, CountsByType, DivergenceReport, RunSummary, StageReport, Totals};
use source::SourceOpenError;
use walker::{
    tally_journal, walk_journal, walk_journal_multiset, StageWalkOptions, StageWalkOutput,
};

#[derive(Debug, Clone)]
pub struct VerifyOptions {
    /// Cap on recorded divergences per journal; counting continues past it.
    pub max_divergences: usize,
    /// FLOWIP-095l: max distinct row-contents a content (order-insensitive) walk
    /// buffers before falling back to a bounded additive-digest verdict. Bounds
    /// content-certification memory; exact row-level reporting is kept under it.
    pub content_pinpoint_cap: usize,
    /// Report destination; defaults to
    /// `<candidate-run-dir>/verification/<baseline-flow-id>.json`.
    pub report_path: Option<PathBuf>,
    /// Whether to persist the report (tests disable this).
    pub write_report: bool,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        Self {
            max_divergences: 5,
            content_pinpoint_cap: 100_000,
            report_path: None,
            write_report: true,
        }
    }
}

/// Verify a candidate run directory against a baseline run directory.
pub fn verify_run_dirs(
    baseline_dir: &Path,
    candidate_dir: &Path,
    options: &VerifyOptions,
) -> Result<VerifyOutcome, VerifyError> {
    let baseline = match DiskRunSource::open(baseline_dir) {
        Ok(source) => source,
        Err(SourceOpenError::Refused(reason)) => return Ok(VerifyOutcome::Refused(*reason)),
        Err(SourceOpenError::Failed(err)) => return Err(err),
    };
    let candidate = match DiskRunSource::open(candidate_dir) {
        Ok(source) => source,
        Err(SourceOpenError::Refused(reason)) => return Ok(VerifyOutcome::Refused(*reason)),
        Err(SourceOpenError::Failed(err)) => return Err(err),
    };
    verify_sources(&baseline, &candidate, options)
}

/// Verify over any [`RunSource`] pair (the seam in-memory tests drive).
pub fn verify_sources(
    baseline: &dyn RunSource,
    candidate: &dyn RunSource,
    options: &VerifyOptions,
) -> Result<VerifyOutcome, VerifyError> {
    let plan = match validate::validate(baseline, candidate) {
        Ok(plan) => plan,
        Err(reason) => return Ok(VerifyOutcome::Refused(*reason)),
    };

    let mut stages: BTreeMap<String, StageReport> = BTreeMap::new();
    let mut totals = Totals::default();

    for (stage_key, certification) in &plan.certification {
        let b_stage = &baseline.manifest().stages[stage_key];
        let c_stage = &candidate.manifest().stages[stage_key];
        let logic_version_changed = plan.logic_version_changed[stage_key];
        let classification = if logic_version_changed {
            "changed-code"
        } else {
            "unexpected"
        };

        let opts = |journal_label: &'static str| StageWalkOptions {
            mode: plan.mode,
            identity: plan.identity,
            max_divergences: options.max_divergences,
            stop_at: None,
            journal_label,
            classification,
            content_pinpoint_cap: options.content_pinpoint_cap,
        };

        let stage_report = match certification {
            StageCertification::Positional => {
                let data = walk_journal(
                    baseline.stage_rows(&b_stage.data_journal_file)?,
                    candidate.stage_rows(&c_stage.data_journal_file)?,
                    &opts("data"),
                )?;
                let errors = walk_journal(
                    baseline.stage_rows(&b_stage.error_journal_file)?,
                    candidate.stage_rows(&c_stage.error_journal_file)?,
                    &opts("error"),
                )?;
                match assemble_certified_report(
                    stage_key,
                    data,
                    errors,
                    plan.mode,
                    classification,
                    logic_version_changed,
                    options.max_divergences,
                    "positional",
                ) {
                    Ok(report) => report,
                    Err(reason) => return Ok(VerifyOutcome::Refused(reason)),
                }
            }
            StageCertification::Content => {
                // Order-insensitive region: compare content as a multiset, so a
                // legitimate reordering matches while a content difference (the
                // count-only advisory could not see) still diverges.
                let data = walk_journal_multiset(
                    baseline.stage_rows(&b_stage.data_journal_file)?,
                    candidate.stage_rows(&c_stage.data_journal_file)?,
                    &opts("data"),
                )?;
                let errors = walk_journal_multiset(
                    baseline.stage_rows(&b_stage.error_journal_file)?,
                    candidate.stage_rows(&c_stage.error_journal_file)?,
                    &opts("error"),
                )?;
                match assemble_certified_report(
                    stage_key,
                    data,
                    errors,
                    plan.mode,
                    classification,
                    logic_version_changed,
                    options.max_divergences,
                    "content",
                ) {
                    Ok(report) => report,
                    Err(reason) => return Ok(VerifyOutcome::Refused(reason)),
                }
            }
            StageCertification::NotCertifiable { blocking } => {
                let b_data = tally_journal(baseline.stage_rows(&b_stage.data_journal_file)?)?;
                let b_err = tally_journal(baseline.stage_rows(&b_stage.error_journal_file)?)?;
                let c_data = tally_journal(candidate.stage_rows(&c_stage.data_journal_file)?)?;
                let c_err = tally_journal(candidate.stage_rows(&c_stage.error_journal_file)?)?;

                let positional_baseline = b_data.positional + b_err.positional;
                let positional_candidate = c_data.positional + c_err.positional;

                // Vacuous certification (FLOWIP-095j section 2): an
                // uncertified stage whose projection has no positionally
                // compared rows in either run has nothing order-dependent to
                // certify. Decided from observed rows, never stage type.
                let vacuous = positional_baseline == 0 && positional_candidate == 0;

                let mut baseline_counts = b_data.counts_by_type;
                merge_counts(&mut baseline_counts, b_err.counts_by_type);
                let mut candidate_counts = c_data.counts_by_type;
                merge_counts(&mut candidate_counts, c_err.counts_by_type);

                StageReport {
                    status: if vacuous {
                        "matched"
                    } else {
                        "not_order_certified"
                    }
                    .to_string(),
                    vacuous,
                    order_certified: false,
                    certification: "none".to_string(),
                    blocking: blocking.clone(),
                    logic_version_changed,
                    positional_rows_baseline: positional_baseline,
                    positional_rows_candidate: positional_candidate,
                    divergence_count: 0,
                    divergences: Vec::new(),
                    informational: if vacuous {
                        Vec::new()
                    } else {
                        vec![
                            "delivery order is unconstrained at or above this stage; per-type row counts are an order-insensitive advisory, never a certificate".to_string(),
                        ]
                    },
                    counts_by_type: if vacuous {
                        None
                    } else {
                        Some(CountsByType {
                            baseline: baseline_counts,
                            candidate: candidate_counts,
                        })
                    },
                }
            }
        };

        totals.stages += 1;
        totals.divergences += stage_report.divergence_count;
        match stage_report.status.as_str() {
            "diverged" => totals.diverged += 1,
            "not_order_certified" => totals.not_order_certified += 1,
            _ => {
                totals.matched += 1;
                if stage_report.vacuous {
                    totals.vacuous += 1;
                }
            }
        }
        stages.insert(stage_key.clone(), stage_report);
    }

    let verdict = if totals.diverged > 0 {
        Verdict::Diverged
    } else if totals.not_order_certified > 0 {
        Verdict::UncertifiedRemainder
    } else {
        Verdict::CertifiedMatch
    };

    let report = VerificationReport {
        report_version: REPORT_VERSION,
        verdict: verdict.as_str().to_string(),
        exit_code: verdict.exit_code(),
        mode: plan.mode.as_str().to_string(),
        identity_mode: plan.identity.as_str().to_string(),
        baseline: run_summary(baseline),
        candidate: run_summary(candidate),
        totals,
        stages,
    };

    let report_path = if options.write_report {
        let path = options.report_path.clone().unwrap_or_else(|| {
            fs::default_report_path(candidate.run_dir(), &baseline.manifest().flow_id)
        });
        fs::write_report(&path, &report)?;
        Some(path)
    } else {
        None
    };

    Ok(VerifyOutcome::Completed {
        report: Box::new(report),
        report_path,
    })
}

/// Human rendering of an outcome. [`MATCHED_LINE`] prints only on a fully
/// certified match.
pub fn render_verdict(outcome: &VerifyOutcome) -> String {
    match outcome {
        VerifyOutcome::Refused(reason) => format!("verification refused: {reason}"),
        VerifyOutcome::Completed {
            report,
            report_path,
        } => {
            let mut lines = Vec::new();
            match outcome.verdict() {
                Verdict::CertifiedMatch => lines.push(MATCHED_LINE.to_string()),
                Verdict::Diverged => {
                    lines.push(format!(
                        "output diverged from the baseline: {} difference(s) across {} stage(s)",
                        report.totals.divergences, report.totals.diverged
                    ));
                    for (stage, stage_report) in &report.stages {
                        if let Some(first) = stage_report.divergences.first() {
                            lines.push(format!(
                                "  {stage} [{}] position {}: {} differs ({})",
                                first.journal, first.position, first.field, first.classification
                            ));
                        }
                    }
                }
                Verdict::UncertifiedRemainder => {
                    let uncertified: Vec<&str> = report
                        .stages
                        .iter()
                        .filter(|(_, s)| s.status == "not_order_certified")
                        .map(|(k, _)| k.as_str())
                        .collect();
                    lines.push(format!(
                        "certified region matched; {} stage(s) not order-certified: {}",
                        uncertified.len(),
                        uncertified.join(", ")
                    ));
                    lines.push(
                        "delivery order is unconstrained at those stages; see the report's per-type row counts".to_string(),
                    );
                }
                Verdict::Refused => unreachable!("refusals render above"),
            }
            if let Some(path) = report_path {
                lines.push(format!("report: {}", path.display()));
            }
            lines.join("\n")
        }
    }
}

fn run_summary(source: &dyn RunSource) -> RunSummary {
    RunSummary {
        run_dir: source.run_dir().to_path_buf(),
        flow_id: source.manifest().flow_id.clone(),
        flow_name: source.manifest().flow_name.clone(),
        status: format!("{:?}", source.status()),
        manifest_version: source.manifest().manifest_version.clone(),
    }
}

fn multiset(values: &[String]) -> BTreeMap<&String, usize> {
    let mut set = BTreeMap::new();
    for value in values {
        *set.entry(value).or_insert(0) += 1;
    }
    set
}

fn sorted_joined(values: &[String]) -> String {
    let mut sorted = values.to_vec();
    sorted.sort();
    sorted.join("\n")
}

fn merge_counts(into: &mut BTreeMap<String, u64>, from: BTreeMap<String, u64>) {
    for (key, count) in from {
        *into.entry(key).or_insert(0) += count;
    }
}

/// Shared post-walk assembly for a certified stage, positional or content: refuse
/// on a lineage conflict, fold in the completion-evidence comparison, and build
/// the `StageReport`. Both certificates share every step but the row comparator,
/// so `data`/`errors` are already-walked outputs and `certification` is
/// `"positional"` or `"content"`.
#[allow(clippy::too_many_arguments)]
fn assemble_certified_report(
    stage_key: &str,
    data: StageWalkOutput,
    errors: StageWalkOutput,
    mode: WalkMode,
    classification: &str,
    logic_version_changed: bool,
    max_divergences: usize,
    certification: &str,
) -> Result<StageReport, RefusalReason> {
    for walk in [&data, &errors] {
        if let Some((baseline_ns, candidate_ns)) = &walk.lineage_conflict {
            return Err(RefusalReason::LineageMismatch {
                stage: stage_key.to_string(),
                baseline_namespace: baseline_ns.clone(),
                candidate_namespace: candidate_ns.clone(),
            });
        }
    }

    let mut divergences: Vec<DivergenceReport> = data.divergences;
    divergences.extend(errors.divergences);
    let mut divergence_count = data.divergence_count + errors.divergence_count;
    let mut informational = Vec::new();

    // Completion evidence: compared only when both runs reached completion;
    // informational against a cancelled baseline. Identical for both certificates.
    match mode {
        WalkMode::WholeRun => {
            if multiset(&data.eof_baseline) != multiset(&data.eof_candidate) {
                divergence_count += 1;
                if divergences.len() < max_divergences {
                    divergences.push(DivergenceReport {
                        journal: "data".to_string(),
                        position: data.positional_baseline,
                        field: "eof_evidence".to_string(),
                        baseline: delta_side(&sorted_joined(&data.eof_baseline)),
                        candidate: delta_side(&sorted_joined(&data.eof_candidate)),
                        classification: classification.to_string(),
                    });
                }
            }
        }
        WalkMode::BaselinePrefixOfCandidate => {
            if !data.eof_baseline.is_empty() || !data.eof_candidate.is_empty() {
                informational.push(format!(
                    "completion evidence not compared against a cancelled baseline ({} baseline rows, {} candidate rows)",
                    data.eof_baseline.len(),
                    data.eof_candidate.len()
                ));
            }
            let surplus = data.candidate_surplus + errors.candidate_surplus;
            if surplus > 0 {
                informational.push(format!(
                    "candidate carries {surplus} rows beyond the cancelled baseline prefix"
                ));
            }
        }
    }

    let diverged = divergence_count > 0;
    Ok(StageReport {
        status: if diverged { "diverged" } else { "matched" }.to_string(),
        vacuous: false,
        order_certified: true,
        certification: certification.to_string(),
        blocking: Vec::new(),
        logic_version_changed,
        positional_rows_baseline: data.positional_baseline + errors.positional_baseline,
        positional_rows_candidate: data.positional_candidate + errors.positional_candidate,
        divergence_count,
        divergences,
        informational,
        counts_by_type: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::effect_payload::{
        EffectCursor, EffectDescriptor, EffectProvenance,
    };
    use obzenflow_core::event::ChainEvent;
    use obzenflow_core::journal::run_manifest::{
        OrderDecision, RunManifest, RunManifestReplayConfig, RunManifestStage,
    };
    use obzenflow_core::journal::ArchiveStatus;
    use obzenflow_core::{StageId, WriterId};
    use serde_json::json;
    use std::collections::HashMap;
    use OrderDecision::{CycleNonDeterministic, OrderInsensitive, Ordered};

    struct MemorySource {
        dir: PathBuf,
        manifest: RunManifest,
        status: ArchiveStatus,
        journals: HashMap<String, Vec<ChainEvent>>,
    }

    impl RunSource for MemorySource {
        fn run_dir(&self) -> &Path {
            &self.dir
        }
        fn manifest(&self) -> &RunManifest {
            &self.manifest
        }
        fn status(&self) -> ArchiveStatus {
            self.status
        }
        fn stage_rows<'a>(
            &'a self,
            journal_file: &str,
        ) -> Result<Box<dyn Iterator<Item = Result<ChainEvent, VerifyError>> + 'a>, VerifyError>
        {
            let rows = self.journals.get(journal_file).cloned().unwrap_or_default();
            Ok(Box::new(rows.into_iter().map(Ok)))
        }
    }

    fn manifest(
        stages: &[(&str, &[&str], OrderDecision)],
        replay: Option<RunManifestReplayConfig>,
        flow_id: &str,
    ) -> RunManifest {
        let mut map = HashMap::new();
        for (key, inbound, order_decision) in stages {
            map.insert(
                key.to_string(),
                RunManifestStage {
                    dsl_var: key.to_string(),
                    stage_type: StageType::Transform,
                    stage_id: format!("stage_{key}"),
                    stage_logic_version: "1".to_string(),
                    data_journal_file: format!("{key}.log"),
                    error_journal_file: format!("{key}_error.log"),
                    inbound: inbound.iter().map(|s| s.to_string()).collect(),
                    order_decision: order_decision.clone(),
                },
            );
        }
        RunManifest {
            manifest_version: "2.0".to_string(),
            journal_format_version: 1,
            obzenflow_version: "0.1.2".to_string(),
            flow_id: flow_id.to_string(),
            flow_name: "test_flow".to_string(),
            created_at: obzenflow_core::chrono::Utc::now(),
            replay,
            stages: map,
            system_journal_file: "system.log".to_string(),
        }
    }

    fn replay_block() -> Option<RunManifestReplayConfig> {
        Some(RunManifestReplayConfig {
            replay_from: "/archives/gen0".to_string(),
            allow_incomplete_archive: false,
        })
    }

    fn writer() -> WriterId {
        WriterId::from(StageId::new())
    }

    fn data(event_type: &str, payload: serde_json::Value) -> ChainEvent {
        ChainEventFactory::data_event(writer(), event_type, payload)
    }

    fn effect_row(namespace: &str, payload: serde_json::Value) -> ChainEvent {
        let provenance = EffectProvenance {
            cursor: EffectCursor::new(namespace, "stage", 0, 0),
            descriptor_hash: "hash".into(),
            descriptor: EffectDescriptor::new("fx", "fx", 1, "1", "input"),
            outcome_fact_ordinal: None,
            outcome_fact_count: None,
            group_id: None,
            fact_owner: Default::default(),
            origin: None,
        };
        data("payment.authorized", payload).with_effect_provenance(provenance)
    }

    fn source(
        manifest: RunManifest,
        status: ArchiveStatus,
        journals: &[(&str, Vec<ChainEvent>)],
        dir: &str,
    ) -> MemorySource {
        MemorySource {
            dir: PathBuf::from(dir),
            manifest,
            status,
            journals: journals
                .iter()
                .map(|(file, rows)| (file.to_string(), rows.clone()))
                .collect(),
        }
    }

    fn no_write() -> VerifyOptions {
        VerifyOptions {
            write_report: false,
            ..Default::default()
        }
    }

    fn topology() -> Vec<(&'static str, &'static [&'static str], OrderDecision)> {
        vec![("a", &[], Ordered), ("b", &["a"], Ordered)]
    }

    #[test]
    fn certified_equal_runs_print_the_headline() {
        let rows = vec![data("t", json!({"n": 1}))];
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Completed,
            &[("a.log", rows.clone()), ("b.log", rows.clone())],
            "/runs/b",
        );
        let candidate = source(
            manifest(&topology(), replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("a.log", rows.clone()), ("b.log", rows)],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 0);
        assert!(render_verdict(&outcome).starts_with(MATCHED_LINE));
    }

    #[test]
    fn certified_divergence_exits_one_and_names_the_stage() {
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Completed,
            &[("a.log", vec![data("t", json!({"n": 1}))])],
            "/runs/b",
        );
        let candidate = source(
            manifest(&topology(), replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("a.log", vec![data("t", json!({"n": 2}))])],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 1);
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.stages["a"].status, "diverged");
        assert_eq!(report.stages["a"].divergences[0].field, "payload");
    }

    #[test]
    fn order_insensitive_fan_in_with_equal_content_certifies() {
        let stages: Vec<(&str, &[&str], OrderDecision)> = vec![
            ("x", &[], Ordered),
            ("y", &[], Ordered),
            ("merge", &["x", "y"], OrderInsensitive),
        ];
        // Same multiset, different delivery order across the two runs.
        let baseline_rows = vec![data("t", json!({"n": 1})), data("t", json!({"n": 2}))];
        let candidate_rows = vec![data("t", json!({"n": 2})), data("t", json!({"n": 1}))];
        let baseline = source(
            manifest(&stages, None, "flow_b"),
            ArchiveStatus::Completed,
            &[("merge.log", baseline_rows)],
            "/runs/b",
        );
        let candidate = source(
            manifest(&stages, replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("merge.log", candidate_rows)],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(
            outcome.exit_code(),
            0,
            "reordered but equal content certifies"
        );
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.stages["merge"].status, "matched");
        assert_eq!(report.stages["merge"].certification, "content");
        assert!(report.stages["merge"].order_certified);
        assert!(render_verdict(&outcome).contains(MATCHED_LINE));
    }

    #[test]
    fn order_insensitive_fan_in_with_content_divergence_exits_one() {
        // The count-only advisory cannot see this: one row per run, same type,
        // different payload (the 316-vs-999 shape).
        let stages: Vec<(&str, &[&str], OrderDecision)> = vec![
            ("x", &[], Ordered),
            ("y", &[], Ordered),
            ("merge", &["x", "y"], OrderInsensitive),
        ];
        let baseline = source(
            manifest(&stages, None, "flow_b"),
            ArchiveStatus::Completed,
            &[("merge.log", vec![data("sum", json!({"total": 316}))])],
            "/runs/b",
        );
        let candidate = source(
            manifest(&stages, replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("merge.log", vec![data("sum", json!({"total": 999}))])],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(
            outcome.exit_code(),
            1,
            "content divergence in an order-insensitive region diverges"
        );
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.stages["merge"].status, "diverged");
        assert!(report.stages["merge"]
            .divergences
            .iter()
            .any(|d| d.field == "multiset_count"));
    }

    #[test]
    fn cycle_stage_with_rows_exits_two_with_counts() {
        let stages: Vec<(&str, &[&str], OrderDecision)> = vec![
            ("x", &[], Ordered),
            ("y", &[], Ordered),
            ("merge", &["x", "y"], CycleNonDeterministic),
        ];
        let rows = vec![data("t", json!({"n": 1}))];
        let baseline = source(
            manifest(&stages, None, "flow_b"),
            ArchiveStatus::Completed,
            &[("merge.log", rows.clone())],
            "/runs/b",
        );
        let candidate = source(
            manifest(&stages, replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("merge.log", rows)],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 2);
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.stages["merge"].status, "not_order_certified");
        assert_eq!(report.stages["merge"].certification, "none");
        assert!(report.stages["merge"].counts_by_type.is_some());
        assert!(!render_verdict(&outcome).contains(MATCHED_LINE));
    }

    #[test]
    fn order_insensitive_over_cap_certifies_and_still_catches_divergence() {
        // Bounded content certification (FLOWIP-095l): with the pinpoint cap set
        // below the stage's distinct-content count, the verdict falls back to the
        // additive digest — which still certifies a reordered-equal match and
        // still catches a content difference, in bounded memory.
        let stages: Vec<(&str, &[&str], OrderDecision)> = vec![
            ("x", &[], Ordered),
            ("y", &[], Ordered),
            ("merge", &["x", "y"], OrderInsensitive),
        ];
        let bounded = VerifyOptions {
            content_pinpoint_cap: 1,
            ..no_write()
        };

        // Reordered-equal content over the cap -> certified match, exit 0.
        let baseline = source(
            manifest(&stages, None, "flow_b"),
            ArchiveStatus::Completed,
            &[(
                "merge.log",
                vec![data("t", json!({"n": 1})), data("t", json!({"n": 2}))],
            )],
            "/runs/b",
        );
        let candidate = source(
            manifest(&stages, replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[(
                "merge.log",
                vec![data("t", json!({"n": 2})), data("t", json!({"n": 1}))],
            )],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &bounded).unwrap();
        assert_eq!(
            outcome.exit_code(),
            0,
            "the bounded digest path certifies a reordered-equal match"
        );
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.stages["merge"].certification, "content");

        // A content difference over the cap still diverges, exit 1.
        let candidate2 = source(
            manifest(&stages, replay_block(), "flow_c2"),
            ArchiveStatus::Completed,
            &[(
                "merge.log",
                vec![data("t", json!({"n": 1})), data("t", json!({"n": 3}))],
            )],
            "/runs/c2",
        );
        let outcome2 = verify_sources(&baseline, &candidate2, &bounded).unwrap();
        assert_eq!(
            outcome2.exit_code(),
            1,
            "the bounded digest path still catches a content difference"
        );
    }

    #[test]
    fn uncertified_stage_with_no_positional_rows_is_vacuously_certified() {
        let stages: Vec<(&str, &[&str], OrderDecision)> = vec![
            ("x", &[], Ordered),
            ("y", &[], Ordered),
            ("sink", &["x", "y"], CycleNonDeterministic),
        ];
        // The sink journal carries only excluded rows (none projected).
        let baseline = source(
            manifest(&stages, None, "flow_b"),
            ArchiveStatus::Completed,
            &[],
            "/runs/b",
        );
        let candidate = source(
            manifest(&stages, replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 0, "vacuous stages do not block exit 0");
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert!(report.stages["sink"].vacuous);
        assert_eq!(report.stages["sink"].status, "matched");
    }

    #[test]
    fn incomplete_archive_replay_candidate_is_refused() {
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Completed,
            &[],
            "/runs/b",
        );
        let candidate = source(
            manifest(
                &topology(),
                Some(RunManifestReplayConfig {
                    replay_from: "/archives/gen0".to_string(),
                    allow_incomplete_archive: true,
                }),
                "flow_c",
            ),
            ArchiveStatus::Completed,
            &[],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 3);
        assert!(matches!(
            outcome,
            VerifyOutcome::Refused(RefusalReason::IncompleteArchiveReplayCandidate)
        ));
    }

    #[test]
    fn lineage_namespace_mismatch_is_a_refusal_not_a_divergence() {
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Completed,
            &[("a.log", vec![effect_row("flow_gen0", json!({}))])],
            "/runs/b",
        );
        let candidate = source(
            manifest(&topology(), replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("a.log", vec![effect_row("flow_other", json!({}))])],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert!(matches!(
            outcome,
            VerifyOutcome::Refused(RefusalReason::LineageMismatch { .. })
        ));
    }

    #[test]
    fn shared_namespace_asserts_identity_and_matches() {
        let row = effect_row("flow_gen0", json!({"amount": 5}));
        let baseline = source(
            manifest(&topology(), None, "flow_gen0"),
            ArchiveStatus::Completed,
            &[("a.log", vec![row.clone()])],
            "/runs/b",
        );
        let candidate = source(
            manifest(&topology(), replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("a.log", vec![row])],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 0);
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.identity_mode, "lineage");
    }

    #[test]
    fn cancelled_baseline_uses_prefix_mode_with_informational_surplus() {
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Cancelled,
            &[("a.log", vec![data("t", json!({"n": 1}))])],
            "/runs/b",
        );
        let candidate = source(
            manifest(&topology(), replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[(
                "a.log",
                vec![
                    data("t", json!({"n": 1})),
                    data("t", json!({"n": 2})),
                    ChainEventFactory::eof_event(writer(), true),
                ],
            )],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 0);
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(report.mode, "baseline_prefix_of_candidate");
        assert!(!report.stages["a"].informational.is_empty());
    }

    #[test]
    fn whole_run_eof_evidence_mismatch_is_divergence() {
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Completed,
            &[(
                "a.log",
                vec![
                    data("t", json!({"n": 1})),
                    ChainEventFactory::eof_event(writer(), true),
                ],
            )],
            "/runs/b",
        );
        let candidate = source(
            manifest(&topology(), replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[("a.log", vec![data("t", json!({"n": 1}))])],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        assert_eq!(outcome.exit_code(), 1);
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert!(report.stages["a"]
            .divergences
            .iter()
            .any(|d| d.field == "eof_evidence"));
    }

    #[test]
    fn stage_set_mismatch_refuses_with_a_delta() {
        let baseline = source(
            manifest(&topology(), None, "flow_b"),
            ArchiveStatus::Completed,
            &[],
            "/runs/b",
        );
        let extended: Vec<(&str, &[&str], OrderDecision)> = vec![
            ("a", &[], Ordered),
            ("b", &["a"], Ordered),
            ("c", &["b"], Ordered),
        ];
        let candidate = source(
            manifest(&extended, replay_block(), "flow_c"),
            ArchiveStatus::Completed,
            &[],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        let VerifyOutcome::Refused(RefusalReason::StageSetMismatch { added, removed }) = outcome
        else {
            panic!("expected stage-set refusal");
        };
        assert_eq!(added, vec!["c".to_string()]);
        assert!(removed.is_empty());
    }

    #[test]
    fn changed_logic_version_classifies_divergences_as_changed_code() {
        let mut b_manifest = manifest(&topology(), None, "flow_b");
        b_manifest.stages.get_mut("a").unwrap().stage_logic_version = "1".to_string();
        let mut c_manifest = manifest(&topology(), replay_block(), "flow_c");
        c_manifest.stages.get_mut("a").unwrap().stage_logic_version = "2".to_string();

        let baseline = source(
            b_manifest,
            ArchiveStatus::Completed,
            &[("a.log", vec![data("t", json!({"n": 1}))])],
            "/runs/b",
        );
        let candidate = source(
            c_manifest,
            ArchiveStatus::Completed,
            &[("a.log", vec![data("t", json!({"n": 2}))])],
            "/runs/c",
        );
        let outcome = verify_sources(&baseline, &candidate, &no_write()).unwrap();
        let VerifyOutcome::Completed { report, .. } = &outcome else {
            panic!()
        };
        assert_eq!(
            report.stages["a"].divergences[0].classification,
            "changed-code"
        );
    }
}
