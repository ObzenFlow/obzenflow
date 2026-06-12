// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The streaming per-journal walk (FLOWIP-095j): a deliberately boring
//! sequence comparison over the verification projection. The first divergence
//! is recorded with its position and a field-level delta, then a bounded
//! number of subsequent divergences, then counts only. Payload comparison
//! streams, so memory stays bounded by row size rather than run size.
//!
//! Two modes: `WholeRun` compares both projections to their ends.
//! `BaselinePrefixOfCandidate` applies when the baseline archive is
//! `Cancelled` (a killed run may not have processed every recorded source row
//! downstream and never wrote completion evidence): the baseline must be a
//! prefix of the candidate, and candidate surplus is informational. The
//! walker takes an optional stop position from day one so prefix verification
//! of resumed runs (FLOWIP-120n) is later a parameterization rather than a
//! rewrite.

use std::collections::BTreeMap;

use obzenflow_core::event::ChainEvent;

use super::error::VerifyError;
use super::lineage::IdentityMode;
use super::projection::{project, PositionalRow, ProjectedRow, RowKind};
use super::report::{absent_side, delta_side, DeltaSide, DivergenceReport};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalkMode {
    WholeRun,
    BaselinePrefixOfCandidate,
}

impl WalkMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WholeRun => "whole_run",
            Self::BaselinePrefixOfCandidate => "baseline_prefix_of_candidate",
        }
    }
}

#[derive(Debug, Clone)]
pub struct StageWalkOptions {
    pub mode: WalkMode,
    pub identity: IdentityMode,
    /// Cap on recorded divergences per journal; counting continues past it.
    pub max_divergences: usize,
    /// Compare only the first `n` positional rows of each side. Plumbed for
    /// resume-prefix verification (FLOWIP-120n); unset in v1 surfaces.
    pub stop_at: Option<u64>,
    /// `data` or `error`, stamped into divergence coordinates.
    pub journal_label: &'static str,
    /// `changed-code` or `unexpected`, from the stage_logic_version delta.
    pub classification: &'static str,
}

#[derive(Debug, Default)]
pub struct StageWalkOutput {
    pub positional_baseline: u64,
    pub positional_candidate: u64,
    pub divergences: Vec<DivergenceReport>,
    pub divergence_count: u64,
    /// Positional rows the candidate carries beyond the baseline prefix
    /// (prefix mode only; informational, never divergence).
    pub candidate_surplus: u64,
    /// Canonicalized completion evidence per side, compared by the caller as
    /// an order-insensitive multiset (and only when both runs completed).
    pub eof_baseline: Vec<String>,
    pub eof_candidate: Vec<String>,
    /// Set when `Lineage` mode met two effect-lane rows minted in different
    /// namespaces: the runs are unrelated, and the caller refuses.
    pub lineage_conflict: Option<(String, String)>,
}

/// Walk two journals of the same stage and compare their projections.
pub fn walk_journal(
    baseline: impl Iterator<Item = Result<ChainEvent, VerifyError>>,
    candidate: impl Iterator<Item = Result<ChainEvent, VerifyError>>,
    opts: &StageWalkOptions,
) -> Result<StageWalkOutput, VerifyError> {
    let mut out = StageWalkOutput::default();
    let mut baseline = baseline.fuse();
    let mut candidate = candidate.fuse();
    let mut position: u64 = 0;

    loop {
        if let Some(stop) = opts.stop_at {
            if position >= stop {
                break;
            }
        }
        let b = next_positional(&mut baseline, &mut out.eof_baseline)?;
        let c = next_positional(&mut candidate, &mut out.eof_candidate)?;

        match (b, c) {
            (Some(b_row), Some(c_row)) => {
                out.positional_baseline += 1;
                out.positional_candidate += 1;

                if opts.identity == IdentityMode::Lineage {
                    if let (Some(b_id), Some(c_id)) = (&b_row.identity, &c_row.identity) {
                        if b_id.namespace != c_id.namespace {
                            out.lineage_conflict =
                                Some((b_id.namespace.clone(), c_id.namespace.clone()));
                            return Ok(out);
                        }
                    }
                }

                if let Some((field, b_raw, c_raw)) = first_differing_field(&b_row, &c_row, opts) {
                    record_divergence(
                        &mut out,
                        opts,
                        position,
                        field,
                        delta_side(&b_raw),
                        delta_side(&c_raw),
                    );
                }
            }
            (Some(b_row), None) => {
                // Baseline is longer. A missing candidate tail is divergence
                // in both modes: in prefix mode the baseline must be a
                // prefix, so a longer baseline violates it.
                out.positional_baseline += 1;
                record_missing(&mut out, opts, position, Side::CandidateAbsent, &b_row);
                let mut p = position + 1;
                while let Some(row) = next_positional(&mut baseline, &mut out.eof_baseline)? {
                    out.positional_baseline += 1;
                    record_missing(&mut out, opts, p, Side::CandidateAbsent, &row);
                    p += 1;
                }
                break;
            }
            (None, Some(c_row)) => {
                out.positional_candidate += 1;
                match opts.mode {
                    WalkMode::WholeRun => {
                        record_missing(&mut out, opts, position, Side::BaselineAbsent, &c_row);
                        let mut p = position + 1;
                        while let Some(row) =
                            next_positional(&mut candidate, &mut out.eof_candidate)?
                        {
                            out.positional_candidate += 1;
                            record_missing(&mut out, opts, p, Side::BaselineAbsent, &row);
                            p += 1;
                        }
                    }
                    WalkMode::BaselinePrefixOfCandidate => {
                        out.candidate_surplus += 1;
                        while next_positional(&mut candidate, &mut out.eof_candidate)?.is_some() {
                            out.positional_candidate += 1;
                            out.candidate_surplus += 1;
                        }
                    }
                }
                break;
            }
            (None, None) => break,
        }
        position += 1;
    }
    Ok(out)
}

/// Count-only walk for stages outside the order-certified region: per-type
/// positional row counts (order-insensitive advisory) plus completion
/// evidence, never positional comparison.
#[derive(Debug, Default)]
pub struct TallyOutput {
    pub positional: u64,
    pub counts_by_type: BTreeMap<String, u64>,
    pub eof: Vec<String>,
}

pub fn tally_journal(
    rows: impl Iterator<Item = Result<ChainEvent, VerifyError>>,
) -> Result<TallyOutput, VerifyError> {
    let mut out = TallyOutput::default();
    for event in rows {
        match project(&event?) {
            Some(ProjectedRow::Positional(row)) => {
                out.positional += 1;
                let key = match &row.kind {
                    RowKind::Data { event_type } => event_type.clone(),
                    RowKind::Watermark => "<watermark>".to_string(),
                };
                *out.counts_by_type.entry(key).or_insert(0) += 1;
            }
            Some(ProjectedRow::EofEvidence(evidence)) => {
                out.eof.push(evidence.to_string());
            }
            None => {}
        }
    }
    Ok(out)
}

fn next_positional(
    rows: &mut impl Iterator<Item = Result<ChainEvent, VerifyError>>,
    eof_sink: &mut Vec<String>,
) -> Result<Option<PositionalRow>, VerifyError> {
    for event in rows {
        match project(&event?) {
            Some(ProjectedRow::Positional(row)) => return Ok(Some(row)),
            Some(ProjectedRow::EofEvidence(evidence)) => eof_sink.push(evidence.to_string()),
            None => {}
        }
    }
    Ok(None)
}

enum Side {
    BaselineAbsent,
    CandidateAbsent,
}

fn record_missing(
    out: &mut StageWalkOutput,
    opts: &StageWalkOptions,
    position: u64,
    side: Side,
    present_row: &PositionalRow,
) {
    let raw = serde_json::to_string(present_row).unwrap_or_default();
    let (baseline, candidate) = match side {
        Side::BaselineAbsent => (absent_side(), delta_side(&raw)),
        Side::CandidateAbsent => (delta_side(&raw), absent_side()),
    };
    record_divergence(out, opts, position, "missing_row", baseline, candidate);
}

fn record_divergence(
    out: &mut StageWalkOutput,
    opts: &StageWalkOptions,
    position: u64,
    field: &str,
    baseline: DeltaSide,
    candidate: DeltaSide,
) {
    out.divergence_count += 1;
    if out.divergences.len() < opts.max_divergences {
        out.divergences.push(DivergenceReport {
            journal: opts.journal_label.to_string(),
            position,
            field: field.to_string(),
            baseline,
            candidate,
            classification: opts.classification.to_string(),
        });
    }
}

/// First differing comparable field, with both raw renderings.
fn first_differing_field(
    baseline: &PositionalRow,
    candidate: &PositionalRow,
    opts: &StageWalkOptions,
) -> Option<(&'static str, String, String)> {
    if baseline.kind != candidate.kind {
        return Some((
            "kind",
            serde_json::to_string(&baseline.kind).unwrap_or_default(),
            serde_json::to_string(&candidate.kind).unwrap_or_default(),
        ));
    }
    if baseline.payload != candidate.payload {
        return Some((
            "payload",
            baseline.payload.to_string(),
            candidate.payload.to_string(),
        ));
    }
    if baseline.status != candidate.status {
        return Some((
            "status",
            serde_json::to_string(&baseline.status).unwrap_or_default(),
            serde_json::to_string(&candidate.status).unwrap_or_default(),
        ));
    }
    if opts.identity == IdentityMode::Lineage && baseline.identity != candidate.identity {
        return Some((
            "effect_identity",
            serde_json::to_string(&baseline.identity).unwrap_or_default(),
            serde_json::to_string(&candidate.identity).unwrap_or_default(),
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use serde_json::json;

    fn writer() -> WriterId {
        WriterId::from(StageId::new())
    }

    fn data(event_type: &str, payload: serde_json::Value) -> ChainEvent {
        ChainEventFactory::data_event(writer(), event_type, payload)
    }

    fn opts(mode: WalkMode) -> StageWalkOptions {
        StageWalkOptions {
            mode,
            identity: IdentityMode::Positional,
            max_divergences: 5,
            stop_at: None,
            journal_label: "data",
            classification: "unexpected",
        }
    }

    fn ok_rows(rows: Vec<ChainEvent>) -> impl Iterator<Item = Result<ChainEvent, VerifyError>> {
        rows.into_iter().map(Ok)
    }

    #[test]
    fn identical_streams_match_with_zero_divergences() {
        let rows = vec![data("a", json!({"n": 1})), data("a", json!({"n": 2}))];
        let out = walk_journal(
            ok_rows(rows.clone()),
            ok_rows(rows),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 0);
        assert_eq!(out.positional_baseline, 2);
        assert_eq!(out.positional_candidate, 2);
    }

    #[test]
    fn payload_divergence_names_position_and_field() {
        let baseline = vec![data("a", json!({"n": 1})), data("a", json!({"n": 2}))];
        let candidate = vec![data("a", json!({"n": 1})), data("a", json!({"n": 99}))];
        let out = walk_journal(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 1);
        assert_eq!(out.divergences[0].position, 1);
        assert_eq!(out.divergences[0].field, "payload");
    }

    #[test]
    fn whole_run_missing_tail_is_divergence() {
        let baseline = vec![data("a", json!({"n": 1})), data("a", json!({"n": 2}))];
        let candidate = vec![data("a", json!({"n": 1}))];
        let out = walk_journal(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 1);
        assert_eq!(out.divergences[0].field, "missing_row");
    }

    #[test]
    fn prefix_mode_surplus_is_informational_not_divergence() {
        let baseline = vec![data("a", json!({"n": 1}))];
        let candidate = vec![
            data("a", json!({"n": 1})),
            data("a", json!({"n": 2})),
            data("a", json!({"n": 3})),
        ];
        let out = walk_journal(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::BaselinePrefixOfCandidate),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 0);
        assert_eq!(out.candidate_surplus, 2);
    }

    #[test]
    fn prefix_mode_divergence_inside_the_prefix_still_reports() {
        let baseline = vec![data("a", json!({"n": 1}))];
        let candidate = vec![data("a", json!({"n": 7})), data("a", json!({"n": 2}))];
        let out = walk_journal(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::BaselinePrefixOfCandidate),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 1);
        assert_eq!(out.divergences[0].field, "payload");
    }

    #[test]
    fn max_divergences_caps_recording_but_not_counting() {
        let baseline: Vec<ChainEvent> = (0..10).map(|n| data("a", json!({ "n": n }))).collect();
        let candidate: Vec<ChainEvent> = (0..10)
            .map(|n| data("a", json!({ "n": n + 100 })))
            .collect();
        let mut o = opts(WalkMode::WholeRun);
        o.max_divergences = 3;
        let out = walk_journal(ok_rows(baseline), ok_rows(candidate), &o).unwrap();
        assert_eq!(out.divergence_count, 10);
        assert_eq!(out.divergences.len(), 3);
    }

    #[test]
    fn eof_rows_are_collected_as_evidence_not_compared_positionally() {
        let baseline = vec![
            data("a", json!({"n": 1})),
            ChainEventFactory::eof_event(writer(), true),
        ];
        let candidate = vec![
            data("a", json!({"n": 1})),
            ChainEventFactory::eof_event(writer(), true),
        ];
        let out = walk_journal(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 0);
        assert_eq!(out.eof_baseline.len(), 1);
        assert_eq!(out.eof_candidate.len(), 1);
    }

    #[test]
    fn tally_counts_by_type_without_comparing() {
        let rows = vec![
            data("a", json!({"n": 1})),
            data("b", json!({"n": 2})),
            data("a", json!({"n": 3})),
        ];
        let out = tally_journal(ok_rows(rows)).unwrap();
        assert_eq!(out.positional, 3);
        assert_eq!(out.counts_by_type["a"], 2);
        assert_eq!(out.counts_by_type["b"], 1);
    }
}
