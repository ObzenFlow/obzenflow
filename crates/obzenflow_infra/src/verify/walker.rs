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

use std::collections::{BTreeMap, BTreeSet};

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
    /// FLOWIP-095l: max distinct row-contents an order-insensitive (content) walk
    /// buffers before falling back to a bounded additive-digest verdict. Caps
    /// content-certification memory; exact row-level reporting is kept under it.
    pub content_pinpoint_cap: usize,
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

/// An order-independent (multiset) digest of a stage's projected rows: the
/// wrapping sum of each row's SHA-256, so equal multisets in any order collapse
/// to one value and duplicates accumulate (MSet-Add-Hash; Clarke et al. 2003,
/// Bellare-Micciancio AdHash 1997). XOR is deliberately avoided: `x ^ x == 0`
/// cancels duplicate pairs, making it a set hash rather than a multiset hash.
/// This is non-adversarial divergence detection, so it is unkeyed; the ~2^-128
/// collision bound matches the trust the report's SHA-256 value deltas carry.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct MsetDigest {
    lanes: [u64; 4],
    rows: u64,
}

impl MsetDigest {
    fn add(&mut self, content: &str) {
        let digest = ring::digest::digest(&ring::digest::SHA256, content.as_bytes());
        for (lane, chunk) in self.lanes.iter_mut().zip(digest.as_ref().chunks_exact(8)) {
            *lane = lane.wrapping_add(u64::from_le_bytes(chunk.try_into().expect("8-byte chunk")));
        }
        self.rows += 1;
    }

    fn hex(&self) -> String {
        let lanes: String = self
            .lanes
            .iter()
            .map(|lane| format!("{lane:016x}"))
            .collect();
        format!("{lanes} ({} rows)", self.rows)
    }
}

/// Content-equality walk for an order-insensitive region (FLOWIP-095l): compare
/// the two journals as multisets of projected rows, tolerating any delivery
/// reordering the region permits while still certifying content.
///
/// Memory is bounded, never O(run size). Up to `content_pinpoint_cap` distinct
/// contents a signed per-content count map gives the exact, row-level result;
/// beyond it (a high-cardinality order-insensitive stage) the verdict falls back
/// to an order-independent additive digest, which certifies a match in constant
/// memory and reports a divergence by per-type counts (the row is in the
/// journals). Prefix mode (a cancelled baseline, FLOWIP-095k) is compared exactly
/// with no cap and no digest, since bounding it is deferred to that work; a
/// cancelled baseline is a truncated prefix, so the exact map stays modest.
///
/// `WholeRun` requires equal multisets. `BaselinePrefixOfCandidate` requires the
/// baseline to be a sub-multiset of the candidate: a candidate deficit diverges,
/// a candidate surplus is informational. Completion evidence is collected per
/// side and compared by the caller exactly as the positional walk does.
pub fn walk_journal_multiset(
    baseline: impl Iterator<Item = Result<ChainEvent, VerifyError>>,
    candidate: impl Iterator<Item = Result<ChainEvent, VerifyError>>,
    opts: &StageWalkOptions,
) -> Result<StageWalkOutput, VerifyError> {
    let mut out = StageWalkOutput::default();

    // `None` = exact/unbounded (prefix); `Some(cap)` = bounded with a digest
    // fallback (WholeRun). The digest is accumulated only when a cap is set.
    let cap = match opts.mode {
        WalkMode::WholeRun => Some(opts.content_pinpoint_cap),
        WalkMode::BaselinePrefixOfCandidate => None,
    };

    let mut counts: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut overflow = false;
    let mut baseline_digest = MsetDigest::default();
    let mut candidate_digest = MsetDigest::default();
    let mut baseline_types: BTreeMap<String, u64> = BTreeMap::new();
    let mut candidate_types: BTreeMap<String, u64> = BTreeMap::new();
    let mut baseline_ns = BTreeSet::new();
    let mut candidate_ns = BTreeSet::new();

    out.positional_baseline = accumulate_side(
        baseline,
        opts,
        cap,
        true,
        &mut counts,
        &mut overflow,
        &mut baseline_digest,
        &mut baseline_types,
        &mut baseline_ns,
        &mut out.eof_baseline,
    )?;
    out.positional_candidate = accumulate_side(
        candidate,
        opts,
        cap,
        false,
        &mut counts,
        &mut overflow,
        &mut candidate_digest,
        &mut candidate_types,
        &mut candidate_ns,
        &mut out.eof_candidate,
    )?;

    // Lineage parity with the positional walk: under `Lineage`, effect-lane rows
    // minted in disjoint namespaces mean the two runs are unrelated. A barrier-
    // shielded effect can legitimately sit in a content region, so guard here too.
    if let Some(conflict) = lineage_conflict(opts.identity, &baseline_ns, &candidate_ns) {
        out.lineage_conflict = Some(conflict);
        return Ok(out);
    }

    if overflow {
        // WholeRun over the cap: certify or diverge by the additive digest.
        if baseline_digest != candidate_digest {
            record_digest_divergence(
                &mut out,
                opts,
                &baseline_digest,
                &candidate_digest,
                &baseline_types,
                &candidate_types,
            );
        }
        return Ok(out);
    }

    // Exact, row-level. `counts` holds every distinct content with both sides'
    // multiplicities, so a per-content mismatch is reported precisely.
    for (position, (content, (baseline_count, candidate_count))) in counts
        .iter()
        .filter(|(_, (baseline, candidate))| baseline != candidate)
        .enumerate()
    {
        let position = position as u64;
        match opts.mode {
            WalkMode::WholeRun => {
                record_multiset_divergence(
                    &mut out,
                    opts,
                    position,
                    content,
                    *baseline_count,
                    *candidate_count,
                );
            }
            WalkMode::BaselinePrefixOfCandidate => {
                if candidate_count < baseline_count {
                    // The baseline must be a sub-multiset of the candidate.
                    record_multiset_divergence(
                        &mut out,
                        opts,
                        position,
                        content,
                        *baseline_count,
                        *candidate_count,
                    );
                } else {
                    out.candidate_surplus += candidate_count - baseline_count;
                }
            }
        }
    }
    Ok(out)
}

/// Fold one journal into the shared per-content counts, the side's digest,
/// per-type counts, namespaces, and completion evidence; return the projected
/// row total. When `cap` is `Some`, a fresh content beyond the cap trips
/// `overflow` (and stops admitting new contents) while the digest keeps
/// accumulating. When `cap` is `None`, the walk is exact and the digest is
/// skipped.
#[allow(clippy::too_many_arguments)]
fn accumulate_side(
    rows: impl Iterator<Item = Result<ChainEvent, VerifyError>>,
    opts: &StageWalkOptions,
    cap: Option<usize>,
    is_baseline: bool,
    counts: &mut BTreeMap<String, (u64, u64)>,
    overflow: &mut bool,
    digest: &mut MsetDigest,
    types: &mut BTreeMap<String, u64>,
    namespaces: &mut BTreeSet<String>,
    eof: &mut Vec<String>,
) -> Result<u64, VerifyError> {
    let mut total = 0;
    for event in rows {
        match project(&event?) {
            Some(ProjectedRow::Positional(row)) => {
                total += 1;
                let key = content_key(&row, opts.identity);
                if cap.is_some() {
                    digest.add(&key);
                }
                *types.entry(type_key(&row)).or_insert(0) += 1;
                if opts.identity == IdentityMode::Lineage {
                    if let Some(id) = &row.identity {
                        namespaces.insert(id.namespace.clone());
                    }
                }
                fold_counts(counts, overflow, cap, key, is_baseline);
            }
            Some(ProjectedRow::EofEvidence(evidence)) => eof.push(evidence.to_string()),
            None => {}
        }
    }
    Ok(total)
}

/// Record one row into the capped per-content counts. Trips `overflow` (and stops
/// admitting new contents) when a fresh content would exceed a set cap.
fn fold_counts(
    counts: &mut BTreeMap<String, (u64, u64)>,
    overflow: &mut bool,
    cap: Option<usize>,
    key: String,
    is_baseline: bool,
) {
    if *overflow {
        return;
    }
    if let Some(cap) = cap {
        if !counts.contains_key(&key) && counts.len() >= cap {
            *overflow = true;
            return;
        }
    }
    let entry = counts.entry(key).or_insert((0, 0));
    if is_baseline {
        entry.0 += 1;
    } else {
        entry.1 += 1;
    }
}

/// The event type of a projected row, the granularity of the over-cap advisory.
fn type_key(row: &PositionalRow) -> String {
    match &row.kind {
        RowKind::Data { event_type } => event_type.clone(),
        RowKind::Watermark => "<watermark>".to_string(),
    }
}

/// Under `Lineage`, disjoint non-empty effect namespaces mean unrelated runs.
fn lineage_conflict(
    identity: IdentityMode,
    baseline_ns: &BTreeSet<String>,
    candidate_ns: &BTreeSet<String>,
) -> Option<(String, String)> {
    if identity == IdentityMode::Lineage
        && !baseline_ns.is_empty()
        && !candidate_ns.is_empty()
        && baseline_ns.is_disjoint(candidate_ns)
    {
        Some((
            baseline_ns.iter().next().cloned().unwrap_or_default(),
            candidate_ns.iter().next().cloned().unwrap_or_default(),
        ))
    } else {
        None
    }
}

/// Over-cap divergence report: one entry per event type whose count differs
/// (bounded by the number of types), or a single digest mismatch when per-type
/// counts agree but a within-type payload differs (localizable only in the
/// journals).
fn record_digest_divergence(
    out: &mut StageWalkOutput,
    opts: &StageWalkOptions,
    baseline_digest: &MsetDigest,
    candidate_digest: &MsetDigest,
    baseline_types: &BTreeMap<String, u64>,
    candidate_types: &BTreeMap<String, u64>,
) {
    let types: BTreeSet<&String> = baseline_types
        .keys()
        .chain(candidate_types.keys())
        .collect();
    let mut any_type_delta = false;
    for (position, event_type) in types.into_iter().enumerate() {
        let baseline_count = baseline_types.get(event_type).copied().unwrap_or(0);
        let candidate_count = candidate_types.get(event_type).copied().unwrap_or(0);
        if baseline_count != candidate_count {
            any_type_delta = true;
            out.divergence_count += 1;
            if out.divergences.len() < opts.max_divergences {
                out.divergences.push(DivergenceReport {
                    journal: opts.journal_label.to_string(),
                    position: position as u64,
                    field: "multiset_type_count".to_string(),
                    baseline: delta_side(&format!("{baseline_count}x {event_type}")),
                    candidate: delta_side(&format!("{candidate_count}x {event_type}")),
                    classification: opts.classification.to_string(),
                });
            }
        }
    }
    if !any_type_delta {
        out.divergence_count += 1;
        if out.divergences.len() < opts.max_divergences {
            out.divergences.push(DivergenceReport {
                journal: opts.journal_label.to_string(),
                position: 0,
                field: "multiset_digest".to_string(),
                baseline: delta_side(&baseline_digest.hex()),
                candidate: delta_side(&candidate_digest.hex()),
                classification: opts.classification.to_string(),
            });
        }
    }
}

/// The canonical multiset key for a projected row: the same comparable fields
/// the positional walk uses (`kind` + `payload` + `status`, and `identity` only
/// under `IdentityMode::Lineage`, mirroring `first_differing_field`). serde_json
/// serializes objects with sorted keys (no `preserve_order` feature), so equal
/// content maps to one key. Effect identity is kept under Lineage, where it is
/// deterministic and an effect shielded by a barrier can legitimately sit in a
/// content region; it is dropped otherwise, or per-run-regenerated event ids
/// would false-diverge.
fn content_key(row: &PositionalRow, identity: IdentityMode) -> String {
    let mut row = row.clone();
    if identity != IdentityMode::Lineage {
        row.identity = None;
    }
    serde_json::to_string(&row).unwrap_or_default()
}

fn record_multiset_divergence(
    out: &mut StageWalkOutput,
    opts: &StageWalkOptions,
    position: u64,
    content: &str,
    baseline_count: u64,
    candidate_count: u64,
) {
    out.divergence_count += 1;
    if out.divergences.len() < opts.max_divergences {
        out.divergences.push(DivergenceReport {
            journal: opts.journal_label.to_string(),
            position,
            field: "multiset_count".to_string(),
            baseline: delta_side(&format!("{baseline_count}x {content}")),
            candidate: delta_side(&format!("{candidate_count}x {content}")),
            classification: opts.classification.to_string(),
        });
    }
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
            content_pinpoint_cap: 100_000,
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

    #[test]
    fn multiset_tolerates_reordering_of_equal_content() {
        let baseline = vec![data("a", json!({"n": 1})), data("a", json!({"n": 2}))];
        let candidate = vec![data("a", json!({"n": 2})), data("a", json!({"n": 1}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(
            out.divergence_count, 0,
            "reordered but equal multiset matches"
        );
        assert_eq!(out.positional_baseline, 2);
        assert_eq!(out.positional_candidate, 2);
    }

    #[test]
    fn multiset_count_difference_on_same_content_diverges() {
        let baseline = vec![data("a", json!({"n": 1})), data("a", json!({"n": 1}))];
        let candidate = vec![data("a", json!({"n": 1}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 1);
        assert_eq!(out.divergences[0].field, "multiset_count");
    }

    #[test]
    fn multiset_payload_difference_diverges() {
        // The 316-vs-999 shape: distinct content the count-only advisory misses.
        let baseline = vec![data("sum", json!({"total": 316}))];
        let candidate = vec![data("sum", json!({"total": 999}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        // Two distinct contents, each with a count mismatch (1 vs 0 and 0 vs 1).
        assert_eq!(out.divergence_count, 2);
        assert!(out.divergences.iter().all(|d| d.field == "multiset_count"));
    }

    #[test]
    fn multiset_prefix_surplus_is_informational() {
        let baseline = vec![data("a", json!({"n": 1}))];
        let candidate = vec![data("a", json!({"n": 1})), data("a", json!({"n": 1}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::BaselinePrefixOfCandidate),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 0);
        assert_eq!(out.candidate_surplus, 1);
    }

    #[test]
    fn multiset_prefix_missing_baseline_row_diverges() {
        let baseline = vec![data("a", json!({"n": 1})), data("a", json!({"n": 1}))];
        let candidate = vec![data("a", json!({"n": 1}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::BaselinePrefixOfCandidate),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 1, "baseline must be a sub-multiset");
    }

    #[test]
    fn multiset_collects_eof_evidence_without_comparing_it_positionally() {
        let baseline = vec![
            data("a", json!({"n": 1})),
            ChainEventFactory::eof_event(writer(), true),
        ];
        let candidate = vec![
            ChainEventFactory::eof_event(writer(), true),
            data("a", json!({"n": 1})),
        ];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &opts(WalkMode::WholeRun),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 0);
        assert_eq!(out.eof_baseline.len(), 1);
        assert_eq!(out.eof_candidate.len(), 1);
    }

    // ---- FLOWIP-095l: bounded over-cap content certification ----

    fn capped(mode: WalkMode, cap: usize) -> StageWalkOptions {
        StageWalkOptions {
            content_pinpoint_cap: cap,
            ..opts(mode)
        }
    }

    #[test]
    fn mset_digest_counts_multiplicity_unlike_xor() {
        let mut once = MsetDigest::default();
        once.add("x");
        let mut twice = MsetDigest::default();
        twice.add("x");
        twice.add("x");
        // XOR would give h(x) ^ h(x) == 0 == the empty digest; the additive
        // digest keeps multiplicity, so {x} != {x,x} != {}.
        assert_ne!(once, twice);
        assert_ne!(twice, MsetDigest::default());
    }

    #[test]
    fn multiset_over_cap_matches_via_digest() {
        // Two distinct contents exceed cap 1, forcing the digest fallback; the
        // content is reordered but equal, so it still certifies.
        let baseline = vec![data("a", json!({"n": 1})), data("a", json!({"n": 2}))];
        let candidate = vec![data("a", json!({"n": 2})), data("a", json!({"n": 1}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &capped(WalkMode::WholeRun, 1),
        )
        .unwrap();
        assert_eq!(
            out.divergence_count, 0,
            "equal multiset certifies via the additive digest over the cap"
        );
    }

    #[test]
    fn multiset_over_cap_within_type_payload_difference_diverges_via_digest() {
        // >cap distinct contents, same event type and per-type counts, different
        // payload: only the digest catches it (the count-only advisory could not).
        let baseline = vec![
            data("sum", json!({"total": 316})),
            data("k", json!({"n": 1})),
        ];
        let candidate = vec![
            data("sum", json!({"total": 999})),
            data("k", json!({"n": 1})),
        ];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &capped(WalkMode::WholeRun, 1),
        )
        .unwrap();
        assert_eq!(out.divergence_count, 1);
        assert_eq!(out.divergences[0].field, "multiset_digest");
    }

    #[test]
    fn multiset_over_cap_type_count_difference_diverges() {
        let baseline = vec![
            data("a", json!({"n": 1})),
            data("a", json!({"n": 2})),
            data("a", json!({"n": 2})),
        ];
        let candidate = vec![data("a", json!({"n": 1})), data("a", json!({"n": 2}))];
        let out = walk_journal_multiset(
            ok_rows(baseline),
            ok_rows(candidate),
            &capped(WalkMode::WholeRun, 1),
        )
        .unwrap();
        assert!(out
            .divergences
            .iter()
            .any(|d| d.field == "multiset_type_count"));
    }
}
