// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A run's place in its history (FLOWIP-134i B6, B7, B12, B13, B16), read from
//! run manifests, continuation claims and system logs. Reading it opens no
//! archive for resume and takes no claim.

use super::JournalReadError;
use crate::journal::disk::continuation::{history_head, read_claim, takeover, Takeover};
use crate::journal::disk::inspect::{load_manifest, JournalInspectError};
use crate::journal::disk::replay_archive::{recorded_reconstruction_completion, replayed_run};
use obzenflow_core::journal::RunManifest;
use std::path::{Path, PathBuf};

/// The run a resumed run continued, and the generation it entered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumedFrom {
    pub parent: PathBuf,
    pub generation: u64,
}

/// Whether a resumed run is resume authority on its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Rebuild {
    /// It recorded reconstruction completion; resume needs no earlier run.
    Complete,
    /// Torn in `generation`. Resuming this head merges `requires`, nearest
    /// first; a continued run requires nothing, since it never resumes again.
    Incomplete {
        generation: u64,
        requires: Vec<PathBuf>,
    },
}

/// An earlier run that still holds evidence this run never re-admits.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceHolder {
    pub run: PathBuf,
    /// The generation it ran in, when its manifest or its successor records it.
    pub generation: Option<u64>,
    pub missing: bool,
}

/// Where a run stands in its history.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Standing {
    /// Nothing continued it; resuming it continues its history.
    #[default]
    Head,
    /// It was continued; the head of its history resumes instead.
    Continued { head: PathBuf },
    /// It never took over its parent and belongs to no history.
    Orphan { resume: PathBuf },
    /// A replay reproduces `replayed` and is never a head.
    Replay { replayed: PathBuf },
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunLineage {
    pub resumed_from: Option<ResumedFrom>,
    pub standing: Standing,
    /// For a resumed run that belongs to its history.
    pub rebuild: Option<Rebuild>,
    /// Following replay and resume links, nearest first.
    pub evidence: Vec<EvidenceHolder>,
}

pub fn read_run_lineage(run_dir: &Path) -> Result<RunLineage, JournalReadError> {
    let run = std::fs::canonicalize(run_dir).map_err(|source| JournalInspectError::Io {
        path: run_dir.to_path_buf(),
        source,
    })?;
    let manifest = load_manifest(&run)?;
    let resumed_from = manifest.resume.as_ref().map(|resume| ResumedFrom {
        parent: resume.resumed_from.clone(),
        generation: resume.resume_generation,
    });
    let standing = standing(&run, &manifest)?;
    let rebuild = match (&resumed_from, &standing) {
        (Some(resumed), Standing::Head | Standing::Continued { .. }) => {
            Some(if recorded_reconstruction_completion(&run, &manifest) {
                Rebuild::Complete
            } else {
                Rebuild::Incomplete {
                    generation: resumed.generation,
                    requires: match standing {
                        Standing::Head => required_ancestors(&resumed.parent),
                        _ => Vec::new(),
                    },
                }
            })
        }
        _ => None,
    };
    let evidence = match (&standing, &rebuild) {
        (Standing::Orphan { .. }, _) => Vec::new(),
        (_, Some(Rebuild::Incomplete { requires, .. })) => {
            evidence_holders(&run, &manifest, requires)
        }
        _ => evidence_holders(&run, &manifest, &[]),
    };
    Ok(RunLineage {
        resumed_from,
        standing,
        rebuild,
        evidence,
    })
}

fn standing(run: &Path, manifest: &RunManifest) -> Result<Standing, JournalReadError> {
    if let Some(replayed) = replayed_run(manifest) {
        return Ok(Standing::Replay { replayed });
    }
    if read_claim(run)?.is_some() {
        return Ok(Standing::Continued {
            head: history_head(run)?,
        });
    }
    if let Some(resume) = &manifest.resume {
        match takeover(run, manifest, &resume.resumed_from)? {
            Takeover::Held => {}
            Takeover::StoppedBefore => {
                return Ok(Standing::Orphan {
                    resume: resume.resumed_from.clone(),
                })
            }
            Takeover::Lost { head } => return Ok(Standing::Orphan { resume: head }),
        }
    }
    Ok(Standing::Head)
}

/// The runs a resume of a torn run merges: its parent, and each further
/// parent while the one before it never completed reconstruction.
fn required_ancestors(parent: &Path) -> Vec<PathBuf> {
    let mut requires = vec![parent.to_path_buf()];
    let mut current = parent.to_path_buf();
    while let Ok(manifest) = load_manifest(&current) {
        let Some(resume) = &manifest.resume else {
            break;
        };
        if recorded_reconstruction_completion(&current, &manifest)
            || requires.contains(&resume.resumed_from)
        {
            break;
        }
        current = resume.resumed_from.clone();
        requires.push(current.clone());
    }
    requires
}

/// Earlier runs reached by replay and resume links that a resume of this run
/// does not require. Replays are skipped: they add nothing a resume reads.
fn evidence_holders(
    run: &Path,
    manifest: &RunManifest,
    required: &[PathBuf],
) -> Vec<EvidenceHolder> {
    let mut holders = Vec::new();
    let mut visited = vec![run.to_path_buf()];
    let mut next = link(manifest);
    while let Some((linked, recorded_generation)) = next {
        if visited.contains(&linked) {
            break;
        }
        visited.push(linked.clone());
        let manifest = load_manifest(&linked).ok();
        let replay = manifest
            .as_ref()
            .is_some_and(|manifest| replayed_run(manifest).is_some());
        if !replay && !required.contains(&linked) {
            holders.push(EvidenceHolder {
                generation: manifest
                    .as_ref()
                    .map(entered_generation)
                    .or(recorded_generation),
                missing: !linked.exists(),
                run: linked,
            });
        }
        next = manifest.as_ref().and_then(link);
    }
    holders
}

/// The run a manifest links back to, with that run's generation when the link
/// records it: a resume of a run that completed its own generation enters the
/// next one. A resume that merged ancestors continued a torn parent, which may
/// not have crossed its generation.
fn link(manifest: &RunManifest) -> Option<(PathBuf, Option<u64>)> {
    match (&manifest.resume, replayed_run(manifest)) {
        (Some(resume), _) => {
            let generation = if resume.merged_from.is_empty() {
                resume.resume_generation.checked_sub(1)
            } else {
                None
            };
            Some((resume.resumed_from.clone(), generation))
        }
        (None, Some(replayed)) => Some((replayed, None)),
        (None, None) => None,
    }
}

fn entered_generation(manifest: &RunManifest) -> u64 {
    manifest
        .resume
        .as_ref()
        .map_or(0, |resume| resume.resume_generation)
}
