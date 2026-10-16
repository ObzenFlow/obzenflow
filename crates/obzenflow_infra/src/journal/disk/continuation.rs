// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One head per history (FLOWIP-134i B7, B12). A resume takes the continued
//! run's write-once claim inside that run's location; a run is the head only
//! when it carries no claim and has no live journal writer.

use obzenflow_core::journal::archive::manifest::{RunManifest, JOURNAL_SCHEMA_VERSION};
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::FlowId;
use serde::{Deserialize, Serialize};
use std::fs::{File, TryLockError};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

pub(crate) const RUN_CONTINUATION_FILENAME: &str = "run_continuation.json";

/// A claim is a few hundred bytes; anything larger is not one.
const CLAIM_BUDGET: u64 = 4096;

/// The run that continued a history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ContinuedBy {
    pub(crate) run_id: FlowId,
    pub(crate) location: PathBuf,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunContinuation {
    schema: String,
    continued_by: ContinuedBy,
}

fn io_error(message: String, source: std::io::Error) -> ReplayError {
    ReplayError::Io { message, source }
}

fn not_writable(run: &Path, message: String, error: std::io::Error) -> ReplayError {
    match error.kind() {
        ErrorKind::PermissionDenied | ErrorKind::ReadOnlyFilesystem => {
            ReplayError::LocationNotWritable {
                archive: run.to_path_buf(),
            }
        }
        _ => io_error(message, error),
    }
}

/// The claim published in `run`, if any.
pub(crate) fn read_claim(run: &Path) -> Result<Option<ContinuedBy>, ReplayError> {
    let path = run.join(RUN_CONTINUATION_FILENAME);
    let file = match File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(io_error(
                format!("Failed to read continuation claim {}", path.display()),
                error,
            ))
        }
    };
    let mut bytes = Vec::new();
    file.take(CLAIM_BUDGET + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| {
            io_error(
                format!("Failed to read continuation claim {}", path.display()),
                error,
            )
        })?;
    let unreadable = |reason: String| ReplayError::Parse {
        message: format!("{} is not a continuation claim: {reason}", path.display()),
    };
    if bytes.len() as u64 > CLAIM_BUDGET {
        return Err(unreadable(format!("larger than {CLAIM_BUDGET} bytes")));
    }
    let claim: RunContinuation =
        serde_json::from_slice(&bytes).map_err(|error| unreadable(error.to_string()))?;
    if claim.schema != JOURNAL_SCHEMA_VERSION {
        return Err(unreadable(format!(
            "schema {} (supported: {JOURNAL_SCHEMA_VERSION})",
            claim.schema
        )));
    }
    Ok(Some(claim.continued_by))
}

/// The head of the history `run` belongs to: the last run reached by
/// following claims from it, named even when it is missing.
pub(crate) fn history_head(run: &Path) -> Result<PathBuf, ReplayError> {
    let mut head = run.to_path_buf();
    let mut visited = vec![head.clone()];
    while head.exists() {
        let Some(claim) = read_claim(&head)? else {
            break;
        };
        if visited.contains(&claim.location) {
            return Err(ReplayError::Parse {
                message: format!(
                    "the continuation claims from {} return to {}",
                    run.display(),
                    claim.location.display()
                ),
            });
        }
        visited.push(claim.location.clone());
        head = claim.location;
    }
    Ok(head)
}

/// The run whose resume finishes `run`'s work: the head of the history of
/// the run it reproduces (B7, B13).
pub(crate) fn run_to_resume(run: &Path) -> Result<PathBuf, ReplayError> {
    let continued =
        super::replay_archive::DiskReplayArchive::resolve_replayed_run(run.to_path_buf())?.0;
    history_head(&continued)
}

/// Publish `run`'s claim for `successor`, complete or absent: staged and
/// synced under a private name, then linked as the claim, which fails when a
/// claim already exists.
pub(crate) fn publish_claim(
    run: &Path,
    successor: FlowId,
    location: &Path,
) -> Result<(), ReplayError> {
    let location = std::fs::canonicalize(location).map_err(|error| {
        io_error(
            format!("Failed to resolve run location {}", location.display()),
            error,
        )
    })?;
    let claim = run.join(RUN_CONTINUATION_FILENAME);
    let staged = run.join(format!("{RUN_CONTINUATION_FILENAME}.{successor}.staged"));
    let bytes = serde_json::to_vec_pretty(&RunContinuation {
        schema: JOURNAL_SCHEMA_VERSION.to_string(),
        continued_by: ContinuedBy {
            run_id: successor,
            location,
        },
    })
    .map_err(|error| ReplayError::Parse {
        message: format!("Failed to encode continuation claim: {error}"),
    })?;
    let failed = || format!("Failed to publish continuation claim {}", claim.display());
    File::options()
        .write(true)
        .create_new(true)
        .open(&staged)
        .and_then(|mut file| {
            file.write_all(&bytes)?;
            file.sync_all()
        })
        .map_err(|error| not_writable(run, failed(), error))?;
    let published = std::fs::hard_link(&staged, &claim);
    if let Err(error) = std::fs::remove_file(&staged) {
        tracing::warn!(
            staged = %staged.display(),
            %error,
            "could not remove a staged continuation claim"
        );
    }
    match published {
        Ok(()) => File::open(run)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error(failed(), error)),
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            Err(ReplayError::AlreadyContinued {
                archive: run.to_path_buf(),
                head: history_head(run)?,
            })
        }
        Err(error) => Err(not_writable(run, failed(), error)),
    }
}

pub(crate) fn admit(run: &Path, manifest: &RunManifest) -> Result<bool, ReplayError> {
    if has_live_writer(run, manifest)? {
        return Ok(true);
    }
    if read_claim(run)?.is_some() {
        return Err(ReplayError::AlreadyContinued {
            archive: run.to_path_buf(),
            head: history_head(run)?,
        });
    }
    if let Some(resume) = &manifest.resume {
        refuse_orphan(run, manifest, &resume.resumed_from)?;
    }
    refuse_unwritable(run)?;
    Ok(false)
}

/// The journal writers' OS locks are the evidence of a live run; the OS
/// releases them when its process dies.
pub(crate) fn has_live_writer(run: &Path, manifest: &RunManifest) -> Result<bool, ReplayError> {
    for file in super::identity::manifest_journal_files(manifest) {
        let path = run.join(file);
        let journal = match File::open(&path) {
            Ok(journal) => journal,
            Err(error) if error.kind() == ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(io_error(
                    format!("Failed to open journal {}", path.display()),
                    error,
                ))
            }
        };
        match journal.try_lock_shared() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => return Ok(true),
            Err(TryLockError::Error(error)) => {
                return Err(io_error(
                    format!("Failed to probe the writer of {}", path.display()),
                    error,
                ))
            }
        }
    }
    Ok(false)
}

/// Whether a resumed run took over the run it resumed from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Takeover {
    /// The parent's claim names it, or it recorded reconstruction completion,
    /// which it reaches only after claiming. A missing parent is B6's refusal
    /// for a torn run.
    Held,
    /// The parent carries no claim: it stopped before claiming.
    StoppedBefore,
    /// The parent's claim names another run, in the history headed by `head`.
    Lost { head: PathBuf },
}

pub(crate) fn takeover(
    run: &Path,
    manifest: &RunManifest,
    parent: &Path,
) -> Result<Takeover, ReplayError> {
    if !parent.exists() {
        return Ok(Takeover::Held);
    }
    let claim = read_claim(parent)?;
    if claim
        .as_ref()
        .is_some_and(|claim| claim.run_id.to_string() == manifest.flow_id)
        || super::replay_archive::recorded_reconstruction_completion(run, manifest)
    {
        return Ok(Takeover::Held);
    }
    Ok(match claim {
        None => Takeover::StoppedBefore,
        Some(_) => Takeover::Lost {
            head: history_head(parent)?,
        },
    })
}

/// A resumed run that never took over its parent is an orphan that ran no
/// stage; resuming it names the run to resume instead.
fn refuse_orphan(run: &Path, manifest: &RunManifest, parent: &Path) -> Result<(), ReplayError> {
    match takeover(run, manifest, parent)? {
        Takeover::Held => Ok(()),
        Takeover::StoppedBefore => Err(ReplayError::StoppedBeforeTakeover {
            archive: run.to_path_buf(),
            parent: parent.to_path_buf(),
        }),
        Takeover::Lost { head } => Err(ReplayError::ContinuedByAnother {
            archive: run.to_path_buf(),
            parent: parent.to_path_buf(),
            head,
        }),
    }
}

/// Refuse before any successor exists when the location cannot hold a claim.
fn refuse_unwritable(run: &Path) -> Result<(), ReplayError> {
    let probe = run.join(format!(
        "{RUN_CONTINUATION_FILENAME}.{}.probe",
        ulid::Ulid::new()
    ));
    let failed = || format!("Failed to probe run location {}", run.display());
    File::options()
        .write(true)
        .create_new(true)
        .open(&probe)
        .map(drop)
        .map_err(|error| not_writable(run, failed(), error))?;
    std::fs::remove_file(&probe).map_err(|error| io_error(failed(), error))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_dirs(root: &Path, names: &[&str]) -> Vec<PathBuf> {
        names
            .iter()
            .map(|name| {
                let dir = root.join(name);
                std::fs::create_dir(&dir).unwrap();
                std::fs::canonicalize(dir).unwrap()
            })
            .collect()
    }

    fn files(run: &Path) -> Vec<String> {
        let mut names: Vec<_> = std::fs::read_dir(run)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn a_claim_is_published_once_and_never_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let runs = run_dirs(temp.path(), &["parent", "first", "second"]);
        let first = FlowId::new();
        publish_claim(&runs[0], first, &runs[1]).unwrap();
        assert_eq!(
            read_claim(&runs[0]).unwrap(),
            Some(ContinuedBy {
                run_id: first,
                location: runs[1].clone(),
            })
        );
        match publish_claim(&runs[0], FlowId::new(), &runs[2]) {
            Err(ReplayError::AlreadyContinued { archive, head }) => {
                assert_eq!((archive, head), (runs[0].clone(), runs[1].clone()));
            }
            other => panic!("a second successor must lose the claim: {other:?}"),
        }
        assert_eq!(read_claim(&runs[0]).unwrap().map(|c| c.run_id), Some(first));
        assert_eq!(
            files(&runs[0]),
            [RUN_CONTINUATION_FILENAME],
            "no staged claim remains"
        );
    }

    #[test]
    fn concurrent_claims_have_exactly_one_winner() {
        let temp = tempfile::tempdir().unwrap();
        let names: Vec<String> = (0..8).map(|n| format!("successor-{n}")).collect();
        let mut runs = run_dirs(temp.path(), &["parent"]);
        runs.extend(run_dirs(
            temp.path(),
            &names.iter().map(String::as_str).collect::<Vec<_>>(),
        ));
        let (parent, successors) = runs.split_first().unwrap();
        let ids: Vec<FlowId> = successors.iter().map(|_| FlowId::new()).collect();
        let barrier = std::sync::Barrier::new(successors.len());
        let outcomes: Vec<Result<(), ReplayError>> = std::thread::scope(|scope| {
            let racers: Vec<_> = ids
                .iter()
                .zip(successors)
                .map(|(id, location)| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        publish_claim(parent, *id, location)
                    })
                })
                .collect();
            racers
                .into_iter()
                .map(|racer| racer.join().unwrap())
                .collect()
        });
        let winners: Vec<FlowId> = ids
            .iter()
            .zip(&outcomes)
            .filter(|(_, outcome)| outcome.is_ok())
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(winners.len(), 1, "{outcomes:?}");
        assert_eq!(
            read_claim(parent).unwrap().map(|c| c.run_id),
            Some(winners[0])
        );
        assert!(outcomes
            .iter()
            .all(|outcome| matches!(outcome, Ok(()) | Err(ReplayError::AlreadyContinued { .. }))));
        assert_eq!(files(parent), [RUN_CONTINUATION_FILENAME]);
    }

    #[test]
    fn the_head_follows_claims_and_is_named_when_missing() {
        let temp = tempfile::tempdir().unwrap();
        let runs = run_dirs(temp.path(), &["r0", "r1", "r2"]);
        assert_eq!(history_head(&runs[0]).unwrap(), runs[0]);
        publish_claim(&runs[0], FlowId::new(), &runs[1]).unwrap();
        publish_claim(&runs[1], FlowId::new(), &runs[2]).unwrap();
        assert_eq!(history_head(&runs[0]).unwrap(), runs[2]);
        std::fs::remove_dir_all(&runs[2]).unwrap();
        assert_eq!(history_head(&runs[0]).unwrap(), runs[2]);
    }

    #[test]
    fn a_staged_claim_is_absent_and_a_malformed_one_refuses() {
        let temp = tempfile::tempdir().unwrap();
        let run = temp.path();
        let staged = run.join(format!(
            "{RUN_CONTINUATION_FILENAME}.{}.staged",
            FlowId::new()
        ));
        std::fs::write(staged, b"{").unwrap();
        assert_eq!(read_claim(run).unwrap(), None);

        let claim = run.join(RUN_CONTINUATION_FILENAME);
        let foreign = serde_json::json!({
            "schema": "0.0",
            "continued_by": { "run_id": FlowId::new(), "location": run },
        });
        for body in [
            b"not json".to_vec(),
            vec![b' '; CLAIM_BUDGET as usize + 1],
            serde_json::to_vec(&foreign).unwrap(),
        ] {
            std::fs::write(&claim, body).unwrap();
            assert!(matches!(read_claim(run), Err(ReplayError::Parse { .. })));
        }
    }

    #[cfg(unix)]
    #[test]
    fn an_unwritable_location_refuses_before_and_at_publication() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let runs = run_dirs(temp.path(), &["run", "successor"]);
        std::fs::set_permissions(&runs[0], std::fs::Permissions::from_mode(0o555)).unwrap();
        let probed = refuse_unwritable(&runs[0]);
        let published = publish_claim(&runs[0], FlowId::new(), &runs[1]);
        std::fs::set_permissions(&runs[0], std::fs::Permissions::from_mode(0o755)).unwrap();
        for outcome in [probed, published] {
            assert!(
                matches!(outcome, Err(ReplayError::LocationNotWritable { ref archive }) if archive == &runs[0]),
                "{outcome:?}"
            );
        }
        refuse_unwritable(&runs[0]).unwrap();
        assert!(
            files(&runs[0]).is_empty(),
            "the probe leaves nothing behind"
        );
    }
}
