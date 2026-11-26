// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The operator diagnostic for a replay that reached the end of its recording
//! (FLOWIP-134i B13), projected from the replay run's stage-fatal fact.

use std::path::{Path, PathBuf};

use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{ChainPayload, StageFatalReason, StageFatalRecorded};
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::archive_actions::{
    decide, ArchiveEvidence, ArchiveOverrides, ArchiveRefusal, FlowEvidence, ReplayVerb,
};
use obzenflow_core::journal::read::RunRecordData;

use crate::application::ApplicationError;
use crate::journal::disk::continuation::{has_live_writer, run_to_resume};
use crate::journal::disk::replay_archive::DiskReplayArchive;
use crate::journal::read::{open_disk_run, JournalReadError};

/// Replace a failed replay's error with the end-of-recording refusal when the
/// replay run journalled one; any other failure passes through unchanged.
pub(super) async fn refine_replay_failure(
    error: ApplicationError,
    replayed: &Path,
    replay_run: &Path,
    flow: &FlowEvidence,
) -> ApplicationError {
    match recording_end(replay_run).await {
        Ok(Some(end)) => ApplicationError::ReplayRecordingEnded {
            replayed: replayed.to_path_buf(),
            stage_key: end.stage_key,
            input_position: end.input_position,
            resume: resume_of(replayed, flow).await,
        },
        Ok(None) => error,
        Err(read_error) => {
            tracing::warn!(
                run = %replay_run.display(),
                error = %read_error,
                "could not read the failed replay's stage-fatal facts"
            );
            error
        }
    }
}

async fn resume_of(replayed: &Path, flow: &FlowEvidence) -> Result<PathBuf, ArchiveRefusal> {
    let head = run_to_resume(replayed).unwrap_or_else(|error| {
        tracing::warn!(
            run = %replayed.display(),
            %error,
            "could not resolve the head of the replayed run's history"
        );
        replayed.to_path_buf()
    });
    match head_evidence(&head).await {
        Ok(evidence) => decide(
            ReplayVerb::Resume,
            &head,
            &evidence,
            flow,
            ArchiveOverrides::default(),
        )
        .map(|()| head),
        Err(error) => {
            tracing::warn!(
                run = %head.display(),
                %error,
                "could not read the evidence for a resume of the head of the history"
            );
            Ok(head)
        }
    }
}

async fn head_evidence(head: &Path) -> Result<ArchiveEvidence, ReplayError> {
    let (run, manifest) = DiskReplayArchive::read_manifest(head.to_path_buf())?;
    let live_writer = has_live_writer(&run, &manifest)?;
    let evidence = DiskReplayArchive::written_run_evidence(run, ReplayVerb::Resume).await?;
    Ok(ArchiveEvidence {
        live_writer,
        ..evidence
    })
}

/// The replay run's end-of-recording fact, the first by stage key when more
/// than one stage reached the end of its recording.
async fn recording_end(run_dir: &Path) -> Result<Option<StageFatalRecorded>, JournalReadError> {
    let mut snapshot = open_disk_run(run_dir).await?;
    let mut end: Option<StageFatalRecorded> = None;
    while let Some(record) = snapshot.next().await? {
        let RunRecordData::Chain(row) = &record.record else {
            continue;
        };
        let ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) = &row.payload
        else {
            continue;
        };
        if fatal.reason == StageFatalReason::RecordingEnded
            && end
                .as_ref()
                .is_none_or(|first| fatal.stage_key < first.stage_key)
        {
            end = Some(fatal.clone());
        }
    }
    Ok(end)
}
