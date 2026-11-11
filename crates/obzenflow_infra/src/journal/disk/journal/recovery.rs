// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::codec::Decoder;
use super::super::scanner::{
    classify_frame, damage_path, dispose, read_frame_sync, tail_after, Disposition,
    InterruptedAppend, ReadPolicy,
};
use obzenflow_core::event::{JournalClock, JournalEvent};
use obzenflow_core::journal::journal_error::JournalError;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::{BufReader, Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use ulid::Ulid;

static RECOVERED_TORN_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(super) struct CommittedFrames {
    pub(super) last_commit: Option<JournalClock>,
    pub(super) last_frame: u64,
    pub(super) committed_end: u64,
    pub(super) interrupted: Option<InterruptedAppend>,
}

pub(super) fn open_append_file(
    path: &Path,
    interrupted: Option<InterruptedAppend>,
) -> Result<StdFile, JournalError> {
    let file = StdFile::options()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| JournalError::Implementation {
            message: format!("Failed to open log file for append: {}", path.display()),
            source: Box::new(e),
        })?;
    if let Some(interrupted) = interrupted {
        recover_torn_tail(&file, path, interrupted)?;
    }
    Ok(file)
}

fn recover_torn_tail(
    file: &StdFile,
    path: &Path,
    interrupted: InterruptedAppend,
) -> Result<(), JournalError> {
    let committed_end = interrupted.committed_end;
    let failure = |step: &str, error: std::io::Error| JournalError::Implementation {
        message: format!(
            "Failed to {step} for the interrupted append at offset {committed_end} of {}",
            path.display()
        ),
        source: Box::new(error),
    };
    let mut tail = StdFile::open(path).map_err(|error| failure("open the journal", error))?;
    std::io::Seek::seek(&mut tail, SeekFrom::Start(committed_end))
        .map_err(|error| failure("seek the journal", error))?;
    let (damage_file, mut damage) = create_damage_file(path, committed_end)
        .map_err(|error| failure("create a damage file", error))?;
    let moved_bytes = std::io::copy(&mut tail, &mut damage)
        .map_err(|error| failure("copy the bytes to a damage file", error))?;
    #[cfg(not(unix))]
    {
        let mut permissions = damage
            .metadata()
            .map_err(|error| failure("inspect the damage file", error))?
            .permissions();
        permissions.set_readonly(true);
        damage
            .set_permissions(permissions)
            .map_err(|error| failure("make the damage file read-only", error))?;
    }
    damage
        .sync_all()
        .map_err(|error| failure("sync the damage file", error))?;
    if let Some(directory) = damage_file.parent() {
        StdFile::open(directory)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| failure("sync the damage file's directory", error))?;
    }
    file.set_len(committed_end)
        .map_err(|error| failure("truncate the journal", error))?;
    file.sync_data()
        .map_err(|error| failure("sync the truncated journal", error))?;
    RECOVERED_TORN_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        path = %path.display(),
        committed_end,
        moved_bytes,
        tail_kind = %interrupted.kind,
        damage_file = %damage_file.display(),
        recovered_torn_frames_total = RECOVERED_TORN_FRAMES_TOTAL.load(Ordering::Relaxed),
        "moved an interrupted journal append to a damage file during recovery"
    );
    Ok(())
}

fn create_damage_file(path: &Path, committed_end: u64) -> std::io::Result<(PathBuf, StdFile)> {
    let mut options = StdFile::options();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o444);
    }
    let first = damage_path(path, committed_end);
    let mut candidate = first.clone();
    let mut sequence = 1u64;
    loop {
        match options.open(&candidate) {
            Ok(file) => return Ok((candidate, file)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                sequence += 1;
                candidate = first.with_extension(format!("{sequence}.damage"));
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn scan_committed_frames<T: JournalEvent>(
    log_path: &Path,
    confirmed_end: Option<u64>,
    mut index: Option<&mut HashMap<Ulid, u64>>,
) -> Result<CommittedFrames, JournalError> {
    let mut admission = super::super::identity::CommitmentAdmission::open(log_path)?;
    let mut last_commit: Option<JournalClock> = None;
    let mut last_frame = 0u64;

    if !log_path.exists() {
        return Ok(CommittedFrames {
            last_commit,
            last_frame,
            committed_end: 0,
            interrupted: None,
        });
    }

    let file = StdFile::open(log_path).map_err(|e| JournalError::Implementation {
        message: "Failed to open log file".to_string(),
        source: Box::new(e),
    })?;
    let mut reader = BufReader::new(file.take(confirmed_end.unwrap_or(u64::MAX)));
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut committed_end = 0u64;
    let mut interrupted = None;
    let mut decoder = Decoder::new(log_path);

    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|e| JournalError::Implementation {
            message: "Failed to read line".to_string(),
            source: Box::new(e),
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        match dispose(
            classify_frame::<T>(&buf, &mut decoder, record_offset),
            termination,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: confirmed_end.is_none(),
            },
            || tail_after(log_path, &buf, record_offset, &mut reader),
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    let commitment = admission.admit(&record)?;
                    if let Some(index) = index.as_deref_mut() {
                        index.insert(record.id().as_ulid(), record_offset);
                    }
                    last_commit = Some(commitment);
                }
                last_frame = record_offset;
                committed_end = offset;
            }
            Disposition::Interrupted(append) => {
                interrupted = Some(append);
                break;
            }
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                return Err(JournalError::Implementation {
                    message: format!(
                        "Corrupt record while scanning committed frames at offset {record_offset} in {}: {problem}",
                        log_path.display()
                    ),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        problem.to_string(),
                    )),
                });
            }
        }
    }

    if confirmed_end.is_some_and(|end| committed_end != end) {
        return Err(JournalError::Implementation {
            message: "Journal ended before its confirmed boundary".into(),
            source: "missing committed records".into(),
        });
    }
    Ok(CommittedFrames {
        last_commit,
        last_frame,
        committed_end,
        interrupted,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::disk::codec;
    use crate::journal::disk::log_record::serialize_atomic_group;
    use crate::journal::disk::DiskJournal;
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::event::provenance::JournalGroupMember;
    use obzenflow_core::event::CausalCoordinate;
    use obzenflow_core::id::{JournalId, StageId};
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::{AppendOptions, Journal};
    use obzenflow_core::{FlowId, WriterId};
    use obzenflow_runtime::runtime_config::JournalSync;

    use uuid::Uuid;

    #[tokio::test]
    async fn atomic_group_torn_tail_is_invisible_at_every_byte_boundary() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/atomic_torn_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("group.log");
        let journal_id = JournalId::new();
        let identity = crate::journal::disk::identity::JournalIdentity {
            run_id: FlowId::new(),
            journal_id,
        };
        let mut previous = None;
        crate::journal::disk::identity::write_fixture_identity(&log_path, identity);
        let writer_id = WriterId::from(StageId::new());
        let records: Vec<_> = (0..3)
            .map(|index| {
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "atomic.member",
                    std::num::NonZeroU32::MIN,
                    serde_json::json!({ "index": index }),
                );
                let mut record =
                    crate::journal::disk::identity::fixture_record(identity, event, &mut previous);
                record.envelope.provenance.journal.journal_group_id =
                    Some("effect-outcome:test".into());
                record.envelope.provenance.journal.journal_group_member =
                    Some(JournalGroupMember { index, size: 3 });
                record
            })
            .collect();
        let frame = serialize_atomic_group("effect-outcome:test", &records).unwrap();

        let damage = test_dir.join("group.log.0.damage");
        for cut in 0..frame.len() {
            std::fs::write(&log_path, &frame[..cut]).unwrap();
            let reopened = DiskJournal::<ChainEvent>::with_owner(
                log_path.clone(),
                obzenflow_core::JournalOwner::stage(StageId::new()),
                JournalSync::BeforeAck,
            )
            .unwrap_or_else(|error| panic!("torn group at byte {cut} should recover: {error}"));
            assert!(
                reopened.read_all_unordered().await.unwrap().is_empty(),
                "no member may be visible after recovery from a tear at byte {cut}"
            );
            drop(reopened);
            if cut > 0 {
                assert_eq!(std::fs::read(&damage).unwrap(), &frame[..cut]);
                std::fs::remove_file(&damage).unwrap();
            }
        }

        std::fs::remove_dir_all(&test_dir).unwrap();
    }

    async fn journal_with_tail(directory: &Path, tail: &[u8]) -> (PathBuf, u64, StageId) {
        let path = directory.join("system.log");
        let stage = StageId::new();
        let journal = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        )
        .unwrap();
        for index in 0..2 {
            journal
                .append(
                    crate::journal::observability::tests::event(stage, index + 1),
                    Default::default(),
                )
                .await
                .unwrap();
        }
        drop(journal);
        let committed_end = std::fs::metadata(&path).unwrap().len();
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(tail);
        std::fs::write(&path, bytes).unwrap();
        (path, committed_end, stage)
    }

    #[tokio::test]
    async fn writer_open_torn_tail_moves_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let torn: Vec<u8> = [&codec::frame::MAGIC[..], &[7; 200]].concat();
        let (path, committed_end, stage) = journal_with_tail(directory.path(), &torn).await;

        let reopened = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        )
        .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), committed_end);
        assert_eq!(reopened.read_all_unordered().await.unwrap().len(), 2);
        let damage = directory
            .path()
            .join(format!("system.log.{committed_end}.damage"));
        assert_eq!(std::fs::read(&damage).unwrap(), torn);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&damage).unwrap().permissions().mode() & 0o777,
                0o444
            );
        }
        reopened
            .append(
                crate::journal::observability::tests::event(stage, 3),
                Default::default(),
            )
            .await
            .unwrap();
        assert_eq!(reopened.read_all_unordered().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn writer_open_existing_damage_file_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let (path, committed_end, stage) = journal_with_tail(directory.path(), &[0; 4096]).await;
        let earlier = directory
            .path()
            .join(format!("system.log.{committed_end}.damage"));
        std::fs::write(&earlier, b"earlier interruption").unwrap();

        let reopened = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        )
        .unwrap();
        assert_eq!(reopened.read_all_unordered().await.unwrap().len(), 2);
        assert_eq!(std::fs::read(&earlier).unwrap(), b"earlier interruption");
        assert_eq!(
            std::fs::read(
                directory
                    .path()
                    .join(format!("system.log.{committed_end}.2.damage"))
            )
            .unwrap(),
            vec![0; 4096]
        );
    }

    #[tokio::test]
    async fn writer_open_damage_before_frame_refused() {
        let directory = tempfile::tempdir().unwrap();
        let (path, committed_end, stage) = journal_with_tail(directory.path(), &[]).await;
        let mut bytes = std::fs::read(&path).unwrap();
        let first_end = {
            let length = codec::frame::frame_length(&bytes).unwrap();
            bytes[codec::frame::HEADER_LEN + 2] ^= 0xff;
            length
        };
        std::fs::write(&path, &bytes).unwrap();

        let refused = DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::BeforeAck,
        );
        let Err(error) = refused else {
            panic!("damage that a valid frame follows must refuse the writer");
        };
        assert!(
            error
                .to_string()
                .contains(&format!("a valid frame follows at offset {first_end}")),
            "{error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes, "nothing is moved");
        assert!(!directory.path().join("system.log.0.damage").exists());
        assert_eq!(committed_end as usize, bytes.len());
    }

    #[tokio::test]
    async fn with_owner_reopen_rebuilds_index_and_last_commit() {
        // FLOWIP-120q P3: reopening an owned framed journal must rebuild the
        // index (so read_event by id works) and the journal clocks (so a new
        // append continues the sequence) via the framed parser.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/with_owner_reopen_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("reopen.log");
        let writer_id = WriterId::from(StageId::new());

        let kept_id;
        {
            let log = DiskJournal::<ChainEvent>::with_owner(
                log_path.clone(),
                obzenflow_core::JournalOwner::stage(StageId::new()),
                JournalSync::BeforeAck,
            )
            .unwrap();
            let e1 = ChainEventFactory::data_event(
                writer_id,
                "a",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"n": 1}),
            );
            let env1 = log.append(e1, Default::default()).await.unwrap();
            let e2 = ChainEventFactory::data_event(
                writer_id,
                "b",
                std::num::NonZeroU32::MIN,
                serde_json::json!({"n": 2}),
            );
            log.append(e2, AppendOptions::from_record(Some(&env1)).unwrap())
                .await
                .unwrap();
            kept_id = env1.envelope.provenance.event.id;
        }

        let reopened = DiskJournal::<ChainEvent>::with_owner(
            log_path,
            obzenflow_core::JournalOwner::stage(StageId::new()),
            JournalSync::BeforeAck,
        )
        .unwrap();

        assert!(
            reopened.read_event(&kept_id).await.unwrap().is_some(),
            "read_event by id must work after reopen (index rebuilt)"
        );
        assert_eq!(reopened.read_causally_ordered().await.unwrap().len(), 2);

        let e3 = ChainEventFactory::data_event(
            writer_id,
            "c",
            std::num::NonZeroU32::MIN,
            serde_json::json!({"n": 3}),
        );
        let env3 = reopened.append(e3, Default::default()).await.unwrap();
        assert!(
            env3.envelope
                .provenance
                .journal
                .vector_clock
                .get(&CausalCoordinate::new((*reopened.id()).into()))
                >= 3,
            "writer clock must continue after reopen, not reset"
        );

        std::fs::remove_dir_all(&test_dir).unwrap();
    }
}
