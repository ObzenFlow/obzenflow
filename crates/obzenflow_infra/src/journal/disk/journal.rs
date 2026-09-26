// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Single event log per flow execution
//!
//! Provides optimal sequential writes and natural event ordering

use super::codec::{self, Decoder, DefinitionStore};
#[cfg(test)]
use super::log_record::serialize_atomic_group;
use super::reader::DiskJournalReader;
use super::reverse_reader::ReverseFrameReader;
use super::scanner::{
    classify_frame, dispose, read_frame_async, read_frame_sync, Disposition, ReadPolicy,
};
use crate::journal::observability::JournalObservability;
use async_trait::async_trait;
use chrono::Utc;
use obzenflow_core::event::identity::{EventId, JournalWriterId};
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::provenance::{JournalGroupMember, JournalProvenance};
use obzenflow_core::event::JournalEvent;
use obzenflow_core::event::{CausalCommit, CausalCoordinate, CausalFrontier};
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::journal::{AppendOptions, Journal, JournalConfig};
use obzenflow_core::FlowId;
#[cfg(test)]
use obzenflow_core::WriterId;
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::{BufReader, Read, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::fs::File;
use tokio::io::AsyncSeekExt;
use tokio::sync::RwLock;
use ulid::Ulid;

/// Global registry of per-path read/write locks so all DiskJournal instances
/// that point at the same file coordinate access and prevent torn reads.
type SharedJournalState = (Arc<RwLock<()>>, JournalObservability);
static JOURNAL_LOCKS: OnceLock<Mutex<HashMap<PathBuf, SharedJournalState>>> = OnceLock::new();
static RECOVERED_TORN_FRAMES_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(super) fn shared_state_for_path(path: &Path) -> (Arc<RwLock<()>>, JournalObservability) {
    // FLOWIP-120q: key by a normalized absolute path so the same file maps to one
    // lock regardless of how the path is expressed (relative vs absolute), so a
    // reader and a writer that reach the same file by different spellings still
    // coordinate. Normalizing never touches the filesystem; fall back to the raw
    // path if it fails (e.g., no current dir).
    let key = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let registry =
        JOURNAL_LOCKS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()));
    let mut guard = registry.lock().unwrap();
    guard
        .entry(key)
        .or_insert_with(|| (Arc::new(RwLock::new(())), JournalObservability::default()))
        .clone()
}

/// Single append-only log for a flow execution
///
/// Uses a mutex to ensure atomic writes from multiple writers
pub struct DiskJournal<T: JournalEvent> {
    /// Owner of this journal (if any)
    owner: Option<JournalOwner>,
    /// Journal ID for this instance
    journal_id: JournalId,
    run_id: FlowId,
    /// Path to the log file
    path: PathBuf,
    /// Shared synchronous file handle for appends (opened once)
    write_file: Arc<Mutex<StdFile>>,
    /// In-memory index: event_id -> file offset
    index: Arc<RwLock<HashMap<Ulid, u64>>>,
    /// Disposable known frame boundary for bounded reverse-tail discovery.
    last_frame: Arc<AtomicU64>,
    /// Shared lock to coordinate readers/writers
    ///
    /// Writers take a write lock; readers take a read lock to avoid torn lines.
    read_write_lock: Arc<RwLock<()>>,
    /// Last committed record of this physical journal, independent of authorship
    last_commit: Arc<RwLock<Option<CausalCommit>>>,
    /// Set when a failed append could not be rolled back, leaving the file in an
    /// unknown state. Further appends are rejected until the journal is reopened.
    poisoned: Arc<AtomicBool>,
    /// Flow-shared admission sequencer (FLOWIP-120n F18). When present, appends
    /// stamp sequence-less events under the write lock, so sequence order
    /// equals append order. None outside factory-built flow journals.
    admission_sequencer: Option<Arc<AtomicU64>>,
    definitions: DefinitionStore,
    observations: super::observations::DiskObservationReader<T>,
    observability: JournalObservability,
    _phantom: std::marker::PhantomData<T>,
}

/// A successfully committed append: the byte offset the record was written at
/// and the resulting end-of-file offset.
#[derive(Debug)]
struct CommittedAppend {
    offset: u64,
    next_offset: u64,
}

/// Why an append's file write did not commit.
#[derive(Debug)]
enum AppendFailure {
    /// The write failed and the file was truncated back to the pre-append EOF,
    /// so the journal is still usable.
    RolledBack(JournalError),
    /// The write failed and the rollback also failed, so the file may carry a
    /// partial record. The journal is unsafe to append to until reopened.
    Poisoned(JournalError),
}

/// The append file operations, behind a trait so the commit/rollback logic is
/// testable with a fake writer instead of induced filesystem failures.
trait FrameSink {
    fn end_offset(&mut self) -> std::io::Result<u64>;
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    fn flush(&mut self) -> std::io::Result<()>;
    fn rollback_to(&mut self, offset: u64) -> std::io::Result<()>;
}

impl FrameSink for StdFile {
    fn end_offset(&mut self) -> std::io::Result<u64> {
        use std::io::Seek;
        self.seek(SeekFrom::End(0))
    }
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        Write::write_all(self, bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        use std::io::Write;
        Write::flush(self)
    }
    fn rollback_to(&mut self, offset: u64) -> std::io::Result<()> {
        use std::io::Seek;
        self.set_len(offset)?;
        self.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

/// Append one framed record, committing the EOF only after write+flush succeed.
/// The offset is read from the file (not a speculative cursor) under the caller's
/// lock. On write failure the file is truncated back to the pre-append EOF; if
/// that rollback also fails the journal must be poisoned, because a partial
/// record may remain that a later append would turn into mid-file corruption.
fn append_frame<S: FrameSink>(
    sink: &mut S,
    bytes: &[u8],
    path: &Path,
) -> Result<CommittedAppend, AppendFailure> {
    let offset = sink.end_offset().map_err(|e| {
        // Nothing was written, so the file is unchanged and still usable.
        AppendFailure::RolledBack(JournalError::Implementation {
            message: format!(
                "Failed to seek journal end before append: {}",
                path.display()
            ),
            source: Box::new(e),
        })
    })?;

    match sink.write_all(bytes).and_then(|()| sink.flush()) {
        Ok(()) => Ok(CommittedAppend {
            offset,
            next_offset: offset + bytes.len() as u64,
        }),
        Err(write_error) => match sink.rollback_to(offset) {
            Ok(()) => Err(AppendFailure::RolledBack(JournalError::Implementation {
                message: format!(
                    "Failed to append record to {}; rolled back to offset {offset}",
                    path.display()
                ),
                source: Box::new(write_error),
            })),
            Err(rollback_error) => Err(AppendFailure::Poisoned(JournalError::Implementation {
                message: format!(
                    "Failed to append record to {}; rollback to offset {offset} also failed: {rollback_error}",
                    path.display()
                ),
                source: Box::new(write_error),
            })),
        },
    }
}

impl<T: JournalEvent> DiskJournal<T> {
    /// Create a new flow event log
    pub fn new(base_path: PathBuf, flow_id: &str) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&base_path).map_err(|e| JournalError::Implementation {
            message: "Failed to create directory".to_string(),
            source: Box::new(e),
        })?;
        let log_path = base_path.join(format!("{flow_id}.log"));

        Self::open(log_path, None, None)
    }

    pub fn with_owner(log_path: PathBuf, owner: JournalOwner) -> Result<Self, JournalError> {
        Self::open(log_path, Some(owner), None)
    }

    pub fn with_owner_in_run(
        log_path: PathBuf,
        owner: JournalOwner,
        run_id: FlowId,
    ) -> Result<Self, JournalError> {
        Self::open(log_path, Some(owner), Some(run_id))
    }

    fn open(
        log_path: PathBuf,
        owner: Option<JournalOwner>,
        run_id: Option<FlowId>,
    ) -> Result<Self, JournalError> {
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| JournalError::Implementation {
                message: "Create journal directory".into(),
                source: error.into(),
            })?;
        }
        let (identity, writer_lease) = super::identity::open_identity(&log_path, run_id)?;
        let (observations, (write_file, index, last_commit)) =
            super::observations::DiskObservationReader::open_writer(log_path.clone(), |end| {
                let (index, last_commit, committed_end) =
                    rebuild_index_from_path::<T>(&log_path, end)?;
                for commitment in last_commit.iter() {
                    if commitment.reference.run_id != identity.run_id
                        || commitment.reference.journal_writer_id.as_journal_id()
                            != &identity.journal_id
                    {
                        return Err(
                            obzenflow_core::event::CausalError::ConflictingCommitment.into()
                        );
                    }
                }
                // Recovery may truncate a torn suffix. Keep the locked original
                // descriptor as the one append FD shared by every journal clone.
                drop(open_append_file(&log_path, end, committed_end)?);
                Ok(((writer_lease, index, last_commit), committed_end))
            })?;
        Ok(Self {
            owner,
            definitions: DefinitionStore::for_archive(&log_path),
            journal_id: identity.journal_id,
            run_id: identity.run_id,
            observations,
            path: log_path.clone(),
            write_file: Arc::new(Mutex::new(write_file)),
            last_frame: Arc::new(AtomicU64::new(index.values().copied().max().unwrap_or(0))),
            index: Arc::new(RwLock::new(index)),
            read_write_lock: shared_state_for_path(&log_path).0,
            observability: shared_state_for_path(&log_path).1,
            last_commit: Arc::new(RwLock::new(last_commit)),
            poisoned: Arc::new(AtomicBool::new(false)),
            admission_sequencer: None,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Attach the flow-shared admission sequencer (FLOWIP-120n F18).
    pub fn with_admission_sequencer(mut self, sequencer: Arc<AtomicU64>) -> Self {
        self.admission_sequencer = Some(sequencer);
        self
    }
}

/// In-memory index (`event_id -> byte offset`) plus the last physical journal commitment
/// rebuilt from a journal file.
type RebuiltIndex = (HashMap<Ulid, u64>, Option<CausalCommit>, u64);

fn open_append_file(
    path: &Path,
    shared_end: Option<u64>,
    recovered_end: u64,
) -> Result<StdFile, JournalError> {
    let file = StdFile::options()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| JournalError::Implementation {
            message: format!("Failed to open log file for append: {}", path.display()),
            source: Box::new(e),
        })?;
    if shared_end.is_none() {
        recover_torn_tail(&file, path, recovered_end)?;
    }
    Ok(file)
}

fn recover_torn_tail(file: &StdFile, path: &Path, committed_end: u64) -> Result<(), JournalError> {
    let physical_len = file
        .metadata()
        .map_err(|error| JournalError::Implementation {
            message: format!("Failed to inspect journal tail: {}", path.display()),
            source: Box::new(error),
        })?
        .len();
    if physical_len <= committed_end {
        return Ok(());
    }
    file.set_len(committed_end)
        .map_err(|error| JournalError::Implementation {
            message: format!(
                "Failed to remove uncommitted journal tail at offset {committed_end}: {}",
                path.display()
            ),
            source: Box::new(error),
        })?;
    RECOVERED_TORN_FRAMES_TOTAL.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        path = %path.display(),
        committed_end,
        removed_bytes = physical_len - committed_end,
        recovered_torn_frames_total = RECOVERED_TORN_FRAMES_TOTAL.load(Ordering::Relaxed),
        "removed an uncommitted torn journal frame during recovery"
    );
    Ok(())
}

/// Rebuild a disk journal's in-memory index and writer clocks from the framed
/// log on disk (FLOWIP-120q). Shared by `DiskJournal::new` and
/// `DiskJournal::with_owner` so the two constructors cannot diverge. Uses the
/// sealed full-scan policy: fail loud on committed corruption, tolerate only a
/// final torn tail (a crash mid-append leaves an unterminated last record).
fn rebuild_index_from_path<T: JournalEvent>(
    log_path: &Path,
    confirmed_end: Option<u64>,
) -> Result<RebuiltIndex, JournalError> {
    let identity = super::identity::read_identity(log_path)?;
    let mut index = HashMap::with_capacity(10000);
    let mut last_commit: Option<CausalCommit> = None;

    if !log_path.exists() {
        return Ok((index, last_commit, 0));
    }

    let file = StdFile::open(log_path).map_err(|e| JournalError::Implementation {
        message: "Failed to open log file".to_string(),
        source: Box::new(e),
    })?;
    let mut reader = BufReader::new(file.take(confirmed_end.unwrap_or(u64::MAX)));
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut committed_end = 0u64;
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
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    let commitment = CausalCommit::from_record(&record)?;
                    let previous = last_commit.as_ref().map(|previous| previous.reference);
                    if record.envelope.provenance.journal.causal.previous != previous {
                        return Err(
                            obzenflow_core::event::CausalError::ConflictingCommitment.into()
                        );
                    }
                    if commitment.reference.run_id != identity.run_id
                        || commitment.reference.journal_writer_id.as_journal_id()
                            != &identity.journal_id
                    {
                        return Err(
                            obzenflow_core::event::CausalError::ConflictingCommitment.into()
                        );
                    }
                    index.insert(record.id().as_ulid(), record_offset);
                    last_commit = Some(commitment);
                }
                committed_end = offset;
            }
            // A tolerated torn tail ends the committed records.
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                return Err(JournalError::Implementation {
                    message: format!(
                        "Corrupt record while rebuilding index at offset {record_offset} in {}: {problem}",
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
    Ok((index, last_commit, committed_end))
}

impl<T: JournalEvent> Clone for DiskJournal<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            journal_id: self.journal_id,
            run_id: self.run_id,
            path: self.path.clone(),
            write_file: self.write_file.clone(),
            index: self.index.clone(),
            last_frame: self.last_frame.clone(),
            read_write_lock: self.read_write_lock.clone(),
            last_commit: self.last_commit.clone(),
            poisoned: self.poisoned.clone(),
            admission_sequencer: self.admission_sequencer.clone(),
            definitions: self.definitions.clone(),
            observations: self.observations.clone(),
            observability: self.observability.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

// The complete commit unit outlives a cancelled waiter. Runtime retains its
// own receipt until this task has finalised clocks and indexes as well as bytes.
async fn retain_commit<R: Send + 'static>(
    poisoned: Arc<AtomicBool>,
    operation: impl std::future::Future<Output = Result<R, JournalError>> + Send + 'static,
) -> Result<R, JournalError> {
    use futures::FutureExt;
    tokio::spawn(async move {
        match std::panic::AssertUnwindSafe(operation).catch_unwind().await {
            Ok(result) => result,
            Err(_) => {
                poisoned.store(true, Ordering::SeqCst);
                Err(JournalError::CommitIndeterminate {
                    source: std::io::Error::other("journal commit task panicked").into(),
                })
            }
        }
    })
    .await
    .map_err(|error| JournalError::CommitIndeterminate {
        source: error.into(),
    })?
}

impl<T: JournalEvent + 'static> DiskJournal<T> {
    async fn append_record(
        &self,
        mut event: T,
        frontier: &CausalFrontier,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        crate::journal::ensure_owned(self.owner.as_ref())?;
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(JournalError::Implementation {
                message: format!(
                    "Journal {} is poisoned after a failed append rollback; reopen to continue",
                    self.path.display()
                ),
                source: "poisoned journal".into(),
            });
        }

        // Acquire the journal write lock before advancing the journal clock.
        //
        // This serialises append operations and ensures concurrent appends
        // cannot compute the same `writer_seq` from a stale snapshot.
        let _lock = self.read_write_lock.write().await;
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(JournalError::Implementation {
                message: "journal publication is closed after indeterminate commit".into(),
                source: "poisoned journal".into(),
            });
        }

        // FLOWIP-120n F18: stamp under the write lock so sequence order equals
        // append order; re-admitted rows already carry theirs and keep it.
        if let Some(sequencer) = &self.admission_sequencer {
            if event.admission_seq().is_none() {
                event.set_admission_seq(obzenflow_core::AdmissionSeq(
                    sequencer.fetch_add(1, Ordering::Relaxed),
                ));
            }
        }

        // Compute the journal's next clock under the write lock. The helper is
        // store-free, so the journal clock is committed only after the file write
        // and flush succeed below.
        let (commitment, causal) = {
            let last_commit = self.last_commit.read().await;
            CausalCommit::prepare(
                self.run_id,
                CausalCoordinate::new(self.journal_id.into()),
                *event.id(),
                last_commit.as_ref(),
                frontier,
            )?
        };

        // Create envelope
        let envelope = {
            let (authored, payload) = event.clone().into_parts();
            JournalRecord::commit(
                authored,
                payload,
                JournalProvenance {
                    journal_writer_id: JournalWriterId::from(self.journal_id),
                    run_id: self.run_id,
                    causal,
                    vector_clock: commitment.clock.clone(),
                    timestamp: Utc::now(),
                    journal_group_id: None,
                    journal_group_member: None,
                },
            )
            .map_err(|error| JournalError::Implementation {
                message: "Invalid journal record".to_string(),
                source: Box::new(error),
            })?
        };

        // Create log record
        let record = envelope.clone();

        let mut prepared = codec::prepare(
            std::slice::from_ref(&record),
            None,
            &self.path,
            self.definitions.clone(),
        )
        .map_err(|e| JournalError::Implementation {
            message: "Failed to serialize record".to_string(),
            source: Box::new(e),
        })?;

        // Write the framed record on a blocking thread. The commit point is the
        // successful write+flush: only then do the index and the journal clock
        // advance. On write failure the file rolls back to the pre-append EOF;
        // if rollback fails the journal is poisoned.
        let path = self.path.clone();
        let write_file = self.write_file.clone();
        let write_bytes = std::mem::take(&mut prepared.bytes);
        let trailer = write_bytes.len() - codec::frame::TRAILER_LEN;
        let observation_crc =
            u32::from_le_bytes(write_bytes[trailer..trailer + 4].try_into().unwrap());

        let outcome =
            tokio::task::spawn_blocking(move || -> Result<CommittedAppend, AppendFailure> {
                let mut file = match write_file.lock() {
                    Ok(guard) => guard,
                    Err(e) => {
                        // A poisoned mutex means a prior writer panicked
                        // mid-append; the file may carry a partial record.
                        return Err(AppendFailure::Poisoned(JournalError::Implementation {
                            message: format!("Failed to lock journal file: {}", path.display()),
                            source: Box::new(std::io::Error::other(format!("Mutex poisoned: {e}"))),
                        }));
                    }
                };
                append_frame(&mut *file, &write_bytes, &path)
            })
            .await;

        let committed = match outcome {
            Ok(Ok(committed)) => committed,
            Ok(Err(AppendFailure::RolledBack(e))) => return Err(e),
            Ok(Err(AppendFailure::Poisoned(e))) => {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(JournalError::CommitIndeterminate {
                    source: Box::new(e),
                });
            }
            Err(join_error) => {
                // The blocking task panicked or was cancelled, so the file state
                // is unknown: poison rather than risk appending past a partial.
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(JournalError::CommitIndeterminate {
                    source: Box::new(JournalError::Implementation {
                        message: format!(
                            "Background writer task {} for journal {}",
                            if join_error.is_cancelled() {
                                "was cancelled"
                            } else if join_error.is_panic() {
                                "panicked"
                            } else {
                                "failed"
                            },
                            self.path.display()
                        ),
                        source: Box::new(join_error),
                    }),
                });
            }
        };

        prepared.commit(committed.offset);
        self.last_frame.store(committed.offset, Ordering::Relaxed);
        tracing::debug!(
            path = %self.path.display(),
            offset = committed.offset,
            bytes = committed.next_offset - committed.offset,
            write_lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
            "DiskJournal appended framed record"
        );

        // Commit in-memory state only after the durable write succeeded.
        self.index
            .write()
            .await
            .insert(record.id().as_ulid(), committed.offset);
        *self.last_commit.write().await = Some(commitment);
        self.observations.committed(
            std::slice::from_ref(&envelope),
            committed.offset,
            committed.next_offset,
            observation_crc,
        );

        Ok(envelope)
    }

    async fn append_records(
        &self,
        group_id: &str,
        mut events: Vec<T>,
        frontier: &CausalFrontier,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        crate::journal::ensure_owned(self.owner.as_ref())?;
        obzenflow_core::journal::limits::validate_group_size(events.len())?;
        if events.is_empty() {
            return Ok(Vec::new());
        }
        if group_id.is_empty() {
            return Err(JournalError::Implementation {
                message: "Atomic journal group id cannot be empty".to_string(),
                source: "empty atomic journal group id".into(),
            });
        }
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(JournalError::Implementation {
                message: format!(
                    "Journal {} is poisoned after a failed append rollback; reopen to continue",
                    self.path.display()
                ),
                source: "poisoned journal".into(),
            });
        }

        // One write lock covers clock calculation, construction, the single
        // physical frame append, and publication of every member.
        let _lock = self.read_write_lock.write().await;
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(JournalError::Implementation {
                message: "journal publication is closed after indeterminate commit".into(),
                source: "poisoned journal".into(),
            });
        }
        if let Some(sequencer) = &self.admission_sequencer {
            for event in &mut events {
                if event.admission_seq().is_none() {
                    event.set_admission_seq(obzenflow_core::AdmissionSeq(
                        sequencer.fetch_add(1, Ordering::Relaxed),
                    ));
                }
            }
        }

        let group_size = u32::try_from(events.len()).map_err(|_| JournalError::Implementation {
            message: format!("Atomic journal group '{group_id}' exceeds u32 member capacity"),
            source: "atomic journal group is too large".into(),
        })?;
        let mut next_last_commit = self.last_commit.read().await.clone();
        let mut envelopes = Vec::with_capacity(events.len());
        let mut budget = obzenflow_core::journal::limits::GroupBudget::default();
        let mut group_frontier = frontier.clone();
        for (index, event) in events.into_iter().enumerate() {
            let (commitment, causal) = CausalCommit::prepare(
                self.run_id,
                CausalCoordinate::new(self.journal_id.into()),
                *event.id(),
                next_last_commit.as_ref(),
                &group_frontier,
            )?;
            group_frontier.merge(&commitment.frontier())?;
            next_last_commit = Some(commitment.clone());
            let timestamp = Utc::now();
            envelopes.push({
                let (authored, payload) = event.into_parts();
                JournalRecord::commit(
                    authored,
                    payload,
                    JournalProvenance {
                        journal_writer_id: JournalWriterId::from(self.journal_id),
                        run_id: self.run_id,
                        causal,
                        vector_clock: commitment.clock.clone(),
                        timestamp,
                        journal_group_id: Some(group_id.to_string()),
                        journal_group_member: Some(JournalGroupMember {
                            index: u32::try_from(index)
                                .expect("group size was checked against u32 capacity"),
                            size: group_size,
                        }),
                    },
                )
                .map_err(|error| JournalError::Implementation {
                    message: "Invalid journal record".to_string(),
                    source: Box::new(error),
                })?
            });
            budget.admit(envelopes.last().expect("prepared record"))?;
        }

        let mut prepared = codec::prepare(
            &envelopes,
            Some(group_id),
            &self.path,
            self.definitions.clone(),
        )
        .map_err(|e| JournalError::Implementation {
            message: format!("Failed to serialize atomic journal group '{group_id}'"),
            source: Box::new(e),
        })?;
        let bytes = std::mem::take(&mut prepared.bytes);
        let trailer = bytes.len() - codec::frame::TRAILER_LEN;
        let observation_crc = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap());

        let path = self.path.clone();
        let write_file = self.write_file.clone();
        let outcome =
            tokio::task::spawn_blocking(move || -> Result<CommittedAppend, AppendFailure> {
                let mut file = match write_file.lock() {
                    Ok(guard) => guard,
                    Err(e) => {
                        return Err(AppendFailure::Poisoned(JournalError::Implementation {
                            message: format!("Failed to lock journal file: {}", path.display()),
                            source: Box::new(std::io::Error::other(format!("Mutex poisoned: {e}"))),
                        }));
                    }
                };
                append_frame(&mut *file, &bytes, &path)
            })
            .await;

        let committed = match outcome {
            Ok(Ok(committed)) => committed,
            Ok(Err(AppendFailure::RolledBack(e))) => return Err(e),
            Ok(Err(AppendFailure::Poisoned(e))) => {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(JournalError::CommitIndeterminate {
                    source: Box::new(e),
                });
            }
            Err(join_error) => {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(JournalError::CommitIndeterminate {
                    source: Box::new(JournalError::Implementation {
                        message: format!(
                            "Background atomic-group writer task failed for journal {}",
                            self.path.display()
                        ),
                        source: Box::new(join_error),
                    }),
                });
            }
        };

        prepared.commit(committed.offset);
        self.last_frame.store(committed.offset, Ordering::Relaxed);
        tracing::debug!(
            path = %self.path.display(),
            group_id,
            members = envelopes.len(),
            offset = committed.offset,
            bytes = committed.next_offset - committed.offset,
            "DiskJournal appended atomic group frame"
        );

        {
            let mut index = self.index.write().await;
            for record in &envelopes {
                index.insert(record.id().as_ulid(), committed.offset);
            }
        }
        *self.last_commit.write().await = next_last_commit;
        self.observations.committed(
            &envelopes,
            committed.offset,
            committed.next_offset,
            observation_crc,
        );

        Ok(envelopes)
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for DiskJournal<T> {
    fn id(&self) -> &JournalId {
        &self.journal_id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    fn observation_reader(&self) -> Option<&dyn obzenflow_core::journal::JournalObservationReader> {
        Some(&self.observations)
    }

    fn configure(&self, config: JournalConfig) -> Result<(), JournalError> {
        self.observability.configure(config.observability)
    }

    async fn append(
        &self,
        event: T,
        options: AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        let AppendOptions { frontier, capture } = options;
        let journal = self.clone();
        retain_commit(self.poisoned.clone(), async move {
            let (mut events, reservation) = journal.observability.prepare(vec![event], capture);
            let result = journal
                .append_record(events.pop().expect("one event"), &frontier)
                .await
                .map(|record| vec![record]);
            reservation.finish::<T>(&result);
            result.map(|mut records| records.pop().expect("one record"))
        })
        .await
    }

    async fn append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        options: AppendOptions<T>,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let AppendOptions { frontier, capture } = options;
        let journal = self.clone();
        let group_id = group_id.to_owned();
        retain_commit(self.poisoned.clone(), async move {
            let (events, reservation) = journal.observability.prepare(events, capture);
            let result = journal.append_records(&group_id, events, &frontier).await;
            reservation.finish::<T>(&result);
            result
        })
        .await
    }

    async fn read_all_unordered(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let mut events = Vec::new();

        if !self.path.exists() {
            return Ok(events);
        }

        // FLOWIP-120q full-read lock: hold the shared read lock so an in-process
        // append cannot be observed mid-frame, and scan with sealed/full-scan
        // policy so corruption fails loud instead of silently shortening the
        // causally-ordered snapshot derived from this enumeration.
        let _read_guard = self.read_write_lock.read().await;

        let file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = Vec::new();
        let mut offset = 0u64;
        let mut decoder = Decoder::new(&self.path);

        while let Some((consumed, termination)) = read_frame_async(&mut reader, &mut buf)
            .await
            .map_err(|e| JournalError::Implementation {
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
                    tolerate_torn_tail: false,
                },
            ) {
                Disposition::Yield(frame) => {
                    events.extend(frame.into_records());
                }
                Disposition::EndOfCommittedRecords | Disposition::Skip => break,
                Disposition::Corrupt(problem) => {
                    return Err(JournalError::Implementation {
                        message: format!(
                            "Corrupt record at offset {record_offset} in {}: {problem}",
                            self.path.display()
                        ),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            problem.to_string(),
                        )),
                    });
                }
            }
        }

        Ok(events)
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        let ulid = event_id.as_ulid();

        // Check index
        let index = self.index.read().await;
        let offset = match index.get(&ulid) {
            Some(o) => *o,
            None => return Ok(None),
        };
        drop(index);

        if !self.path.exists() {
            return Ok(None);
        }

        // Keep direct reads under the same journal read lock as full scans. Drop
        // the index lock first so read_event never holds both locks with append.
        let _read_guard = self.read_write_lock.read().await;

        // Read from file at specific offset
        let mut file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file".to_string(),
                source: Box::new(e),
            })?;
        file.seek(SeekFrom::Start(offset))
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to seek in file".to_string(),
                source: Box::new(e),
            })?;

        let mut reader = tokio::io::BufReader::new(file);
        let mut buf = Vec::new();

        match read_frame_async(&mut reader, &mut buf).await.map_err(|e| {
            JournalError::Implementation {
                message: "Failed to read line".to_string(),
                source: Box::new(e),
            }
        })? {
            Some((_, termination)) => match dispose(
                classify_frame::<T>(&buf, &mut Decoder::new(&self.path), offset),
                termination,
                // Indexed offsets name committed records; an unterminated frame
                // here is corruption.
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: false,
                },
            ) {
                Disposition::Yield(frame) => {
                    let journal_group_id = frame.group_id().map(str::to_string);
                    let records = frame.into_records();
                    let _group_size = journal_group_id.as_ref().map(|_| {
                        u32::try_from(records.len())
                            .expect("a materialised journal frame fits in addressable memory")
                    });
                    Ok(records
                        .into_iter()
                        .enumerate()
                        .find(|(_, record)| record.id().as_ulid() == ulid)
                        .map(|(_index, record)| record))
                }
                Disposition::EndOfCommittedRecords | Disposition::Skip => Ok(None),
                Disposition::Corrupt(problem) => Err(JournalError::Implementation {
                    message: format!(
                        "Failed to parse record at {}: {problem}",
                        self.path.display()
                    ),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        problem.to_string(),
                    )),
                }),
            },
            None => Ok(None),
        }
    }

    async fn committed_position(&self) -> Result<u64, JournalError> {
        let _guard = self.read_write_lock.read().await;
        Ok(self
            .last_commit
            .read()
            .await
            .as_ref()
            .map_or(0, |commit| commit.reference.sequence))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        // Writer-confirmed progress is independent of the optional observation
        // index. Later appends cannot extend this connection's bootstrap cut.
        let end = self
            .observations
            .confirmed_end()
            .ok_or(JournalError::InitialPrefixUnsupported)?;
        Ok(Box::new(
            DiskJournalReader::from_position(
                self.path.clone(),
                self.journal_id,
                position,
                self.read_write_lock.clone(),
            )
            .await?
            .with_initial_end(end),
        ))
    }

    async fn read_metrics_tail(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.observations.metrics_tail().await
    }

    async fn read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        use tokio::io::AsyncSeekExt;

        if count == 0 || !self.path.exists() {
            return Ok(Vec::new());
        }

        // Acquire read lock to prevent torn reads
        let _read_guard = self.read_write_lock.read().await;

        let mut file = File::open(&self.path)
            .await
            .map_err(|e| JournalError::Implementation {
                message: "Failed to open file for backwards read".to_string(),
                source: Box::new(e),
            })?;

        // Get file size
        let file_len =
            file.seek(SeekFrom::End(0))
                .await
                .map_err(|e| JournalError::Implementation {
                    message: "Failed to seek to end".to_string(),
                    source: Box::new(e),
                })?;

        if file_len == 0 {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(count);
        let known = self.last_frame.load(Ordering::Relaxed);
        let anchor = if known < file_len { known } else { 0 };
        let mut reader = ReverseFrameReader::new(file, file_len, anchor);
        let mut buffer = Vec::new();
        let mut decoder = Decoder::new(&self.path);
        while results.len() < count {
            let Some(termination) = reader.read_frame(&mut buffer).await.map_err(|error| {
                JournalError::Implementation {
                    message: format!("Failed to read journal backwards: {}", self.path.display()),
                    source: Box::new(error),
                }
            })?
            else {
                break;
            };
            // This remains a best-effort observability helper, but commitment
            // and frame validation use the same policy as forward readers.
            match dispose(
                classify_frame::<T>(&buffer, &mut decoder, reader.offset()),
                termination,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: true,
                },
            ) {
                Disposition::Yield(frame) => {
                    let journal_group_id = frame.group_id().map(str::to_string);
                    let records = frame.into_records();
                    let _group_size = journal_group_id.as_ref().map(|_| {
                        u32::try_from(records.len())
                            .expect("a materialised journal frame fits in addressable memory")
                    });
                    for (_index, record) in records.into_iter().enumerate().rev() {
                        if results.len() >= count {
                            break;
                        }
                        results.push(record);
                    }
                }
                Disposition::EndOfCommittedRecords | Disposition::Skip => {
                    // Reverse traversal continues to older committed frames
                    // after skipping an uncommitted final tail.
                    continue;
                }
                Disposition::Corrupt(problem) => {
                    tracing::warn!(
                        path = %self.path.display(),
                        parse_error = %problem,
                        "Skipping corrupt record during backwards read"
                    );
                }
            }
        }

        tracing::debug!(
            path = %self.path.display(),
            requested = count,
            returned = results.len(),
            "read_last_n completed"
        );

        Ok(results)
    }
}

#[cfg(test)]
#[path = "journal_tail_tests.rs"]
mod tail_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::event::vector_clock::CausalOrderingService;
    use obzenflow_core::id::StageId;
    use obzenflow_core::journal::ObservabilityPolicy;
    use tokio::sync::Barrier;

    use uuid::Uuid;

    #[tokio::test]
    async fn cancelled_append_retains_index_clock_and_writer_serialisation() {
        use futures::FutureExt;
        for grouped in [false, true] {
            let directory = tempfile::tempdir().expect("temporary journal directory");
            let path = directory.path().join("retained.log");
            let stage = StageId::new();
            let writer = WriterId::from(stage);
            let journal = DiskJournal::<ChainEvent>::with_owner(
                path.clone(),
                obzenflow_core::JournalOwner::stage(stage),
            )
            .unwrap();
            journal
                .configure(JournalConfig {
                    observability: ObservabilityPolicy::Periodic {
                        interval: std::time::Duration::from_secs(60),
                    },
                })
                .unwrap();
            // Hold metadata publication after the physical frame is written.
            let index_guard = journal.index.write().await;
            let first = crate::journal::observability::tests::event(stage, 1);
            let second = crate::journal::observability::tests::event(stage, 2);
            let observation_reader =
                super::super::observations::DiskObservationReader::<ChainEvent>::open(path.clone())
                    .unwrap();
            let first_id = first.id;
            let pending_journal = journal.clone();
            let mut receipt = Box::pin(async move {
                if grouped {
                    pending_journal
                        .append_group("retained", vec![first, second], Default::default())
                        .await
                        .map(|_| ())
                } else {
                    pending_journal
                        .append(first, Default::default())
                        .await
                        .map(|_| ())
                }
            });
            assert!(futures::poll!(receipt.as_mut()).is_pending());
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while std::fs::metadata(&path).unwrap().len() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            // Bytes are present, but the retained commit has not published its
            // protected metadata. Neither lookup nor a second writable opener
            // may promote this frame (or individual group members) into view.
            assert!(DiskJournal::<ChainEvent>::with_owner(
                path.clone(),
                JournalOwner::stage(stage)
            )
            .is_err());
            let reopened = journal.clone();
            let physical_len = std::fs::metadata(&path).unwrap().len();
            for reader in [
                &observation_reader as &dyn obzenflow_core::journal::JournalObservationReader,
                reopened.observation_reader().unwrap(),
            ] {
                assert!(matches!(
                    tokio::time::timeout(std::time::Duration::from_secs(5), reader.latest_observations(writer)).await.unwrap().unwrap(),
                    obzenflow_core::journal::ObservationLookup::Ready { committed_len: 0, observation } if observation.is_empty()
                ));
            }
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                physical_len,
                "opening another handle must not truncate an unsettled append"
            );
            drop(receipt);
            let next = crate::journal::observability::tests::event(stage, 3);
            let mut next_receipt = Box::pin(journal.append(next, Default::default()));
            assert!(next_receipt.as_mut().now_or_never().is_none());
            assert!(journal.read_write_lock.try_read().is_err());
            drop(index_guard);
            let appended = tokio::time::timeout(std::time::Duration::from_secs(5), next_receipt)
                .await
                .unwrap()
                .unwrap();
            assert!(appended.envelope.observability.is_none());
            assert_eq!(
                appended
                    .envelope
                    .provenance
                    .journal
                    .vector_clock
                    .get(&appended.causal_coordinate()),
                if grouped { 3 } else { 2 }
            );
            assert!(journal.read_event(&first_id).await.unwrap().is_some());
            assert_eq!(
                journal.read_all_unordered().await.unwrap().len(),
                if grouped { 3 } else { 2 }
            );
            for reader in [
                &observation_reader as &dyn obzenflow_core::journal::JournalObservationReader,
                reopened.observation_reader().unwrap(),
            ] {
                let obzenflow_core::journal::ObservationLookup::Ready {
                    committed_len,
                    observation,
                } = reader.latest_observations(writer).await.unwrap()
                else {
                    panic!("the confirmed boundary must be shared across handles");
                };
                assert_eq!(committed_len, if grouped { 3 } else { 2 });
                assert_eq!(
                    observation.len(),
                    1,
                    "the periodic gate selects one packet for the whole group"
                );
            }
        }
    }

    #[tokio::test]
    async fn failed_appends_do_not_publish_observations_and_reopen_recovers_with_a_surviving_reader(
    ) {
        use obzenflow_core::journal::{JournalObservationReader, ObservationLookup};
        use std::io::Write;

        for grouped in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("failed.log");
            let stage = StageId::new();
            let journal =
                DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage))
                    .unwrap();
            let observed = crate::journal::observability::tests::event(stage, 1);
            let expected = journal
                .append(observed.clone(), Default::default())
                .await
                .unwrap();
            let reader =
                super::super::observations::DiskObservationReader::<ChainEvent>::open(path.clone())
                    .unwrap();
            let committed_end = std::fs::metadata(&path).unwrap().len();
            // A read-only descriptor makes both writing and rollback fail through
            // the real append path, including retained commitment and poisoning.
            *journal.write_file.lock().unwrap() = StdFile::open(&path).unwrap();
            let failed = if grouped {
                journal
                    .append_group(
                        "failed",
                        vec![observed.clone(), observed.clone()],
                        Default::default(),
                    )
                    .await
                    .map(|_| ())
            } else {
                journal
                    .append(observed.clone(), Default::default())
                    .await
                    .map(|_| ())
            };
            assert!(matches!(
                failed,
                Err(JournalError::CommitIndeterminate { .. })
            ));
            assert!(journal.poisoned.load(Ordering::SeqCst));
            assert!(
                matches!(reader.latest_observations(stage.into()).await.unwrap(),
                ObservationLookup::Ready { committed_len: 1, observation } if observation.len() == 1)
            );
            drop(journal);

            // Simulate an indeterminate append's torn bytes. The observation
            // handle outlives the writer; it must not disable recovery on reopen.
            Write::write_all(
                &mut StdFile::options().append(true).open(&path).unwrap(),
                &super::super::codec::frame::MAGIC,
            )
            .unwrap();
            assert!(matches!(
                reader.latest_observations(stage.into()).await.unwrap(),
                ObservationLookup::Ready {
                    committed_len: 1,
                    ..
                }
            ));
            let reopened =
                DiskJournal::<ChainEvent>::new(directory.path().to_owned(), "failed").unwrap();
            assert_eq!(std::fs::metadata(&path).unwrap().len(), committed_end);
            assert_eq!(
                serde_json::to_value(reopened.read_all_unordered().await.unwrap()).unwrap(),
                serde_json::to_value(vec![expected]).unwrap()
            );
            drop(reopened);
            let writable = DiskJournal::with_owner(path, JournalOwner::stage(stage)).unwrap();
            writable.append(observed, Default::default()).await.unwrap();
            assert!(matches!(
                reader.latest_observations(stage.into()).await.unwrap(),
                ObservationLookup::Ready {
                    committed_len: 2,
                    ..
                }
            ));
        }
    }

    #[tokio::test]
    async fn confirmed_observation_prefix_does_not_cross_an_indeterminate_frame() {
        use obzenflow_core::journal::{JournalObservationReader, ObservationLookup};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("indeterminate.log");
        let stage = StageId::new();
        let first = DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage))
            .unwrap();
        let committed = first
            .append(
                crate::journal::observability::tests::event(stage, 1),
                Default::default(),
            )
            .await
            .unwrap();
        let second = first.clone();
        let observer =
            super::super::observations::DiskObservationReader::<ChainEvent>::open(path.clone())
                .unwrap();
        let identity = super::super::identity::read_identity(&path).unwrap();
        let mut previous = Some(CausalCommit::from_record(&committed).unwrap());
        let uncertain = super::super::identity::fixture_record(
            identity,
            crate::journal::observability::tests::event(stage, 2),
            &mut previous,
        );
        let complete_frame = super::super::log_record::serialize_record(&uncertain).unwrap();
        let failing = first.clone();
        // Retain a complete frame, then panic before commitment bookkeeping.
        let result = retain_commit::<()>(first.poisoned.clone(), async move {
            let _guard = failing.read_write_lock.write().await;
            let mut file = failing.write_file.lock().unwrap();
            FrameSink::write_all(&mut *file, &complete_frame).unwrap();
            FrameSink::flush(&mut *file).unwrap();
            panic!("commit bookkeeping fault after complete frame write");
        })
        .await;
        assert!(matches!(
            result,
            Err(JournalError::CommitIndeterminate { .. })
        ));
        // All clones reject writes after an indeterminate publication.
        assert!(second
            .append(
                crate::journal::observability::tests::event(stage, 3),
                Default::default()
            )
            .await
            .is_err());
        assert!(matches!(
            second
                .observation_reader()
                .unwrap()
                .latest_observations(stage.into())
                .await
                .unwrap(),
            ObservationLookup::Ready {
                committed_len: 1,
                ..
            }
        ));
        drop(first);
        drop(second);
        let recovered =
            DiskJournal::<ChainEvent>::with_owner(path, JournalOwner::stage(stage)).unwrap();
        assert_eq!(recovered.read_all_unordered().await.unwrap().len(), 2);
        assert!(matches!(
            observer.latest_observations(stage.into()).await.unwrap(),
            ObservationLookup::Ready {
                committed_len: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_basic_append_and_read() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_basic_append_and_read_{test_id}"
        ));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let log_path = test_dir.join("test_flow_1.log");
        let log = DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap();

        let writer_id = WriterId::from(StageId::new());
        let event = ChainEventFactory::data_event(
            writer_id,
            "test.event",
            serde_json::json!({"data": "test value"}),
        );

        // Append event
        let envelope = log.append(event.clone(), Default::default()).await.unwrap();

        // Read back
        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "test.event");
        assert_eq!(events[0].payload()["data"], "test value");

        // Read by ID
        let event_by_id = log
            .read_event(&envelope.envelope.provenance.event.id)
            .await
            .unwrap();
        assert!(event_by_id.is_some());
        assert_eq!(
            event_by_id.unwrap().envelope.provenance.event.id,
            envelope.envelope.provenance.event.id
        );

        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn atomic_group_uses_one_frame_and_all_read_surfaces_expand_every_member() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/atomic_group_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("group.log");
        let owner = obzenflow_core::JournalOwner::stage(StageId::new());
        let log = DiskJournal::<ChainEvent>::with_owner(log_path.clone(), owner).unwrap();
        let writer_id = WriterId::from(StageId::new());
        let events: Vec<_> = (0..3)
            .map(|index| {
                ChainEventFactory::data_event(
                    writer_id,
                    "atomic.member",
                    serde_json::json!({ "index": index }),
                )
            })
            .collect();
        let ids: Vec<_> = events.iter().map(|event| event.id).collect();

        let written = log
            .append_group("effect-outcome:test", events, Default::default())
            .await
            .expect("atomic group append");
        assert_eq!(written.len(), 3);
        let bytes = std::fs::read(&log_path).unwrap();
        assert_eq!(
            codec::frame::frame_length(&bytes).unwrap(),
            bytes.len(),
            "the whole group must have one physical commit marker"
        );
        codec::frame::validate(&bytes).unwrap();

        let all = log.read_all_unordered().await.unwrap();
        assert_eq!(all.len(), 3);
        for id in ids {
            assert!(log.read_event(&id).await.unwrap().is_some());
        }
        let mut from_second = log.reader_from(1).await.unwrap();
        assert_eq!(
            from_second.next().await.unwrap().unwrap().payload()["index"],
            1
        );
        assert_eq!(log.read_last_n(2).await.unwrap().len(), 2);

        drop(log);
        let reopened = DiskJournal::<ChainEvent>::with_owner(
            log_path,
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        assert_eq!(reopened.read_all_unordered().await.unwrap().len(), 3);
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn atomic_group_torn_tail_is_invisible_at_every_byte_boundary() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/atomic_torn_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("group.log");
        let journal_id = JournalId::new();
        let identity = super::super::identity::JournalIdentity {
            run_id: FlowId::new(),
            journal_id,
        };
        let mut previous = None;
        super::super::identity::write_fixture_identity(&log_path, identity);
        let writer_id = WriterId::from(StageId::new());
        let records: Vec<_> = (0..3)
            .map(|index| {
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "atomic.member",
                    serde_json::json!({ "index": index }),
                );
                let mut record =
                    super::super::identity::fixture_record(identity, event, &mut previous);
                record.envelope.provenance.journal.journal_group_id =
                    Some("effect-outcome:test".into());
                record.envelope.provenance.journal.journal_group_member =
                    Some(JournalGroupMember { index, size: 3 });
                record
            })
            .collect();
        let frame = serialize_atomic_group("effect-outcome:test", &records).unwrap();

        for cut in 0..frame.len() {
            std::fs::write(&log_path, &frame[..cut]).unwrap();
            let reopened = DiskJournal::<ChainEvent>::with_owner(
                log_path.clone(),
                obzenflow_core::JournalOwner::stage(StageId::new()),
            )
            .unwrap_or_else(|error| panic!("torn group at byte {cut} should recover: {error}"));
            assert!(
                reopened.read_all_unordered().await.unwrap().is_empty(),
                "no member may be visible after recovery from a tear at byte {cut}"
            );
            drop(reopened);
        }

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_causal_ordering() {
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/test_causal_ordering_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_system_id = obzenflow_core::SystemId::new();
        let owner = obzenflow_core::JournalOwner::system(test_system_id);
        let log_path = test_dir.join("test_flow_2.log");
        let log = DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap();

        let writer1 = WriterId::from(StageId::new());
        let writer2 = WriterId::from(StageId::new());

        // First event from writer1
        let event1 =
            ChainEventFactory::data_event(writer1, "event.1", serde_json::json!({"seq": 1}));
        let envelope1 = log.append(event1, Default::default()).await.unwrap();

        // Second event from writer2, causally dependent on event1
        let event2 =
            ChainEventFactory::data_event(writer2, "event.2", serde_json::json!({"seq": 2}));
        let envelope2 = log
            .append(
                event2,
                AppendOptions::from_record(Some(&envelope1)).unwrap(),
            )
            .await
            .unwrap();

        // Verify vector clocks show causal relationship
        assert!(CausalOrderingService::happened_before(
            &envelope1.envelope.provenance.journal.vector_clock,
            &envelope2.envelope.provenance.journal.vector_clock
        ));

        // Read all events
        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), 2);

        // Verify causal order
        assert_eq!(
            events[0].envelope.provenance.event.id,
            envelope1.envelope.provenance.event.id
        );
        assert_eq!(
            events[1].envelope.provenance.event.id,
            envelope2.envelope.provenance.event.id
        );

        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_concurrent_writers() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_concurrent_writers_{test_id}"
        ));
        std::fs::create_dir_all(&test_dir).unwrap();
        // Create a test journal with a proper owner
        let test_system_id = obzenflow_core::SystemId::new();
        let owner = obzenflow_core::JournalOwner::system(test_system_id);
        let log_path = test_dir.join("test_flow_3.log");
        let log = Arc::new(DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap());

        // Spawn multiple concurrent writers
        let mut handles = vec![];

        for i in 0..5 {
            let log_clone = log.clone();
            let handle = tokio::spawn(async move {
                let writer_id = WriterId::from(StageId::new());
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "concurrent.event",
                    serde_json::json!({"writer": i}),
                );
                log_clone.append(event, Default::default()).await
            });
            handles.push(handle);
        }

        // Wait for all writers
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all events were written
        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), 5);

        // Verify each event has unique writer
        let writer_ids: std::collections::HashSet<_> = events
            .iter()
            .map(|e| e.writer_id().as_ulid().to_string())
            .collect();
        assert_eq!(writer_ids.len(), 5);

        // Cleanup
        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn test_same_writer_concurrent_appends_have_unique_writer_seq() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!(
            "target/test-logs/test_same_writer_concurrent_appends_have_unique_writer_seq_{test_id}"
        ));
        std::fs::create_dir_all(&test_dir).unwrap();

        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let log_path = test_dir.join("same_writer_concurrent.log");
        let log = Arc::new(DiskJournal::<ChainEvent>::with_owner(log_path, owner).unwrap());

        let writer_id = WriterId::from(StageId::new());

        let task_count: usize = 20;
        let barrier = Arc::new(Barrier::new(task_count));

        let mut handles = Vec::with_capacity(task_count);
        for i in 0..task_count {
            let log_clone = log.clone();
            let barrier_clone = barrier.clone();
            let handle = tokio::spawn(async move {
                barrier_clone.wait().await;
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "concurrent.same_writer",
                    serde_json::json!({ "i": i }),
                );
                log_clone.append(event, Default::default()).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        let events = log.read_causally_ordered().await.unwrap();
        assert_eq!(events.len(), task_count);

        let writer_seqs: Vec<u64> = events
            .iter()
            .map(|e| {
                e.envelope
                    .provenance
                    .journal
                    .vector_clock
                    .get(&CausalCoordinate::new(
                        e.envelope.provenance.journal.journal_writer_id,
                    ))
            })
            .collect();

        let expected: Vec<u64> = (1..=task_count as u64).collect();
        assert_eq!(writer_seqs, expected);

        std::fs::remove_dir_all(&test_dir).ok();
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
            )
            .unwrap();
            let e1 = ChainEventFactory::data_event(writer_id, "a", serde_json::json!({"n": 1}));
            let env1 = log.append(e1, Default::default()).await.unwrap();
            let e2 = ChainEventFactory::data_event(writer_id, "b", serde_json::json!({"n": 2}));
            log.append(e2, AppendOptions::from_record(Some(&env1)).unwrap())
                .await
                .unwrap();
            kept_id = env1.envelope.provenance.event.id;
        }

        let reopened = DiskJournal::<ChainEvent>::with_owner(
            log_path,
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();

        assert!(
            reopened.read_event(&kept_id).await.unwrap().is_some(),
            "read_event by id must work after reopen (index rebuilt)"
        );
        assert_eq!(reopened.read_causally_ordered().await.unwrap().len(), 2);

        let e3 = ChainEventFactory::data_event(writer_id, "c", serde_json::json!({"n": 3}));
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

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn read_causally_ordered_fails_loud_on_mid_file_corruption() {
        // FLOWIP-120q: a corrupt committed record must fail loud rather than be
        // silently skipped to produce a shortened stream.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/read_all_corrupt_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("corrupt.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        for i in 0..3 {
            let e = ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": i}));
            log.append(e, Default::default()).await.unwrap();
        }

        // Flip a byte inside the first record's body (mid-file): its CRC no
        // longer matches, so it is committed corruption.
        let mut bytes = std::fs::read(&log_path).unwrap();
        let brace = bytes.iter().position(|&b| b == b'{').unwrap();
        bytes[brace + 5] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        assert!(
            log.read_causally_ordered().await.is_err(),
            "mid-file corruption must fail loud"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn live_tail_reader_handles_concurrent_appends_without_misalignment() {
        // FLOWIP-120q: a live-tail reader sharing the per-path lock with the
        // writer must read every committed record under concurrent append, never
        // misaligning into a mid-record "malformed" read and never erroring.
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/live_tail_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("tail.log");
        let log = Arc::new(
            DiskJournal::<ChainEvent>::with_owner(
                log_path,
                obzenflow_core::JournalOwner::stage(StageId::new()),
            )
            .unwrap(),
        );
        let writer_id = WriterId::from(StageId::new());

        const N: usize = 50;
        let writer_log = log.clone();
        let writer = tokio::spawn(async move {
            for i in 0..N {
                let e = ChainEventFactory::data_event(
                    writer_id,
                    "tail.event",
                    serde_json::json!({"i": i}),
                );
                writer_log.append(e, Default::default()).await.unwrap();
                tokio::task::yield_now().await;
            }
        });

        let mut reader = log.reader().await.unwrap();
        let mut seen = 0usize;
        let mut polls = 0u32;
        while seen < N {
            match reader.next().await {
                Ok(Some(_)) => seen += 1,
                Ok(None) => {
                    tokio::task::yield_now().await;
                    polls += 1;
                    assert!(polls < 1_000_000, "live-tail reader stalled at {seen}/{N}");
                }
                Err(e) => panic!("live-tail reader must not error on concurrent append: {e}"),
            }
        }
        writer.await.unwrap();
        assert_eq!(seen, N);

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn live_tail_reader_resumes_after_a_corrupt_record() {
        // FLOWIP-120q live-tail observer resilience: a live reader that hits a
        // corrupt committed record reports it once, then resumes at the next
        // record on the following poll, so a best-effort observer that keeps
        // polling recovers rather than losing the rest of the stream.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/resume_corrupt_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("resume.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        for i in 0..3 {
            let e = ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": i}));
            log.append(e, Default::default()).await.unwrap();
        }

        // Flip a byte inside the second record's body so its CRC no longer matches: committed corruption
        // with intact records on either side.
        let mut bytes = std::fs::read(&log_path).unwrap();
        let first_end = codec::frame::frame_length(&bytes).unwrap();
        let second_end = first_end + codec::frame::frame_length(&bytes[first_end..]).unwrap();
        let target = second_end - codec::frame::TRAILER_LEN - 1;
        bytes[target] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        let mut reader = log.reader().await.unwrap();
        assert!(
            reader.next().await.unwrap().is_some(),
            "the first record reads cleanly"
        );
        assert!(
            reader.next().await.is_err(),
            "the corrupt middle record is reported as an error"
        );
        assert!(
            reader.next().await.unwrap().is_some(),
            "the reader resumes at the record after the corrupt one"
        );

        // Without a checked header, continuation has no known frame boundary.
        // Repeated polls must report the same position instead of searching
        // inside the payload for bytes that resemble another record.
        bytes[first_end + 12] ^= 1;
        std::fs::write(&log_path, &bytes).unwrap();
        let mut reader = log.reader().await.unwrap();
        assert!(reader.next().await.unwrap().is_some());
        for _ in 0..3 {
            assert!(reader.next().await.is_err());
            assert_eq!(reader.position(), 1);
        }

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn read_event_fails_loud_on_externally_truncated_record() {
        // A committed record truncated externally must fail loud, not return None.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/read_event_trunc_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("trunc.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        let env = log
            .append(
                ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": 1})),
                Default::default(),
            )
            .await
            .unwrap();
        let id = env.envelope.provenance.event.id;

        // Truncate the committed record's frame (drop part of the commit trailer),
        // leaving the index pointing at now-torn bytes.
        let bytes = std::fs::read(&log_path).unwrap();
        std::fs::write(&log_path, &bytes[..bytes.len() - 5]).unwrap();

        assert!(
            log.read_event(&id).await.is_err(),
            "a truncated indexed record must fail loud, not return None"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn reader_from_fails_loud_on_mid_file_corruption() {
        // FLOWIP-120t: from_position drives the same dispose path as next(), so a
        // corrupt committed record before the target position fails loud instead
        // of being silently counted and skipped (the old read_line behaviour).
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/reader_from_corrupt_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("corrupt.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        for i in 0..4 {
            let e = ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": i}));
            log.append(e, Default::default()).await.unwrap();
        }

        // Flip a byte in the second record's body so its CRC fails: mid-file
        // committed corruption with intact records on either side.
        let mut bytes = std::fs::read(&log_path).unwrap();
        let first_end = codec::frame::frame_length(&bytes).unwrap();
        let second_end = first_end + codec::frame::frame_length(&bytes[first_end..]).unwrap();
        let target = second_end - codec::frame::TRAILER_LEN - 1;
        bytes[target] ^= 0xff;
        std::fs::write(&log_path, &bytes).unwrap();

        assert!(
            log.reader_from(3).await.is_err(),
            "from_position past a mid-file corrupt record must fail loud"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[tokio::test]
    async fn reader_from_stops_short_at_torn_tail_without_counting_it() {
        // FLOWIP-120t: an unterminated final record (torn tail) is not a
        // committed position, so from_position stops at it rather than counting
        // it as a position the way the old read_line skip did.
        let test_id = Uuid::new_v4();
        let test_dir =
            std::path::PathBuf::from(format!("target/test-logs/reader_from_torn_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("torn.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path.clone(),
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        for i in 0..3 {
            let e = ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": i}));
            log.append(e, Default::default()).await.unwrap();
        }

        // Drop the final commit-marker byte: the last record becomes a torn tail.
        let mut bytes = std::fs::read(&log_path).unwrap();
        assert_eq!(*bytes.last().unwrap(), b'O');
        bytes.pop();
        std::fs::write(&log_path, &bytes).unwrap();

        // Only two records are committed; asking for position 3 stops at 2.
        let reader = log.reader_from(3).await.unwrap();
        assert_eq!(
            reader.position(),
            2,
            "a torn tail must not be counted as a committed position"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }

    #[derive(Clone, Copy)]
    enum FailMode {
        Ok,
        FailBeforeWrite,
        PartialThenFail(usize),
        FailOnFlush,
        FailFlushAndRollback,
    }

    /// In-memory FrameSink so append_frame's commit/rollback path is tested
    /// without inducing real filesystem failures (FLOWIP-120t).
    struct MockSink {
        buf: Vec<u8>,
        mode: FailMode,
    }

    impl FrameSink for MockSink {
        fn end_offset(&mut self) -> std::io::Result<u64> {
            Ok(self.buf.len() as u64)
        }
        fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()> {
            match self.mode {
                FailMode::FailBeforeWrite => Err(std::io::Error::other("fail before write")),
                FailMode::PartialThenFail(k) => {
                    let n = k.min(bytes.len());
                    self.buf.extend_from_slice(&bytes[..n]);
                    Err(std::io::Error::other("fail after partial write"))
                }
                _ => {
                    self.buf.extend_from_slice(bytes);
                    Ok(())
                }
            }
        }
        fn flush(&mut self) -> std::io::Result<()> {
            match self.mode {
                FailMode::FailOnFlush | FailMode::FailFlushAndRollback => {
                    Err(std::io::Error::other("fail on flush"))
                }
                _ => Ok(()),
            }
        }
        fn rollback_to(&mut self, offset: u64) -> std::io::Result<()> {
            match self.mode {
                FailMode::FailFlushAndRollback => Err(std::io::Error::other("rollback failed")),
                _ => {
                    self.buf.truncate(offset as usize);
                    Ok(())
                }
            }
        }
    }

    #[test]
    fn append_frame_commits_on_success() {
        let mut sink = MockSink {
            buf: b"PRE".to_vec(),
            mode: FailMode::Ok,
        };
        let rec = b"record-bytes";
        let committed = append_frame(&mut sink, rec, Path::new("x.log")).unwrap();
        assert_eq!(committed.offset, 3);
        assert_eq!(committed.next_offset, 3 + rec.len() as u64);
        assert_eq!(sink.buf, b"PRErecord-bytes");
    }

    #[test]
    fn append_frame_rolls_back_when_write_fails_before_any_bytes() {
        let mut sink = MockSink {
            buf: b"PRE".to_vec(),
            mode: FailMode::FailBeforeWrite,
        };
        let err = append_frame(&mut sink, b"rec", Path::new("x.log")).unwrap_err();
        assert!(matches!(err, AppendFailure::RolledBack(_)));
        assert_eq!(sink.buf, b"PRE", "no bytes written, file unchanged");
    }

    #[test]
    fn append_frame_rolls_back_a_partial_write() {
        let mut sink = MockSink {
            buf: b"PRE".to_vec(),
            mode: FailMode::PartialThenFail(2),
        };
        let err = append_frame(&mut sink, b"record", Path::new("x.log")).unwrap_err();
        assert!(matches!(err, AppendFailure::RolledBack(_)));
        assert_eq!(
            sink.buf, b"PRE",
            "partial bytes truncated back to pre-append EOF"
        );
    }

    #[test]
    fn atomic_group_rolls_back_at_every_partial_write_boundary() {
        let journal_id = JournalId::new();
        let identity = super::super::identity::JournalIdentity {
            run_id: FlowId::new(),
            journal_id,
        };
        let mut previous = None;
        let writer_id = WriterId::from(StageId::new());
        let records: Vec<_> = (0..3)
            .map(|index| {
                let event = ChainEventFactory::data_event(
                    writer_id,
                    "atomic.member",
                    serde_json::json!({ "index": index }),
                );
                let mut record =
                    super::super::identity::fixture_record(identity, event, &mut previous);
                record.envelope.provenance.journal.journal_group_id =
                    Some("effect-outcome:test".into());
                record.envelope.provenance.journal.journal_group_member =
                    Some(JournalGroupMember { index, size: 3 });
                record
            })
            .collect();
        let frame = serialize_atomic_group("effect-outcome:test", &records).unwrap();

        for fail_after in 0..=frame.len() {
            let mut sink = MockSink {
                buf: b"PRE".to_vec(),
                mode: FailMode::PartialThenFail(fail_after),
            };
            let failure = append_frame(&mut sink, &frame, Path::new("group.log"))
                .expect_err("every injected write failure must abort the group");
            assert!(matches!(failure, AppendFailure::RolledBack(_)));
            assert_eq!(
                sink.buf, b"PRE",
                "no group member may remain visible after failure at byte {fail_after}"
            );
        }
    }

    #[test]
    fn append_frame_rolls_back_on_flush_failure() {
        let mut sink = MockSink {
            buf: b"PRE".to_vec(),
            mode: FailMode::FailOnFlush,
        };
        let err = append_frame(&mut sink, b"record", Path::new("x.log")).unwrap_err();
        assert!(matches!(err, AppendFailure::RolledBack(_)));
        assert_eq!(sink.buf, b"PRE", "unflushed bytes truncated back");
    }

    #[test]
    fn append_frame_poisons_when_rollback_also_fails() {
        let mut sink = MockSink {
            buf: b"PRE".to_vec(),
            mode: FailMode::FailFlushAndRollback,
        };
        let err = append_frame(&mut sink, b"record", Path::new("x.log")).unwrap_err();
        assert!(matches!(err, AppendFailure::Poisoned(_)));
    }

    #[tokio::test]
    async fn poisoned_journal_rejects_appends() {
        let test_id = Uuid::new_v4();
        let test_dir = std::path::PathBuf::from(format!("target/test-logs/poisoned_{test_id}"));
        std::fs::create_dir_all(&test_dir).unwrap();
        let log_path = test_dir.join("poison.log");
        let writer_id = WriterId::from(StageId::new());

        let log = DiskJournal::<ChainEvent>::with_owner(
            log_path,
            obzenflow_core::JournalOwner::stage(StageId::new()),
        )
        .unwrap();
        log.append(
            ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": 0})),
            Default::default(),
        )
        .await
        .unwrap();

        // Simulate an unrecoverable rollback failure.
        log.poisoned.store(true, Ordering::SeqCst);

        assert!(
            log.append(
                ChainEventFactory::data_event(writer_id, "e", serde_json::json!({"i": 1})),
                Default::default(),
            )
            .await
            .is_err(),
            "a poisoned journal must reject further appends"
        );

        std::fs::remove_dir_all(&test_dir).ok();
    }
}
