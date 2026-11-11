// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Single event log per flow execution
//!
//! Provides optimal sequential writes and natural event ordering

mod append;
mod recovery;

#[cfg(test)]
use super::codec;
use super::codec::{Decoder, DefinitionStore};
use super::reader::DiskJournalReader;
use super::reverse_reader::ReverseFrameReader;
use super::scanner::{
    classify_frame, dispose, read_frame_async, unscanned_tail, Disposition, ReadPolicy,
};
use crate::journal::observability::JournalObservability;
use append::retain_commit;
use async_trait::async_trait;
use obzenflow_core::event::identity::EventId;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::JournalClock;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::reader::JournalReader;
#[cfg(test)]
use obzenflow_core::journal::Journal;
use obzenflow_core::journal::{AppendOptions, JournalConfig};
use obzenflow_core::FlowId;
#[cfg(test)]
use obzenflow_core::WriterId;
use obzenflow_runtime::runtime_config::JournalSync;
use recovery::{open_append_file, scan_committed_frames, CommittedFrames};
use std::collections::HashMap;
use std::fs::File as StdFile;
use std::io::SeekFrom;
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
    sync: JournalSync,
    /// Event id to frame offset, built by the first `read_event`
    index: Arc<RwLock<Option<HashMap<Ulid, u64>>>>,
    /// Disposable known frame boundary for bounded reverse-tail discovery.
    last_frame: Arc<AtomicU64>,
    /// Shared lock to coordinate readers/writers
    ///
    /// Writers take a write lock; readers take a read lock to avoid torn lines.
    read_write_lock: Arc<RwLock<()>>,
    /// Last committed record of this physical journal, independent of authorship
    last_commit: Arc<RwLock<Option<JournalClock>>>,
    /// Set when a failed append could not be rolled back, leaving the file in an
    /// unknown state. Further appends are rejected until the journal is reopened.
    poisoned: Arc<AtomicBool>,
    definitions: DefinitionStore,
    observations: super::observations::DiskObservationReader<T>,
    observability: JournalObservability,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> DiskJournal<T> {
    /// Create a new flow event log
    pub fn new(base_path: PathBuf, flow_id: &str, sync: JournalSync) -> Result<Self, JournalError> {
        std::fs::create_dir_all(&base_path).map_err(|e| JournalError::Implementation {
            message: "Failed to create directory".to_string(),
            source: Box::new(e),
        })?;
        let log_path = base_path.join(format!("{flow_id}.log"));

        Self::open(log_path, None, None, sync)
    }

    pub fn with_owner(
        log_path: PathBuf,
        owner: JournalOwner,
        sync: JournalSync,
    ) -> Result<Self, JournalError> {
        Self::open(log_path, Some(owner), None, sync)
    }

    pub fn with_owner_in_run(
        log_path: PathBuf,
        owner: JournalOwner,
        run_id: FlowId,
        sync: JournalSync,
    ) -> Result<Self, JournalError> {
        Self::open(log_path, Some(owner), Some(run_id), sync)
    }

    fn open(
        log_path: PathBuf,
        owner: Option<JournalOwner>,
        run_id: Option<FlowId>,
        sync: JournalSync,
    ) -> Result<Self, JournalError> {
        if let Some(parent) = log_path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| JournalError::Implementation {
                message: "Create journal directory".into(),
                source: error.into(),
            })?;
        }
        let (identity, writer_lease) = super::identity::open_identity(&log_path, run_id)?;
        let (observations, (write_file, last_frame, last_commit)) =
            super::observations::DiskObservationReader::open_writer(log_path.clone(), |end| {
                let CommittedFrames {
                    last_commit,
                    last_frame,
                    committed_end,
                    interrupted,
                } = scan_committed_frames::<T>(&log_path, end, None)?;
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
                drop(open_append_file(&log_path, interrupted)?);
                Ok(((writer_lease, last_frame, last_commit), committed_end))
            })?;
        Ok(Self {
            owner,
            definitions: DefinitionStore::for_archive(&log_path),
            journal_id: identity.journal_id,
            run_id: identity.run_id,
            observations,
            path: log_path.clone(),
            write_file: Arc::new(Mutex::new(write_file)),
            sync,
            last_frame: Arc::new(AtomicU64::new(last_frame)),
            index: Arc::new(RwLock::new(None)),
            read_write_lock: shared_state_for_path(&log_path).0,
            observability: shared_state_for_path(&log_path).1,
            last_commit: Arc::new(RwLock::new(last_commit)),
            poisoned: Arc::new(AtomicBool::new(false)),
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T: JournalEvent> Clone for DiskJournal<T> {
    fn clone(&self) -> Self {
        Self {
            owner: self.owner.clone(),
            journal_id: self.journal_id,
            run_id: self.run_id,
            path: self.path.clone(),
            write_file: self.write_file.clone(),
            sync: self.sync,
            index: self.index.clone(),
            last_frame: self.last_frame.clone(),
            read_write_lock: self.read_write_lock.clone(),
            last_commit: self.last_commit.clone(),
            poisoned: self.poisoned.clone(),
            definitions: self.definitions.clone(),
            observations: self.observations.clone(),
            observability: self.observability.clone(),
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: JournalEvent + 'static> DiskJournal<T> {
    async fn event_offset(&self, ulid: &Ulid) -> Result<Option<u64>, JournalError> {
        if let Some(index) = self.index.read().await.as_ref() {
            return Ok(index.get(ulid).copied());
        }
        let append_lock = self.read_write_lock.write().await;
        let mut index = self.index.write().await;
        if index.is_none() {
            let path = self.path.clone();
            let confirmed_end = self.observations.confirmed_end();
            let offsets = tokio::task::spawn_blocking(move || {
                let mut offsets = HashMap::new();
                scan_committed_frames::<T>(&path, confirmed_end, Some(&mut offsets))
                    .map(|_| offsets)
            })
            .await
            .map_err(|error| JournalError::Implementation {
                message: "Journal index task failed".into(),
                source: Box::new(error),
            })??;
            *index = Some(offsets);
        }
        let offset = index
            .as_ref()
            .and_then(|offsets| offsets.get(ulid).copied());
        drop(index);
        drop(append_lock);
        Ok(offset)
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> obzenflow_core::journal::JournalStorage<T> for DiskJournal<T> {
    fn storage_id(&self) -> &JournalId {
        &self.journal_id
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    fn storage_observation_reader(
        &self,
    ) -> Option<&dyn obzenflow_core::journal::JournalObservationReader> {
        Some(&self.observations)
    }

    fn storage_configure(&self, config: JournalConfig) -> Result<(), JournalError> {
        self.observability.configure(config.observability)
    }

    async fn storage_append(
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

    async fn storage_append_group(
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

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
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
        let mut admission = super::identity::CommitmentAdmission::open(&self.path)?;

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
                unscanned_tail,
            ) {
                Disposition::Yield(frame) => {
                    for record in frame.into_records() {
                        admission.admit(&record)?;
                        events.push(record);
                    }
                }
                Disposition::EndOfCommittedRecords
                | Disposition::Interrupted(_)
                | Disposition::Skip => break,
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

    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        let ulid = event_id.as_ulid();

        let Some(offset) = self.event_offset(&ulid).await? else {
            return Ok(None);
        };

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
                unscanned_tail,
            ) {
                Disposition::Yield(frame) => Ok(frame
                    .into_records()
                    .into_iter()
                    .find(|record| record.id().as_ulid() == ulid)),
                Disposition::EndOfCommittedRecords
                | Disposition::Interrupted(_)
                | Disposition::Skip => Ok(None),
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

    async fn storage_committed_position(&self) -> Result<u64, JournalError> {
        let _guard = self.read_write_lock.read().await;
        Ok(self
            .last_commit
            .read()
            .await
            .as_ref()
            .map_or(0, |commit| commit.reference.sequence))
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
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

    async fn storage_read_metrics_tail(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.observations.metrics_tail().await
    }

    async fn storage_read_last_n(
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
            match dispose(
                classify_frame::<T>(&buffer, &mut decoder, reader.offset()),
                termination,
                ReadPolicy::LiveTail,
                unscanned_tail,
            ) {
                Disposition::Yield(frame) => {
                    for record in frame.into_records().into_iter().rev() {
                        if results.len() >= count {
                            break;
                        }
                        results.push(record);
                    }
                }
                Disposition::EndOfCommittedRecords
                | Disposition::Interrupted(_)
                | Disposition::Skip => {
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
mod tail_tests;

#[cfg(test)]
mod tests;
