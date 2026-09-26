// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cursor-based reader for DiskJournal
//!
//! Tracks logical position and byte offset. Complete buffered frames can be
//! reused between polls. A poll takes ownership of the buffer before awaiting;
//! cancellation discards it and the next poll reopens at the committed offset.

use super::codec::Decoder;
use super::scanner::{classify_frame, dispose, read_frame_async, Disposition, ReadPolicy};
use async_trait::async_trait;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::provenance::JournalGroupMember;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::reader::JournalReader;
use std::collections::VecDeque;
use std::fs::File as StdFile;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, BufReader};
use tokio::sync::RwLock;

/// Live-tail polls allowed at the same unterminated record before a stuck or
/// crashed writer is treated as a hard error rather than an endless retry
/// (FLOWIP-120q). Sealed readers never reach this path.
const MAX_STALL_POLLS: u32 = 5;

// Amortise open/seek/read costs across ordinary wide-event frames. Only fully
// buffered, terminated frames are reused; a partial tail is always reread.
const READ_BUFFER_BYTES: usize = 64 * 1024;

fn open_existing_std_file(path: &Path) -> Result<StdFile, JournalError> {
    StdFile::open(path).map_err(|e| {
        tracing::error!(
            path = %path.display(),
            os_error = %e,
            "DiskJournalReader failed to open journal file"
        );
        JournalError::Implementation {
            message: format!("Failed to open journal file: {}", path.display()),
            source: Box::new(e),
        }
    })
}

fn observer_io(error: std::io::Error) -> JournalError {
    JournalError::Implementation {
        message: "Failed to read admitted journal".into(),
        source: error.into(),
    }
}

/// Reader for DiskJournal that maintains logical and byte position.
pub struct DiskJournalReader<T: JournalEvent> {
    identity: super::identity::JournalIdentity,
    last_commit: Option<obzenflow_core::event::CausalCommit>,
    decoder: Decoder,
    /// Current position (number of committed events read)
    position: u64,
    /// Byte offset of the next unread record. Advances only past a committed
    /// record, so it is both the rewind point for a torn tail (FLOWIP-120q) and
    /// the record position reported in corruption errors.
    read_offset: u64,
    initial_end: Option<u64>,
    /// Consecutive `Skip` polls at `read_offset` (live-tail stall guard).
    stall_polls: u32,
    /// Read policy: live-tail retries a torn tail, a sealed scan fails loud or
    /// tolerates a final torn tail per `tolerate_torn_tail`.
    policy: ReadPolicy,
    /// Reusable byte buffer for the framed-line reader.
    buf: Vec<u8>,
    /// Taken out before any read await, restored only after a settled frame.
    /// No journal lock is retained with this buffer.
    buffered_reader: Option<BufReader<File>>,
    #[cfg(test)]
    frame_read_gate: Option<Arc<tests::FrameReadGate>>,
    /// Remaining logical records from an already committed atomic-group frame.
    /// `read_offset` advances past the physical frame as soon as it is parsed;
    /// these members are then yielded one-by-one without another disk read.
    pending: VecDeque<super::log_record::LogRecord<T>>,
    pending_group_id: Option<String>,
    pending_group_size: Option<u32>,
    pending_group_next_index: u32,
    yielded_group_member: Option<JournalGroupMember>,
    /// Path to the journal file (for error messages)
    path: PathBuf,
    /// Whether we've reached EOF
    at_end: bool,
    /// Shared lock to avoid reading partial writes
    read_write_lock: Arc<RwLock<()>>,
    /// Whether a live reader may create a missing empty file.
    /// Observers never turn an unfinished append into corruption by timing out.
    observer: bool,
    /// Retain the admitted file identity, refusing replacement on a later poll.
    pinned_file: Option<Arc<StdFile>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> DiskJournalReader<T> {
    pub(super) fn with_initial_end(mut self, end: u64) -> Self {
        self.initial_end = Some(end);
        self
    }

    /// Create a new live-tail reader starting from the beginning
    pub async fn new(
        path: PathBuf,
        journal_id: JournalId,
        read_write_lock: Arc<RwLock<()>>,
    ) -> Result<Self, JournalError> {
        let identity = super::identity::read_identity(&path)?;
        if identity.journal_id != journal_id {
            return Err(obzenflow_core::event::CausalError::ConflictingCommitment.into());
        }
        // Validate that the file is readable. Polls lease their buffered handle;
        // cancellation forces a fresh handle at the committed cursor.
        let _std_file = StdFile::open(&path).map_err(|e| {
            tracing::error!(
                path = %path.display(),
                os_error = %e,
                "DiskJournalReader failed to open journal file"
            );
            JournalError::Implementation {
                message: format!("Failed to open journal file: {}", path.display()),
                source: Box::new(e),
            }
        })?;

        Ok(Self {
            identity,
            last_commit: None,
            decoder: Decoder::new(&path),
            position: 0,
            read_offset: 0,
            initial_end: None,
            stall_polls: 0,
            policy: ReadPolicy::LiveTail,
            buf: Vec::new(),
            buffered_reader: None,
            #[cfg(test)]
            frame_read_gate: None,
            pending: VecDeque::new(),
            pending_group_id: None,
            pending_group_size: None,
            pending_group_next_index: 0,
            yielded_group_member: None,
            path,
            at_end: false,
            read_write_lock,
            observer: false,
            pinned_file: None,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Open an existing journal for read-only sequential access under an explicit
    /// read policy. Missing journals are never created by readers.
    /// Archive readers pass a sealed policy (FLOWIP-120q).
    pub(crate) async fn open_existing(
        path: PathBuf,
        journal_id: JournalId,
        read_write_lock: Arc<RwLock<()>>,
        policy: ReadPolicy,
    ) -> Result<Self, JournalError> {
        let identity = super::identity::read_identity(&path)?;
        if identity.journal_id != journal_id {
            return Err(obzenflow_core::event::CausalError::ConflictingCommitment.into());
        }
        let _std_file = StdFile::open(&path).map_err(|e| {
            tracing::error!(
                path = %path.display(),
                os_error = %e,
                "DiskJournalReader failed to open existing journal file"
            );
            JournalError::Implementation {
                message: format!("Failed to open existing journal file: {}", path.display()),
                source: Box::new(e),
            }
        })?;

        Ok(Self {
            identity,
            last_commit: None,
            decoder: Decoder::new(&path),
            position: 0,
            read_offset: 0,
            initial_end: None,
            stall_polls: 0,
            policy,
            buf: Vec::new(),
            buffered_reader: None,
            #[cfg(test)]
            frame_read_gate: None,
            pending: VecDeque::new(),
            pending_group_id: None,
            pending_group_size: None,
            pending_group_next_index: 0,
            yielded_group_member: None,
            path,
            at_end: false,
            read_write_lock,
            observer: false,
            pinned_file: None,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Read-only consumer admission with a fixed, verified committed prefix.
    pub(crate) async fn open_observer(
        path: PathBuf,
        journal_id: JournalId,
    ) -> Result<Self, JournalError> {
        let scan_path = path.clone();
        let (file, end) = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let file = open_existing_std_file(&scan_path)?;
            let scan = || -> Result<u64, JournalError> {
                let length = file.metadata().map_err(observer_io)?.len();
                let mut input =
                    std::io::BufReader::new(file.try_clone().map_err(observer_io)?.take(length));
                let mut decoder = Decoder::new(&scan_path);
                let mut buf = Vec::new();
                let mut end = 0;
                while let Some((consumed, termination)) =
                    super::scanner::read_frame_sync(&mut input, &mut buf).map_err(observer_io)?
                {
                    match dispose(
                        classify_frame::<T>(&buf, &mut decoder, end),
                        termination,
                        ReadPolicy::LiveTail,
                    ) {
                        Disposition::Yield(_) => end += consumed as u64,
                        Disposition::Skip | Disposition::EndOfCommittedRecords => break,
                        Disposition::Corrupt(problem) => {
                            return Err(JournalError::Implementation {
                                message: format!(
                                    "Corrupt journal {} at offset {end}: {problem}",
                                    scan_path.display()
                                ),
                                source: std::io::Error::new(
                                    std::io::ErrorKind::InvalidData,
                                    problem.to_string(),
                                )
                                .into(),
                            })
                        }
                    }
                }
                Ok(end)
            };
            let end = scan()?;
            Ok::<_, JournalError>((file, end))
        })
        .await
        .map_err(|error| JournalError::Implementation {
            message: "Journal snapshot admission task failed".into(),
            source: error.into(),
        })??;
        let mut reader = Self::open_existing(
            path,
            journal_id,
            Arc::new(RwLock::new(())),
            ReadPolicy::LiveTail,
        )
        .await?;
        reader.initial_end = Some(end);
        reader.observer = true;
        reader.pinned_file = Some(Arc::new(file));
        Ok(reader)
    }

    /// Create a new live-tail reader advanced to a specific append position.
    /// Equivalent to `new()` followed by advancing past `start_position`
    /// committed records, so a reader created at position N matches a reader
    /// advanced past N records, including corruption and torn-tail handling
    /// (FLOWIP-120t: one parsing path, the same `dispose` traversal as `next()`).
    pub async fn from_position(
        path: PathBuf,
        journal_id: JournalId,
        start_position: u64,
        read_write_lock: Arc<RwLock<()>>,
    ) -> Result<Self, JournalError> {
        let mut reader = Self::new(path, journal_id, read_write_lock).await?;
        reader.advance_to(start_position).await?;
        Ok(reader)
    }

    /// Advance the cursor forward to `target` committed records from the start,
    /// driving the same framed scanner and `dispose` path as `next()`. Stops
    /// early at a clean EOF or an unterminated torn tail; fails loud on committed
    /// corruption before the target.
    async fn advance_to(&mut self, target: u64) -> Result<(), JournalError> {
        if self.position >= target {
            return Ok(());
        }
        // Lock through a cloned Arc so the guard borrows a local, not `self`,
        // leaving `self` free for the `&mut self` advance below.
        let lock = self.read_write_lock.clone();
        let _read_guard = lock.read().await;
        let mut reader = self.reader_at_offset().await?;
        while self.position < target {
            let (disposition, frame_start) = self.advance_one(&mut reader).await?;
            match disposition {
                Disposition::Yield(_) => {}
                Disposition::EndOfCommittedRecords => {
                    self.at_end = true;
                    break;
                }
                Disposition::Skip => {
                    self.at_end = false;
                    break;
                }
                Disposition::Corrupt(problem) => {
                    return Err(JournalError::Implementation {
                        message: format!(
                            "Failed to parse journal record at offset {} in {}: {problem}",
                            frame_start,
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
        self.buffered_reader = Some(reader);
        Ok(())
    }

    /// Read one logical record from `reader` under the current policy, advancing
    /// `read_offset` and `position` exactly as a committed read does. The
    /// reader advances past corruption only when a checked header establishes
    /// the next physical boundary. Returns the disposition plus
    /// the byte offset where the disposed frame began, for
    /// corruption reporting. Leaves `at_end`/`stall_polls` to the caller. Clean
    /// EOF maps to `EndOfCommittedRecords`.
    async fn advance_one<B: tokio::io::AsyncBufRead + Unpin>(
        &mut self,
        reader: &mut B,
    ) -> Result<(Disposition<T>, u64), JournalError> {
        self.yielded_group_member = None;
        if let Some(record) = self.pending.pop_front() {
            let group_id = self.pending_group_id.clone();
            self.yielded_group_member = group_id.as_ref().map(|_| JournalGroupMember {
                index: self.pending_group_next_index,
                size: self
                    .pending_group_size
                    .expect("pending atomic group has a member count"),
            });
            self.pending_group_next_index = self.pending_group_next_index.saturating_add(1);
            if self.pending.is_empty() {
                self.pending_group_id = None;
                self.pending_group_size = None;
                self.pending_group_next_index = 0;
            }
            self.position += 1;
            return Ok((
                Disposition::Yield(match group_id {
                    Some(group_id) => super::log_record::LogFrame::AtomicGroup {
                        group_id,
                        records: vec![record],
                    },
                    None => super::log_record::LogFrame::Record(record),
                }),
                self.read_offset,
            ));
        }

        {
            let frame_start = self.read_offset;
            let Some((consumed, termination)) = read_frame_async(reader, &mut self.buf)
                .await
                .map_err(|e| JournalError::Implementation {
                    message: format!("Failed to read from journal at offset {}", self.read_offset),
                    source: Box::new(e),
                })?
            else {
                return Ok((Disposition::EndOfCommittedRecords, frame_start));
            };

            #[cfg(test)]
            if let Some(gate) = self.frame_read_gate.take() {
                gate.entered.notify_one();
                gate.release.notified().await;
            }

            // Large atomic frames are bounded but may still be expensive to
            // decode. Keep that CPU work off Tokio's executor workers.
            let bytes = std::mem::take(&mut self.buf);
            let mut decoder = std::mem::replace(&mut self.decoder, Decoder::new(&self.path));
            let (classification, decoder, bytes) = tokio::task::spawn_blocking(move || {
                let classification = classify_frame::<T>(&bytes, &mut decoder, frame_start);
                (classification, decoder, bytes)
            })
            .await
            .map_err(|error| JournalError::Implementation {
                message: "Journal decoder task failed".into(),
                source: Box::new(error),
            })?;
            self.decoder = decoder;
            self.buf = bytes;
            match dispose(classification, termination, self.policy) {
                Disposition::Yield(frame) => {
                    let group_id = frame.group_id().map(str::to_string);
                    let records = frame.into_records();
                    let mut last_commit = self.last_commit.clone();
                    for record in &records {
                        let commitment = obzenflow_core::event::CausalCommit::from_record(record)?;
                        let previous = last_commit.as_ref().map(|previous| previous.reference);
                        if commitment.reference.run_id != self.identity.run_id
                            || commitment.reference.journal_writer_id.as_journal_id()
                                != &self.identity.journal_id
                            || previous.is_some_and(|previous| {
                                commitment.reference.sequence <= previous.sequence
                                    || record
                                        .envelope
                                        .provenance
                                        .journal
                                        .causal
                                        .previous
                                        .is_some_and(|claimed| {
                                            claimed.sequence == previous.sequence
                                                && claimed != previous
                                        })
                            })
                        {
                            return Err(
                                obzenflow_core::event::CausalError::ConflictingCommitment.into()
                            );
                        }
                        last_commit = Some(commitment);
                    }
                    self.last_commit = last_commit;
                    self.read_offset += consumed as u64;
                    self.pending_group_id = group_id;
                    self.pending_group_size = self
                        .pending_group_id
                        .as_ref()
                        .map(|_| u32::try_from(records.len()))
                        .transpose()
                        .map_err(|_| JournalError::Implementation {
                            message: "Atomic journal group exceeds u32 member capacity".to_string(),
                            source: "atomic journal group is too large".into(),
                        })?;
                    self.pending_group_next_index = 0;
                    self.pending.extend(records);
                    let record =
                        self.pending
                            .pop_front()
                            .ok_or_else(|| JournalError::Implementation {
                                message: "Committed journal frame contained no records".to_string(),
                                source: "empty committed journal frame".into(),
                            })?;
                    let group_id = self.pending_group_id.clone();
                    self.yielded_group_member = group_id.as_ref().map(|_| JournalGroupMember {
                        index: self.pending_group_next_index,
                        size: self
                            .pending_group_size
                            .expect("pending atomic group has a member count"),
                    });
                    self.pending_group_next_index = self.pending_group_next_index.saturating_add(1);
                    if self.pending.is_empty() {
                        self.pending_group_id = None;
                        self.pending_group_size = None;
                        self.pending_group_next_index = 0;
                    }
                    self.position += 1;
                    Ok((
                        Disposition::Yield(match group_id {
                            Some(group_id) => super::log_record::LogFrame::AtomicGroup {
                                group_id,
                                records: vec![record],
                            },
                            None => super::log_record::LogFrame::Record(record),
                        }),
                        frame_start,
                    ))
                }
                Disposition::Corrupt(problem) => {
                    if super::codec::frame::frame_length(&self.buf)
                        .is_ok_and(|length| length == consumed)
                    {
                        self.read_offset += consumed as u64;
                    }
                    Ok((Disposition::Corrupt(problem), frame_start))
                }
                other => Ok((other, frame_start)),
            }
        }
    }

    async fn reader_at_offset(&self) -> Result<BufReader<File>, JournalError> {
        let std_file = open_existing_std_file(&self.path)?;
        if let Some(pinned) = &self.pinned_file {
            let actual = std_file.metadata().map_err(observer_io)?;
            let expected = pinned.metadata().map_err(observer_io)?;
            #[cfg(unix)]
            let replaced = {
                use std::os::unix::fs::MetadataExt;
                actual.dev() != expected.dev() || actual.ino() != expected.ino()
            };
            #[cfg(not(unix))]
            let replaced = actual.created().ok() != expected.created().ok();
            if replaced || actual.len() < self.read_offset {
                return Err(JournalError::Implementation {
                    message: format!(
                        "Admitted journal was replaced or truncated: {}",
                        self.path.display()
                    ),
                    source: std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "journal identity changed",
                    )
                    .into(),
                });
            }
        }
        let mut reader = BufReader::with_capacity(READ_BUFFER_BYTES, File::from_std(std_file));
        reader
            .seek(SeekFrom::Start(self.read_offset))
            .await
            .map_err(|e| JournalError::Implementation {
                message: format!("Failed to seek journal to offset {}: {e}", self.read_offset),
                source: Box::new(e),
            })?;
        Ok(reader)
    }
}

#[async_trait]
impl<T: JournalEvent> JournalReader<T> for DiskJournalReader<T> {
    fn initial_prefix_complete(&self) -> Result<bool, JournalError> {
        let end = self
            .initial_end
            .ok_or(JournalError::InitialPrefixUnsupported)?;
        Ok(self.read_offset > end || (self.read_offset == end && self.pending.is_empty()))
    }

    async fn next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        // Don't permanently latch at_end: a live-tail reader retries after EOF to
        // pick up new appends. Only previously buffered complete frames can be
        // reused. A partial buffered suffix may have been repaired since the
        // last poll, so reopen from read_offset before reading that suffix.
        // Lock through a cloned Arc so the guard borrows a local, not `self`,
        // leaving `self` free for the `&mut self` advance below.
        let lock = self.read_write_lock.clone();
        let _read_guard = lock.read().await;
        let mut reader = match self.buffered_reader.take() {
            Some(reader)
                if !self.pending.is_empty()
                    || super::codec::frame::frame_length(reader.buffer())
                        .is_ok_and(|length| length <= reader.buffer().len()) =>
            {
                reader
            }
            _ => self.reader_at_offset().await?,
        };

        let (disposition, frame_start) = self.advance_one(&mut reader).await?;
        // There is no await between committed cursor advancement and returning
        // the record. An interrupted I/O await leaves buffered_reader empty;
        // read_offset still identifies the next unconsumed physical frame.
        if matches!(&disposition, Disposition::Yield(_))
            || (matches!(&disposition, Disposition::Corrupt(_)) && self.read_offset > frame_start)
        {
            self.buffered_reader = Some(reader);
        }
        match disposition {
            Disposition::Yield(frame) => {
                let _journal_group_id = frame.group_id().map(str::to_string);
                let _journal_group_member = self.yielded_group_member.take();
                let mut records = frame.into_records();
                let record = records
                    .pop()
                    .expect("reader yields exactly one logical record");
                self.at_end = false;
                self.stall_polls = 0;
                Ok(Some(record))
            }
            Disposition::EndOfCommittedRecords => {
                // Clean EOF (live tail) or a tolerated final torn tail (sealed).
                self.at_end = true;
                self.stall_polls = 0;
                Ok(None)
            }
            Disposition::Skip => {
                // Live-tail unterminated tail: advance_one left read_offset at the
                // record start, so the next poll re-reads it once the writer
                // completes it.
                self.at_end = false;
                self.stall_polls = self.stall_polls.saturating_add(1);
                if !self.observer && self.stall_polls > MAX_STALL_POLLS {
                    let msg = format!(
                        "Partial read retries exceeded at offset {} in {}",
                        self.read_offset,
                        self.path.display()
                    );
                    tracing::error!(
                        read_offset = self.read_offset,
                        path = %self.path.display(),
                        stall_polls = self.stall_polls,
                        "Journal record partial retry budget exceeded"
                    );
                    return Err(JournalError::Implementation {
                        message: msg.clone(),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::UnexpectedEof,
                            msg,
                        )),
                    });
                }
                Ok(None)
            }
            Disposition::Corrupt(problem) => {
                // A checked header permits best-effort continuation at its next
                // boundary. An invalid header leaves the cursor at the error.
                self.stall_polls = 0;
                tracing::error!(
                    read_offset = frame_start,
                    path = %self.path.display(),
                    parse_error = %problem,
                    "Failed to parse journal record"
                );
                Err(JournalError::Implementation {
                    message: format!(
                        "Failed to parse journal record at offset {} in {}: {problem}",
                        frame_start,
                        self.path.display()
                    ),
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        problem.to_string(),
                    )),
                })
            }
        }
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn is_at_end(&self) -> bool {
        self.at_end
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::disk::log_record::serialize_record;
    use obzenflow_core::event::chain_event::ChainEventFactory;
    use obzenflow_core::event::payloads::JournalPayload;
    use obzenflow_core::event::JournalRecord;
    use obzenflow_core::{ChainEvent, JournalId, StageId, WriterId};
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[derive(Default)]
    pub(super) struct FrameReadGate {
        pub(super) entered: tokio::sync::Notify,
        pub(super) release: tokio::sync::Notify,
    }

    #[tokio::test]
    async fn cancelled_buffered_read_reopens_at_committed_cursor_and_preserves_group_members() {
        use crate::journal::disk::DiskJournal;
        use obzenflow_core::Journal;
        for observer in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("cancelled.log");
            let stage = StageId::new();
            let journal = DiskJournal::<ChainEvent>::with_owner(
                path.clone(),
                obzenflow_core::JournalOwner::stage(stage),
            )
            .unwrap();
            let first = journal
                .append(
                    ChainEventFactory::data_event(stage.into(), "first", serde_json::json!({})),
                    Default::default(),
                )
                .await
                .unwrap();
            let large = journal
                .append(
                    ChainEventFactory::data_event(
                        stage.into(),
                        "large",
                        serde_json::json!({"body": "x".repeat(READ_BUFFER_BYTES * 3)}),
                    ),
                    Default::default(),
                )
                .await
                .unwrap();
            let group = journal
                .append_group(
                    "after-cancel",
                    (0..2)
                        .map(|i| {
                            ChainEventFactory::data_event(
                                stage.into(),
                                "member",
                                serde_json::json!({"i": i}),
                            )
                        })
                        .collect(),
                    Default::default(),
                )
                .await
                .unwrap();
            let mut reader = if observer {
                DiskJournalReader::<ChainEvent>::open_observer(path, *journal.id())
                    .await
                    .unwrap()
            } else {
                DiskJournalReader::<ChainEvent>::new(path, *journal.id(), Arc::new(RwLock::new(())))
                    .await
                    .unwrap()
            };
            assert_eq!(
                reader
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .envelope
                    .provenance
                    .event
                    .id,
                first.envelope.provenance.event.id
            );
            let gate = Arc::new(FrameReadGate::default());
            reader.frame_read_gate = Some(gate.clone());
            tokio::select! {
                result = reader.next() => panic!("read must pause before cursor commitment: {result:?}"),
                _ = gate.entered.notified() => {}
            }
            assert_eq!(reader.position(), 1);
            assert!(
                reader.buffered_reader.is_none(),
                "cancellation must not retain a stream positioned after the unconsumed frame"
            );
            assert_eq!(
                reader
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .envelope
                    .provenance
                    .event
                    .id,
                large.envelope.provenance.event.id
            );
            for (index, expected) in group.iter().enumerate() {
                let row = reader.next().await.unwrap().unwrap();
                assert_eq!(
                    row.envelope.provenance.event.id,
                    expected.envelope.provenance.event.id
                );
                assert_eq!(
                    row.envelope.provenance.journal.journal_group_member,
                    Some(JournalGroupMember {
                        index: index as u32,
                        size: 2
                    })
                );
            }
            assert_eq!(reader.position(), 4);
            assert!(reader.next().await.unwrap().is_none());
            assert!(reader.is_at_end());
        }
    }

    #[tokio::test]
    async fn buffered_partial_tail_is_reread_after_repair_and_after_live_eof() {
        use std::io::Seek;
        for observe_partial in [false, true] {
            let mut file = NamedTempFile::new().unwrap();
            let (identity, lease) =
                super::super::identity::open_identity(file.path(), None).unwrap();
            drop(lease);
            let journal_id = identity.journal_id;
            let mut previous = None;
            let stage = StageId::new();
            let mut make_record = || {
                let event =
                    ChainEventFactory::data_event(stage.into(), "tail", serde_json::json!({}));
                super::super::identity::fixture_record(identity, event, &mut previous)
            };
            let first = make_record();
            write_framed_record(&mut file, &first);
            let committed_end = file.as_file().metadata().unwrap().len();
            file.write_all(&serialize_record(&make_record()).unwrap()[..20])
                .unwrap();
            let mut reader = DiskJournalReader::<ChainEvent>::new(
                file.path().to_path_buf(),
                journal_id,
                Arc::new(RwLock::new(())),
            )
            .await
            .unwrap()
            .with_initial_end(committed_end);
            assert!(!reader.initial_prefix_complete().unwrap());
            assert_eq!(
                reader
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .envelope
                    .provenance
                    .event
                    .id,
                first.envelope.provenance.event.id
            );
            assert!(reader.initial_prefix_complete().unwrap());
            if observe_partial {
                assert!(reader.next().await.unwrap().is_none());
                assert!(!reader.is_at_end());
                assert!(
                    reader.initial_prefix_complete().unwrap(),
                    "prefix completion is independent of a partial live tail"
                );
                assert_eq!(reader.position(), 1);
            }
            file.as_file_mut().set_len(committed_end).unwrap();
            file.as_file_mut()
                .seek(SeekFrom::Start(committed_end))
                .unwrap();
            let second = make_record();
            write_framed_record(&mut file, &second);
            assert_eq!(
                reader
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .envelope
                    .provenance
                    .event
                    .id,
                second.envelope.provenance.event.id
            );
            assert!(reader.next().await.unwrap().is_none());
            assert!(reader.is_at_end());
            let third = make_record();
            write_framed_record(&mut file, &third);
            assert_eq!(
                reader
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .envelope
                    .provenance
                    .event
                    .id,
                third.envelope.provenance.event.id
            );
            assert_eq!(reader.position(), 3);
        }
    }

    fn write_framed_record<P: JournalPayload>(file: &mut NamedTempFile, record: &JournalRecord<P>) {
        let bytes = serialize_record(record).unwrap();
        file.write_all(&bytes).unwrap();
    }

    #[tokio::test]
    async fn test_sequential_reading() {
        // Create a temporary journal file
        let mut temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write some test records
        let writer_id = WriterId::from(StageId::new());
        let (identity, lease) = super::super::identity::open_identity(&path, None).unwrap();
        drop(lease);
        let journal_id = identity.journal_id;
        let mut previous = None;
        for i in 0..5 {
            let event = ChainEventFactory::data_event(
                writer_id,
                "test.event",
                serde_json::json!({"index": i}),
            );
            let record = super::super::identity::fixture_record(identity, event, &mut previous);
            write_framed_record(&mut temp_file, &record);
        }
        temp_file.flush().unwrap();

        let read_write_lock = Arc::new(RwLock::new(()));
        // Create reader and read all events
        let mut reader =
            DiskJournalReader::<ChainEvent>::new(path, journal_id, read_write_lock.clone())
                .await
                .unwrap();

        for i in 0..5 {
            let envelope = reader.next().await.unwrap().expect("Should have event");
            assert_eq!(envelope.payload()["index"], i);
            assert_eq!(reader.position(), i as u64 + 1);
        }

        // Should be at end
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.is_at_end());
    }

    #[tokio::test]
    async fn test_from_position_resume() {
        // Create a temporary journal file
        let mut temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_path_buf();

        // Write some test records
        let writer_id = WriterId::from(StageId::new());
        let (identity, lease) = super::super::identity::open_identity(&path, None).unwrap();
        drop(lease);
        let journal_id = identity.journal_id;
        let mut previous = None;
        for i in 0..10 {
            let event = ChainEventFactory::data_event(
                writer_id,
                "test.event",
                serde_json::json!({"index": i}),
            );
            let record = super::super::identity::fixture_record(identity, event, &mut previous);
            write_framed_record(&mut temp_file, &record);
        }
        temp_file.flush().unwrap();

        let read_write_lock = Arc::new(RwLock::new(()));
        // A reader created at position 5 reads index 5 next (FLOWIP-120t: the same
        // dispose-based traversal as next(), no separate read_line path).
        let mut reader = DiskJournalReader::<ChainEvent>::from_position(
            path.clone(),
            journal_id,
            5,
            read_write_lock.clone(),
        )
        .await
        .unwrap();
        assert_eq!(reader.position(), 5);
        let envelope = reader.next().await.unwrap().expect("Should have event");
        assert_eq!(envelope.payload()["index"], 5);

        // Create new reader from position 7
        let mut reader2 =
            DiskJournalReader::<ChainEvent>::from_position(path, journal_id, 7, read_write_lock)
                .await
                .unwrap();
        assert_eq!(reader2.position(), 7);

        // Should read index 7
        let envelope = reader2.next().await.unwrap().expect("Should have event");
        assert_eq!(envelope.payload()["index"], 7);
    }

    #[tokio::test]
    async fn missing_journal_is_not_created_by_a_reader() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("missing.log");
        assert!(DiskJournalReader::<ChainEvent>::new(
            path.clone(),
            JournalId::new(),
            Arc::new(RwLock::new(()))
        )
        .await
        .is_err());
        assert!(!path.exists());
    }
}
