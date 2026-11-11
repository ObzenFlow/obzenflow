// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::codec;
use super::DiskJournal;
use chrono::Utc;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::provenance::{JournalGroupMember, JournalProvenance};
use obzenflow_core::event::{CausalCoordinate, CausalFrontier, JournalClock, JournalEvent};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_runtime::runtime_config::JournalSync;
use std::fs::File as StdFile;
use std::io::SeekFrom;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
    Poisoned(JournalError),
}

/// The append file operations, behind a trait so the commit/rollback logic is
/// testable with a fake writer instead of induced filesystem failures.
trait FrameSink {
    fn end_offset(&mut self) -> std::io::Result<u64>;
    fn write_all(&mut self, bytes: &[u8]) -> std::io::Result<()>;
    fn sync(&mut self) -> std::io::Result<()>;
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
    fn sync(&mut self) -> std::io::Result<()> {
        self.sync_data()
    }
    fn rollback_to(&mut self, offset: u64) -> std::io::Result<()> {
        use std::io::Seek;
        self.set_len(offset)?;
        self.seek(SeekFrom::Start(offset))?;
        Ok(())
    }
}

fn append_frame<S: FrameSink>(
    sink: &mut S,
    bytes: &[u8],
    path: &Path,
    sync: JournalSync,
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

    if let Err(write_error) = sink.write_all(bytes) {
        return Err(match sink.rollback_to(offset) {
            Ok(()) => AppendFailure::RolledBack(JournalError::Implementation {
                message: format!(
                    "Failed to append record to {}; rolled back to offset {offset}",
                    path.display()
                ),
                source: Box::new(write_error),
            }),
            Err(rollback_error) => AppendFailure::Poisoned(JournalError::Implementation {
                message: format!(
                    "Failed to append record to {}; rollback to offset {offset} also failed: {rollback_error}",
                    path.display()
                ),
                source: Box::new(write_error),
            }),
        });
    }
    match sync {
        JournalSync::BeforeAck => sink.sync().map_err(|sync_error| {
            AppendFailure::Poisoned(JournalError::Implementation {
                message: format!(
                    "Failed to sync the record appended to {} at offset {offset}",
                    path.display()
                ),
                source: Box::new(sync_error),
            })
        })?,
        JournalSync::Off => {}
    }
    Ok(CommittedAppend {
        offset,
        next_offset: offset + bytes.len() as u64,
    })
}

// The complete commit unit outlives a cancelled waiter. Runtime retains its
// own receipt until this task has finalized clocks and indexes as well as bytes.
pub(super) async fn retain_commit<R: Send + 'static>(
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
    pub(super) async fn append_record(
        &self,
        event: T,
        frontier: &CausalFrontier,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        crate::journal::ensure_owned(self.owner.as_ref())?;
        let mut records = self.commit_frame(None, vec![event], frontier).await?;
        Ok(records.pop().expect("one record"))
    }

    pub(super) async fn append_records(
        &self,
        group_id: &str,
        events: Vec<T>,
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
        self.commit_frame(Some(group_id), events, frontier).await
    }

    async fn commit_frame(
        &self,
        group_id: Option<&str>,
        events: Vec<T>,
        frontier: &CausalFrontier,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
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

        let group_size = group_id
            .map(|group_id| {
                u32::try_from(events.len()).map_err(|_| JournalError::Implementation {
                    message: format!(
                        "Atomic journal group '{group_id}' exceeds u32 member capacity"
                    ),
                    source: "atomic journal group is too large".into(),
                })
            })
            .transpose()?;
        let last_commit = self.last_commit.read().await;
        let mut latest = None;
        let mut envelopes = Vec::with_capacity(events.len());
        for (index, event) in events.into_iter().enumerate() {
            let (commitment, predecessor) = JournalClock::prepare(
                self.run_id,
                CausalCoordinate::new(self.journal_id.into()),
                *event.id(),
                latest.as_ref().or(last_commit.as_ref()),
                frontier,
            )?;
            let (authored, payload) = event.into_parts();
            envelopes.push(
                JournalRecord::commit(
                    authored,
                    payload,
                    JournalProvenance {
                        journal_writer_id: JournalWriterId::from(self.journal_id),
                        run_id: self.run_id,
                        previous: predecessor,
                        vector_clock: commitment.clock.clone(),
                        timestamp: Utc::now(),
                        journal_group_id: group_id.map(str::to_owned),
                        journal_group_member: group_size.map(|size| JournalGroupMember {
                            index: u32::try_from(index)
                                .expect("group size was checked against u32 capacity"),
                            size,
                        }),
                    },
                )
                .map_err(|error| JournalError::Implementation {
                    message: "Invalid journal record".to_string(),
                    source: Box::new(error),
                })?,
            );
            latest = Some(commitment);
        }
        drop(last_commit);

        let prepared = codec::prepare(&envelopes, group_id, &self.path, self.definitions.clone());
        let mut prepared = prepared.map_err(|e| JournalError::Implementation {
            message: match group_id {
                Some(group_id) => format!("Failed to serialize atomic journal group '{group_id}'"),
                None => "Failed to serialize record".to_string(),
            },
            source: Box::new(e),
        })?;

        let path = self.path.clone();
        let write_file = self.write_file.clone();
        let sync = self.sync;
        let bytes = std::mem::take(&mut prepared.bytes);
        let trailer = bytes.len() - codec::frame::TRAILER_LEN;
        let observation_crc = u32::from_le_bytes(bytes[trailer..trailer + 4].try_into().unwrap());

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
                append_frame(&mut *file, &bytes, &path, sync)
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
            group_id,
            members = envelopes.len(),
            offset = committed.offset,
            bytes = committed.next_offset - committed.offset,
            write_lock_ptr = ?Arc::as_ptr(&self.read_write_lock),
            "DiskJournal appended frame"
        );

        if let Some(index) = self.index.write().await.as_mut() {
            for record in &envelopes {
                index.insert(record.id().as_ulid(), committed.offset);
            }
        }
        *self.last_commit.write().await = latest;
        self.observations.committed(
            &envelopes,
            committed.offset,
            committed.next_offset,
            observation_crc,
        );

        Ok(envelopes)
    }
}

#[cfg(test)]
mod tests;
