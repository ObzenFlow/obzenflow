// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded payload-blind scans, retaining lock acquisition and I/O across cancellation.
use super::codec::{frame, Decoder};
use super::identity::{read_identity, CommitmentAdmission};
use super::scanner::read_frame_sync;
use async_trait::async_trait;
use obzenflow_core::event::{JournalEvent, JournalRecord};
use obzenflow_core::journal::reader::{
    JournalReportStorageReader, ReportScan, ReportScanBudget, ReportScanItem,
};
use obzenflow_core::journal::JournalError;
use obzenflow_core::JournalId;
use std::collections::VecDeque;
use std::fs::File;
use std::future::Future;
use std::io::{BufReader, Seek, SeekFrom};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::{OwnedRwLockReadGuard, RwLock};
use tokio::task::JoinError;

#[cfg(test)]
mod tests;

fn error(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> JournalError {
    JournalError::Implementation {
        message: "Selective journal read failed".into(),
        source: source.into(),
    }
}

struct ScanState {
    path: PathBuf,
    input: BufReader<File>,
    buf: Vec<u8>,
    offset: u64,
    through: u64,
    decoder: Decoder,
    admission: CommitmentAdmission,
    lock: Arc<RwLock<()>>,
    cancelled: Arc<AtomicBool>,
    #[cfg(test)]
    after_frame_read: Option<Box<dyn FnOnce() + Send + Sync>>,
}

struct Batch<T: JournalEvent> {
    records: VecDeque<JournalRecord<T::Payload>>,
    through: u64,
    bytes: usize,
    tail: Option<bool>,
}

impl ScanState {
    fn scan<T: JournalEvent>(
        &mut self,
        budget: ReportScanBudget,
        first_guard: OwnedRwLockReadGuard<()>,
    ) -> Result<Batch<T>, JournalError> {
        // Pin the opened incarnation, refusing replacement/truncation on a later quantum.
        let actual = std::fs::metadata(&self.path).map_err(error)?;
        let opened = self.input.get_ref().metadata().map_err(error)?;
        #[cfg(unix)]
        let replaced = {
            use std::os::unix::fs::MetadataExt;
            actual.dev() != opened.dev() || actual.ino() != opened.ino()
        };
        #[cfg(not(unix))]
        let replaced = actual.created().ok() != opened.created().ok();
        if replaced || actual.len() < self.offset {
            return Err(error("journal replaced or truncated"));
        }
        let from = self.through;
        let mut bytes = 0;
        let mut first_guard = Some(first_guard);
        loop {
            if self.cancelled.load(Ordering::Relaxed) {
                return Err(error("reader dropped"));
            }
            // The first guard was acquired asynchronously, without occupying a
            // blocking worker. Between frames never wait for a writer which may
            // need this same pool to commit and release its lock. A contended
            // quantum yields only its already verified prefix, never a tail.
            let next_guard = if first_guard.is_some() {
                None
            } else {
                match self.lock.try_read() {
                    Ok(guard) => Some(guard),
                    Err(_) => {
                        return Ok(Batch {
                            records: VecDeque::new(),
                            through: self.through,
                            bytes,
                            tail: None,
                        });
                    }
                }
            };
            let raw = read_frame_sync(&mut self.input, &mut self.buf);
            // Only raw I/O holds the journal lock. Parsing, metadata loading
            // and report reconstruction cannot block the next append.
            drop(next_guard);
            drop(first_guard.take());
            let raw = raw.map_err(error)?;
            #[cfg(test)]
            if let Some(after_frame_read) = self.after_frame_read.take() {
                after_frame_read();
            }
            let Some((consumed, _)) = raw else {
                return Ok(Batch {
                    records: VecDeque::new(),
                    through: self.through,
                    bytes,
                    tail: Some(true),
                });
            };
            let body = match frame::validate(&self.buf) {
                Ok(body) => body,
                Err(frame::FrameProblem::Incomplete(_)) => {
                    // Never retain a partial buffered tail across a poll.
                    self.input
                        .seek(SeekFrom::Start(self.offset))
                        .map_err(error)?;
                    return Ok(Batch {
                        records: VecDeque::new(),
                        through: self.through,
                        bytes: bytes + consumed,
                        tail: Some(false),
                    });
                }
                Err(problem) => return Err(error(frame::io_error(problem))),
            };
            let selected = self
                .decoder
                .decode_selected::<T>(body, self.offset)
                .map_err(error)?;
            let route = selected.routing;
            self.admission
                .admit_range(route.first, route.previous, route.last)?;
            self.offset += consumed as u64;
            self.through = route.last.sequence;
            bytes += consumed;
            // Stop at the first frame with candidates. Only its selected members
            // are privately retained, under the existing hard group byte bound.
            if !selected.records.is_empty()
                || self.through - from >= budget.records.max(1) as u64
                || bytes >= budget.bytes.max(1)
            {
                return Ok(Batch {
                    records: selected.records.into(),
                    through: self.through,
                    bytes,
                    tail: None,
                });
            }
        }
    }
}

type ScanResult<T> = Result<(ScanState, Result<Batch<T>, JournalError>), JoinError>;
type ScanJob<T> = Pin<Box<dyn Future<Output = ScanResult<T>> + Send + Sync>>;

async fn run_scan<T: JournalEvent>(
    mut state: ScanState,
    budget: ReportScanBudget,
) -> ScanResult<T> {
    let first_guard = state.lock.clone().read_owned().await;
    #[cfg(feature = "bench-instrumentation")]
    obzenflow_core::benchmark::add(obzenflow_core::benchmark::Counter::DecodeBlockingJobs, 1);
    tokio::task::spawn_blocking(move || {
        let result = state.scan::<T>(budget, first_guard);
        (state, result)
    })
    .await
}

pub(super) struct DiskReportReader<T: JournalEvent> {
    state: Option<ScanState>,
    job: Option<ScanJob<T>>,
    ready: Option<Batch<T>>,
    position: u64,
    initial_position: u64,
    at_end: bool,
    stall_polls: u32,
    failed: Option<String>,
    seek_limit: Option<u64>,
    cancelled: Arc<AtomicBool>,
}

impl<T: JournalEvent> Drop for DiskReportReader<T> {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}

impl<T: JournalEvent> DiskReportReader<T> {
    pub(super) async fn new(
        path: PathBuf,
        journal: JournalId,
        lock: Arc<RwLock<()>>,
        initial_position: u64,
        position: u64,
    ) -> Result<Self, JournalError> {
        let identity = read_identity(&path)?;
        if identity.journal_id != journal {
            return Err(error("journal identity mismatch"));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let state = ScanState {
            input: BufReader::with_capacity(64 * 1024, File::open(&path).map_err(error)?),
            decoder: Decoder::new(&path),
            admission: CommitmentAdmission::new(identity),
            path,
            buf: Vec::new(),
            offset: 0,
            through: 0,
            lock,
            cancelled: cancelled.clone(),
            #[cfg(test)]
            after_frame_read: None,
        };
        let target = position.min(initial_position);
        let mut reader = Self {
            state: Some(state),
            job: None,
            ready: None,
            position: 0,
            initial_position,
            at_end: false,
            stall_polls: 0,
            failed: None,
            seek_limit: Some(target),
            cancelled,
        };
        while reader.position < target {
            if matches!(
                reader
                    .storage_next_report(ReportScanBudget::default())
                    .await?
                    .item,
                ReportScanItem::Tail { .. }
            ) {
                break;
            }
        }
        reader.seek_limit = None;
        Ok(reader)
    }
}

#[async_trait]
impl<T: JournalEvent> JournalReportStorageReader<T> for DiskReportReader<T> {
    async fn storage_next_report(
        &mut self,
        budget: ReportScanBudget,
    ) -> Result<ReportScan<T::Payload>, JournalError> {
        if let Some(problem) = &self.failed {
            return Err(error(problem.clone()));
        }
        loop {
            if let Some(batch) = &mut self.ready {
                let bytes = std::mem::take(&mut batch.bytes);
                if let Some(record) = batch.records.front() {
                    if let Some(limit) = self
                        .seek_limit
                        .filter(|limit| record.local_sequence() > *limit)
                    {
                        self.position = limit;
                        return Ok(ReportScan {
                            item: ReportScanItem::Progress,
                            scanned_bytes: bytes,
                        });
                    }
                    let record = batch.records.pop_front().unwrap();
                    self.position = record.local_sequence();
                    self.at_end = false;
                    self.stall_polls = 0;
                    return Ok(ReportScan {
                        item: ReportScanItem::Record(Box::new(record)),
                        scanned_bytes: bytes,
                    });
                }
                let through = self
                    .seek_limit
                    .map_or(batch.through, |limit| batch.through.min(limit));
                if self.position < through {
                    self.position = through;
                    self.at_end = false;
                    self.stall_polls = 0;
                    return Ok(ReportScan {
                        item: ReportScanItem::Progress,
                        scanned_bytes: bytes,
                    });
                }
                let tail = batch.tail;
                self.ready = None;
                if let Some(committed_end) = tail {
                    self.at_end = committed_end;
                    self.stall_polls = if committed_end {
                        0
                    } else {
                        self.stall_polls.saturating_add(1)
                    };
                    if self.stall_polls > super::reader::MAX_STALL_POLLS {
                        let problem = "partial frame retry budget exceeded";
                        self.failed = Some(problem.into());
                        return Err(error(problem));
                    }
                    return Ok(ReportScan {
                        item: ReportScanItem::Tail { committed_end },
                        scanned_bytes: bytes,
                    });
                }
            }
            if self.job.is_none() {
                let state = self.state.take().expect("one owned scan state");
                self.job = Some(Box::pin(run_scan::<T>(state, budget)));
            }
            // Cancellation retains the entire operation, including its place in
            // the lock queue, its cursor and any submitted blocking job.
            let settled = self.job.as_mut().unwrap().await;
            self.job = None;
            let result = match settled {
                Ok((state, result)) => {
                    self.state = Some(state);
                    result
                }
                Err(join) => Err(error(join)),
            };
            match result {
                Ok(batch) => self.ready = Some(batch),
                Err(problem) => {
                    self.failed = Some(problem.to_string());
                    return Err(problem);
                }
            }
        }
    }
    fn storage_position(&self) -> u64 {
        self.position
    }
    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Ok(self.position >= self.initial_position)
    }
    fn storage_is_at_end(&self) -> bool {
        self.at_end
    }
}
