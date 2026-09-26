// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Lossless, independently pending report readers. A reader owns one cursor and
//! one bounded handoff permit; only its consumer admits causal evidence.

use super::SupervisorRecord;
use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
use obzenflow_core::{Journal, JournalId, JournalRecord};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, thiserror::Error)]
pub enum ReportReaderError {
    #[error("failed to open report journal {journal}: {source}")]
    Open {
        journal: JournalId,
        #[source]
        source: obzenflow_core::journal::JournalError,
    },
    #[error("failed to read report journal {journal}: {source}")]
    Read {
        journal: JournalId,
        #[source]
        source: obzenflow_core::journal::JournalError,
    },
}
const SCAN_RECORDS: usize = 64;
const SCAN_BYTES: usize = 512 * 1024;
const IDLE: std::time::Duration = std::time::Duration::from_millis(10);

#[derive(Default)]
struct Counters {
    pending: AtomicBool,
    scanned: AtomicU64,
    scanned_bytes: AtomicU64,
    selected: AtomicU64,
    through: AtomicU64,
    retained: AtomicUsize,
    high_water: AtomicUsize,
}

/// Optional scheduling diagnostics; these values never authorize coverage or
/// causality. Byte counts are canonical record bytes, excluding provider buffers
/// and allocator overhead. A partially consumed batch stays fully charged.
#[derive(Debug)]
pub struct ReaderDiagnostics {
    pub journal: JournalId,
    pub ready: bool,
    pub pending: bool,
    pub scanned_records: u64,
    pub scanned_bytes: u64,
    pub selected_records: u64,
    pub scanned_through: u64,
    pub delivered_through: u64,
    pub retained_record_bytes: usize,
    pub high_water_record_bytes: usize,
}

struct RetainedBytes {
    counters: Arc<Counters>,
    bytes: usize,
}
impl RetainedBytes {
    fn new(counters: Arc<Counters>) -> Self {
        Self { counters, bytes: 0 }
    }
    fn add(&mut self, bytes: usize) {
        self.bytes += bytes;
        let retained = self.counters.retained.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.counters
            .high_water
            .fetch_max(retained, Ordering::Relaxed);
    }
}
impl Drop for RetainedBytes {
    fn drop(&mut self) {
        self.counters
            .retained
            .fetch_sub(self.bytes, Ordering::Relaxed);
    }
}
struct PendingRead<'a>(&'a AtomicBool);
impl Drop for PendingRead<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

struct Batch {
    records: VecDeque<Box<SupervisorRecord>>,
    through: u64,
    initial_complete: bool,
    at_tail: bool,
    epoch: u64,
    _retained: RetainedBytes,
}

struct Slot {
    journal: JournalId,
    receiver: mpsc::Receiver<Batch>,
    active: Option<Batch>,
    initial_complete: bool,
    at_tail: bool,
    counters: Arc<Counters>,
    delivered_through: u64,
}

pub enum ReportRead {
    Record(Box<SupervisorRecord>),
    /// All selected records through this position have already been delivered
    /// and folded. Filtered rows advance coverage, never the causal frontier.
    Coverage {
        journal: JournalId,
        through: u64,
    },
}

#[derive(Default)]
pub struct ReportReaders {
    tasks: JoinSet<Result<(), Error>>,
    slots: Vec<Slot>,
    next: usize,
    tail_epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl ReportReaders {
    pub fn diagnostics(&self) -> Vec<ReaderDiagnostics> {
        self.slots
            .iter()
            .map(|slot| {
                let c = &slot.counters;
                ReaderDiagnostics {
                    journal: slot.journal,
                    ready: slot.active.is_some() || !slot.receiver.is_empty(),
                    pending: c.pending.load(Ordering::Relaxed),
                    scanned_records: c.scanned.load(Ordering::Relaxed),
                    scanned_bytes: c.scanned_bytes.load(Ordering::Relaxed),
                    selected_records: c.selected.load(Ordering::Relaxed),
                    scanned_through: c.through.load(Ordering::Relaxed),
                    delivered_through: slot.delivered_through,
                    retained_record_bytes: c.retained.load(Ordering::Relaxed),
                    high_water_record_bytes: c.high_water.load(Ordering::Relaxed),
                }
            })
            .collect()
    }
    pub fn reconfirm_ends(&mut self) {
        for slot in &mut self.slots {
            slot.at_tail = false;
        }
        self.tail_epoch
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn initial_prefix_complete(&self) -> bool {
        self.slots.iter().all(|slot| slot.initial_complete)
    }
    pub fn is_at_end(&self) -> bool {
        self.slots
            .iter()
            .all(|slot| slot.at_tail && slot.active.is_none() && slot.receiver.is_empty())
    }
    pub fn contains(&self, journal: &JournalId) -> bool {
        self.slots.iter().any(|slot| &slot.journal == journal)
    }

    pub fn stage(&mut self, journal: Arc<dyn Journal<ChainEvent>>) {
        let owner = journal.owner().and_then(|owner| match owner {
            obzenflow_core::JournalOwner::Stage { stage_id } => {
                Some(obzenflow_core::WriterId::from(*stage_id))
            }
            _ => None,
        });
        self.spawn(journal, move |record| {
            if owner.is_some_and(|writer| writer != *record.writer_id()) {
                None
            } else {
                SupervisorRecord::from_chain(record)
            }
        });
    }

    pub fn system(&mut self, journal: Arc<dyn Journal<SystemEvent>>) {
        self.spawn(journal, |record| Some(record.into()));
    }

    fn spawn<T: JournalEvent + 'static>(
        &mut self,
        journal: Arc<dyn Journal<T>>,
        select: impl Fn(JournalRecord<T::Payload>) -> Option<SupervisorRecord> + Send + 'static,
    ) {
        let id = *journal.id();
        self.spawn_reader(id, Box::pin(async move { journal.reader().await }), select);
    }

    #[cfg(any(test, feature = "test-support"))]
    pub(crate) fn from_system_reader(
        id: JournalId,
        reader: Box<dyn obzenflow_core::journal::JournalReader<SystemEvent>>,
    ) -> Self {
        let mut readers = Self::default();
        readers.spawn_reader(id, Box::pin(async { Ok(reader) }), |record| {
            Some(record.into())
        });
        readers
    }

    fn spawn_reader<T: JournalEvent + 'static>(
        &mut self,
        id: JournalId,
        opened: futures::future::BoxFuture<
            'static,
            Result<
                Box<dyn obzenflow_core::journal::JournalReader<T>>,
                obzenflow_core::journal::JournalError,
            >,
        >,
        select: impl Fn(JournalRecord<T::Payload>) -> Option<SupervisorRecord> + Send + 'static,
    ) {
        let (sender, receiver) = mpsc::channel(1);
        let counters = Arc::new(Counters::default());
        self.slots.push(Slot {
            journal: id,
            receiver,
            active: None,
            initial_complete: false,
            at_tail: false,
            counters: counters.clone(),
            delivered_through: 0,
        });
        let tail_epoch = self.tail_epoch.clone();
        self.tasks.spawn(async move {
            let mut reader = opened.await.map_err(|source| ReportReaderError::Open {
                journal: id,
                source,
            })?;
            let mut announced_initial = false;
            let mut announced_tail = false;
            let mut announced_epoch = tail_epoch.load(std::sync::atomic::Ordering::SeqCst);
            loop {
                // Reserve before reading. Queued and assembling batches cannot
                // coexist for this reader; a consumer may hold one active batch.
                let Ok(permit) = sender.reserve().await else {
                    return Ok(());
                };
                let epoch = tail_epoch.load(std::sync::atomic::Ordering::SeqCst);
                // Publish a fixed-prefix boundary before attempting an I/O
                // operation that may remain pending indefinitely at live tail.
                if !announced_initial && reader.initial_prefix_complete()? {
                    let at_tail = reader.is_at_end();
                    permit.send(Batch {
                        records: VecDeque::new(),
                        through: reader.position(),
                        initial_complete: true,
                        at_tail,
                        epoch,
                        _retained: RetainedBytes::new(counters.clone()),
                    });
                    announced_initial = true;
                    announced_tail = at_tail;
                    announced_epoch = epoch;
                    continue;
                }
                let mut records = VecDeque::new();
                let mut retained = RetainedBytes::new(counters.clone());
                let from = reader.position();
                let mut scanned = 0;
                let mut bytes = 0;
                let mut at_tail = false;
                while scanned < SCAN_RECORDS && bytes < SCAN_BYTES {
                    counters.pending.store(true, Ordering::Relaxed);
                    let pending = PendingRead(&counters.pending);
                    let Some(record) =
                        reader
                            .next()
                            .await
                            .map_err(|source| ReportReaderError::Read {
                                journal: id,
                                source,
                            })?
                    else {
                        at_tail = true;
                        break;
                    };
                    drop(pending);
                    // Count discarded business records too. The providers bound
                    // individual records and decoder/group materialisation.
                    let mut counter = ByteCount(0);
                    serde_json::to_writer(&mut counter, &record)?;
                    bytes += counter.0;
                    scanned += 1;
                    counters.scanned.fetch_add(1, Ordering::Relaxed);
                    counters
                        .scanned_bytes
                        .fetch_add(counter.0 as u64, Ordering::Relaxed);
                    counters.through.store(reader.position(), Ordering::Relaxed);
                    if let Some(report) = select(record) {
                        counters.selected.fetch_add(1, Ordering::Relaxed);
                        retained.add(counter.0);
                        records.push_back(Box::new(report));
                    }
                    if !announced_initial && reader.initial_prefix_complete()? {
                        break;
                    }
                }
                let initial_complete = reader.initial_prefix_complete()?;
                let temporary_empty = at_tail;
                let at_tail = at_tail && reader.is_at_end();
                if scanned != 0
                    || (initial_complete && !announced_initial)
                    || at_tail != announced_tail
                    || (at_tail && epoch != announced_epoch)
                {
                    tracing::trace!(journal = %id, from, through = reader.position(),
                        scanned, scanned_bytes = bytes, selected = records.len(),
                        retained_record_bytes = counters.retained.load(Ordering::Relaxed),
                        high_water_record_bytes = counters.high_water.load(Ordering::Relaxed),
                        "Report reader offered bounded journal batch");
                    permit.send(Batch {
                        records,
                        through: reader.position(),
                        initial_complete,
                        at_tail,
                        epoch,
                        _retained: retained,
                    });
                    announced_initial |= initial_complete;
                    announced_tail = at_tail;
                    announced_epoch = epoch;
                } else {
                    drop(permit);
                }
                if temporary_empty {
                    // Temporary EOF, including a stage's data EOF, does not
                    // end reporting. Each reader has its own fallback timer.
                    tokio::time::sleep(IDLE).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<ReportRead, Error>> {
        if let Poll::Ready(Some(result)) = self.tasks.poll_join_next(cx) {
            return Poll::Ready(Err(match result {
                Ok(Err(error)) => error,
                Err(error) => Box::new(error),
                Ok(Ok(())) => {
                    std::io::Error::other("Report reader stopped before its owner").into()
                }
            }));
        }
        // One record per admission, explicitly rotating among ready journals.
        // Pending/quiet journals do not hold up ready siblings.
        for offset in 0..self.slots.len() {
            let index = (self.next + offset) % self.slots.len();
            let slot = &mut self.slots[index];
            if slot.active.is_none() {
                match slot.receiver.poll_recv(cx) {
                    Poll::Ready(Some(batch)) => slot.active = Some(batch),
                    Poll::Ready(None) => {
                        return Poll::Ready(Err(std::io::Error::other(
                            "Report reader channel closed",
                        )
                        .into()))
                    }
                    Poll::Pending => continue,
                }
            }
            let batch = slot.active.as_mut().expect("active batch");
            let item = if let Some(record) = batch.records.pop_front() {
                slot.delivered_through = record.position();
                ReportRead::Record(record)
            } else {
                let through = batch.through;
                slot.delivered_through = through;
                slot.initial_complete = batch.initial_complete;
                slot.at_tail = batch.at_tail
                    && batch.epoch == self.tail_epoch.load(std::sync::atomic::Ordering::SeqCst);
                slot.active = None;
                ReportRead::Coverage {
                    journal: slot.journal,
                    through,
                }
            };
            self.next = (index + 1) % self.slots.len();
            return Poll::Ready(Ok(item));
        }
        Poll::Pending
    }
}

struct ByteCount(usize);
impl std::io::Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::journal_record::SystemJournalRecord;
    use obzenflow_core::journal::{JournalError, JournalReader};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::Notify;

    struct ControlledReader {
        rows: VecDeque<SystemJournalRecord>,
        position: u64,
        reads: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
        gate: Option<(Arc<Notify>, Arc<Notify>)>,
    }

    impl Drop for ControlledReader {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl JournalReader<SystemEvent> for ControlledReader {
        async fn next(&mut self) -> Result<Option<SystemJournalRecord>, JournalError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            if let Some((entered, release)) = self.gate.take() {
                entered.notify_one();
                release.notified().await;
            }
            let row = self.rows.pop_front();
            self.position += u64::from(row.is_some());
            Ok(row)
        }
        fn position(&self) -> u64 {
            self.position
        }
        fn is_at_end(&self) -> bool {
            self.rows.is_empty()
        }
        fn initial_prefix_complete(&self) -> Result<bool, JournalError> {
            Ok(self.rows.is_empty())
        }
    }

    fn reader(journal: JournalId, count: usize) -> ControlledReader {
        let mut records = Vec::new();
        for _ in 0..count {
            let record = crate::testing::causal_fixture::commit(
                journal,
                SystemEvent::stage_running(obzenflow_core::StageId::new_const(1)),
                &Default::default(),
                &records,
            )
            .unwrap();
            records.push(record);
        }
        ControlledReader {
            rows: records.into(),
            position: 0,
            reads: Arc::default(),
            dropped: Arc::default(),
            gate: None,
        }
    }

    #[tokio::test]
    async fn a_stalled_read_is_retained_while_siblings_progress_and_drop_cancels_it() {
        let blocked = JournalId::new();
        let ready = JournalId::new();
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let mut stalled = reader(blocked, 1);
        stalled.gate = Some((entered.clone(), release));
        let calls = stalled.reads.clone();
        let dropped = stalled.dropped.clone();
        let mut readers = ReportReaders::from_system_reader(blocked, Box::new(stalled));
        readers.spawn_reader(
            ready,
            Box::pin(async move {
                Ok(Box::new(reader(ready, 3)) as Box<dyn JournalReader<SystemEvent>>)
            }),
            |row| Some(row.into()),
        );
        entered.notified().await;
        for position in 1..=3 {
            let item = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                std::future::poll_fn(|cx| readers.poll_next(cx)),
            )
            .await
            .unwrap()
            .unwrap();
            let ReportRead::Record(row) = item else {
                panic!("expected sibling record");
            };
            assert_eq!(row.journal_id(), ready);
            assert_eq!(row.position(), position);
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "pending I/O must not be restarted"
            );
        }
        assert!(
            !readers.initial_prefix_complete(),
            "prefetch is not applied coverage"
        );
        drop(readers);
        tokio::task::yield_now().await;
        assert!(
            dropped.load(Ordering::SeqCst),
            "the owner retains and cancels its read tasks"
        );
    }

    #[tokio::test]
    async fn stopped_admission_bounds_queued_assembling_and_active_records() {
        let journal = JournalId::new();
        let source = reader(journal, 1000);
        let calls = source.reads.clone();
        let mut readers = ReportReaders::from_system_reader(journal, Box::new(source));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            SCAN_RECORDS,
            "one queued batch reserves the only assembly permit"
        );
        let first = std::future::poll_fn(|cx| readers.poll_next(cx))
            .await
            .unwrap();
        assert!(matches!(first, ReportRead::Record(_)));
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2 * SCAN_RECORDS,
            "active plus queued/assembling is the complete retained allowance"
        );
        assert!(!readers.initial_prefix_complete());
        assert!(!readers.is_at_end());
        for position in 2..=SCAN_RECORDS {
            let ReportRead::Record(row) = std::future::poll_fn(|cx| readers.poll_next(cx))
                .await
                .unwrap()
            else {
                panic!("coverage cannot overtake a report");
            };
            assert_eq!(row.position(), position as u64);
        }
        assert!(matches!(
            std::future::poll_fn(|cx| readers.poll_next(cx))
                .await
                .unwrap(),
            ReportRead::Coverage { through: 64, .. }
        ));
    }
}
