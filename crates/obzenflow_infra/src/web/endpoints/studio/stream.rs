// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Reads ordinary live journal histories independently for one Studio connection.
//! Reconnect cursors record per-journal applied positions. Dropping the response
//! releases its readers, pending reads, and saved projection state.

use super::*;
use futures::stream::BoxStream;
use futures::Stream;
use obzenflow_adapters::studio::{bootstrap, server_shutdown, StudioStreamError};
use obzenflow_core::event::{ChainEvent, SystemEvent};
use obzenflow_core::journal::read::RunRecordData;
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::{web::SseFrame, JournalId};
use std::collections::BTreeMap;
use std::task::{Context, Poll};
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::time::Instant;

const READ_QUANTUM: usize = 64;
const TAIL_INTERVAL: Duration = Duration::from_millis(100);

/// Each stream owns its ordinary reader and at most one issued operation.
/// Issued I/O must settle even while the response is unpolled: a disk read can
/// otherwise retain a writer lock until the client asks for another frame.
struct LiveReaders {
    entries: Vec<LiveReader>,
    next: usize,
}
struct LiveReader {
    id: JournalId,
    owner: Option<obzenflow_core::WriterId>,
    position: u64,
    stream: BoxStream<'static, Result<ReadStep, ReaderError>>,
    initial_complete: bool,
    at_end: bool,
}
struct ReadStep {
    record: Option<RunRecordData>,
    initial_complete: bool,
    at_end: bool,
}
enum ReaderError {
    Open(JournalError),
    Read(JournalError),
}

type ReaderResult = Result<
    Option<(
        JournalId,
        u64,
        Option<obzenflow_core::WriterId>,
        RunRecordData,
    )>,
    ReaderError,
>;

struct ReadOperation<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for ReadOperation<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn live_reader<T: obzenflow_core::event::JournalEvent + 'static>(
    journal: Arc<dyn Journal<T>>,
    wrap: fn(obzenflow_core::JournalRecord<T::Payload>) -> RunRecordData,
) -> LiveReader {
    let id = *journal.id();
    let owner = match journal.owner() {
        Some(obzenflow_core::JournalOwner::Stage { stage_id }) => Some((*stage_id).into()),
        Some(obzenflow_core::JournalOwner::System { system_id }) => Some((*system_id).into()),
        None => None,
    };
    let stream = futures::stream::unfold(
        (journal, None::<Box<dyn JournalReader<T>>>),
        move |(journal, reader)| async move {
            let opening = reader.is_none();
            let operation_journal = journal.clone();
            let mut operation = ReadOperation(tokio::spawn(async move {
                let mut reader = match reader {
                    Some(reader) => reader,
                    None => match operation_journal.reader_from(0).await {
                        Ok(reader) => {
                            let opened = reader
                                .initial_prefix_complete()
                                .map(|initial_complete| ReadStep {
                                    record: None,
                                    initial_complete,
                                    at_end: false,
                                })
                                .map_err(ReaderError::Read);
                            return (opened, Some(reader));
                        }
                        Err(error) => return (Err(ReaderError::Open(error)), None),
                    },
                };
                let result = match reader.next().await {
                    Ok(record) => reader
                        .initial_prefix_complete()
                        .map(|initial_complete| ReadStep {
                            record: record.map(wrap),
                            initial_complete,
                            at_end: reader.is_at_end(),
                        })
                        .map_err(ReaderError::Read),
                    Err(error) => Err(ReaderError::Read(error)),
                };
                (result, Some(reader))
            }));
            let (result, reader) = match (&mut operation.0).await {
                Ok(completed) => completed,
                Err(error) => {
                    let error = JournalError::Implementation {
                        message: "Studio journal operation failed".into(),
                        source: Box::new(error),
                    };
                    (
                        Err(if opening {
                            ReaderError::Open(error)
                        } else {
                            ReaderError::Read(error)
                        }),
                        None,
                    )
                }
            };
            Some((result, (journal, reader)))
        },
    );
    LiveReader {
        id,
        owner,
        position: 0,
        stream: Box::pin(stream),
        initial_complete: false,
        at_end: false,
    }
}
impl LiveReaders {
    fn new(
        stages: Vec<Arc<dyn Journal<ChainEvent>>>,
        systems: Vec<Arc<dyn Journal<SystemEvent>>>,
    ) -> Self {
        let entries = stages
            .into_iter()
            .map(|journal| live_reader(journal, |record| RunRecordData::Chain(Box::new(record))))
            .chain(systems.into_iter().map(|journal| {
                live_reader(journal, |record| RunRecordData::System(Box::new(record)))
            }))
            .collect();
        Self { entries, next: 0 }
    }
    fn contains(&self, journal: &JournalId) -> bool {
        self.entries.iter().any(|reader| reader.id == *journal)
    }
    fn initial_prefix_complete(&self) -> bool {
        self.entries.iter().all(|reader| reader.initial_complete)
    }
    fn is_at_end(&self) -> bool {
        self.entries.iter().all(|reader| reader.at_end)
    }
    fn reconfirm_ends(&mut self) {
        for reader in &mut self.entries {
            reader.at_end = false;
        }
    }
    fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
        resume: Option<&BTreeMap<JournalId, u64>>,
    ) -> Poll<ReaderResult> {
        let count = self.entries.len();
        for offset in 0..count {
            let index = (self.next + offset) % count;
            let reader = &mut self.entries[index];
            if reader.at_end
                || resume.is_some_and(|positions| {
                    reader.position >= positions.get(&reader.id).copied().unwrap_or(0)
                })
            {
                continue;
            }
            match reader.stream.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(step))) => {
                    reader.initial_complete = step.initial_complete;
                    // A positive end hint following a record is not a completed tail read.
                    reader.at_end = step.record.is_none() && step.at_end;
                    if let Some(record) = step.record {
                        let position = match &record {
                            RunRecordData::Chain(record) => record.local_sequence(),
                            RunRecordData::System(record) => record.local_sequence(),
                        };
                        reader.position = position;
                        self.next = (index + 1) % count;
                        return Poll::Ready(Ok(Some((reader.id, position, reader.owner, record))));
                    }
                    self.next = (index + 1) % count;
                    return Poll::Ready(Ok(None));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Err(error)),
                Poll::Ready(None) => unreachable!("live journal streams remain open"),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    }
}

enum Phase {
    Fresh,
    Resume(BTreeMap<JournalId, u64>),
    Live,
    Closed,
}

struct Connection {
    readers: LiveReaders,
    phase: Phase,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    checkpoint: BTreeMap<JournalId, u64>,
    applied: BTreeMap<JournalId, u64>,
    closing_reconciled: bool,
    pending: VecDeque<SseFrame>,
    physical_end: bool,
    initial_complete: bool,
    opened_at: Instant,
    records_scanned: u64,
    observation_interval: Duration,
    next_observation: Option<Instant>,
    read_since_observation: bool,
}

pub(super) fn connection(
    stage_journals: Vec<Arc<dyn Journal<ChainEvent>>>,
    system_journals: Vec<Arc<dyn Journal<SystemEvent>>>,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    cursor: Option<&str>,
    observation_interval: Duration,
) -> impl Stream<Item = SseFrame> + Send + 'static {
    let readers = LiveReaders::new(stage_journals, system_journals);
    let mut pending = VecDeque::new();
    let mut checkpoint = BTreeMap::new();
    let phase = match cursor {
        Some(cursor) => match cursor
            .strip_prefix("jr1:")
            .filter(|cursor| cursor.len() <= 256 * 1024)
            .and_then(|cursor| serde_json::from_str::<BTreeMap<JournalId, u64>>(cursor).ok())
        {
            Some(positions) if positions.keys().all(|journal| readers.contains(journal)) => {
                checkpoint = positions.clone();
                Phase::Resume(positions)
            }
            _ => {
                pending.push_back(
                    StudioStreamError::InvalidCursor(
                        "expected current journal-position cursor".into(),
                    )
                    .frame(),
                );
                Phase::Fresh
            }
        },
        None => Phase::Fresh,
    };
    let state = Connection {
        readers,
        phase,
        projection,
        runtime_instance_id,
        closing,
        checkpoint,
        applied: BTreeMap::new(),
        closing_reconciled: false,
        pending,
        physical_end: false,
        initial_complete: false,
        opened_at: Instant::now(),
        records_scanned: 0,
        observation_interval,
        next_observation: None,
        read_since_observation: false,
    };
    futures::stream::unfold(state, |mut state| async move {
        state.next_frame().await.map(|frame| (frame, state))
    })
}

impl Connection {
    async fn next_frame(&mut self) -> Option<SseFrame> {
        let mut scanned = 0;
        loop {
            // Send accompanying composite updates before the shutdown notice.
            if let Some(frame) = self.pending.pop_front() {
                return Some(frame);
            }
            if matches!(self.phase, Phase::Closed) {
                return None;
            }
            if let Phase::Resume(positions) = &self.phase {
                if self.readers.entries.iter().any(|reader| {
                    reader.initial_complete
                        && reader.position < positions.get(&reader.id).copied().unwrap_or(0)
                }) {
                    self.pending.push_back(
                        StudioStreamError::InvalidCursor(
                            "cursor exceeds a committed journal prefix".into(),
                        )
                        .frame(),
                    );
                    self.checkpoint = self.applied.clone();
                    self.phase = Phase::Fresh;
                    continue;
                }
                if positions.iter().all(|(journal, position)| {
                    self.applied.get(journal).copied().unwrap_or(0) >= *position
                }) {
                    self.pending.extend(self.projection.resume_snapshots());
                    self.phase = Phase::Live;
                    continue;
                }
            }
            if matches!(self.phase, Phase::Live)
                && self.physical_end
                && *self.closing.borrow()
                && self.closing_reconciled
                && self.projection.terminal_observed()
            {
                self.phase = Phase::Closed;
                return Some(server_shutdown(
                    self.runtime_instance_id
                        .as_ref()
                        .map(RuntimeInstanceId::as_str),
                ));
            }
            // Large journals can satisfy reads immediately; let other tasks run.
            if scanned == READ_QUANTUM {
                tokio::task::yield_now().await;
                scanned = 0;
            }
            if !self.initial_complete && self.readers.initial_prefix_complete() {
                self.initial_complete = true;
                self.finish_initial_prefix();
                continue;
            }
            if *self.closing.borrow()
                && self.projection.terminal_observed()
                && !self.closing_reconciled
            {
                self.readers.reconfirm_ends();
                self.closing_reconciled = true;
                self.physical_end = false;
            }
            if self.read_since_observation
                && self.next_observation.is_some_and(|at| at <= Instant::now())
            {
                // Reader streams retain pending I/O independently of this
                // optional yield. Skip missed slots; do not replay measurements.
                self.pending.extend(self.projection.current_measurements());
                self.next_observation = Some(Instant::now() + self.observation_interval);
                self.read_since_observation = false;
                continue;
            }
            self.read_since_observation = true;
            let wake_at = self
                .next_observation
                .map_or(Instant::now() + TAIL_INTERVAL, |at| {
                    at.min(Instant::now() + TAIL_INTERVAL)
                });
            let positions = match &self.phase {
                Phase::Resume(positions) => Some(positions),
                _ => None,
            };
            let read = std::future::poll_fn(|cx| self.readers.poll_next(cx, positions));
            let result = tokio::select! {
                result = read => Some(result),
                _ = tokio::time::sleep_until(wake_at) => None,
            };
            match result {
                Some(Ok(None)) => {
                    self.physical_end = self.readers.is_at_end();
                }
                Some(Ok(Some((journal, position, owner, envelope)))) => {
                    self.physical_end = false;
                    scanned += 1;
                    self.records_scanned += 1;
                    self.applied.insert(journal, position);
                    let known = self.checkpoint.entry(journal).or_default();
                    *known = (*known).max(position);
                    let owned = match &envelope {
                        RunRecordData::Chain(record) => owner.is_some_and(|owner| {
                            *record.writer_id() == owner
                                && owner.as_stage()
                                    == Some(&record.envelope.provenance.event.flow_context.stage_id)
                        }),
                        RunRecordData::System(record) => {
                            owner.is_some_and(|owner| *record.writer_id() == owner)
                        }
                    };
                    if !owned {
                        continue;
                    }
                    let frames = match &self.phase {
                        Phase::Fresh => {
                            self.projection.rebuild_deferred(&envelope);
                            Vec::new()
                        }
                        Phase::Resume(positions)
                            if position <= positions.get(&journal).copied().unwrap_or(0) =>
                        {
                            self.projection.rebuild_deferred(&envelope);
                            Vec::new()
                        }
                        Phase::Resume(_) | Phase::Live => {
                            self.projection.project_deferred(&envelope)
                        }
                        Phase::Closed => unreachable!("closed connections do not read"),
                    };
                    self.enqueue(frames);
                }
                Some(Err(error)) => {
                    self.phase = Phase::Closed;
                    return Some(match error {
                        ReaderError::Open(error) => {
                            StudioStreamError::JournalOpen(error.to_string()).frame()
                        }
                        ReaderError::Read(error) => {
                            StudioStreamError::JournalRead(error.to_string()).frame()
                        }
                    });
                }
                None => {
                    self.physical_end = self.readers.is_at_end();
                    // Recheck live tails on the next bounded polling turn.
                    self.readers.reconfirm_ends();
                }
            }
        }
    }

    fn enqueue(&mut self, mut frames: Vec<SseFrame>) {
        // A cursor covers the records applied across ALL journals. It is
        // independent of this connection's interleaving of ready readers.
        for frame in &mut frames {
            frame.id = None;
        }
        if let Some(last) = frames.last_mut() {
            if self.checkpoint.values().any(|position| *position != 0) {
                last.id = Some(format!(
                    "jr1:{}",
                    serde_json::to_string(&self.checkpoint).expect("position map")
                ));
            }
        }
        self.pending.extend(frames);
    }

    fn finish_initial_prefix(&mut self) {
        if let Phase::Resume(positions) = &self.phase {
            if positions.iter().any(|(journal, position)| {
                *position > self.applied.get(journal).copied().unwrap_or(0)
            }) {
                self.pending.push_back(
                    StudioStreamError::InvalidCursor(
                        "cursor exceeds a committed journal prefix".into(),
                    )
                    .frame(),
                );
                self.checkpoint = self.applied.clone();
                self.phase = Phase::Fresh;
            }
        }
        tracing::debug!(records_scanned = self.records_scanned,
            elapsed_ms = self.opened_at.elapsed().as_millis(), checkpoint = ?self.checkpoint,
            "Studio initial committed prefix complete");
        let measurements = self.projection.current_measurements();
        if matches!(self.phase, Phase::Fresh) {
            let mut frames = self.projection.snapshots();
            frames.extend(self.projection.middleware_snapshot(timestamp_ms()));
            frames.push(bootstrap(
                None,
                self.runtime_instance_id
                    .as_ref()
                    .map(RuntimeInstanceId::as_str),
            ));
            self.enqueue(frames);
            self.phase = Phase::Live;
        }
        self.pending.extend(measurements);
        self.next_observation = Some(Instant::now() + self.observation_interval);
        self.read_since_observation = false;
    }
}

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
