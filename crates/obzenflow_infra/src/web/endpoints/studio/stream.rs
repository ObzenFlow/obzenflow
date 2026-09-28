// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Reads owned report histories independently for one Studio connection.
//! Reconnect cursors record per-journal applied positions. Dropping the response
//! releases all reader tasks, bounded handoffs, and saved projection state.

use super::*;
use futures::Stream;
use obzenflow_adapters::studio::{bootstrap, server_shutdown, StudioStreamError};
use obzenflow_core::{web::SseFrame, JournalId};
use obzenflow_runtime::supervised_base::{
    report_reader::{ReportRead, ReportReaders},
    SupervisorJournal,
};
use std::collections::BTreeMap;
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::time::Instant;

const READ_QUANTUM: usize = 64;
const TAIL_INTERVAL: Duration = Duration::from_millis(100);

enum Phase {
    Fresh,
    Resume(BTreeMap<JournalId, u64>),
    Live,
    Closed,
}

struct Connection {
    readers: ReportReaders,
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
    journals: Vec<SupervisorJournal>,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    cursor: Option<&str>,
    observation_interval: Duration,
) -> impl Stream<Item = SseFrame> + Send + 'static {
    let mut readers = ReportReaders::default();
    for journal in journals {
        match journal {
            SupervisorJournal::Stage { journal, .. } => readers.stage(journal),
            SupervisorJournal::System(journal) => readers.system(journal),
        }
    }
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
                // Reader tasks retain their pending I/O independently of this
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
            let read = std::future::poll_fn(|cx| self.readers.poll_next(cx));
            let result = tokio::select! {
                result = read => Some(result),
                _ = tokio::time::sleep_until(wake_at) => None,
            };
            match result {
                Some(Ok(ReportRead::Record(envelope))) => {
                    self.physical_end = false;
                    scanned += 1;
                    self.records_scanned += 1;
                    let journal = envelope.journal_id();
                    let position = envelope.position();
                    self.applied.insert(journal, position);
                    let known = self.checkpoint.entry(journal).or_default();
                    *known = (*known).max(position);
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
                Some(Ok(ReportRead::Coverage { journal, through })) => {
                    self.applied.insert(journal, through);
                    let known = self.checkpoint.entry(journal).or_default();
                    *known = (*known).max(through);
                    self.physical_end = self.readers.is_at_end();
                }
                Some(Err(error)) => {
                    self.phase = Phase::Closed;
                    if matches!(error.downcast_ref::<obzenflow_runtime::supervised_base::report_reader::ReportReaderError>(), Some(obzenflow_runtime::supervised_base::report_reader::ReportReaderError::Open { .. })) {
                        return Some(StudioStreamError::JournalOpen(error.to_string()).frame());
                    }
                    return Some(StudioStreamError::JournalRead(error.to_string()).frame());
                }
                None => self.physical_end = self.readers.is_at_end(),
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
