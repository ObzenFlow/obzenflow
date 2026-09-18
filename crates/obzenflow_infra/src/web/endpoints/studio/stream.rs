// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Reads the system journal for one Studio connection. Reconnects resume after
//! a known `Last-Event-ID`; otherwise, the connection starts with status snapshots.
//! Dropping the response releases its journal reader and saved state.

use super::*;
use futures::Stream;
use obzenflow_adapters::studio::{bootstrap, server_shutdown, StudioStreamError};
use obzenflow_core::{journal::JournalReader, web::SseFrame, EventId};
use std::{
    collections::VecDeque,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::time::Instant;

const READ_QUANTUM: usize = 64;
const TAIL_INTERVAL: Duration = Duration::from_millis(100);

enum Phase {
    Fresh,
    Resume(EventId),
    Live,
    Closed,
}

enum Reader {
    Unopened(Arc<dyn Journal<SystemEvent>>),
    Open(Box<dyn JournalReader<SystemEvent>>),
}

struct Connection {
    reader: Reader,
    phase: Phase,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    checkpoint: Option<EventId>,
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
    journal: Arc<dyn Journal<SystemEvent>>,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    cursor: Option<&str>,
    observation_interval: Duration,
) -> impl Stream<Item = SseFrame> + Send + 'static {
    let mut pending = VecDeque::new();
    let phase = match cursor.map(EventId::from_string) {
        Some(Ok(id)) => Phase::Resume(id),
        Some(Err(error)) => {
            pending.push_back(StudioStreamError::InvalidCursor(error.to_string()).frame());
            Phase::Fresh
        }
        None => Phase::Fresh,
    };
    let state = Connection {
        reader: Reader::Unopened(journal),
        phase,
        projection,
        runtime_instance_id,
        closing,
        checkpoint: None,
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
            if matches!(self.phase, Phase::Live)
                && self.physical_end
                && *self.closing.borrow()
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
            if let Reader::Unopened(journal) = &self.reader {
                match journal.reader().await {
                    Ok(reader) => {
                        self.reader = Reader::Open(reader);
                        self.opened_at = Instant::now();
                    }
                    Err(error) => {
                        self.phase = Phase::Closed;
                        return Some(StudioStreamError::JournalOpen(error.to_string()).frame());
                    }
                }
            }
            let Reader::Open(reader) = &mut self.reader else {
                unreachable!("reader opened above");
            };
            if !self.initial_complete {
                match reader.initial_prefix_complete() {
                    Ok(true) => {
                        self.initial_complete = true;
                        self.finish_initial_prefix();
                        continue;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        self.phase = Phase::Closed;
                        return Some(StudioStreamError::JournalRead(error.to_string()).frame());
                    }
                }
            }
            if self.read_since_observation
                && self.next_observation.is_some_and(|at| at <= Instant::now())
            {
                // No read future or provider lock survives an optional yield.
                // The next deadline skips missed slots; no pending history is replayed.
                self.pending.extend(self.projection.current_measurements());
                self.next_observation = Some(Instant::now() + self.observation_interval);
                self.read_since_observation = false;
                continue;
            }
            self.read_since_observation = true;
            match reader.next().await {
                Ok(Some(envelope)) => {
                    self.physical_end = false;
                    scanned += 1;
                    self.records_scanned += 1;
                    self.checkpoint = Some(envelope.envelope.provenance.event.id);
                    match self.phase {
                        Phase::Fresh => self.projection.rebuild_deferred(&envelope),
                        Phase::Resume(id) => {
                            self.projection.rebuild_deferred(&envelope);
                            if envelope.envelope.provenance.event.id == id {
                                self.phase = Phase::Live;
                                self.pending.extend(self.projection.resume_snapshots());
                            }
                        }
                        Phase::Live => self
                            .pending
                            .extend(self.projection.project_deferred(&envelope)),
                        Phase::Closed => unreachable!("closed connections do not read"),
                    }
                }
                // A partial-frame None is not physical end evidence.
                Ok(None) => {
                    self.physical_end = reader.is_at_end();
                    if !(self.physical_end
                        && *self.closing.borrow()
                        && self.projection.terminal_observed())
                    {
                        let poll = Instant::now() + TAIL_INTERVAL;
                        tokio::time::sleep_until(
                            self.next_observation.map_or(poll, |at| at.min(poll)),
                        )
                        .await;
                    }
                }
                Err(error) => {
                    self.phase = Phase::Closed;
                    return Some(StudioStreamError::JournalRead(error.to_string()).frame());
                }
            }
        }
    }

    fn finish_initial_prefix(&mut self) {
        tracing::debug!(records_scanned = self.records_scanned,
            elapsed_ms = self.opened_at.elapsed().as_millis(), checkpoint = ?self.checkpoint,
            "Studio initial committed prefix complete");
        let measurements = self.projection.current_measurements();
        if matches!(self.phase, Phase::Fresh | Phase::Resume(_)) {
            if matches!(self.phase, Phase::Resume(_)) {
                self.pending
                    .push_back(StudioStreamError::UnknownCursor.frame());
            }
            self.pending.extend(self.projection.snapshots());
            // Deliver all factual snapshots before advancing the resume cursor.
            self.pending
                .extend(self.projection.middleware_snapshot(timestamp_ms()));
            self.pending.push_back(bootstrap(
                self.checkpoint,
                self.runtime_instance_id
                    .as_ref()
                    .map(RuntimeInstanceId::as_str),
            ));
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
