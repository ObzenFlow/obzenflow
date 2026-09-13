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

const CATCH_UP_QUANTUM: usize = 64;
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
}

pub(super) fn connection(
    journal: Arc<dyn Journal<SystemEvent>>,
    projection: StudioProjection,
    runtime_instance_id: Option<RuntimeInstanceId>,
    closing: watch::Receiver<bool>,
    cursor: Option<&str>,
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
            if scanned == CATCH_UP_QUANTUM {
                tokio::task::yield_now().await;
                scanned = 0;
            }
            if let Reader::Unopened(journal) = &self.reader {
                match journal.reader().await {
                    Ok(reader) => self.reader = Reader::Open(reader),
                    Err(error) => {
                        self.phase = Phase::Closed;
                        return Some(StudioStreamError::JournalOpen(error.to_string()).frame());
                    }
                }
            }
            let Reader::Open(reader) = &mut self.reader else {
                unreachable!("reader opened above");
            };
            match reader.next().await {
                Ok(Some(envelope)) => {
                    scanned += 1;
                    self.checkpoint = Some(envelope.event.id);
                    match self.phase {
                        Phase::Fresh => self.projection.rebuild(&envelope),
                        Phase::Resume(id) => {
                            self.projection.rebuild(&envelope);
                            if envelope.event.id == id {
                                self.phase = Phase::Live;
                                self.pending.extend(self.projection.resume_snapshots());
                            }
                        }
                        Phase::Live => self
                            .pending
                            .extend(self.projection.project(&envelope, timestamp_ms())),
                        Phase::Closed => unreachable!("closed connections do not read"),
                    }
                }
                // None means caught up for now; further entries may arrive later.
                Ok(None) => match self.phase {
                    Phase::Fresh | Phase::Resume(_) => {
                        let fresh = matches!(self.phase, Phase::Fresh);
                        if !fresh {
                            self.pending
                                .push_back(StudioStreamError::UnknownCursor.frame());
                        }
                        self.pending.extend(self.projection.snapshots());
                        self.pending.push_back(bootstrap(
                            self.checkpoint,
                            self.runtime_instance_id
                                .as_ref()
                                .map(RuntimeInstanceId::as_str),
                        ));
                        if fresh && self.projection.active_observed() {
                            self.pending
                                .extend(self.projection.middleware_snapshot(timestamp_ms()));
                        }
                        self.phase = Phase::Live;
                    }
                    Phase::Live => tokio::time::sleep(TAIL_INTERVAL).await,
                    Phase::Closed => unreachable!("closed connections do not read"),
                },
                Err(error) => {
                    self.phase = Phase::Closed;
                    return Some(StudioStreamError::JournalRead(error.to_string()).frame());
                }
            }
        }
    }
}

fn timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
