// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! One response owns all reader state and pending reads. No producer task exists.

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
    Opening(Arc<dyn Journal<SystemEvent>>),
    Tailing(Box<dyn JournalReader<SystemEvent>>),
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
        reader: Reader::Opening(journal),
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
            // Complete a fact's derived frames before considering shutdown.
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
            // JournalReader may complete synchronously for every historical row.
            // Bound work per executor turn even before the first bootstrap frame.
            if scanned == CATCH_UP_QUANTUM {
                tokio::task::yield_now().await;
                scanned = 0;
            }
            if let Reader::Opening(journal) = &self.reader {
                match journal.reader().await {
                    Ok(reader) => self.reader = Reader::Tailing(reader),
                    Err(error) => {
                        self.phase = Phase::Closed;
                        return Some(StudioStreamError::JournalOpen(error.to_string()).frame());
                    }
                }
            }
            let Reader::Tailing(reader) = &mut self.reader else {
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
