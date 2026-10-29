// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Earlier runs' source error records, read one at a time. Each waits until the
//! shown run re-admits the row it followed, so a stream holds one record.

use super::{clock_of, JournalCursor, JournalReadError};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::CausalCoordinate;
use obzenflow_core::journal::read::{RunIdentity, RunJournal, RunRecord};
use std::collections::BTreeMap;

/// An earlier run whose source error records a listing includes.
#[derive(Debug, Clone)]
pub struct EarlierRun {
    pub identity: RunIdentity,
    /// The generation it entered.
    pub generation: u64,
    pub journals: Vec<RunJournal>,
}

#[derive(Default)]
pub(super) struct EarlierRuns {
    streams: Vec<Stream>,
    reached: BTreeMap<String, VectorClock>,
}

struct Stream {
    cursor: JournalCursor,
    /// The source, and its data journal's coordinate in the earlier run.
    anchor: Option<(String, CausalCoordinate)>,
    front: Front,
}

enum Front {
    /// Not read yet, or taken and not replaced yet.
    Unread,
    /// Waits until the shown run re-admits the row it followed.
    Waiting {
        count: u64,
        record: RunRecord,
    },
    /// In the merge and not taken yet.
    Placed,
    Exhausted,
}

impl EarlierRuns {
    pub(super) fn add(
        &mut self,
        cursor: JournalCursor,
        anchor: Option<(String, CausalCoordinate)>,
    ) {
        self.streams.push(Stream {
            cursor,
            anchor,
            front: Front::Unread,
        });
        self.streams.sort_by_key(|stream| {
            (
                stream.anchor.is_none(),
                stream.anchor.is_some().then_some(stream.cursor.journal.id),
            )
        });
    }

    /// Reads the next record of each stream whose last one was taken, and
    /// returns those the shown run already reached, with the clock that did.
    pub(super) async fn fill(&mut self) -> Result<Vec<(RunRecord, VectorClock)>, JournalReadError> {
        let mut placed = Vec::new();
        for stream in &mut self.streams {
            if !matches!(stream.front, Front::Unread) {
                continue;
            }
            let Some(record) = stream.cursor.next().await? else {
                stream.front = Front::Exhausted;
                continue;
            };
            let Some((source, anchor)) = &stream.anchor else {
                stream.front = Front::Waiting { count: 0, record };
                continue;
            };
            let count = clock_of(&record.record).get(anchor);
            match self
                .reached
                .get(source)
                .filter(|clock| clock.get(anchor) >= count)
            {
                Some(clock) => {
                    placed.push((record, clock.clone()));
                    stream.front = Front::Placed;
                }
                None => stream.front = Front::Waiting { count, record },
            }
        }
        Ok(placed)
    }

    /// The shown run re-admitted `source`'s rows up to `clock`, so each waiting
    /// record that followed one of them is placed.
    pub(super) fn reach(&mut self, source: &str, clock: &VectorClock) -> Vec<RunRecord> {
        if !self.streams.iter().any(|stream| stream.follows(source)) {
            return Vec::new();
        }
        self.reached.insert(source.to_owned(), clock.clone());
        let mut placed = Vec::new();
        for stream in &mut self.streams {
            let Some((key, anchor)) = &stream.anchor else {
                continue;
            };
            let reached = key == source
                && matches!(&stream.front, Front::Waiting { count, .. } if *count <= clock.get(anchor));
            if !reached {
                continue;
            }
            if let Front::Waiting { record, .. } =
                std::mem::replace(&mut stream.front, Front::Placed)
            {
                placed.push(record);
            }
        }
        placed
    }

    /// Whether `source`'s next row waits for a record placed after its last one.
    pub(super) fn holds_back(&self, source: &str) -> bool {
        self.streams
            .iter()
            .any(|stream| stream.follows(source) && matches!(stream.front, Front::Placed))
    }

    /// The merge returned `record`; a placed stream reads its next record.
    pub(super) fn taken(&mut self, record: &RunRecord) {
        if let Some(stream) = self
            .streams
            .iter_mut()
            .find(|stream| stream.cursor.journal.id == record.journal.id)
        {
            if matches!(stream.front, Front::Placed) {
                stream.front = Front::Unread;
            }
        }
    }

    /// The next record no row of the shown run reached, anchored streams first.
    pub(super) fn take_unreached(&mut self) -> Option<RunRecord> {
        self.streams.iter_mut().find_map(|stream| {
            match std::mem::replace(&mut stream.front, Front::Unread) {
                Front::Waiting { record, .. } => Some(record),
                front => {
                    stream.front = front;
                    None
                }
            }
        })
    }

    pub(super) fn is_empty(&self) -> bool {
        self.streams
            .iter()
            .all(|stream| matches!(stream.front, Front::Exhausted))
    }

    #[cfg(test)]
    pub(super) fn waiting(&self) -> usize {
        self.streams
            .iter()
            .filter(|stream| matches!(stream.front, Front::Waiting { .. }))
            .count()
    }
}

impl Stream {
    fn follows(&self, source: &str) -> bool {
        self.anchor.as_ref().is_some_and(|(key, _)| key == source)
    }
}
