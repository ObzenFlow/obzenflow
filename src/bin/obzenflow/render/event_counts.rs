// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded counts of displayed entries within each physical journal. Payload
//! subjects and original writers never move entries into a different journal.

use super::{event_type, writer_id, RunJournal, RunRecord};
use std::collections::BTreeMap;

const MAX_ROWS: usize = 1024;

pub(super) struct JournalEventCounts {
    pub journal: RunJournal,
    pub event_types: BTreeMap<(String, String), u64>,
    pub omitted: u64,
}

#[derive(Default)]
pub(super) struct EventCounts {
    journals: BTreeMap<String, JournalEventCounts>,
    rows: usize,
}

impl EventCounts {
    pub(super) fn record(&mut self, record: &RunRecord) {
        let journal = self
            .journals
            .entry(record.journal.id.to_string())
            .or_insert_with(|| JournalEventCounts {
                journal: record.journal.clone(),
                event_types: BTreeMap::new(),
                omitted: 0,
            });
        let key = (event_type(record).to_owned(), writer_id(record));
        if let Some(count) = journal.event_types.get_mut(&key) {
            *count += 1;
        } else if self.rows < MAX_ROWS {
            journal.event_types.insert(key, 1);
            self.rows += 1;
        } else {
            journal.omitted += 1;
        }
    }

    pub(super) fn journals(&self) -> impl Iterator<Item = &JournalEventCounts> {
        self.journals.values()
    }
}
