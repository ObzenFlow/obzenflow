// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! An in-memory `Journal<T>` for tests.
//!
//! Code under test depends on the `Journal<T>` port, so a test injects this
//! adapter and reads back through the trait (`read_all_unordered`,
//! `reader_from`) rather than hand-rolling a bespoke double per test.

use async_trait::async_trait;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::types::EventId;
use obzenflow_core::event::EventEnvelope;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use std::sync::{Arc, Mutex};

/// An in-memory journal that retains every appended event for read-back.
pub struct MemoryJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
}

impl<T: JournalEvent> Default for MemoryJournal<T> {
    fn default() -> Self {
        Self {
            id: JournalId::new(),
            owner: None,
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl<T: JournalEvent> MemoryJournal<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_owner(owner: JournalOwner) -> Self {
        Self {
            owner: Some(owner),
            ..Self::default()
        }
    }
}

struct MemoryJournalReader<T: JournalEvent> {
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    pos: usize,
}

#[async_trait]
impl<T> JournalReader<T> for MemoryJournalReader<T>
where
    T: JournalEvent,
{
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self
            .events
            .lock()
            .expect("MemoryJournalReader: poisoned lock");
        if self.pos >= guard.len() {
            return Ok(None);
        }
        let envelope = guard[self.pos].clone();
        drop(guard);
        self.pos += 1;
        Ok(Some(envelope))
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }
}

#[async_trait]
impl<T> Journal<T> for MemoryJournal<T>
where
    T: JournalEvent + 'static,
{
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        guard.push(envelope.clone());
        Ok(envelope)
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.clone())
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader {
            events: Arc::clone(&self.events),
            pos: position as usize,
        }))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        let len = guard.len();
        let start = len.saturating_sub(count);
        Ok(guard[start..].iter().rev().cloned().collect())
    }
}
