// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{JournalError, JournalReader};
use crate::event::{JournalEvent, JournalRecord};
use async_trait::async_trait;

/// Trusted reader implementation port. A successful `storage_next` asserts
/// that the record belongs to the admitted journal prefix. It must validate
/// framing, namespace, continuity and complete groups before advancing, and
/// preserve the fail-closed retry behaviour required by [`JournalReader`].
///
/// Core admits evidence only as the successful operation returns through
/// [`JournalReader`]. Deserialisation itself never admits evidence. Implementing
/// this storage port, like implementing a journal, is a provider trust boundary.
#[async_trait]
pub trait JournalStorageReader<T: JournalEvent>: Send + Sync {
    async fn storage_next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError>;
    fn storage_position(&self) -> u64;
    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Err(JournalError::InitialPrefixUnsupported)
    }
    fn storage_is_at_end(&self) -> bool {
        false
    }
}

#[async_trait]
impl<T: JournalEvent, R: JournalStorageReader<T> + ?Sized> JournalReader<T> for R {
    async fn next(&mut self) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        self.storage_next()
            .await?
            .map(|record| Ok(record.admit()?))
            .transpose()
    }
    fn position(&self) -> u64 {
        self.storage_position()
    }
    fn initial_prefix_complete(&self) -> Result<bool, JournalError> {
        self.storage_initial_prefix_complete()
    }
    fn is_at_end(&self) -> bool {
        self.storage_is_at_end()
    }
}
