// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::{JournalEvent, JournalRecord};
use obzenflow_core::journal::JournalReader;
use std::path::Path;

/// Observe committed records in physical append order without opening a
/// writer or recovering its tail.
pub async fn read_journal_envelopes_appended<T: JournalEvent>(
    path: &Path,
) -> Vec<JournalRecord<T::Payload>> {
    // Probes also run while the stage is writing. Opening another writer here
    // would recover (truncate) an in-flight tail; a live reader only observes it.
    let mut reader = obzenflow_infra::journal::disk::reader::DiskJournalReader::<T>::new(
        path.to_path_buf(),
        obzenflow_infra::journal::disk::identity::read_identity(path)
            .expect("persisted journal identity")
            .journal_id,
        std::sync::Arc::new(tokio::sync::RwLock::new(())),
    )
    .await
    .expect("stage journal reader");
    let mut envelopes = Vec::new();
    while let Some(envelope) = reader.next().await.expect("stage journal read") {
        envelopes.push(envelope);
    }
    envelopes
}
