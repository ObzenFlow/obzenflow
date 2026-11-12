// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::journal::read_journal_envelopes_appended;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{JournalEvent, JournalRecord};
use std::path::Path;

/// Observe committed records, causally ordered, without opening a writer or
/// recovering its tail.
pub async fn read_journal_envelopes<T: JournalEvent>(
    path: &Path,
) -> Vec<JournalRecord<T::Payload>> {
    CausalOrderingService::order_envelopes_by_event_id(
        read_journal_envelopes_appended::<T>(path).await,
    )
    .expect("journal should order causally")
}
