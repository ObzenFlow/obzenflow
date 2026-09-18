// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Refresh optional measurements independently of factual accounting and coverage.
use obzenflow_core::event::JournalEvent;
use obzenflow_core::journal::{Journal, ObservationLookup};
use obzenflow_core::WriterId;

pub(crate) async fn retain_journal_observations<T: JournalEvent>(
    journal: &dyn Journal<T>,
    observer: WriterId,
    retained: &super::observations::LatestObservationMap,
) {
    let Some(reader) = journal.observation_reader() else {
        return;
    };
    match reader.latest_observations(observer).await {
        Ok(ObservationLookup::Ready { observation, .. }) => {
            for located in observation {
                retained.offer_recorded(located.observation);
            }
        }
        Ok(ObservationLookup::Rebuilding { .. }) => {}
        Err(error) => tracing::debug!(journal_id = %journal.id(), %error,
            "Optional observation lookup unavailable; retaining prior measurements"),
    }
}
