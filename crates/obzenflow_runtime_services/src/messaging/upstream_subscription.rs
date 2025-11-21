//! Simple subscription helper for reading from upstream journals
//!
//! This provides a straightforward round-robin reader for upstream journals.

use obzenflow_core::event::JournalEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::EventEnvelope;
use obzenflow_core::Result;
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::Duration;

/// Simple subscription that reads from multiple journals
///
/// No complex generics, no optional fields - just reads events
/// from a list of upstream journals in round-robin fashion.
pub struct UpstreamSubscription<T>
where
    T: JournalEvent,
{
    /// Readers for each upstream journal
    readers: Vec<(StageId, Box<dyn JournalReader<T>>)>,

    /// Current round-robin index
    current_index: usize,
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent,
{
    /// Create a new subscription from upstream journals
    pub async fn new(upstream_journals: &[(StageId, Arc<dyn Journal<T>>)]) -> Result<Self> {
        let mut readers = Vec::new();

        tracing::info!(
            "UpstreamSubscription::new() creating subscription for {} journals",
            upstream_journals.len()
        );

        for (stage_id, journal) in upstream_journals {
            tracing::info!(
                "UpstreamSubscription::new() creating reader for stage {:?}, journal ptr: {:p}",
                stage_id,
                journal.as_ref() as *const _
            );
            let reader = journal
                .reader()
                .await
                .map_err(|e| format!("Failed to create reader for stage {:?}: {}", stage_id, e))?;
            readers.push((*stage_id, reader));
            tracing::info!(
                "UpstreamSubscription::new() successfully created reader for stage {:?}",
                stage_id
            );
        }

        tracing::info!(
            "UpstreamSubscription::new() created with {} readers",
            readers.len()
        );

        Ok(Self {
            readers,
            current_index: 0,
        })
    }

    /// Read the next event from any upstream journal
    ///
    /// Uses round-robin to ensure fairness across journals.
    /// Returns one event at a time to maintain FSM semantics.
    pub async fn recv(&mut self) -> Result<EventEnvelope<T>> {
        let mut attempts = 0;

        loop {
            // No upstream? Just wait
            if self.readers.is_empty() {
                tracing::error!("UpstreamSubscription::recv() called but readers list is EMPTY!");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }

            let total_readers = self.readers.len();
            let (stage_id, reader) = &mut self.readers[self.current_index];

            tracing::debug!(
                "UpstreamSubscription::recv() attempting to read from journal for stage {:?} at index {} (total readers: {})",
                stage_id,
                self.current_index,
                total_readers
            );

            // Try to read from current journal
            match reader.next().await {
                Ok(Some(envelope)) => {
                    tracing::debug!(
                        upstream_stage_id = ?stage_id,
                        reader_index = self.current_index,
                        total_readers,
                        event_id = %envelope.event.id(),
                        event_type = envelope.event.event_type_name(),
                        "UpstreamSubscription::recv() received upstream event"
                    );
                    // Advance to next journal for fairness
                    self.current_index = (self.current_index + 1) % self.readers.len();
                    return Ok(envelope);
                }
                Ok(None) => {
                    // No events available from this reader
                    tracing::trace!(
                        "UpstreamSubscription::recv() got None from reader for stage {:?} at index {}",
                        stage_id,
                        self.current_index
                    );
                }
                Err(e) => {
                    tracing::error!("Error reading from journal for stage {:?}: {}", stage_id, e);
                    // Return the error - journal corruption is a critical failure
                    // Silent data loss is worse than failing fast
                    return Err(Box::new(e));
                }
            }

            // Move to next journal
            self.current_index = (self.current_index + 1) % self.readers.len();
            attempts += 1;

            // Brief sleep after checking all journals
            if attempts >= self.readers.len() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                attempts = 0;
            }
        }
    }

    /// Check if there are any upstream journals
    pub fn has_upstream(&self) -> bool {
        !self.readers.is_empty()
    }
}
