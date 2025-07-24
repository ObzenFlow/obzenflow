//! Simple subscription helper for reading from upstream journals
//!
//! This provides a straightforward round-robin reader for upstream journals.

use obzenflow_core::Result;
use obzenflow_core::StageId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::EventEnvelope;
use std::sync::Arc;
use std::time::Duration;

/// Simple subscription that reads from multiple journals
/// 
/// No complex generics, no optional fields - just reads events
/// from a list of upstream journals in round-robin fashion.
pub struct UpstreamSubscription<T>
where
    T: JournalEvent
{
    /// Readers for each upstream journal
    readers: Vec<(StageId, Box<dyn JournalReader<T>>)>,
    
    /// Current round-robin index
    current_index: usize,
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent
{
    /// Create a new subscription from upstream journals
    pub async fn new(
        upstream_journals: &[(StageId, Arc<dyn Journal<T>>)]
    ) -> Result<Self> {
        let mut readers = Vec::new();
        
        for (stage_id, journal) in upstream_journals {
            let reader = journal.reader().await
                .map_err(|e| format!("Failed to create reader for stage {:?}: {}", stage_id, e))?;
            readers.push((*stage_id, reader));
        }
        
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
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            
            let (stage_id, reader) = &mut self.readers[self.current_index];
            
            // Try to read from current journal
            match reader.next().await {
                Ok(Some(envelope)) => {
                    // Advance to next journal for fairness
                    self.current_index = (self.current_index + 1) % self.readers.len();
                    return Ok(envelope);
                }
                Ok(None) => {
                    // No events available from this reader
                }
                Err(e) => {
                    tracing::warn!("Error reading from journal for stage {:?}: {}", stage_id, e);
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