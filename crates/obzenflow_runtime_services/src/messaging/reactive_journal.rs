//! ReactiveJournal - Stage-local journal architecture (FLOWIP-008)
//!
//! This module implements the simplified stage-local journal design where:
//! - Each stage has its own journal for data events
//! - All stages share a control journal for system events
//! - No pub/sub, no notifications, just direct journal reads

use obzenflow_core::Result;
use obzenflow_core::{StageId, PipelineId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::JournalError;
use obzenflow_core::ChainEvent;
use obzenflow_core::EventEnvelope;
use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use async_trait::async_trait;

/// Special stage ID for the control journal
pub const CONTROL_STAGE_ID: StageId = StageId::new_const(0);

/// ReactiveJournal - simplified stage-local architecture (FLOWIP-008)
/// 
/// Each stage has its own journal for data events, plus all stages share
/// a control journal for system events. No pub/sub, no notifications.
/// 
/// IMPORTANT: Vector clock causality is preserved across journals through
/// parent relationships. When a stage processes an upstream event and creates
/// a new event, it passes the upstream event as the parent. The journal then
/// inherits the parent's vector clock components, ensuring causal ordering
/// is maintained even with stage-local journals. This is how distributed
/// vector clocks are supposed to work - no centralized state needed!
pub struct ReactiveJournal {
    /// My journal for writing data events
    pub my_journal: Arc<dyn Journal>,
    
    /// My writer ID
    pub writer_id: WriterId,
    
    /// Control journal for all system/pipeline events (shared by all)
    pub control_journal: Arc<dyn Journal>,
    
    /// Upstream stage journals I read from (set by topology)
    pub upstream_journals: Vec<(StageId, Arc<dyn Journal>)>,
}

impl ReactiveJournal {
    /// Create a new reactive journal for stage-local architecture
    pub fn new(
        my_journal: Arc<dyn Journal>,
        writer_id: WriterId,
        control_journal: Arc<dyn Journal>,
        upstream_journals: Vec<(StageId, Arc<dyn Journal>)>,
    ) -> Self {
        Self {
            my_journal,
            writer_id,
            control_journal,
            upstream_journals,
        }
    }
    
    /// Write to my journal - no notifications!
    pub async fn write(
        &self,
        event: ChainEvent,
        parent: Option<&EventEnvelope>,
    ) -> Result<EventEnvelope> {
        self.my_journal.append(&self.writer_id, event, parent).await
            .map_err(|e| e.into())
    }
    
    /// Write control event (system.* events) to control journal
    pub async fn write_control_event(&self, event: ChainEvent) -> Result<EventEnvelope> {
        self.control_journal.append(&self.writer_id, event, None).await
            .map_err(|e| e.into())
    }
    
    /// Create a subscription to all journals I need to read
    pub async fn subscribe(&self) -> Result<JournalSubscription> {
        // Build list of readers for all journals
        let mut readers = vec![];
        
        // ALWAYS read from control journal for system/pipeline events
        let control_reader = self.control_journal.reader().await
            .map_err(|e| format!("Failed to create control journal reader: {}", e))?;
        readers.push((CONTROL_STAGE_ID, control_reader));
        
        // Then add upstream stage journal readers for data
        for (stage_id, journal) in &self.upstream_journals {
            let reader = journal.reader().await
                .map_err(|e| format!("Failed to create reader for stage {:?}: {}", stage_id, e))?;
            readers.push((*stage_id, reader));
        }
        
        Ok(JournalSubscription {
            upstream_readers: readers,
            current_index: 0,
        })
    }
}

/// Simple subscription that reads directly from journals (FLOWIP-008a)
/// 
/// Uses JournalReader for efficient cursor-based reading with O(1) performance
pub struct JournalSubscription {
    /// Readers for each upstream journal - keeps files open!
    upstream_readers: Vec<(StageId, Box<dyn JournalReader>)>,
    
    /// Round-robin index
    current_index: usize,
}

impl JournalSubscription {
    /// Read next event from upstream journals
    /// 
    /// IMPORTANT: Returns ONE event at a time to maintain FSM semantics.
    /// Per FLOWIP-056zzz, FSMs process one event at a time.
    /// 
    /// ORDERING: Each journal maintains causal order internally via vector clocks.
    /// When we call read_causally_ordered(), we get events in causal order from
    /// that journal. However, we read from multiple journals in round-robin fashion,
    /// so events from different journals may be interleaved. While vector clocks
    /// track causal relationships across ALL writers (even across journals), we
    /// don't attempt to merge-sort across journals because:
    /// 1. It would require unbounded buffering (the "horizon problem")
    /// 2. Stages can handle events as they arrive
    /// 3. Critical ordering can be enforced by event dependencies (parent relationships)
    pub async fn recv(&mut self) -> Result<EventEnvelope> {
        let mut attempts = 0;
        
        loop {
            // Round-robin through upstream journals
            if self.upstream_readers.is_empty() {
                // Source stage - no upstream, just wait
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            
            let (stage_id, reader) = &mut self.upstream_readers[self.current_index];
            
            // Just call next() - no reopening files, no re-reading!
            match reader.next().await {
                Ok(Some(envelope)) => {
                    return Ok(envelope);
                }
                Ok(None) => {
                    // No more events from this reader (for now)
                }
                Err(e) => {
                    tracing::warn!("Error reading from journal for stage {:?}: {}", stage_id, e);
                }
            }
            
            // Move to next journal
            self.current_index = (self.current_index + 1) % self.upstream_readers.len();
            attempts += 1;
            
            // Brief sleep after checking all journals
            if attempts >= self.upstream_readers.len() {
                tokio::time::sleep(Duration::from_millis(10)).await;
                attempts = 0;
            }
        }
    }
    
    // NO recv_batch()! Per FLOWIP-056zzz, FSMs process one event at a time.
    // Batching violates FSM semantics - only one state transition per dispatch_state.
}

/// Builder for creating stage-local journal architecture
pub struct ReactiveJournalBuilder {
    topology: HashMap<StageId, Vec<StageId>>, // stage -> upstream stages
    control_journal: Arc<dyn Journal>,
    stage_journals: HashMap<StageId, Arc<dyn Journal>>,
    pipeline_id: PipelineId,
}

impl ReactiveJournalBuilder {
    pub fn new(
        topology: HashMap<StageId, Vec<StageId>>,
        control_journal: Arc<dyn Journal>,
        stage_journals: HashMap<StageId, Arc<dyn Journal>>,
        pipeline_id: PipelineId,
    ) -> Self {
        Self {
            topology,
            control_journal,
            stage_journals,
            pipeline_id,
        }
    }
    
    pub fn build(self) -> Result<JournalSet> {
        let mut reactive_journals = HashMap::new();
        
        // Use the passed-in journals directly
        let control_journal = self.control_journal;
        let raw_journals = self.stage_journals;
        
        // Create ReactiveJournals with correct upstream references
        for (stage_id, upstream_ids) in &self.topology {
            let upstream_journals: Vec<(StageId, Arc<dyn Journal>)> = upstream_ids
                .iter()
                .filter_map(|upstream_id| {
                    raw_journals.get(upstream_id).map(|journal| {
                        (*upstream_id, journal.clone())
                    })
                })
                .collect();
            
            let stage_journal = raw_journals.get(stage_id)
                .ok_or_else(|| format!("Missing journal for stage {:?}", stage_id))?;
            
            let reactive_journal = ReactiveJournal::new(
                stage_journal.clone(),
                WriterId::new(),
                control_journal.clone(),
                upstream_journals,
            );
            
            reactive_journals.insert(*stage_id, reactive_journal);
        }
        
        // Create pipeline's ReactiveJournal (writes to control)
        let pipeline_journal = ReactiveJournal::new(
            control_journal.clone(), // Pipeline writes to control journal
            WriterId::new(),
            control_journal.clone(),
            vec![], // Pipeline has no upstream
        );
        
        Ok(JournalSet {
            pipeline_journal,
            stage_journals: reactive_journals,
            _raw_journals: raw_journals, // Keep raw journals alive
        })
    }
}

/// Result of journal building - ensures correct lifetime management
pub struct JournalSet {
    pub pipeline_journal: ReactiveJournal,
    pub stage_journals: HashMap<StageId, ReactiveJournal>,
    _raw_journals: HashMap<StageId, Arc<dyn Journal>>, // Keep raw journals alive
}

// Implement Journal trait for ReactiveJournal to maintain compatibility
#[async_trait]
impl Journal for ReactiveJournal {
    fn owner(&self) -> Option<&obzenflow_core::JournalOwner> {
        self.my_journal.owner()
    }
    
    async fn append(
        &self,
        writer_id: &WriterId,
        event: ChainEvent,
        parent: Option<&EventEnvelope>
    ) -> std::result::Result<EventEnvelope, JournalError> {
        // Use our writer_id, not the passed one
        self.my_journal.append(&self.writer_id, event, parent).await
    }

    async fn read_causally_ordered(&self) -> std::result::Result<Vec<EventEnvelope>, JournalError> {
        // Read from my journal
        self.my_journal.read_causally_ordered().await
    }

    async fn read_causally_after(&self, after_event_id: &obzenflow_core::EventId) -> std::result::Result<Vec<EventEnvelope>, JournalError> {
        // Read from my journal
        self.my_journal.read_causally_after(after_event_id).await
    }

    async fn read_event(&self, event_id: &obzenflow_core::EventId) -> std::result::Result<Option<EventEnvelope>, JournalError> {
        // Read from my journal
        self.my_journal.read_event(event_id).await
    }
    
    async fn reader(&self) -> std::result::Result<Box<dyn obzenflow_core::journal::journal_reader::JournalReader>, JournalError> {
        // Create a reader for my journal
        self.my_journal.reader().await
    }
    
    async fn reader_from(&self, position: u64) -> std::result::Result<Box<dyn obzenflow_core::journal::journal_reader::JournalReader>, JournalError> {
        // Create a reader for my journal starting from position
        self.my_journal.reader_from(position).await
    }
}