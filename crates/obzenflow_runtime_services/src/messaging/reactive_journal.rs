//! ReactiveJournal - Stage-local journal architecture (FLOWIP-008)
//!
//! This module implements the simplified stage-local journal design where:
//! - Each stage has its own journal for data events
//! - All stages share a control journal for system events
//! - No pub/sub, no notifications, just direct journal reads

use obzenflow_core::Result;
use obzenflow_core::{StageId, PipelineId};
use obzenflow_core::journal::journal::Journal;
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
        // Build list of all journals to read from
        let mut readers = vec![];
        
        // ALWAYS read from control journal for system/pipeline events
        readers.push((CONTROL_STAGE_ID, self.control_journal.clone()));
        
        // Then add upstream stage journals for data
        for (stage_id, journal) in &self.upstream_journals {
            readers.push((*stage_id, journal.clone()));
        }
        
        Ok(JournalSubscription {
            upstream_readers: readers,
            current_index: 0,
            read_positions: HashMap::new(),
        })
    }
}

/// Simple subscription that reads directly from journals (FLOWIP-008)
/// 
/// No channels, no notifications - just reads from journals in round-robin
pub struct JournalSubscription {
    /// Upstream journals we're reading from
    upstream_readers: Vec<(StageId, Arc<dyn Journal>)>,
    
    /// Current position in each journal
    read_positions: HashMap<StageId, u64>,
    
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
        loop {
            // Round-robin through upstream journals
            if self.upstream_readers.is_empty() {
                // Source stage - no upstream, just wait
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            
            let (stage_id, journal) = &self.upstream_readers[self.current_index];
            let position = self.read_positions.get(stage_id).copied().unwrap_or(0);
            
            // Try to read next event using causally ordered read
            match journal.read_causally_ordered().await {
                Ok(events) => {
                    // Find first event we haven't seen
                    if let Some(envelope) = events.into_iter().skip(position as usize).next() {
                        self.read_positions.insert(*stage_id, position + 1);
                        return Ok(envelope);
                    }
                }
                Err(e) => {
                    tracing::warn!("Error reading from journal for stage {:?}: {}", stage_id, e);
                }
            }
            
            // Move to next journal
            self.current_index = (self.current_index + 1) % self.upstream_readers.len();
            
            // If we've checked all journals, brief sleep
            if self.current_index == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }
    
    // NO recv_batch()! Per FLOWIP-056zzz, FSMs process one event at a time.
    // Batching violates FSM semantics - only one state transition per dispatch_state.
}

/// Builder for creating stage-local journal architecture
pub struct ReactiveJournalBuilder<'a> {
    topology: HashMap<StageId, Vec<StageId>>, // stage -> upstream stages
    journal_creator: Box<dyn Fn(&str) -> Arc<dyn Journal> + 'a>,
    pipeline_id: PipelineId,
}

impl<'a> ReactiveJournalBuilder<'a> {
    pub fn new(
        topology: HashMap<StageId, Vec<StageId>>,
        journal_creator: impl Fn(&str) -> Arc<dyn Journal> + 'a,
        pipeline_id: PipelineId,
    ) -> Self {
        Self {
            topology,
            journal_creator: Box::new(journal_creator),
            pipeline_id,
        }
    }
    
    pub fn build(self) -> Result<JournalSet> {
        let mut raw_journals = HashMap::new();
        let mut reactive_journals = HashMap::new();
        
        // Step 1: Create control journal (owned by pipeline)
        let control_journal = (self.journal_creator)("control");
        
        // Step 2: Create all stage data journals
        for stage_id in self.topology.keys() {
            let journal_name = format!("stage_{}", stage_id);
            let journal = (self.journal_creator)(&journal_name);
            raw_journals.insert(*stage_id, journal);
        }
        
        // Step 3: Create ReactiveJournals with correct upstream references
        for (stage_id, upstream_ids) in &self.topology {
            let upstream_journals: Vec<(StageId, Arc<dyn Journal>)> = upstream_ids
                .iter()
                .filter_map(|upstream_id| {
                    raw_journals.get(upstream_id).map(|journal| {
                        (*upstream_id, journal.clone())
                    })
                })
                .collect();
            
            let reactive_journal = ReactiveJournal::new(
                raw_journals[stage_id].clone(),
                WriterId::new(),
                control_journal.clone(),
                upstream_journals,
            );
            
            reactive_journals.insert(*stage_id, reactive_journal);
        }
        
        // Step 4: Create pipeline's ReactiveJournal (writes to control)
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
}