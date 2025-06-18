use crate::chain_event::ChainEvent;
use crate::step::Result;
use crate::event_store::{EventEnvelope, EventStore, WriterId, VectorClock};
use crate::event_store::flow_log::FlowEventLog;
use std::sync::{Arc, Weak};

/// Writer handle for appending events to the store
/// Uses the shared flow log for optimal sequential writes
pub struct EventWriter {
    /// Stage/writer ID
    writer_id: WriterId,
    /// Last vector clock used by this writer - ensures monotonic incrementing
    last_vector_clock: VectorClock,
    /// Reference to the shared flow log
    flow_log: Arc<FlowEventLog>,
    /// Weak reference to EventStore for notifications
    store: Weak<EventStore>,
}

impl EventWriter {
    pub(crate) fn new(writer_id: WriterId, flow_log: Arc<FlowEventLog>, store: Weak<EventStore>) -> Self {
        Self {
            writer_id,
            last_vector_clock: VectorClock::new(),
            flow_log,
            store,
        }
    }
    
    /// Append an event to the store with optional parent reference
    /// 
    /// If parent is provided, the vector clock will be updated to reflect
    /// the causal dependency. Otherwise, a new causal chain is started.
    /// 
    /// This method is &mut self to ensure single-threaded access and
    /// avoid any need for synchronization primitives.
    pub async fn append(
        &mut self, 
        event: ChainEvent,
        parent: Option<&EventEnvelope>
    ) -> Result<EventEnvelope> {
        // Append to the shared flow log, passing our last vector clock
        let envelope = self.flow_log.append(
            &self.writer_id, 
            event.clone(), 
            parent,
            &self.last_vector_clock
        ).await?;
        
        // Update our last vector clock to maintain state
        self.last_vector_clock = envelope.vector_clock.clone();
        
        tracing::debug!("Writer {:?} appended event {} type '{}'", self.writer_id, envelope.event.ulid, event.event_type);
        
        // Notify subscribers inline - try_send is non-blocking
        if let Some(store) = self.store.upgrade() {
            store.notify_subscribers(&envelope).await;
        } else {
            tracing::warn!("Writer '{}' store dropped - cannot notify subscribers", self.writer_id);
        }
        
        Ok(envelope)
    }
    
    
    /// Get the writer's ID
    pub fn writer_id(&self) -> &WriterId {
        &self.writer_id
    }
    
}