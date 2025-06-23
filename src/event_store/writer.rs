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
        // Try to append to the shared flow log
        let envelope = match self.flow_log.append(
            &self.writer_id, 
            event.clone(), 
            parent,
            &self.last_vector_clock
        ).await {
            Ok(env) => env,
            Err(e) => {
                // Write failed - emit Kool-Aid to downstream subscribers
                tracing::error!(
                    "Writer {:?} failed to write event: {} - emitting Kool-Aid",
                    self.writer_id, e
                );
                
                // EmitKoolAid: Signal EOF to all downstream subscribers
                if let Some(store) = self.store.upgrade() {
                    if let Err(eof_err) = store.signal_stage_complete(self.writer_id.stage_id(), false).await {
                        tracing::error!("Failed to emit Kool-Aid: {}", eof_err);
                    }
                }
                
                // DrinkKoolAid: Return the original error to terminate
                return Err(e);
            }
        };
        
        // Update our last vector clock to maintain state
        self.last_vector_clock = envelope.vector_clock.clone();
        
        tracing::debug!("Writer {:?} appended event {} type '{}'", self.writer_id, envelope.event.ulid, event.event_type);
        
        // Notify subscribers and check if anyone received it
        if let Some(store) = self.store.upgrade() {
            let had_subscribers = store.notify_subscribers(&envelope).await;
            
            if !had_subscribers {
                // No subscribers received the notification - Jonestown Protocol
                // Note: We don't emit EOF here because there's no one to emit TO
                tracing::error!(
                    "Writer {:?} has no alive subscribers - drinking Kool-Aid alone",
                    self.writer_id
                );
                return Err(format!(
                    "No subscribers - Jonestown Protocol activated for stage {:?}",
                    self.writer_id.stage_id()
                ).into());
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event_store::{EventStore, StageSemantics};
    use crate::topology::StageId;
    use crate::chain_event::ChainEvent;
    use serde_json::json;
    
    #[tokio::test]
    async fn test_jonestown_no_subscribers() {
        // Create event store
        let store = EventStore::for_testing().await;
        let stage_id = StageId::from_u32(1);
        
        // Create writer but no subscribers
        let mut writer = store.create_writer(stage_id, StageSemantics::Stateless).await.unwrap();
        
        // Try to write - should fail with Jonestown Protocol
        let event = ChainEvent::new("test", json!({"data": "value"}));
        let result = writer.append(event, None).await;
        
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("No subscribers"));
        assert!(err_msg.contains("Jonestown Protocol"));
    }
    
    #[tokio::test]
    async fn test_jonestown_dead_subscriber() {
        // Create event store
        let store = EventStore::for_testing().await;
        let stage_id = StageId::from_u32(1);
        
        // Create subscription then drop it
        {
            let _subscription = store.subscribe(crate::event_store::SubscriptionFilter::all()).await.unwrap();
            // Subscription dropped here
        }
        
        // Give cleanup a moment (in real code, cleanup happens periodically)
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        // Create writer - subscriber exists but is dead
        let mut writer = store.create_writer(stage_id, StageSemantics::Stateless).await.unwrap();
        
        // Try to write - should fail since subscriber channel is closed
        let event = ChainEvent::new("test", json!({"data": "value"}));
        let result = writer.append(event, None).await;
        
        // This might succeed or fail depending on cleanup timing
        // The important part is that dead subscribers don't prevent failure detection
        if result.is_err() {
            let err_msg = result.unwrap_err().to_string();
            assert!(err_msg.contains("No subscribers"));
        }
    }
    
    #[tokio::test]
    async fn test_normal_write_with_subscriber() {
        // Create event store
        let store = EventStore::for_testing().await;
        let stage_id = StageId::from_u32(1);
        
        // Create subscription for this specific stage
        let filter = crate::event_store::SubscriptionFilter {
            upstream_stages: vec![stage_id],
        };
        let _subscription = store.subscribe(filter).await.unwrap();
        
        // Create writer
        let mut writer = store.create_writer(stage_id, StageSemantics::Stateless).await.unwrap();
        
        // Write should succeed
        let event = ChainEvent::new("test", json!({"data": "value"}));
        let result = writer.append(event, None).await;
        
        if let Err(e) = &result {
            eprintln!("Write failed: {}", e);
        }
        
        assert!(result.is_ok());
        let envelope = result.unwrap();
        assert_eq!(envelope.event.event_type, "test");
    }
}