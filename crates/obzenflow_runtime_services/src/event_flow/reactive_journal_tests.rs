//! Tests for ReactiveJournal subscription-based metrics

#[cfg(test)]
mod tests {
    use super::super::*;
    use obzenflow_core::{ChainEvent, EventId};
    use obzenflow_infra::journal::MemoryJournal;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_subscription_receives_data_events() {
        // Create a memory journal
        let journal = Arc::new(MemoryJournal::new());
        
        // Create reactive journal (no observer needed - uses subscriptions now)
        let reactive_journal = ReactiveJournal::new(journal.clone());
        
        // Create a subscription for all events
        let mut subscription = reactive_journal.subscribe(
            crate::event_flow::reactive_journal::SubscriptionFilter::All
        ).await.unwrap();
        
        // Register a writer
        let stage_id = obzenflow_topology_services::stages::StageId::new();
        let writer_id = reactive_journal.register_writer(stage_id, None).await.unwrap();
        
        // Write a data event
        let event = ChainEvent::new(
            EventId::new(),
            writer_id.clone(),
            "test.data.event",
            serde_json::json!({"value": 42}),
        );
        
        reactive_journal.write(&writer_id, event, None).await.unwrap();
        
        // Verify subscription received the event
        let events = subscription.recv_batch().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "test.data.event");
    }
    
    #[tokio::test]
    async fn test_subscription_receives_control_events() {
        // Create a memory journal
        let journal = Arc::new(MemoryJournal::new());
        
        // Create reactive journal (no observer needed - uses subscriptions now)
        let reactive_journal = ReactiveJournal::new(journal.clone());
        
        // Create a subscription for control events
        let mut subscription = reactive_journal.subscribe(
            crate::event_flow::reactive_journal::SubscriptionFilter::EventTypes {
                event_types: vec!["control.metrics.state".to_string()],
            }
        ).await.unwrap();
        
        // Register a writer
        let stage_id = obzenflow_topology_services::stages::StageId::new();
        let writer_id = reactive_journal.register_writer(stage_id, None).await.unwrap();
        
        // Write a control event
        let event = ChainEvent::control(
            "control.metrics.state",
            serde_json::json!({"queue_depth": 10}),
        );
        
        reactive_journal.write(&writer_id, event, None).await.unwrap();
        
        // Verify subscription received the control event
        let events = subscription.recv_batch().await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event.event_type, "control.metrics.state");
    }
    
    #[tokio::test]
    async fn test_journal_write_without_subscriptions() {
        // Create a memory journal
        let journal = Arc::new(MemoryJournal::new());
        
        // Create reactive journal (no subscriptions)
        let reactive_journal = ReactiveJournal::new(journal.clone());
        
        // Register a writer
        let stage_id = obzenflow_topology_services::stages::StageId::new();
        let writer_id = reactive_journal.register_writer(stage_id, None).await.unwrap();
        
        // Write events - should not panic even without subscriptions
        let data_event = ChainEvent::new(
            EventId::new(),
            writer_id.clone(),
            "test.event",
            serde_json::json!({}),
        );
        
        let control_event = ChainEvent::control(
            "control.test",
            serde_json::json!({}),
        );
        
        reactive_journal.write(&writer_id, data_event, None).await.unwrap();
        reactive_journal.write(&writer_id, control_event, None).await.unwrap();
        
        // Test passes if no panic occurs
    }
}