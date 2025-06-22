//! Test the BehaviorEventHandler pattern

use flowstate_rs::chain_event::ChainEvent;
use flowstate_rs::lifecycle::{
    EventHandler, ProcessingMode, BehaviorBuilder, 
    BehaviorCondition
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use serde_json::json;

/// Simple event generator that tracks how many events it has created
struct CountingGenerator {
    count: Arc<AtomicUsize>,
    max_events: usize,
}

impl CountingGenerator {
    fn new(max_events: usize) -> Self {
        Self {
            count: Arc::new(AtomicUsize::new(0)),
            max_events,
        }
    }
}

impl EventHandler for CountingGenerator {
    fn transform(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        let current = self.count.fetch_add(1, Ordering::Relaxed);
        if current < self.max_events {
            event.payload = json!({ "event_number": current + 1 });
            vec![event]
        } else {
            vec![] // No more events
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[test]
fn test_completable_behavior() {
    // Create a generator that produces 5 events
    let generator = CountingGenerator::new(5);
    let count_ref = generator.count.clone();
    
    // Wrap it with completion behavior
    let mut handler = BehaviorBuilder::new(generator)
        .with_completion(move || count_ref.load(Ordering::Relaxed) >= 5);
    
    // Initially, no completion
    if let Some(condition) = handler.check_behaviors() {
        match condition {
            BehaviorCondition::Normal => {}, // Expected
            _ => panic!("Expected Normal condition"),
        }
    }
    
    // Generate events
    let dummy_event = ChainEvent::new("test", json!({}));
    for i in 0..5 {
        let events = handler.transform(dummy_event.clone());
        if i < 5 {
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].payload["event_number"], i + 1);
        }
    }
    
    // After 5 events, should signal completion
    if let Some(condition) = handler.check_behaviors() {
        match condition {
            BehaviorCondition::SourceComplete { natural: true } => {}, // Expected
            _ => panic!("Expected SourceComplete condition"),
        }
    }
}

#[test]
fn test_behavior_without_completion() {
    // Regular handler without behaviors
    let mut handler = CountingGenerator::new(10);
    
    // Should return None (no behaviors)
    assert!(handler.check_behaviors().is_none());
}