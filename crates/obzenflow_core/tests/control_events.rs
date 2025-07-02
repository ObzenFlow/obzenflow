use obzenflow_core::{ChainEvent, EventId, WriterId};

#[test]
fn test_control_event_constants() {
    // Verify the constants are properly defined
    assert_eq!(ChainEvent::CONTROL_EVENT_PREFIX, "control.");
    assert_eq!(ChainEvent::EOF_EVENT_TYPE, "control.eof");
    assert_eq!(ChainEvent::WATERMARK_EVENT_TYPE, "control.watermark");
    assert_eq!(ChainEvent::CHECKPOINT_EVENT_TYPE, "control.checkpoint");
    assert_eq!(ChainEvent::DRAIN_EVENT_TYPE, "control.drain");
}

#[test]
fn test_is_control_detection() {
    // Test EOF event
    let eof_event = ChainEvent::eof(
        EventId::new(),
        WriterId::new(),
        true
    );
    assert!(eof_event.is_control());
    assert_eq!(eof_event.as_control_type(), Some("control.eof"));
    
    // Test data event
    let data_event = ChainEvent::new(
        EventId::new(),
        WriterId::new(),
        "user.data.processed",
        serde_json::json!({"value": 42})
    );
    assert!(!data_event.is_control());
    assert_eq!(data_event.as_control_type(), None);
}

#[test]
fn test_all_control_types_detected() {
    let test_cases = vec![
        ("control.eof", true),
        ("control.watermark", true),
        ("control.checkpoint", true),
        ("control.drain", true),
        ("control.unknown", true), // Any control. prefix should be detected
        ("user.data", false),
        ("system.metrics", false),
        ("controldata", false), // Must have the dot
    ];
    
    for (event_type, should_be_control) in test_cases {
        let event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            event_type,
            serde_json::json!({})
        );
        
        assert_eq!(
            event.is_control(), 
            should_be_control,
            "Event type '{}' should {}be detected as control event",
            event_type,
            if should_be_control { "" } else { "not " }
        );
    }
}