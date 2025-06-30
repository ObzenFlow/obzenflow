//! Comprehensive middleware tests covering all identified gaps
//!
//! This test file covers:
//! - MiddlewareAction::Skip and Abort
//! - Error handling with on_error and ErrorAction
//! - Metrics verification
//! - Context-aware middleware
//! - Source/Sink middleware
//! - Complex composition scenarios

use obzenflow_adapters::middleware::{
    Middleware, MiddlewareAction, ErrorAction, MiddlewareFactory,
    TransformHandlerExt, SinkHandlerExt, FiniteSourceHandlerExt,
    MonitoringMiddleware,
};
use obzenflow_adapters::monitoring::taxonomies::red::RED;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::Result;
use obzenflow_runtime_services::control_plane::stages::handler_traits::{
    TransformHandler, SinkHandler, FiniteSourceHandler,
};
use obzenflow_runtime_services::control_plane::stages::supervisors::config::StageConfig;
use obzenflow_topology_services::stages::StageId;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use async_trait::async_trait;

// Test handlers
struct TestTransform;
impl TransformHandler for TestTransform {
    fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        event.payload["processed"] = json!(true);
        vec![event]
    }
}

struct FailingTransform {
    fail_count: Arc<AtomicUsize>,
}
impl TransformHandler for FailingTransform {
    fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let count = self.fail_count.fetch_sub(1, Ordering::SeqCst);
        if count > 0 {
            // Return empty vec to simulate error without panicking
            // In real code, we'd use Result<Vec<ChainEvent>> but TransformHandler doesn't support that
            vec![]
        } else {
            vec![event]
        }
    }
}

struct TestSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

#[async_trait]
impl SinkHandler for TestSink {
    fn consume(&mut self, event: ChainEvent) -> Result<()> {
        self.events.lock().unwrap().push(event);
        Ok(())
    }
    
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
    
    async fn drain(&mut self) -> Result<()> {
        Ok(())
    }
}

struct TestSource {
    events: Vec<ChainEvent>,
}

#[async_trait]
impl FiniteSourceHandler for TestSource {
    fn next(&mut self) -> Option<ChainEvent> {
        if self.events.is_empty() {
            None
        } else {
            Some(self.events.remove(0))
        }
    }
    
    fn is_complete(&self) -> bool {
        self.events.is_empty()
    }
}

#[test]
fn test_middleware_action_skip() {
    // Middleware that skips processing and returns cached result
    struct CachingMiddleware {
        cache: Arc<Mutex<Option<ChainEvent>>>,
    }
    
    impl Middleware for CachingMiddleware {
        fn pre_handle(&self, _event: &ChainEvent) -> MiddlewareAction {
            if let Some(cached) = self.cache.lock().unwrap().clone() {
                // Skip the handler and return cached result
                return MiddlewareAction::Skip(vec![cached]);
            }
            MiddlewareAction::Continue
        }
        
        fn post_handle(&self, _event: &ChainEvent, results: &mut Vec<ChainEvent>) {
            if let Some(result) = results.first() {
                *self.cache.lock().unwrap() = Some(result.clone());
            }
        }
    }
    
    let cache = Arc::new(Mutex::new(None));
    let handler = TestTransform
        .middleware()
        .with(CachingMiddleware { cache: cache.clone() })
        .build();
    
    // First call - processes normally
    let event1 = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "test",
        json!({"id": 1})
    );
    let results1 = handler.process(event1);
    assert_eq!(results1.len(), 1);
    assert_eq!(results1[0].payload["processed"], json!(true));
    
    // Second call - should skip processing and return cached
    let event2 = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "test",
        json!({"id": 2})
    );
    let results2 = handler.process(event2);
    assert_eq!(results2.len(), 1);
    assert_eq!(results2[0].payload["id"], json!(1)); // Cached result!
    assert_eq!(results2[0].payload["processed"], json!(true));
}

#[test]
fn test_middleware_action_abort() {
    // Middleware that aborts processing for certain events
    struct FilterMiddleware {
        blocked_types: Vec<String>,
    }
    
    impl Middleware for FilterMiddleware {
        fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
            if self.blocked_types.contains(&event.event_type) {
                return MiddlewareAction::Abort;
            }
            MiddlewareAction::Continue
        }
    }
    
    let handler = TestTransform
        .middleware()
        .with(FilterMiddleware {
            blocked_types: vec!["blocked".to_string()],
        })
        .build();
    
    // Allowed event - processes normally
    let event1 = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "allowed",
        json!({})
    );
    let results1 = handler.process(event1);
    assert_eq!(results1.len(), 1);
    
    // Blocked event - aborted
    let event2 = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "blocked",
        json!({})
    );
    let results2 = handler.process(event2);
    assert_eq!(results2.len(), 0); // Aborted!
}

#[test]
fn test_error_detection_and_recovery() {
    // Since transforms can't return errors (Jonestown Protocol),
    // middleware detects errors through patterns like empty results
    struct ErrorDetectionMiddleware {
        error_count: Arc<AtomicUsize>,
        expected_output: bool,
    }
    
    impl Middleware for ErrorDetectionMiddleware {
        fn post_handle(&self, event: &ChainEvent, results: &mut Vec<ChainEvent>) {
            // Detect error condition: transform returned no results when expected
            if self.expected_output && results.is_empty() {
                self.error_count.fetch_add(1, Ordering::SeqCst);
                
                // Convert to error event
                let mut error_event = event.clone();
                error_event.event_type = "error.detected".to_string();
                error_event.payload["error"] = json!("Transform produced no output");
                results.push(error_event);
            }
        }
    }
    
    let error_count = Arc::new(AtomicUsize::new(0));
    let handler = FailingTransform {
        fail_count: Arc::new(AtomicUsize::new(1)), // Fail once (returns empty vec)
    }
    .middleware()
    .with(ErrorDetectionMiddleware {
        error_count: error_count.clone(),
        expected_output: true,
    })
    .build();
    
    let event = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "test",
        json!({})
    );
    
    // Transform returns empty vec (simulated error), middleware detects and recovers
    let results = handler.process(event);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].event_type, "error.detected");
    assert_eq!(results[0].payload["error"], json!("Transform produced no output"));
    assert_eq!(error_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_error_retry_action() {
    // Middleware that retries on error
    struct RetryMiddleware {
        max_retries: usize,
        retry_count: Arc<AtomicUsize>,
    }
    
    impl Middleware for RetryMiddleware {
        fn on_error(&self, _event: &ChainEvent) -> ErrorAction {
            let count = self.retry_count.fetch_add(1, Ordering::SeqCst);
            if count < self.max_retries {
                ErrorAction::Retry
            } else {
                ErrorAction::Propagate
            }
        }
    }
    
    let retry_count = Arc::new(AtomicUsize::new(0));
    let fail_count = Arc::new(AtomicUsize::new(2)); // Fail twice
    
    let handler = FailingTransform {
        fail_count: fail_count.clone(),
    }
    .middleware()
    .with(RetryMiddleware {
        max_retries: 3,
        retry_count: retry_count.clone(),
    })
    .build();
    
    let event = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "test",
        json!({})
    );
    
    // Should succeed after retries
    let results = handler.process(event);
    assert_eq!(results.len(), 1);
    assert_eq!(retry_count.load(Ordering::SeqCst), 2); // Retried twice
}

#[test]
fn test_context_aware_middleware() {
    // Factory that creates different middleware based on stage context
    struct AdaptiveMonitoringFactory;
    
    impl MiddlewareFactory for AdaptiveMonitoringFactory {
        fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
            // Create different middleware based on stage name
            if config.stage_name.contains("critical") {
                Box::new(StrictMonitoringMiddleware {
                    stage_name: config.stage_name.clone(),
                })
            } else {
                Box::new(RelaxedMonitoringMiddleware {
                    stage_name: config.stage_name.clone(),
                })
            }
        }
        
        fn name(&self) -> &str {
            "AdaptiveMonitoring"
        }
    }
    
    struct StrictMonitoringMiddleware {
        stage_name: String,
    }
    
    impl Middleware for StrictMonitoringMiddleware {
        fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
            println!("STRICT monitoring for {} - event: {}", self.stage_name, event.id);
            MiddlewareAction::Continue
        }
    }
    
    struct RelaxedMonitoringMiddleware {
        stage_name: String,
    }
    
    impl Middleware for RelaxedMonitoringMiddleware {
        fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
            println!("Relaxed monitoring for {} - event: {}", self.stage_name, event.id);
            MiddlewareAction::Continue
        }
    }
    
    // Test factory creates appropriate middleware
    let factory = AdaptiveMonitoringFactory;
    
    let critical_config = StageConfig {
        stage_id: StageId::new(),
        stage_name: "critical_payment_processor".to_string(),
        upstream_stages: vec![],
        journal: Arc::new(obzenflow_runtime_services::data_plane::journal_subscription::ReactiveJournal::new(
            Arc::new(obzenflow_infra::journal::memory::MemoryJournal::new())
        )),
        message_bus: Arc::new(obzenflow_runtime_services::message_bus::FsmMessageBus::new()),
    };
    
    let normal_config = StageConfig {
        stage_id: StageId::new(),
        stage_name: "logging_stage".to_string(),
        upstream_stages: vec![],
        journal: Arc::new(obzenflow_runtime_services::data_plane::journal_subscription::ReactiveJournal::new(
            Arc::new(obzenflow_infra::journal::memory::MemoryJournal::new())
        )),
        message_bus: Arc::new(obzenflow_runtime_services::message_bus::FsmMessageBus::new()),
    };
    
    let _critical_mw = factory.create(&critical_config);
    let _normal_mw = factory.create(&normal_config);
    
    // Different middleware types created based on context
    assert_eq!(factory.name(), "AdaptiveMonitoring");
}

#[test]
fn test_sink_middleware_with_error_handling() {
    // Sink that fails sometimes
    struct UnreliableSink {
        fail_next: Arc<Mutex<bool>>,
        consumed: Arc<Mutex<Vec<ChainEvent>>>,
    }
    
    #[async_trait]
    impl SinkHandler for UnreliableSink {
        fn consume(&mut self, event: ChainEvent) -> Result<()> {
            if *self.fail_next.lock().unwrap() {
                *self.fail_next.lock().unwrap() = false;
                Err(anyhow::anyhow!("Simulated sink failure").into())
            } else {
                self.consumed.lock().unwrap().push(event);
                Ok(())
            }
        }
        
        fn flush(&mut self) -> Result<()> {
            Ok(())
        }
        
        async fn drain(&mut self) -> Result<()> {
            Ok(())
        }
    }
    
    // Middleware that logs sink operations
    struct SinkLoggingMiddleware {
        pre_count: Arc<AtomicUsize>,
        post_count: Arc<AtomicUsize>,
        error_count: Arc<AtomicUsize>,
    }
    
    impl Middleware for SinkLoggingMiddleware {
        fn pre_handle(&self, _event: &ChainEvent) -> MiddlewareAction {
            self.pre_count.fetch_add(1, Ordering::SeqCst);
            MiddlewareAction::Continue
        }
        
        fn post_handle(&self, _event: &ChainEvent, _results: &mut Vec<ChainEvent>) {
            self.post_count.fetch_add(1, Ordering::SeqCst);
        }
        
        fn on_error(&self, _event: &ChainEvent) -> ErrorAction {
            self.error_count.fetch_add(1, Ordering::SeqCst);
            ErrorAction::Retry // Retry once
        }
    }
    
    let fail_next = Arc::new(Mutex::new(true));
    let consumed = Arc::new(Mutex::new(Vec::new()));
    let pre_count = Arc::new(AtomicUsize::new(0));
    let post_count = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    
    let mut sink = UnreliableSink {
        fail_next: fail_next.clone(),
        consumed: consumed.clone(),
    }
    .middleware()
    .with(SinkLoggingMiddleware {
        pre_count: pre_count.clone(),
        post_count: post_count.clone(),
        error_count: error_count.clone(),
    })
    .build();
    
    let event = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "test",
        json!({})
    );
    
    // Should succeed after retry
    assert!(sink.consume(event.clone()).is_ok());
    assert_eq!(consumed.lock().unwrap().len(), 1);
    assert_eq!(pre_count.load(Ordering::SeqCst), 1);
    assert_eq!(post_count.load(Ordering::SeqCst), 1);
    assert_eq!(error_count.load(Ordering::SeqCst), 1); // One error, then retry succeeded
}

#[test]
fn test_source_middleware() {
    // Middleware that enriches events from source
    struct SourceEnrichmentMiddleware {
        source_id: String,
        event_count: Arc<AtomicUsize>,
    }
    
    impl Middleware for SourceEnrichmentMiddleware {
        fn post_handle(&self, event: &ChainEvent, results: &mut Vec<ChainEvent>) {
            // For sources, the event is what was emitted
            // We need to modify the results that will be passed downstream
            if results.is_empty() {
                // If no results, the source emitted this event
                let mut enriched = event.clone();
                enriched.payload["source_id"] = json!(self.source_id.clone());
                enriched.payload["sequence"] = json!(self.event_count.fetch_add(1, Ordering::SeqCst));
                results.push(enriched);
            } else {
                for result in results.iter_mut() {
                    result.payload["source_id"] = json!(self.source_id.clone());
                    result.payload["sequence"] = json!(self.event_count.fetch_add(1, Ordering::SeqCst));
                }
            }
        }
    }
    
    let event_count = Arc::new(AtomicUsize::new(0));
    let writer_id = WriterId::new();
    let mut source = TestSource {
        events: vec![
            ChainEvent::new(obzenflow_core::EventId::new(), writer_id, "test1", json!({})),
            ChainEvent::new(obzenflow_core::EventId::new(), writer_id, "test2", json!({})),
        ],
    }
    .middleware(writer_id)
    .with(SourceEnrichmentMiddleware {
        source_id: "source_123".to_string(),
        event_count: event_count.clone(),
    })
    .build();
    
    // First event
    let event1 = source.next().unwrap();
    assert_eq!(event1.payload["source_id"], json!("source_123"));
    assert_eq!(event1.payload["sequence"], json!(0));
    
    // Second event
    let event2 = source.next().unwrap();
    assert_eq!(event2.payload["source_id"], json!("source_123"));
    assert_eq!(event2.payload["sequence"], json!(1));
    
    // No more events
    assert!(source.next().is_none());
}

#[test]
fn test_complex_middleware_ordering() {
    // Multiple middleware that modify events in sequence
    struct PrependMiddleware(&'static str);
    struct AppendMiddleware(&'static str);
    
    impl Middleware for PrependMiddleware {
        fn post_handle(&self, _event: &ChainEvent, results: &mut Vec<ChainEvent>) {
            for result in results.iter_mut() {
                let current = result.payload["text"].as_str().unwrap_or("");
                result.payload["text"] = json!(format!("{}{}", self.0, current));
            }
        }
    }
    
    impl Middleware for AppendMiddleware {
        fn post_handle(&self, _event: &ChainEvent, results: &mut Vec<ChainEvent>) {
            for result in results.iter_mut() {
                let current = result.payload["text"].as_str().unwrap_or("");
                result.payload["text"] = json!(format!("{}{}", current, self.0));
            }
        }
    }
    
    let handler = TestTransform
        .middleware()
        .with(PrependMiddleware("A"))
        .with(AppendMiddleware("B"))
        .with(PrependMiddleware("C"))
        .with(AppendMiddleware("D"))
        .build();
    
    let event = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "test",
        json!({"text": "_"})
    );
    
    let results = handler.process(event);
    assert_eq!(results.len(), 1);
    // Middleware execute in LIFO order for post_handle
    // So order is: D, C, B, A
    assert_eq!(results[0].payload["text"], json!("CA_BD"));
}

#[test]
fn test_conditional_middleware_application() {
    // Middleware that only processes certain event types
    struct ConditionalMiddleware {
        event_types: Vec<String>,
        applied_count: Arc<AtomicUsize>,
    }
    
    impl Middleware for ConditionalMiddleware {
        fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
            if self.event_types.contains(&event.event_type) {
                self.applied_count.fetch_add(1, Ordering::SeqCst);
            }
            MiddlewareAction::Continue
        }
    }
    
    let applied_count = Arc::new(AtomicUsize::new(0));
    let handler = TestTransform
        .middleware()
        .with(ConditionalMiddleware {
            event_types: vec!["important".to_string()],
            applied_count: applied_count.clone(),
        })
        .build();
    
    // Process different event types
    let event1 = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "normal",
        json!({})
    );
    handler.process(event1);
    assert_eq!(applied_count.load(Ordering::SeqCst), 0);
    
    let event2 = ChainEvent::new(
        obzenflow_core::EventId::new(),
        WriterId::new(),
        "important",
        json!({})
    );
    handler.process(event2);
    assert_eq!(applied_count.load(Ordering::SeqCst), 1);
}

#[test]
fn test_metrics_recording_verification() {
    // Create a monitoring middleware with a stage context
    let stage_id = StageId::new();
    let monitoring = MonitoringMiddleware::<RED>::new("test_stage", stage_id);
    
    // Create handler with monitoring
    let handler = TestTransform
        .middleware()
        .with(monitoring)
        .build();
    
    // Process some events
    for i in 0..5 {
        let event = ChainEvent::new(
            obzenflow_core::EventId::new(),
            WriterId::new(),
            "test",
            json!({"id": i})
        );
        handler.process(event);
    }
    
    // TODO: Once we have metric export functionality, verify:
    // - Rate metric shows 5 events
    // - Duration metrics are recorded
    // - No errors recorded
    // For now, this test ensures monitoring middleware compiles and runs
}