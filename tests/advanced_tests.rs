// tests/advanced_tests.rs
use obzenflow_dsl_infra::{flow, source, transform, sink};
use obzenflow_runtime_services::stages::common::handlers::{
    FiniteSourceHandler, TransformHandler, SinkHandler
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_adapters::monitoring::taxonomies::{
    golden_signals::GoldenSignals,
    red::RED,
    use_taxonomy::USE,
    saafe::SAAFE,
};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::tempdir;
use anyhow::Result;
use async_trait::async_trait;

/// Test using the DSL macros with EventStore
#[tokio::test]
async fn test_dsl_pipeline() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_dsl_event_store");
    
    // Define pipeline stages
    struct EventGenerator {
        events: Vec<(String, serde_json::Value)>,
        emitted: usize,
        writer_id: WriterId,
    }
    
    impl EventGenerator {
        fn new() -> Self {
            let events = vec![
                ("Input".to_string(), json!({ "value": 10 })),
                ("Input".to_string(), json!({ "value": 20 })),
                ("Input".to_string(), json!({ "value": 30 })),
            ];
            Self {
                events,
                emitted: 0,
                writer_id: WriterId::new(),
            }
        }
    }
    
    impl FiniteSourceHandler for EventGenerator {
        fn next(&mut self) -> Option<ChainEvent> {
            if self.emitted < self.events.len() {
                let (event_type, payload) = &self.events[self.emitted];
                self.emitted += 1;
                Some(ChainEvent::new(
                    EventId::new(),
                    self.writer_id.clone(),
                    event_type,
                    payload.clone(),
                ))
            } else {
                None
            }
        }
        
        fn is_complete(&self) -> bool {
            self.emitted >= self.events.len()
        }
    }
    
    struct Doubler;
    
    impl Doubler {
        fn new() -> Self {
            Self
        }
    }
    
    impl TransformHandler for Doubler {
        fn process(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
            if let Some(value) = event.payload["value"].as_u64() {
                event.payload["doubled"] = json!(value * 2);
                event.event_type = "Doubled".to_string();
            }
            vec![event]
        }
    }
    
    #[derive(Clone)]
    struct Summer {
        total: Arc<AtomicU64>,
    }
    
    impl Summer {
        fn new(total: Arc<AtomicU64>) -> Self {
            Self {
                total,
            }
        }
    }
    
    #[async_trait]
    impl SinkHandler for Summer {
        fn consume(&mut self, event: ChainEvent) -> obzenflow_core::Result<()> {
            if let Some(doubled) = event.payload["doubled"].as_u64() {
                self.total.fetch_add(doubled, Ordering::Relaxed);
            }
            Ok(())
        }
    }

    // Create shared state
    let total = Arc::new(AtomicU64::new(0));
    let summer = Summer::new(total.clone());

    // Create journal
    let journal_path = temp_dir.path().to_path_buf();
    let journal = Arc::new(DiskJournal::new(journal_path, "test_dsl").await?);

    // Run pipeline with DSL
    let handle = flow! {
        journal: journal,
        middleware: [GoldenSignals::monitoring()],
        
        stages: {
            gen = source!("generator" => EventGenerator::new(), [RED::monitoring()]);
            dbl = transform!("doubler" => Doubler::new(), [USE::monitoring()]);
            sum = sink!("summer" => summer, [SAAFE::monitoring()]);
        },
        
        topology: {
            gen |> dbl;
            dbl |> sum;
        }
    }.await.map_err(|e| anyhow::anyhow!("Failed to create flow: {:?}", e))?;

    // Run the pipeline
    handle.run().await?;

    // Verify sum: (10 + 20 + 30) * 2 = 120
    assert_eq!(total.load(Ordering::Relaxed), 120);

    // Clean up
    // Cleanup handled by tempdir
    Ok(())
}

// Additional tests for more complex scenarios will be added as the DSL evolves
// to support features like multi-sink fanout, complex event filtering, etc.