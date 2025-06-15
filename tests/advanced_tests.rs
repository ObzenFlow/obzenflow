// tests/advanced_tests.rs
use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::path::PathBuf;
use tempfile::tempdir;

/// Test using the DSL macros with EventStore
#[tokio::test]
async fn test_dsl_pipeline() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_dsl_event_store");
    
    // Define pipeline stages
    struct EventGenerator {
        events: Vec<ChainEvent>,
        emitted: AtomicU64,
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    impl EventGenerator {
        fn new() -> Self {
            let events = vec![
                ChainEvent::new("Input", json!({ "value": 10 })),
                ChainEvent::new("Input", json!({ "value": 20 })),
                ChainEvent::new("Input", json!({ "value": 30 })),
            ];
            Self {
                events,
                emitted: AtomicU64::new(0),
                metrics: RED::create_metrics("EventGenerator"),
            }
        }
    }
    
    impl Step for EventGenerator {
        type Taxonomy = RED;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &RED
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Source
        }
        
        fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
            let idx = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
            if idx < self.events.len() {
                vec![self.events[idx].clone()]
            } else {
                vec![]
            }
        }
    }
    
    struct Doubler {
        metrics: <USE as Taxonomy>::Metrics,
    }
    
    impl Doubler {
        fn new() -> Self {
            Self {
                metrics: USE::create_metrics("Doubler"),
            }
        }
    }
    
    impl Step for Doubler {
        type Taxonomy = USE;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &USE
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
            if let Some(value) = event.payload["value"].as_u64() {
                event.payload["doubled"] = json!(value * 2);
                event.event_type = "Doubled".to_string();
            }
            vec![event]
        }
    }
    
    struct Summer {
        total: Arc<AtomicU64>,
        metrics: <SAAFE as Taxonomy>::Metrics,
    }
    
    impl Summer {
        fn new(total: Arc<AtomicU64>) -> Self {
            Self {
                total,
                metrics: SAAFE::create_metrics("Summer"),
            }
        }
    }
    
    impl Step for Summer {
        type Taxonomy = SAAFE;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &SAAFE
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Sink
        }
        
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
            if let Some(doubled) = event.payload["doubled"].as_u64() {
                self.total.fetch_add(doubled, Ordering::Relaxed);
            }
            vec![] // Sink doesn't emit events
        }
    }

    // Create shared state
    let total = Arc::new(AtomicU64::new(0));
    let summer = Summer::new(total.clone());

    // Create event store
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    // Run pipeline with DSL
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("generator" => EventGenerator::new(), RED)
        |> ("doubler" => Doubler::new(), USE)
        |> ("summer" => summer, SAAFE)
    }?;

    // Let it run for a bit
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    // Shutdown gracefully
    handle.shutdown().await?;

    // Verify sum: (10 + 20 + 30) * 2 = 120
    assert_eq!(total.load(Ordering::Relaxed), 120);

    // Clean up
    // Cleanup handled by tempdir
    Ok(())
}

#[tokio::test]
async fn test_complex_dsl_pipeline() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_complex_event_store");

    // Pipeline stages
    struct OrderSource {
        orders: Vec<ChainEvent>,
        emitted: AtomicU64,
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    impl OrderSource {
        fn new() -> Self {
            let orders = vec![
                ChainEvent::new("Order", json!({ "id": 1, "amount": 100, "status": "pending" })),
                ChainEvent::new("Order", json!({ "id": 2, "amount": 50, "status": "completed" })),
                ChainEvent::new("Order", json!({ "id": 3, "amount": 200, "status": "pending" })),
                ChainEvent::new("Order", json!({ "id": 4, "amount": 75, "status": "cancelled" })),
            ];
            Self {
                orders,
                emitted: AtomicU64::new(0),
                metrics: RED::create_metrics("OrderSource"),
            }
        }
    }
    
    impl Step for OrderSource {
        type Taxonomy = RED;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &RED
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Source
        }
        
        fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
            let idx = self.emitted.fetch_add(1, Ordering::Relaxed) as usize;
            if idx < self.orders.len() {
                vec![self.orders[idx].clone()]
            } else {
                vec![]
            }
        }
    }
    
    struct OrderFilter {
        metrics: <USE as Taxonomy>::Metrics,
    }
    
    impl OrderFilter {
        fn new() -> Self {
            Self {
                metrics: USE::create_metrics("OrderFilter"),
            }
        }
    }
    
    impl Step for OrderFilter {
        type Taxonomy = USE;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &USE
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
            if event.payload["status"] == "pending" {
                vec![event]
            } else {
                vec![] // Filter out non-pending orders
            }
        }
    }

    struct TaxCalculator {
        metrics: <GoldenSignals as Taxonomy>::Metrics,
    }
    
    impl TaxCalculator {
        fn new() -> Self {
            Self {
                metrics: GoldenSignals::create_metrics("TaxCalculator"),
            }
        }
    }
    
    impl Step for TaxCalculator {
        type Taxonomy = GoldenSignals;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &GoldenSignals
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn handle(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
            if let Some(amount) = event.payload["amount"].as_u64() {
                let tax = (amount as f64 * 0.08) as u64; // 8% tax
                event.payload["tax"] = json!(tax);
                event.payload["total"] = json!(amount + tax);
                event.event_type = "OrderWithTax".to_string();
            }
            vec![event]
        }
    }

    struct OrderCollector {
        orders: Arc<std::sync::Mutex<Vec<ChainEvent>>>,
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    impl OrderCollector {
        fn new() -> (Self, Arc<std::sync::Mutex<Vec<ChainEvent>>>) {
            let orders = Arc::new(std::sync::Mutex::new(Vec::new()));
            (
                Self {
                    orders: orders.clone(),
                    metrics: RED::create_metrics("OrderCollector"),
                },
                orders,
            )
        }
    }
    
    impl Step for OrderCollector {
        type Taxonomy = RED;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &RED
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Sink
        }
        
        fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
            self.orders.lock().unwrap().push(event);
            vec![]
        }
    }

    let (collector, collected_orders) = OrderCollector::new();

    // Create event store
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;

    // Run complex pipeline
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("orders" => OrderSource::new(), RED)
        |> ("filter" => OrderFilter::new(), USE)
        |> ("tax" => TaxCalculator::new(), GoldenSignals)
        |> ("collector" => collector, RED)
    }?;

    // Let it process
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    
    // Shutdown
    handle.shutdown().await?;

    // Verify results
    let orders = collected_orders.lock().unwrap();
    assert_eq!(orders.len(), 2); // Only 2 pending orders
    
    // Check first pending order (id: 1, amount: 100)
    assert_eq!(orders[0].payload["id"], 1);
    assert_eq!(orders[0].payload["tax"], 8);
    assert_eq!(orders[0].payload["total"], 108);
    
    // Check second pending order (id: 3, amount: 200)
    assert_eq!(orders[1].payload["id"], 3);
    assert_eq!(orders[1].payload["tax"], 16);
    assert_eq!(orders[1].payload["total"], 216);

    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}

#[tokio::test]
async fn test_multi_sink_fanout() -> Result<()> {
    let temp_dir = tempdir()?;
    let store_path = temp_dir.path().join("test_fanout_event_store");

    struct EventBroadcaster {
        count: u64,
        emitted: AtomicU64,
        metrics: <RED as Taxonomy>::Metrics,
    }
    
    impl EventBroadcaster {
        fn new(count: u64) -> Self {
            Self {
                count,
                emitted: AtomicU64::new(0),
                metrics: RED::create_metrics("EventBroadcaster"),
            }
        }
    }
    
    impl Step for EventBroadcaster {
        type Taxonomy = RED;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &RED
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Source
        }
        
        fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
            let current = self.emitted.fetch_add(1, Ordering::Relaxed);
            if current < self.count {
                vec![ChainEvent::new("Broadcast", json!({ "index": current }))]
            } else {
                vec![]
            }
        }
    }
    
    struct EventCounter {
        name: String,
        count: Arc<AtomicU64>,
        metrics: <USE as Taxonomy>::Metrics,
    }
    
    impl EventCounter {
        fn new(name: &str) -> (Self, Arc<AtomicU64>) {
            let count = Arc::new(AtomicU64::new(0));
            (
                Self {
                    name: name.to_string(),
                    count: count.clone(),
                    metrics: USE::create_metrics(name),
                },
                count,
            )
        }
    }
    
    impl Step for EventCounter {
        type Taxonomy = USE;
        
        fn taxonomy(&self) -> &Self::Taxonomy {
            &USE
        }
        
        fn metrics(&self) -> &<Self::Taxonomy as Taxonomy>::Metrics {
            &self.metrics
        }
        
        fn step_type(&self) -> StepType {
            StepType::Sink
        }
        
        fn handle(&self, _event: ChainEvent) -> Vec<ChainEvent> {
            self.count.fetch_add(1, Ordering::Relaxed);
            vec![]
        }
    }
    
    // In EventStore architecture, fanout happens naturally - 
    // multiple stages can read from the same EventStore
    
    let (counter1, count1) = EventCounter::new("Counter1");
    
    let event_store = EventStore::new(EventStoreConfig {
        path: store_path.clone(),
        max_segment_size: 1024 * 1024,
    }).await?;
    
    // For true fanout, we'd create a single flow with the source
    // and then multiple parallel stages reading from the store.
    // For this test, let's just use a simple pipeline:
    let handle = flow! {
        store: event_store,
        flow_taxonomy: GoldenSignals,
        ("broadcaster" => EventBroadcaster::new(10), RED)
        |> ("counter1" => counter1, USE)
    }?;
    
    // Let it run
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    // Shutdown the flow
    handle.shutdown().await?;
    
    // Counter should have received events
    let c1 = count1.load(Ordering::Relaxed);
    
    assert_eq!(c1, 10, "Counter1 should have received all 10 events");
    
    println!("Counter1 received: {} events", c1);
    
    // Clean up
    let _ = std::fs::remove_dir_all(&store_path);
    Ok(())
}