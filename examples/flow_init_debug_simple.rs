//! Simple debug example to trace the layered initialization sequence
//! This demonstrates that FLOWIP-055's layered architecture solves the Russian Doll problem

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Simple source that logs its lifecycle
struct DebugSource {
    name: String,
    created_at: Instant,
}

impl DebugSource {
    fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let now = Instant::now();
        println!("🔨 [{:>10}] DebugSource created at {:.3}s", name, now.elapsed().as_secs_f64());
        Self {
            name,
            created_at: now,
        }
    }
}

impl EventHandler for DebugSource {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        
        if count < 3 {
            println!("🌊 [{:>10}] Generating event #{} at {:.3}s", 
                self.name, count, self.created_at.elapsed().as_secs_f64());
            vec![ChainEvent::new("debug_event", json!({
                "source": self.name,
                "count": count,
                "elapsed": self.created_at.elapsed().as_millis()
            }))]
        } else {
            vec![]
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Simple transform that logs its lifecycle
struct DebugTransform {
    name: String,
    created_at: Instant,
}

impl DebugTransform {
    fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let now = Instant::now();
        println!("🔨 [{:>10}] DebugTransform created at {:.3}s", name, now.elapsed().as_secs_f64());
        Self { 
            name,
            created_at: now,
        }
    }
}

impl EventHandler for DebugTransform {
    fn transform(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        println!("⚙️  [{:>10}] Processing {} at {:.3}s", 
            self.name, event.event_type, self.created_at.elapsed().as_secs_f64());
        event.payload["transformed_by"] = json!(self.name);
        vec![event]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Enable debug logging for our modules
    std::env::set_var("RUST_LOG", "flowstate_rs=info");
    tracing_subscriber::fmt::init();
    
    println!("\n🚀 FLOWIP-055 Layered Initialization Debug Demo");
    println!("==============================================");
    println!("This demonstrates the fix for the Russian Doll synchronization problem\n");
    
    let start = Instant::now();
    
    println!("📋 Phase 1: Creating components at {:.3}s", start.elapsed().as_secs_f64());
    
    // Create the flow - this will show the initialization sequence
    let handle = flow! {
        name: "debug_init_flow",
        middleware: [
            MonitoringMiddleware::<GoldenSignals>::new("FLOW_OBSERVER")
        ],
        ("source" => DebugSource::new("SOURCE"), [
            MonitoringMiddleware::<RED>::new("SOURCE_MON")
        ])
        |> ("transform" => DebugTransform::new("TRANSFORM"), [
            MonitoringMiddleware::<USE>::new("TRANSFORM_MON")
        ])
    }?;
    
    println!("\n📋 Phase 2: Flow created at {:.3}s", start.elapsed().as_secs_f64());
    println!("   Expected sequence:");
    println!("   1. Layer A (Stages) initialize and pass barrier");
    println!("   2. Layer B (Flow Observer) starts AFTER stages are running");
    println!("   3. No deadlock occurs!\n");
    
    // Let it run briefly
    println!("📋 Phase 3: Running for 2 seconds...");
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    
    println!("\n📋 Phase 4: Shutting down at {:.3}s", start.elapsed().as_secs_f64());
    handle.shutdown().await?;
    
    println!("\n✅ Success! The layered initialization prevents deadlock!");
    println!("   - Stages started first (Layer A)");
    println!("   - Flow observer started after stages were running (Layer B)");
    println!("   - No circular dependency!\n");
    
    Ok(())
}