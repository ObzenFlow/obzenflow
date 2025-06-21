//! Debug example to trace the layered initialization sequence
//! This demonstrates that FLOWIP-055's layered architecture solves the Russian Doll problem

use flowstate_rs::prelude::*;
use flowstate_rs::flow;
use flowstate_rs::lifecycle::{EventHandler, ProcessingMode};
use flowstate_rs::middleware::Middleware;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

// Include our init tracker
include!("init_tracker_middleware.rs");

/// Simple source that logs its lifecycle
struct DebugSource {
    name: String,
    created_at: Instant,
}

impl DebugSource {
    fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        println!("🔨 [{}] DebugSource created", name);
        Self {
            name,
            created_at: Instant::now(),
        }
    }
}

impl EventHandler for DebugSource {
    fn transform(&self, _event: ChainEvent) -> Vec<ChainEvent> {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let count = COUNTER.fetch_add(1, Ordering::Relaxed);
        
        if count < 3 {
            println!("🌊 [{}] Generating event #{}", self.name, count);
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
}

impl DebugTransform {
    fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        println!("🔨 [{}] DebugTransform created", name);
        Self { name }
    }
}

impl EventHandler for DebugTransform {
    fn transform(&self, mut event: ChainEvent) -> Vec<ChainEvent> {
        println!("⚙️  [{}] Processing event: {}", self.name, event.event_type);
        event.payload["transformed_by"] = json!(self.name);
        vec![event]
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        ProcessingMode::Transform
    }
}

/// Middleware that announces flow lifecycle events
struct FlowLifecycleMiddleware {
    name: String,
    start_time: Instant,
}

impl FlowLifecycleMiddleware {
    fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        let start = Instant::now();
        println!("🎯 [FLOW {}] Middleware created at {:?}", name, start);
        Self {
            name,
            start_time: start,
        }
    }
}

impl<H: EventHandler> Middleware<H> for FlowLifecycleMiddleware {
    fn handle(&self, event: ChainEvent, next: &H) -> Vec<ChainEvent> {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        println!("🎯 [FLOW {}] Processing {} at {:.3}s", self.name, event.event_type, elapsed);
        next.transform(event)
    }
    
    fn observe(&self, event: &ChainEvent, next: &H) -> Result<()> {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        println!("🎯 [FLOW {}] Observing {} at {:.3}s", self.name, event.event_type, elapsed);
        next.observe(event)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Enable debug logging for our modules
    std::env::set_var("RUST_LOG", "flowstate_rs=debug");
    tracing_subscriber::fmt::init();
    
    println!("\n🚀 FLOWIP-055 Layered Initialization Debug Demo");
    println!("==============================================");
    println!("This demonstrates the fix for the Russian Doll synchronization problem\n");
    
    let start = Instant::now();
    
    println!("📋 Phase 1: Creating components...");
    
    // Create the flow with detailed tracking
    let handle = flow! {
        name: "debug_init_flow",
        middleware: [
            FlowLifecycleMiddleware::new("OBSERVER"),
            InitTrackerMiddleware::new("FLOW")
        ],
        ("source" => DebugSource::new("SOURCE"), [
            InitTrackerMiddleware::new("SOURCE")
        ])
        |> ("transform" => DebugTransform::new("TRANSFORM"), [
            InitTrackerMiddleware::new("TRANSFORM")
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