//! Simple middleware to track initialization sequence

use flowstate_rs::prelude::*;
use flowstate_rs::middleware::Middleware;
use flowstate_rs::lifecycle::EventHandler;
use std::sync::Arc;
use std::time::Instant;

/// Middleware that logs initialization and event processing milestones
pub struct InitTrackerMiddleware {
    name: String,
    start_time: Instant,
}

impl InitTrackerMiddleware {
    pub fn new(name: impl Into<String>) -> Self {
        let name = name.into();
        println!("🏗️  [{}] InitTracker middleware created at {:?}", name, Instant::now());
        Self {
            name,
            start_time: Instant::now(),
        }
    }
    
    fn log(&self, event: &str) {
        let elapsed = self.start_time.elapsed();
        println!("📍 [{:>20}] {:>30} at {:>6.2}s", self.name, event, elapsed.as_secs_f64());
    }
}

impl<H: EventHandler> Middleware<H> for InitTrackerMiddleware {
    fn handle(&self, event: ChainEvent, next: &H) -> Vec<ChainEvent> {
        self.log(&format!("Processing {}", event.event_type));
        let results = next.transform(event);
        self.log(&format!("Produced {} events", results.len()));
        results
    }
    
    fn observe(&self, event: &ChainEvent, next: &H) -> Result<()> {
        self.log(&format!("Observing {}", event.event_type));
        next.observe(event)
    }
}