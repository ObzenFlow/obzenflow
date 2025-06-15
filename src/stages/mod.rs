// src/stages/mod.rs
//! Stage hierarchy for flowstate_rs
//! 
//! This module provides both utility stages and a clean hierarchy of domain-specific stages:
//! 
//! ## Utility Stages
//! - `Filter`: Filter events based on predicates
//! - `RateLimiter`: Control event flow rate
//! - `Window`: Collect events into time-based windows
//! - `Dedupe`: Remove duplicate events
//! 
//! ## Domain-Specific Stage Hierarchy
//! - **Sources**: Generate events from external/stored data (fetchers, log sources)
//! - **Transforms**: Process events (chunkers, RAG, generators)  
//! - **Sinks**: Persist events to external systems (vector DBs, APIs, logs)
//! 
//! Each category provides traits that make it easy to add new implementations.

use crate::step::{ChainEvent};
use tokio::time::{Duration, Instant};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex};

/// Simple function-based stage
pub struct FunctionStage<F>
where
    F: Fn(ChainEvent) -> Vec<ChainEvent> + Send + Sync,
{
    f: F,
}

impl<F> FunctionStage<F>
where
    F: Fn(ChainEvent) -> Vec<ChainEvent> + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> Stage for FunctionStage<F>
where
    F: Fn(ChainEvent) -> Vec<ChainEvent> + Send + Sync,
{
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        (self.f)(event)
    }
}

/// Async function stage
#[allow(dead_code)]
pub struct AsyncStage<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<ChainEvent>> + Send,
{
    f: F,
}

impl<P> Stage for Filter<P>
where
    P: Fn(&ChainEvent) -> bool + Send + Sync,
{
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        if (self.predicate)(&event) {
            vec![event]
        } else {
            vec![]
        }
    }
}

/// Filter stage
pub struct Filter<P>
where
    P: Fn(&ChainEvent) -> bool + Send + Sync,
{
    predicate: P,
}

impl<P> Filter<P>
where
    P: Fn(&ChainEvent) -> bool + Send + Sync,
{
    pub fn new(predicate: P) -> Self {
        Self { predicate }
    }
}


/// Rate limiter stage
#[allow(dead_code)]
pub struct RateLimiter {
    events_per_second: f64,
    tokens: f64,
    last_update: Instant,
    max_tokens: f64,
}

impl RateLimiter {
    pub fn new(events_per_second: f64) -> Self {
        Self {
            events_per_second,
            tokens: events_per_second,
            last_update: Instant::now(),
            max_tokens: events_per_second * 2.0, // Allow burst
        }
    }
}


/// Windowing stage - collects events into time-based windows
#[allow(dead_code)]
pub struct Window {
    duration: Duration,
    max_size: usize,
}

impl Window {
    pub fn new(duration: Duration, max_size: usize) -> Self {
        Self { duration, max_size }
    }
}


/// Deduplication stage - removes duplicate events within a time window
#[allow(dead_code)]
pub struct Dedupe {
    window: Duration,
    seen: Arc<Mutex<VecDeque<(String, Instant)>>>, // Thread-safe for async
}

impl Dedupe {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            seen: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    #[allow(dead_code)]
    fn cleanup_old(&self, now: Instant) {
        let mut seen = self.seen.lock().unwrap();
        while let Some((_, timestamp)) = seen.front() {
            if now.duration_since(*timestamp) > self.window {
                seen.pop_front();
            } else {
                break;
            }
        }
    }
}


// Base traits
pub mod base;
pub mod monitored;

// Domain-specific stage hierarchy
pub mod sources;
pub mod transforms; 
pub mod sinks;

pub use base::{Source, Stage, Sink};
pub use monitored::{Monitor, MonitoredStep, MonitoredSource, MonitoredStage, MonitoredSink, MonitorSource, MonitorStage, MonitorSink};

// Re-export key traits and types for convenience
pub use sources::{Fetcher, FetchedItem, HttpSource, JsonParser, XmlParser, ResponseParser};
pub use transforms::{Transform, LLMTransform, LLMProvider, LLMConfig};
pub use sinks::{HttpSink, PayloadFormatter, JsonFormatter};
