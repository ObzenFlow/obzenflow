// src/stages/mod.rs
use crate::step::{PipelineStep, ChainEvent, Result, StepMetrics};
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{interval, Duration, Instant};
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

#[async_trait]
impl<F> PipelineStep for FunctionStage<F>
where
    F: Fn(ChainEvent) -> Vec<ChainEvent> + Send + Sync,
{
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        (self.f)(event)
    }
}

/// Async function stage
pub struct AsyncStage<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<ChainEvent>> + Send,
{
    f: F,
}

#[async_trait]
impl<F, Fut> PipelineStep for AsyncStage<F, Fut>
where
    F: Fn(ChainEvent) -> Fut + Send + Sync,
    Fut: Future<Output = Vec<ChainEvent>> + Send,
{
    async fn process_stream(
        &mut self,
        input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let mut input = input;

        while let Some(event) = input.recv().await {
            let start = Instant::now();
            let results = (self.f)(event).await;
            let duration = start.elapsed();

            metrics.record_processing_time(duration);
            metrics.increment_processed(1);

            for result in results {
                if output.send(result).await.is_err() {
                    metrics.increment_errors(1);
                    return Err("Output channel closed".into());
                }
                metrics.increment_emitted(1);
            }
        }

        Ok(())
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

#[async_trait]
impl<P> PipelineStep for Filter<P>
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

/// Rate limiter stage
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

#[async_trait]
impl PipelineStep for RateLimiter {
    async fn process_stream(
        &mut self,
        input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let mut input = input;
        let mut ticker = interval(Duration::from_millis(10)); // 100Hz check rate

        while let Some(event) = input.recv().await {
            // Token bucket algorithm
            loop {
                let now = Instant::now();
                let elapsed = now.duration_since(self.last_update);
                self.last_update = now;

                // Add tokens based on time elapsed
                self.tokens += elapsed.as_secs_f64() * self.events_per_second;
                self.tokens = self.tokens.min(self.max_tokens);

                if self.tokens >= 1.0 {
                    self.tokens -= 1.0;
                    break;
                }

                ticker.tick().await;
            }

            if output.send(event).await.is_err() {
                return Err("Output channel closed".into());
            }

            metrics.increment_processed(1);
            metrics.increment_emitted(1);
        }

        Ok(())
    }
}

/// Windowing stage - collects events into time-based windows
pub struct Window {
    duration: Duration,
    max_size: usize,
}

impl Window {
    pub fn new(duration: Duration, max_size: usize) -> Self {
        Self { duration, max_size }
    }
}

#[async_trait]
impl PipelineStep for Window {
    async fn process_stream(
        &mut self,
        input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let mut input = input;
        let mut window: Vec<ChainEvent> = Vec::new();
        let mut window_start = Instant::now();

        loop {
            let timeout = self.duration.saturating_sub(window_start.elapsed());

            match tokio::time::timeout(timeout, input.recv()).await {
                Ok(Some(event)) => {
                    window.push(event);
                    metrics.increment_processed(1);

                    if window.len() >= self.max_size {
                        // Emit window early if it's full
                        let window_event = ChainEvent::new(
                            "Window",
                            serde_json::json!({
                                "events": window,
                                "duration_ms": window_start.elapsed().as_millis(),
                            }),
                        );

                        if output.send(window_event).await.is_err() {
                            return Err("Output channel closed".into());
                        }

                        metrics.increment_emitted(1);
                        window = Vec::new();
                        window_start = Instant::now();
                    }
                }
                Ok(None) => {
                    // Input closed, emit final window if non-empty
                    if !window.is_empty() {
                        let window_event = ChainEvent::new(
                            "Window",
                            serde_json::json!({
                                "events": window,
                                "duration_ms": window_start.elapsed().as_millis(),
                            }),
                        );
                        output.send(window_event).await.ok();
                        metrics.increment_emitted(1);
                    }
                    break;
                }
                Err(_) => {
                    // Timeout - emit current window
                    if !window.is_empty() {
                        let window_event = ChainEvent::new(
                            "Window",
                            serde_json::json!({
                                "events": window,
                                "duration_ms": self.duration.as_millis(),
                            }),
                        );

                        if output.send(window_event).await.is_err() {
                            return Err("Output channel closed".into());
                        }

                        metrics.increment_emitted(1);
                    }

                    window = Vec::new();
                    window_start = Instant::now();
                }
            }
        }

        Ok(())
    }
}

/// Deduplication stage - removes duplicate events within a time window
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

#[async_trait]
impl PipelineStep for Dedupe {
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        let now = Instant::now();
        self.cleanup_old(now);

        let event_id = event.ulid.clone();
        let mut seen = self.seen.lock().unwrap();

        // Check if we've seen this event recently
        if seen.iter().any(|(id, _)| id == &event_id) {
            vec![] // Drop duplicate
        } else {
            seen.push_back((event_id, now));
            vec![event]
        }
    }
}
