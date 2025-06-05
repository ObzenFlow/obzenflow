// src/runtime.rs
use crate::step::{ChainEvent, PipelineStep, StepMetrics, MetricsSnapshot, Result};
use tokio::sync::mpsc::{channel, Receiver};
use tokio::task::JoinHandle;
use std::time::{Duration, Instant};

pub struct Pipeline {
    handles: Vec<JoinHandle<Result<()>>>,
    start_time: Instant,
}

impl Pipeline {
    /// Wait for pipeline completion
    pub async fn wait(self) -> Result<PipelineReport> {
        let mut errors = Vec::new();

        for handle in self.handles {
            if let Err(e) = handle.await? {
                errors.push(e);
            }
        }

        let duration = self.start_time.elapsed();

        if errors.is_empty() {
            Ok(PipelineReport {
                duration,
                stages: vec![], // TODO: collect actual metrics
            })
        } else {
            Err(format!("Pipeline failed with {} errors", errors.len()).into())
        }
    }
}

pub struct PipelineBuilder {
    stages: Vec<(String, Box<dyn PipelineStep>)>,
    buffer_size: usize,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            buffer_size: 1000,
        }
    }

    pub fn buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    pub fn stage<S: PipelineStep + 'static>(mut self, name: &str, stage: S) -> Self {
        self.stages.push((name.to_string(), Box::new(stage)));
        self
    }

    pub async fn build(self) -> Result<Pipeline> {
        let start_time = Instant::now();
        let mut handles = Vec::new();
        let mut prev_rx: Option<Receiver<ChainEvent>> = None;

        // Create a chain of stages connected by channels
        for (_i, (name, mut step)) in self.stages.into_iter().enumerate() {
            let (tx, rx) = channel(self.buffer_size);

            // Connect previous stage output to this stage input
            let input = if let Some(prev) = prev_rx {
                prev
            } else {
                // First stage needs a dummy receiver
                let (_dummy_tx, dummy_rx) = channel(1);
                dummy_rx
            };

            let metrics = StepMetrics::new(&name);

            // Spawn the stage task
            let handle = tokio::spawn(async move {
                step.initialize().await?;
                let result = step.process_stream(input, tx, metrics).await;
                step.shutdown().await?;
                result
            });

            handles.push(handle);
            prev_rx = Some(rx);
        }

        Ok(Pipeline {
            handles,
            start_time,
        })
    }
}

#[derive(Debug)]
pub struct PipelineReport {
    pub duration: Duration,
    pub stages: Vec<MetricsSnapshot>,
}

impl PipelineReport {
    pub fn total_throughput(&self) -> f64 {
        self.stages
            .first()
            .map(|s| s.throughput(self.duration))
            .unwrap_or(0.0)
    }
}
