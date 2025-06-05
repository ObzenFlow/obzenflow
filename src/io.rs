// src/io.rs
use crate::step::{ChainEvent, PipelineStep, Result, StepMetrics};
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use std::path::PathBuf;

/// Source that reads events from a newline-delimited JSON log file
pub struct LogSource {
    path: PathBuf,
    event_type_filter: Option<String>,
}

impl LogSource {
    pub fn new(path: PathBuf, event_type: &str) -> Self {
        Self {
            path,
            event_type_filter: Some(event_type.to_string()),
        }
    }

    pub fn new_unfiltered(path: PathBuf) -> Self {
        Self {
            path,
            event_type_filter: None,
        }
    }

    pub async fn load_events_async(&self) -> Result<Vec<ChainEvent>> {
        let mut events = Vec::new();

        if !self.path.exists() {
            return Ok(events);
        }

        let file = File::open(&self.path).await?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<ChainEvent>(&line) {
                Ok(event) => {
                    if let Some(filter) = &self.event_type_filter {
                        if event.event_type == *filter {
                            events.push(event);
                        }
                    } else {
                        events.push(event);
                    }
                }
                Err(e) => {
                    eprintln!("Failed to parse event: {}", e);
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl PipelineStep for LogSource {
    async fn process_stream(
        &mut self,
        _input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let events = self.load_events_async().await?;

        for event in events {
            if output.send(event).await.is_err() {
                break;
            }
            metrics.increment_processed(1);
            metrics.increment_emitted(1);
        }

        Ok(())
    }
}

/// Sink that appends events to a newline-delimited JSON log file
pub struct LogSink {
    path: PathBuf,
}

impl LogSink {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub async fn append(&self, event: &ChainEvent) -> Result<()> {
        self.append_async(event).await
    }

    async fn append_async(&self, event: &ChainEvent) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        let json = serde_json::to_string(event)?;
        file.write_all(json.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;

        Ok(())
    }
}

#[async_trait]
impl PipelineStep for LogSink {
    async fn process_stream(
        &mut self,
        mut input: Receiver<ChainEvent>,
        _output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;

        while let Some(event) = input.recv().await {
            let json = serde_json::to_string(&event)?;
            file.write_all(json.as_bytes()).await?;
            file.write_all(b"\n").await?;

            metrics.increment_processed(1);
        }

        file.flush().await?;
        Ok(())
    }
}
