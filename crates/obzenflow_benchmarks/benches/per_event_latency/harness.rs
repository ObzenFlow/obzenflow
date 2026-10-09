// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared latency workload: monotonic process-local stamps, exactly-once
//! delivery by source-assigned ID and fail-closed completion.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Bound on joining a flow cancelled after its deadline.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// Fixed inputs, warm-up exclusion and completion deadline for one case.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub inputs: u64,
    pub warm_up: u64,
    pub deadline: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Event {
    id: u64,
    emitted_ns: u64,
}

impl TypedPayload for Event {
    const EVENT_TYPE: &'static str = "bench.timestamped_event";
}

#[derive(Clone, Debug)]
pub struct Source {
    inputs: u64,
    next: Arc<AtomicU64>,
    origin: Instant,
}

impl TypedFiniteSourceHandler for Source {
    type Output = Event;

    fn next(&mut self) -> Result<Option<Vec<Event>>, SourceError> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        if id >= self.inputs {
            return Ok(None);
        }
        Ok(Some(vec![Event {
            id,
            emitted_ns: self.origin.elapsed().as_nanos() as u64,
        }]))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Passthrough;

impl TypedTransformHandler for Passthrough {
    type Input = Event;
    type Output = Event;

    fn process(&self, event: Event) -> Result<Event, HandlerError> {
        Ok(event)
    }
}

type Delivered = Arc<Mutex<Vec<(u64, Duration)>>>;

#[derive(Clone, Debug)]
pub struct Sink {
    origin: Instant,
    delivered: Delivered,
}

#[async_trait]
impl InlineSink for Sink {
    type Input = Event;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Noop)
    }

    async fn write(&mut self, event: Event) -> Result<(), SinkWriteFailure> {
        let latency = self
            .origin
            .elapsed()
            .checked_sub(Duration::from_nanos(event.emitted_ns))
            .expect("monotonic latency");
        self.delivered.lock().unwrap().push((event.id, latency));
        Ok(())
    }
}

/// One flow's source, sink and the deliveries they share.
pub struct Delivery {
    workload: Workload,
    origin: Instant,
    delivered: Delivered,
}

impl Delivery {
    pub fn new(workload: Workload) -> Self {
        Self {
            workload,
            origin: Instant::now(),
            delivered: Arc::new(Mutex::new(Vec::with_capacity(workload.inputs as usize))),
        }
    }

    pub fn source(&self) -> Source {
        Source {
            inputs: self.workload.inputs,
            next: Arc::new(AtomicU64::new(0)),
            origin: self.origin,
        }
    }

    pub fn sink(&self) -> Sink {
        Sink {
            origin: self.origin,
            delivered: self.delivered.clone(),
        }
    }

    /// Runs the flow to `Drained` and returns the median post-warm-up latency.
    /// Timeout, failure, missing, duplicate or empty work is an error, never a sample.
    pub async fn median_latency(&self, handle: FlowHandle) -> anyhow::Result<Duration> {
        let Workload {
            inputs,
            warm_up,
            deadline,
        } = self.workload;
        handle
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to start pipeline: {e:?}"))?;
        match tokio::time::timeout(deadline, handle.wait_for_completion()).await {
            Ok(result) => result.map_err(|e| anyhow::anyhow!("Pipeline failed: {e:?}"))?,
            Err(_) => {
                // Cancel and join first so no flow outlives its sample.
                handle
                    .stop_cancel()
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to cancel pipeline: {e:?}"))?;
                tokio::time::timeout(STOP_GRACE, handle.wait_for_completion())
                    .await
                    .map_err(|_| anyhow::anyhow!("Cancelled pipeline did not stop"))?
                    .map_err(|e| anyhow::anyhow!("Cancelled pipeline failed: {e:?}"))?;
                anyhow::bail!("Pipeline did not drain within {deadline:?}");
            }
        }
        let state = handle.current_state();
        anyhow::ensure!(
            matches!(state, PipelineState::Drained),
            "Pipeline ended in {state:?}, not Drained"
        );

        let mut delivered = std::mem::take(&mut *self.delivered.lock().unwrap());
        delivered.sort_unstable_by_key(|(id, _)| *id);
        anyhow::ensure!(
            delivered.iter().map(|(id, _)| *id).eq(0..inputs),
            "missing, duplicate or unexpected inputs: {} delivered of {inputs}",
            delivered.len()
        );
        let mut measured: Vec<_> = delivered
            .into_iter()
            .filter(|(id, _)| *id >= warm_up)
            .map(|(_, latency)| latency)
            .collect();
        anyhow::ensure!(!measured.is_empty(), "no post-warm-up inputs to measure");
        measured.sort_unstable();
        Ok(measured[measured.len() / 2])
    }
}
