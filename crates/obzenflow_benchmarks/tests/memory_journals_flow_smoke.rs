// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::commands::perform;
use obzenflow_runtime::effects::EffectError;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::systems::Port;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SmokeEvent {
    n: u64,
}

impl TypedPayload for SmokeEvent {
    const EVENT_TYPE: &'static str = "bench.smoke_event";
}
use obzenflow_adapters::sources::ValuesSource;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Serialize)]
struct Received;

#[obzenflow_runtime::effects::system("smoke-counter")]
struct CountingSink {
    received: Arc<AtomicU64>,
}

#[obzenflow_runtime::effects::port]
impl CountingSink {
    #[effect(type = "bench.smoke.receive", at_least_once, records = reply)]
    fn receive(&self, _: &Received) -> Result<(), EffectError> {
        self.received.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_journals_flow_runs_to_completion() {
    let total_events = 10;
    let received = Arc::new(AtomicU64::new(0));
    let received_for_flow = Arc::clone(&received);

    let handle = tokio::time::timeout(Duration::from_secs(10), async {
        FlowDefinition::materialize(move |_runtime_config| {
            let source = ValuesSource::new(
                (0..total_events)
                    .map(|n| SmokeEvent { n })
                    .collect::<Vec<_>>(),
            );
            let sink = perform(|_: &SmokeEvent| Received).through(Port::to(CountingSink {
                received: received_for_flow,
            }));

            Ok(flow! {
                journals: memory_journals(),

                stages: {
                    src = source!(SmokeEvent => source);
                    snk = sink!(SmokeEvent => sink);
                },

                topology: {
                    src |> snk;
                }
            })
        })
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
        .await
    })
    .await
    .expect("flow creation did not complete within timeout")
    .expect("failed to create flow");

    let mut state_rx = handle.state_receiver();

    handle.start().await.expect("failed to start flow");

    let wait_for_sink = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if received.load(Ordering::Relaxed) >= total_events {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    if wait_for_sink.is_err() {
        eprintln!(
            "sink did not receive all events in time (received={}, expected={})",
            received.load(Ordering::Relaxed),
            total_events
        );
        eprintln!(
            "pipeline state at sink timeout: {:?}",
            handle.current_state()
        );
    }

    let observed_terminal_state = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match &*state_rx.borrow() {
                PipelineState::Drained
                | PipelineState::Failed { .. }
                | PipelineState::FailingChildren { .. } => break,
                _ => {}
            }
            if state_rx.changed().await.is_err() {
                break;
            }
        }
    })
    .await;

    if observed_terminal_state.is_err() && handle.is_running() {
        handle.stop_cancel().await.expect("stop_cancel failed");
    }

    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("flow did not complete within timeout")
        .expect("flow run failed");

    assert_eq!(
        received.load(Ordering::Relaxed),
        total_events,
        "sink did not receive expected event count"
    );
}
