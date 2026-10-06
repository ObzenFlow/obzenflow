// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115b: end-to-end proof that a circuit breaker on a journal sink guards
//! delivery through the new sink-delivery boundary.
//!
//! A sink whose delivery always fails drives the breaker open. Once open, the
//! breaker rejects further deliveries at the boundary, so the sink handler is
//! not invoked for them: the handler call count stays strictly below the number
//! of events. This proves the breaker reaches the sink through the carrier and
//! the `SinkDeliveryBoundary` short-circuits delivery, rather than every event
//! reaching the handler as it would without a working sink boundary.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    circuit_breaker, sink_delivery_observer, CircuitBreaker, SinkDeliveryObserver,
};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::observer::{
    ObserverResult, SinkDeliveryObserverContext, SinkDeliveryObserverOutcome,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

fn breaker(failures: usize) -> CircuitBreaker {
    circuit_breaker().consecutive_failures(failures.try_into().expect("test threshold fits u32"))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SinkBreakerEvent {
    sequence: u64,
}

impl TypedPayload for SinkBreakerEvent {
    const EVENT_TYPE: &'static str = "circuit_breaker_sink.event";
}

/// Finite source emitting `count` events with no inter-event delay, so the whole
/// run completes well within the breaker cooldown (no half-open probe noise).
#[derive(Clone, Debug)]
struct BurstSource {
    count: u64,
    index: u64,
}

impl BurstSource {
    fn new(count: u64) -> Self {
        Self { count, index: 0 }
    }
}

impl TypedFiniteSourceHandler for BurstSource {
    type Output = SinkBreakerEvent;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.index >= self.count {
            return Ok(None);
        }
        let event = SinkBreakerEvent {
            sequence: self.index,
        };
        self.index += 1;
        Ok(Some(vec![event]))
    }
}

/// Sink whose delivery always fails, counting how many times the handler is
/// actually invoked. Deliveries the breaker rejects never reach this counter.
#[derive(Clone, Debug)]
struct AlwaysFailingSink {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for AlwaysFailingSink {
    type Input = SinkBreakerEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                "test.failure".to_string(),
            ),
        )
    }

    async fn write(&mut self, _event: SinkBreakerEvent) -> Result<(), SinkWriteFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(SinkWriteFailure::current_only(
            obzenflow_runtime::stages::sink::SinkWritePhase::Execute,
            obzenflow_runtime::stages::sink::SinkOperationError::remote("sink delivery failed"),
        ))
    }
}

struct RecordsDeliveryClassifications {
    outcomes: Arc<Mutex<Vec<SinkDeliveryObserverOutcome>>>,
    deliveries: Arc<AtomicUsize>,
}

impl SinkDeliveryObserver for RecordsDeliveryClassifications {
    type Input = SinkBreakerEvent;

    fn on_delivered(&self, _input: &Self::Input) -> ObserverResult {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_attempt(&self, ctx: &SinkDeliveryObserverContext<'_>) -> ObserverResult {
        self.outcomes
            .lock()
            .expect("sink observer outcome lock")
            .push(ctx.outcome().clone());
        Ok(())
    }
}

#[tokio::test]
async fn sink_observer_input_mismatch_fails_flow_construction() {
    #[derive(Serialize, Deserialize)]
    struct OtherSinkEvent {
        sequence: u64,
    }
    impl TypedPayload for OtherSinkEvent {
        // Sharing the wire name must not erase the distinct Rust input witness.
        const EVENT_TYPE: &'static str = SinkBreakerEvent::EVENT_TYPE;
    }
    struct WrongInputObserver;
    impl SinkDeliveryObserver for WrongInputObserver {
        type Input = OtherSinkEvent;
    }

    let temp = tempfile::tempdir().expect("isolated flow directory");
    let root = temp.path().to_path_buf();
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_sink = calls.clone();
    let result = FlowDefinition::materialize(move |_runtime_config| {
        let source = BurstSource::new(1);
        let sink_handler = AlwaysFailingSink {
            calls: calls_for_sink,
        };
        Ok(flow! {
            name: "mismatched_sink_observer",
            journals: disk_journals(root),
            stages: {
                source = source!(SinkBreakerEvent => source);
                destination = sink!(SinkBreakerEvent => sink_handler with { sink_delivery_observer("wrong-input", WrongInputObserver) });
            },
            topology: { source |> destination; }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;
    let error = match result {
        Ok(_) => panic!("mismatched observer must fail flow construction"),
        Err(error) => error.to_string(),
    };
    assert!(error.contains("wrong-input"), "{error}");
    assert!(error.contains("OtherSinkEvent"), "{error}");
    assert!(error.contains("SinkBreakerEvent"), "{error}");
    assert!(error.contains("destination"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn circuit_breaker_on_sink_opens_and_rejects_delivery() -> Result<()> {
    let metrics_model =
        std::sync::Arc::new(obzenflow_adapters::monitoring::MetricsReadModel::default());
    let metrics_context = obzenflow_runtime::run_context::FlowBuildContext::for_tests()
        .with_metrics_exporter(metrics_model.clone());
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_test_writer()
        .try_init();

    const TOTAL_EVENTS: u64 = 12;
    const THRESHOLD: usize = 3;

    let calls = Arc::new(AtomicUsize::new(0));
    let calls_for_flow = Arc::clone(&calls);
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let outcomes_for_flow = Arc::clone(&outcomes);
    let deliveries = Arc::new(AtomicUsize::new(0));
    let deliveries_for_flow = Arc::clone(&deliveries);

    let flow_handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = BurstSource::new(TOTAL_EVENTS);
        let sink_handler = AlwaysFailingSink {
            calls: calls_for_flow,
        };

        Ok(flow! {
            name: "circuit_breaker_sink_delivery_test",
            journals: disk_journals(std::path::PathBuf::from("target/cb_sink_delivery")),

            stages: {
                cb_source = source!(SinkBreakerEvent => source);
                cb_sink = sink!(SinkBreakerEvent => sink_handler with { breaker(THRESHOLD), sink_delivery_observer(
                    "delivery-classifications",
                    RecordsDeliveryClassifications {
                        outcomes: outcomes_for_flow,
                        deliveries: deliveries_for_flow,
                    }
                ) });
            },

            topology: {
                cb_source |> cb_sink;
            }
        })
    })
    .build(metrics_context)
    .await
    .map_err(|e| anyhow::anyhow!("Flow creation failed: {e:?}"))?;

    // The strict source-delivery contract may abort once the breaker rejects
    // downstream traffic; that is an expected terminal state for this flow.
    let run_result = flow_handle.run().await;
    if let Err(e) = run_result {
        let error = format!("{e:?}");
        assert!(
            error.contains("SeqDivergence")
                || error.contains("Pipeline abort")
                || error.contains("abort"),
            "unexpected sink breaker flow failure: {error}"
        );
    }

    let invoked = calls.load(Ordering::SeqCst);
    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        0,
        "failed writes and policy rejections must not produce successful delivery notifications"
    );

    // The breaker needs at least `THRESHOLD` real delivery failures to open.
    assert!(
        invoked >= THRESHOLD,
        "expected at least {THRESHOLD} sink invocations to open the breaker, got {invoked}"
    );
    // Once open, the breaker rejects further deliveries at the boundary without
    // invoking the sink handler, so it is invoked strictly fewer than the total.
    assert!(
        (invoked as u64) < TOTAL_EVENTS,
        "expected the breaker to reject some deliveries (invoked {invoked} < {TOTAL_EVENTS}); \
         the sink-delivery boundary did not short-circuit"
    );

    let outcomes = outcomes.lock().expect("sink outcome assertion lock");
    let attempted = outcomes
        .iter()
        .filter(|outcome| matches!(outcome, SinkDeliveryObserverOutcome::Attempted { .. }))
        .count();
    let rejected = outcomes
        .iter()
        .filter(|outcome| {
            matches!(
                outcome,
                SinkDeliveryObserverOutcome::Rejected { policy: Some(policy) }
                    if policy == "circuit_breaker"
            )
        })
        .count();
    assert_eq!(
        attempted, invoked,
        "every live sink attempt receives exactly one observer classification"
    );
    assert!(
        rejected > 0,
        "the observer must receive the circuit breaker's control rejection"
    );
    assert_eq!(
        outcomes.len(),
        attempted + rejected,
        "every observed sink delivery is exactly one attempt or rejection"
    );

    Ok(())
}
