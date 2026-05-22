// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for continuous contract evaluation and divergence detection (FLOWIP-080r).
//!
//! The key property being exercised is that `check_progress` is evaluated mid-flight
//! under sustained load (not only when subscriptions go idle), and that divergence
//! violations abort the pipeline via the standard gating edge contract pathway.
//!
//! FLOWIP-114n migrated this test away from wall-clock sleeps and disk-backed
//! `system.log` reads. The assertions are now based on the system journal
//! (`JournalSnapshot<SystemEvent>`) captured from in-memory journals under paused
//! Tokio time.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::system_event::{ContractResultStatusLabel, SystemEvent};
use obzenflow_core::event::types::ViolationCause as EventViolationCause;
use obzenflow_core::event::SystemEventType;
use obzenflow_core::TypedPayload;
use obzenflow_core::{CycleDepth, DivergenceContract, StageId, TransportContract, WriterId};
use obzenflow_dsl::{async_transform, sink, source, test_flow, transform};
use obzenflow_infra::journal::memory_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::source::traits::SourceError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::testing::{EventShape, JournalOrder, JournalSnapshot, TestClock};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// File-local payload for the mid-flight-divergence abort test. The JSON
/// shape matches what `SeedSource` emits; the type fingerprints the
/// stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SeedEvent {
    n: u64,
}

impl TypedPayload for SeedEvent {
    const EVENT_TYPE: &'static str = "divergence.seed";
}
use std::time::Duration;
use tokio::time::sleep;

#[derive(Clone, Debug)]
struct SeedSource {
    remaining: usize,
    writer_id: WriterId,
}

impl SeedSource {
    fn new(count: usize) -> Self {
        Self {
            remaining: count,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for SeedSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn next(&mut self) -> std::result::Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }

        self.remaining -= 1;
        Ok(Some(vec![ChainEventFactory::data_event(
            self.writer_id,
            "divergence.seed",
            json!({ "n": self.remaining }),
        )]))
    }
}

#[derive(Clone, Debug)]
struct SlowEntryTransform {
    delay: Duration,
}

impl SlowEntryTransform {
    fn new(delay: Duration) -> Self {
        Self { delay }
    }
}

#[async_trait]
impl AsyncTransformHandler for SlowEntryTransform {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Ensure the pipeline stays busy long enough for the supervisor tick to
        // schedule contract checks from the active processing path.
        sleep(self.delay).await;

        // Prevent a data feedback loop: we only need a cycle topology for SCC
        // wiring, and a signal storm on the SCC-internal edge.
        if event.is_data() && event.flow_context.stage_name == "iter" {
            return Ok(Vec::new());
        }

        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct SignalStormTransform {
    signals_per_event: usize,
    signal_writer_id: WriterId,
}

impl SignalStormTransform {
    fn new(signals_per_event: usize) -> Self {
        Self {
            signals_per_event,
            signal_writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl TransformHandler for SignalStormTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let mut out = Vec::with_capacity(self.signals_per_event.saturating_add(1));
        out.push(event);
        for i in 0..self.signals_per_event {
            out.push(ChainEventFactory::watermark_event(
                self.signal_writer_id,
                i as u64,
                None,
            ));
        }
        Ok(out)
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct PassThroughTransform;

#[async_trait]
impl TransformHandler for PassThroughTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FanInEntryTransform;

#[async_trait]
impl TransformHandler for FanInEntryTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        // Prevent a data feedback loop: SCC wiring is structural, but this test
        // wants a finite run that exercises SCC-internal fan-in without cycling
        // data indefinitely.
        if event.is_data() && event.flow_context.stage_name == "merge" {
            return Ok(Vec::new());
        }
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CycleDepthInjectionEntryTransform {
    delay: Duration,
    bump_to: u16,
}

impl CycleDepthInjectionEntryTransform {
    fn new(delay: Duration, bump_to: u16) -> Self {
        Self { delay, bump_to }
    }
}

#[async_trait]
impl AsyncTransformHandler for CycleDepthInjectionEntryTransform {
    async fn process(
        &self,
        mut event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        sleep(self.delay).await;

        // The CycleGuard stamps cycle SCC metadata at the SCC entry point.
        // Bump depth beyond the configured threshold to force an end-to-end
        // DivergenceContract cycle-depth violation on SCC-internal edges.
        if event.is_data() && event.cycle_scc_id.is_some() {
            event.cycle_depth = Some(CycleDepth::new(self.bump_to));
        }

        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DropAllDataTransform;

#[async_trait]
impl TransformHandler for DropAllDataTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        if event.is_data() {
            Ok(Vec::new())
        } else {
            Ok(vec![event])
        }
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CountingSink;

#[async_trait]
impl SinkHandler for CountingSink {
    async fn consume(
        &mut self,
        _event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(
            "counting_sink",
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn divergence_aborts_on_mid_flight_violation() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let harness = test_flow! {
        name: "divergence_mid_flight_abort",
        journals: memory_journals(),
        middleware: [],

        stages: {
            src = source!(SeedEvent => SeedSource::new(30));
            entry = async_transform!(SeedEvent -> SeedEvent => SlowEntryTransform::new(Duration::from_millis(10)));
            iter = transform!(SeedEvent -> SeedEvent => SignalStormTransform::new(50));
            snk = sink!(SeedEvent => CountingSink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let system_journal = harness.system_journal().expect("system journal");
    let handle = harness.into_inner();
    let run = tokio::spawn(handle.run());

    // Drive paused time until the flow terminates (expected abort).
    for _ in 0..200 {
        if run.is_finished() {
            break;
        }
        clock.advance(Duration::from_millis(50)).await?;
        tokio::task::yield_now().await;
    }
    assert!(
        run.is_finished(),
        "flow did not terminate under paused time (expected abort)"
    );

    let run = run.await.expect("join handle");
    match run {
        Ok(()) => anyhow::bail!("expected flow to abort due to divergence contract violation"),
        Err(_) => {}
    }

    // Ensure the system journal is stable before snapshot capture.
    let _stable_len = TestClock::settle_scheduler(|| async {
        let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;
        Ok::<usize, obzenflow_runtime::testing::JournalProbeError>(
            snapshot.events(JournalOrder::Append).len(),
        )
    })
    .await?;

    let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;

    let divergence_contract_result = EventShape::<SystemEvent>::system_event_predicate(
        "DivergenceContract failed ContractResult",
        |ev| match ev {
            SystemEventType::ContractResult {
                contract_name,
                status,
                cause,
                ..
            } => {
                contract_name == DivergenceContract::NAME
                    && status == ContractResultStatusLabel::Failed.as_str()
                    && cause.as_deref() == Some("divergence")
            }
            _ => false,
        },
    );
    assert!(
        snapshot
            .find(JournalOrder::Causal, &divergence_contract_result, 1)
            .is_some(),
        "expected a failed DivergenceContract ContractResult with cause=divergence"
    );

    let divergence_contract_status = EventShape::<SystemEvent>::system_event_predicate(
        "ContractStatus divergence predicate=signal_to_data_ratio",
        |ev| match ev {
            SystemEventType::ContractStatus {
                pass: false,
                reason: Some(EventViolationCause::Divergence { predicate, .. }),
                ..
            } => predicate == "signal_to_data_ratio",
            _ => false,
        },
    );

    let pipeline_failed =
        EventShape::<SystemEvent>::system_event_predicate("PipelineLifecycle::Failed", |ev| {
            matches!(
                ev,
                SystemEventType::PipelineLifecycle(
                    obzenflow_core::event::system_event::PipelineLifecycleEvent::Failed { .. }
                )
            )
        });

    // System events are appended without explicit parent chaining, so their vector clocks
    // do not necessarily encode strict happened-before across different system writers.
    // We assert the ordering in append order instead.
    let append = snapshot.events(JournalOrder::Append);
    let status_idx = append
        .iter()
        .position(|env| divergence_contract_status.matches(env))
        .expect("expected a failing ContractStatus with ViolationCause::Divergence");
    let failed_idx = append
        .iter()
        .position(|env| pipeline_failed.matches(env))
        .expect("expected pipeline to emit PipelineLifecycle::Failed");
    assert!(
        status_idx < failed_idx,
        "expected divergence evidence to be appended before PipelineLifecycle::Failed (status_idx={status_idx}, failed_idx={failed_idx})"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn divergence_emits_mid_flight_contract_health_heartbeats() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let harness = test_flow! {
        name: "divergence_mid_flight_contract_health",
        journals: memory_journals(),
        middleware: [],

        stages: {
            src = source!(SeedEvent => SeedSource::new(50));
            delay = async_transform!(SeedEvent -> SeedEvent => SlowEntryTransform::new(Duration::from_millis(1)));
            snk = sink!(SeedEvent => CountingSink);
        },

        topology: {
            src |> delay;
            delay |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let system_journal = harness.system_journal().expect("system journal");
    let handle = harness.into_inner();
    let run = tokio::spawn(handle.run());

    for _ in 0..200 {
        if run.is_finished() {
            break;
        }
        clock.advance(Duration::from_millis(20)).await?;
        tokio::task::yield_now().await;
    }
    assert!(
        run.is_finished(),
        "flow did not terminate under paused time"
    );

    let _ = run.await.expect("join handle")?;

    let _stable_len = TestClock::settle_scheduler(|| async {
        let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;
        Ok::<usize, obzenflow_runtime::testing::JournalProbeError>(
            snapshot.events(JournalOrder::Append).len(),
        )
    })
    .await?;

    let mut seen_transport_healthy_pre_eof = false;

    let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;

    for env in snapshot.events(JournalOrder::Append) {
        match &env.event.event {
            SystemEventType::ContractResult {
                contract_name,
                status,
                cause,
                advertised_writer_seq,
                ..
            } if contract_name == TransportContract::NAME => {
                if status == ContractResultStatusLabel::Healthy.as_str()
                    && cause.is_none()
                    && advertised_writer_seq.is_none()
                {
                    seen_transport_healthy_pre_eof = true;
                }
            }
            _ => {}
        }
    }

    assert!(
        seen_transport_healthy_pre_eof,
        "expected a TransportContract ContractResult with status=healthy before EOF"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn divergence_does_not_false_positive_on_fan_in_inside_cycle() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let harness = test_flow! {
        name: "divergence_fan_in_inside_cycle",
        journals: memory_journals(),
        middleware: [],

        stages: {
            src = source!(SeedEvent => SeedSource::new(50));
            entry = transform!(SeedEvent -> SeedEvent => FanInEntryTransform);
            a = transform!(SeedEvent -> SeedEvent => PassThroughTransform);
            b = transform!(SeedEvent -> SeedEvent => PassThroughTransform);
            merge = transform!(SeedEvent -> SeedEvent => PassThroughTransform);
            snk = sink!(SeedEvent => CountingSink);
        },

        topology: {
            src |> entry;
            entry |> a;
            entry |> b;
            a |> merge;
            b |> merge;
            entry <| merge;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let system_journal = harness.system_journal().expect("system journal");
    let handle = harness.into_inner();
    let run = tokio::spawn(handle.run());

    for _ in 0..200 {
        if run.is_finished() {
            break;
        }
        clock.advance(Duration::from_millis(20)).await?;
        tokio::task::yield_now().await;
    }
    assert!(
        run.is_finished(),
        "flow did not terminate under paused time"
    );
    run.await.expect("join handle")?;

    let _stable_len = TestClock::settle_scheduler(|| async {
        let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;
        Ok::<usize, obzenflow_runtime::testing::JournalProbeError>(
            snapshot.events(JournalOrder::Append).len(),
        )
    })
    .await?;

    let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;

    let mut seen_divergence_healthy = false;
    let mut seen_divergence_violation = false;

    for env in snapshot.events(JournalOrder::Append) {
        match &env.event.event {
            SystemEventType::ContractResult {
                contract_name,
                status,
                cause,
                ..
            } if contract_name == DivergenceContract::NAME => {
                if status == ContractResultStatusLabel::Healthy.as_str() && cause.is_none() {
                    seen_divergence_healthy = true;
                }
                if status == ContractResultStatusLabel::Failed.as_str()
                    && cause.as_deref() == Some("divergence")
                {
                    seen_divergence_violation = true;
                }
            }
            SystemEventType::ContractStatus {
                pass: false,
                reason: Some(EventViolationCause::Divergence { .. }),
                ..
            } => {
                seen_divergence_violation = true;
            }
            _ => {}
        }
    }

    assert!(
        seen_divergence_healthy,
        "expected at least one DivergenceContract ContractResult heartbeat with status=healthy"
    );
    assert!(
        !seen_divergence_violation,
        "did not expect any divergence violations in system.log for SCC-internal fan-in topology"
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn divergence_aborts_on_cycle_depth_violation() -> Result<()> {
    let clock = TestClock::new().await.expect("paused runtime");

    let harness = test_flow! {
        name: "divergence_cycle_depth_abort",
        journals: memory_journals(),
        middleware: [],

        stages: {
            src = source!(SeedEvent => SeedSource::new(10));
            entry = async_transform!(SeedEvent -> SeedEvent => CycleDepthInjectionEntryTransform::new(Duration::from_millis(5), 100));
            iter = transform!(SeedEvent -> SeedEvent => DropAllDataTransform);
            snk = sink!(SeedEvent => CountingSink);
        },

        topology: {
            src |> entry;
            entry |> iter;
            entry <| iter;
            entry |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    let system_journal = harness.system_journal().expect("system journal");
    let handle = harness.into_inner();
    let run = tokio::spawn(handle.run());

    use std::convert::Infallible;
    for _ in 0..400 {
        if run.is_finished() {
            break;
        }
        clock.advance(Duration::from_millis(50)).await?;
        // Give supervisors a chance to run timers and propagate contract evidence.
        let _ = TestClock::settle_scheduler(|| async { Ok::<bool, Infallible>(run.is_finished()) })
            .await?;
    }
    assert!(
        run.is_finished(),
        "flow did not terminate under paused time (expected abort)"
    );

    let run = run.await.expect("join handle");
    match run {
        Ok(()) => anyhow::bail!("expected flow to abort due to cycle_depth divergence violation"),
        Err(_) => {}
    }

    let _stable_len = TestClock::settle_scheduler(|| async {
        let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;
        Ok::<usize, obzenflow_runtime::testing::JournalProbeError>(
            snapshot.events(JournalOrder::Append).len(),
        )
    })
    .await?;

    let snapshot = JournalSnapshot::capture_system_journal(system_journal.clone()).await?;

    let mut seen_divergence_contract_result = false;
    let mut seen_cycle_depth_contract_status = false;

    for env in snapshot.events(JournalOrder::Append) {
        match &env.event.event {
            SystemEventType::ContractResult {
                contract_name,
                status,
                cause,
                ..
            } if contract_name == DivergenceContract::NAME => {
                if status == ContractResultStatusLabel::Failed.as_str()
                    && cause.as_deref() == Some("divergence")
                {
                    seen_divergence_contract_result = true;
                }
            }
            SystemEventType::ContractStatus {
                pass: false,
                reason: Some(EventViolationCause::Divergence { predicate, .. }),
                ..
            } if predicate == "cycle_depth" => {
                seen_cycle_depth_contract_status = true;
            }
            _ => {}
        }
    }

    assert!(
        seen_divergence_contract_result,
        "expected a failed DivergenceContract ContractResult with cause=divergence in system.log"
    );
    assert!(
        seen_cycle_depth_contract_status,
        "expected a failing ContractStatus with predicate=cycle_depth in system.log"
    );

    Ok(())
}
