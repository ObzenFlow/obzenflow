// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration test for continuous contract evaluation and divergence detection (FLOWIP-080r).
//!
//! The key property being exercised is that `check_progress` is evaluated mid-flight
//! under sustained load (not only when subscriptions go idle), and that divergence
//! violations propagate through the existing evidence pipeline (`system.log`) and
//! abort the pipeline via the standard gating edge contract pathway.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::system_event::{ContractResultStatusLabel, SystemEvent};
use obzenflow_core::event::types::ViolationCause as EventViolationCause;
use obzenflow_core::event::SystemEventType;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{
    CycleDepth, DivergenceContract, StageId, SystemId, TransportContract, WriterId,
};
use obzenflow_dsl_infra::{async_transform, flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError;
use obzenflow_runtime_services::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler, TransformHandler,
};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

fn unique_journal_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

fn single_flow_system_log(base: &Path) -> Result<PathBuf> {
    let flows_dir = base.join("flows");
    anyhow::ensure!(flows_dir.exists(), "expected flows dir at {flows_dir:?}");

    let mut system_logs: Vec<PathBuf> = fs::read_dir(&flows_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path().join("system.log"))
        .filter(|p| p.exists())
        .collect();

    anyhow::ensure!(
        system_logs.len() == 1,
        "expected exactly one system.log under {flows_dir:?}, got {}",
        system_logs.len()
    );

    Ok(system_logs
        .pop()
        .expect("system_logs is non-empty due to length check"))
}

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
            "flowip_080r.seed",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_080r_aborts_on_mid_flight_divergence_violation() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_080r_mid_flight_divergence_abort");
    let _ = fs::remove_dir_all(&journal_root);
    let journal_root_for_flow = journal_root.clone();

    let handle = flow! {
        name: "flowip_080r_mid_flight_divergence_abort",
        journals: disk_journals(journal_root_for_flow),
        middleware: [],

        stages: {
            src = source!("src" => SeedSource::new(30));
            entry = async_transform!("entry" => SlowEntryTransform::new(Duration::from_millis(10)));
            iter = transform!("iter" => SignalStormTransform::new(50));
            snk = sink!("snk" => CountingSink);
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

    let run = tokio::time::timeout(Duration::from_secs(10), handle.run()).await;
    match run {
        Ok(Ok(())) => anyhow::bail!("expected flow to abort due to divergence contract violation"),
        Ok(Err(_)) => {}
        Err(_) => anyhow::bail!("flow run timed out (expected abort via contract violation)"),
    }

    // Give system journal writers a brief moment to flush.
    sleep(Duration::from_millis(200)).await;

    let system_log = single_flow_system_log(&journal_root)?;
    let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            system_log,
            JournalOwner::system(SystemId::new()),
        )?;

    let envelopes = journal.read_causally_ordered().await?;

    let mut seen_divergence_contract_result = false;
    let mut seen_divergence_contract_status = false;

    for env in envelopes {
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
            } if predicate == "signal_to_data_ratio" => {
                seen_divergence_contract_status = true;
            }
            _ => {}
        }
    }

    assert!(
        seen_divergence_contract_result,
        "expected a failed DivergenceContract ContractResult with cause=divergence in system.log"
    );
    assert!(
        seen_divergence_contract_status,
        "expected a failing ContractStatus with ViolationCause::Divergence in system.log"
    );

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_080r_emits_mid_flight_contract_health_heartbeats() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_080r_mid_flight_contract_health");
    let _ = fs::remove_dir_all(&journal_root);
    let journal_root_for_flow = journal_root.clone();

    let handle = flow! {
        name: "flowip_080r_mid_flight_contract_health",
        journals: disk_journals(journal_root_for_flow),
        middleware: [],

        stages: {
            src = source!("src" => SeedSource::new(50));
            delay = async_transform!("delay" => SlowEntryTransform::new(Duration::from_millis(1)));
            snk = sink!("snk" => CountingSink);
        },

        topology: {
            src |> delay;
            delay |> snk;
        }
    }
    .await
    .map_err(|e| anyhow::anyhow!("failed to create flow: {e}"))?;

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed unexpectedly: {e}"))?;

    // Give system journal writers a brief moment to flush.
    sleep(Duration::from_millis(200)).await;

    let system_log = single_flow_system_log(&journal_root)?;
    let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            system_log,
            JournalOwner::system(SystemId::new()),
        )?;

    let envelopes = journal.read_causally_ordered().await?;

    let mut seen_transport_healthy_pre_eof = false;

    for env in envelopes {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_080r_does_not_false_positive_on_fan_in_inside_cycle() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_080r_fan_in_inside_cycle");
    let _ = fs::remove_dir_all(&journal_root);
    let journal_root_for_flow = journal_root.clone();

    let handle = flow! {
        name: "flowip_080r_fan_in_inside_cycle",
        journals: disk_journals(journal_root_for_flow),
        middleware: [],

        stages: {
            src = source!("src" => SeedSource::new(50));
            entry = transform!("entry" => FanInEntryTransform);
            a = transform!("a" => PassThroughTransform);
            b = transform!("b" => PassThroughTransform);
            merge = transform!("merge" => PassThroughTransform);
            snk = sink!("snk" => CountingSink);
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

    tokio::time::timeout(Duration::from_secs(10), handle.run())
        .await
        .map_err(|_| anyhow::anyhow!("flow run timed out"))?
        .map_err(|e| anyhow::anyhow!("flow run failed unexpectedly: {e}"))?;

    sleep(Duration::from_millis(200)).await;

    let system_log = single_flow_system_log(&journal_root)?;
    let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            system_log,
            JournalOwner::system(SystemId::new()),
        )?;

    let envelopes = journal.read_causally_ordered().await?;

    let mut seen_divergence_healthy = false;
    let mut seen_divergence_violation = false;

    for env in envelopes {
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flowip_080r_aborts_on_cycle_depth_divergence_violation() -> Result<()> {
    let journal_root = unique_journal_dir("flowip_080r_cycle_depth_divergence_abort");
    let _ = fs::remove_dir_all(&journal_root);
    let journal_root_for_flow = journal_root.clone();

    let handle = flow! {
        name: "flowip_080r_cycle_depth_divergence_abort",
        journals: disk_journals(journal_root_for_flow),
        middleware: [],

        stages: {
            src = source!("src" => SeedSource::new(10));
            entry = async_transform!("entry" => CycleDepthInjectionEntryTransform::new(Duration::from_millis(5), 100));
            iter = transform!("iter" => DropAllDataTransform);
            snk = sink!("snk" => CountingSink);
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

    let run = tokio::time::timeout(Duration::from_secs(10), handle.run()).await;
    match run {
        Ok(Ok(())) => {
            anyhow::bail!("expected flow to abort due to cycle_depth divergence violation")
        }
        Ok(Err(_)) => {}
        Err(_) => anyhow::bail!("flow run timed out (expected abort via contract violation)"),
    }

    sleep(Duration::from_millis(200)).await;

    let system_log = single_flow_system_log(&journal_root)?;
    let journal: obzenflow_infra::journal::DiskJournal<SystemEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            system_log,
            JournalOwner::system(SystemId::new()),
        )?;

    let envelopes = journal.read_causally_ordered().await?;

    let mut seen_divergence_contract_result = false;
    let mut seen_cycle_depth_contract_status = false;

    for env in envelopes {
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
