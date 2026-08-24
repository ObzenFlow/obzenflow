// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122a durable sink-operation failure-chain proofs.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, SinkDestinationErrorCode, SinkOperationFailed,
    SinkOperationPhase, SinkWritePhase, StageLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::{Journal, JournalError, JournalReader, RunManifest};
use obzenflow_core::{
    AdmissionSeq, EventEnvelope, EventId, FlowId, JournalId, StageId, SystemId, TypedPayload,
};
use obzenflow_dsl::{async_source, flow, sink, source, FlowDefinition};
use obzenflow_infra::application::{ApplicationError, FlowApplication};
use obzenflow_infra::journal::{disk_journals, DiskJournal, DiskJournalFactory};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::journal::{FlowJournalFactory, RunResourcePlan, RunSubstrateState};
use obzenflow_runtime::replay::ReplayArchive;
use obzenflow_runtime::stages::sink::{
    PendingSinkInput, SinkCommitReceipt, SinkConnector, SinkDescription, SinkOperationError,
    SinkOperationResult, SinkTerminalOutcome, SinkWriteContext, SinkWriteFailure, SinkWriteReport,
    SinkWriteResult, SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
};
use obzenflow_runtime::stages::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedFiniteSourceHandler,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppendBoundary {
    Begin,
    Returned,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AppendObservation {
    fact: &'static str,
    boundary: AppendBoundary,
    admission_seq: Option<AdmissionSeq>,
}

type AppendProbe = Arc<Mutex<Vec<AppendObservation>>>;

struct ProbedJournal<T: obzenflow_core::event::JournalEvent> {
    inner: Arc<dyn Journal<T>>,
    probe: AppendProbe,
    fact: fn(&T) -> Option<&'static str>,
    admission_seq: fn(&T) -> Option<AdmissionSeq>,
}

#[async_trait]
impl<T: obzenflow_core::event::JournalEvent> Journal<T> for ProbedJournal<T> {
    fn id(&self) -> &JournalId {
        self.inner.id()
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn append(
        &self,
        event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        let fact = (self.fact)(&event);
        if let Some(fact) = fact {
            self.probe
                .lock()
                .expect("append probe")
                .push(AppendObservation {
                    fact,
                    boundary: AppendBoundary::Begin,
                    admission_seq: (self.admission_seq)(&event),
                });
        }
        let result = self.inner.append(event, parent).await;
        if let (Some(fact), Ok(envelope)) = (fact, &result) {
            self.probe
                .lock()
                .expect("append probe")
                .push(AppendObservation {
                    fact,
                    boundary: AppendBoundary::Returned,
                    admission_seq: (self.admission_seq)(&envelope.event),
                });
        }
        result
    }

    async fn append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.inner.append_group(group_id, events, parent).await
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.inner.read_all_unordered().await
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        self.inner.read_event(event_id).await
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        self.inner.reader_from(position).await
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.inner.read_last_n(count).await
    }
}

fn chain_fact(event: &ChainEvent) -> Option<&'static str> {
    if matches!(
        &event.content,
        ChainEventContent::Delivery(payload)
            if matches!(payload.result, DeliveryResult::Failed { .. })
    ) {
        Some("R")
    } else if SinkOperationFailed::from_event(event).is_some() {
        Some("O")
    } else if ProbeInput::from_event(event).is_some()
        && matches!(event.processing_info.status, ProcessingStatus::Error { .. })
    {
        Some("X")
    } else {
        None
    }
}

fn chain_admission_seq(event: &ChainEvent) -> Option<AdmissionSeq> {
    event.admission_seq
}

fn system_fact(event: &SystemEvent) -> Option<&'static str> {
    matches!(
        event.event,
        SystemEventType::StageLifecycle {
            event: StageLifecycleEvent::Failed { .. },
            ..
        }
    )
    .then_some("P")
}

fn no_system_admission_seq(_event: &SystemEvent) -> Option<AdmissionSeq> {
    None
}

struct ProbedDiskJournalFactory {
    inner: DiskJournalFactory,
    probe: AppendProbe,
}

impl FlowJournalFactory for ProbedDiskJournalFactory {
    fn run_state(&self) -> RunSubstrateState {
        FlowJournalFactory::run_state(&self.inner)
    }

    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        let inner = FlowJournalFactory::create_chain_journal(&mut self.inner, name, owner)?;
        Ok(Arc::new(ProbedJournal {
            inner,
            probe: self.probe.clone(),
            fact: chain_fact,
            admission_seq: chain_admission_seq,
        }))
    }

    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        let inner = FlowJournalFactory::create_system_journal(&mut self.inner, name, owner)?;
        Ok(Arc::new(ProbedJournal {
            inner,
            probe: self.probe.clone(),
            fact: system_fact,
            admission_seq: no_system_admission_seq,
        }))
    }

    fn resource_preflight(&self, plan: &RunResourcePlan) -> Result<(), JournalError> {
        FlowJournalFactory::resource_preflight(&self.inner, plan)
    }

    fn write_run_manifest(&self, manifest: &RunManifest) -> Result<(), JournalError> {
        FlowJournalFactory::write_run_manifest(&self.inner, manifest)
    }

    fn seed_admission_from_archive(&self, archive: &dyn ReplayArchive) {
        FlowJournalFactory::seed_admission_from_archive(&self.inner, archive);
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ProbeInput {
    value: u64,
}

impl TypedPayload for ProbeInput {
    const EVENT_TYPE: &'static str = "flowip_122a.failure_probe";
}

#[derive(Clone, Debug)]
struct ProbeSource {
    next: u64,
    end: u64,
}

impl TypedFiniteSourceHandler for ProbeSource {
    type Output = ProbeInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > self.end {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![ProbeInput { value }]))
    }
}

#[derive(Clone, Debug)]
struct FanInCoordination {
    write_count: Arc<AtomicUsize>,
    write_advanced: Arc<tokio::sync::Notify>,
}

impl FanInCoordination {
    fn new() -> Self {
        Self {
            write_count: Arc::new(AtomicUsize::new(0)),
            write_advanced: Arc::new(tokio::sync::Notify::new()),
        }
    }

    async fn wait_for_writes(&self, required: usize) {
        loop {
            let advanced = self.write_advanced.notified();
            if self.write_count.load(Ordering::SeqCst) >= required {
                return;
            }
            advanced.await;
        }
    }

    fn record_write(&self, count: usize) {
        self.write_count.store(count, Ordering::SeqCst);
        self.write_advanced.notify_waiters();
    }
}

#[derive(Clone, Copy, Debug)]
enum CoordinatedPoll {
    AfterWrites { required: usize, value: u64 },
    Pending,
}

#[derive(Clone, Debug)]
struct CoordinatedAsyncSource {
    schedule: Vec<CoordinatedPoll>,
    next: usize,
    coordination: FanInCoordination,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for CoordinatedAsyncSource {
    type Output = ProbeInput;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        let Some(poll) = self.schedule.get(self.next).copied() else {
            return Ok(None);
        };
        match poll {
            CoordinatedPoll::AfterWrites { required, value } => {
                self.coordination.wait_for_writes(required).await;
                self.next += 1;
                Ok(Some(vec![ProbeInput { value }]))
            }
            CoordinatedPoll::Pending => std::future::pending().await,
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum FailureMode {
    CurrentOnlyFirst,
    ConfirmedRollbackSecond,
    DeferredOriginPoisonSecond,
    ConfirmedRollbackThirdFanIn,
    PoisonedFirst,
    PoisonedThirdFanIn,
    OpenFailed,
    FlushFailed,
    FlushDeferredOrigin,
    DrainFailed,
}

#[derive(Clone, Debug)]
struct ProbeConnector {
    mode: FailureMode,
    calls: Arc<Mutex<Vec<String>>>,
    fan_in_coordination: Option<FanInCoordination>,
}

#[async_trait]
impl SinkConnector for ProbeConnector {
    type Input = ProbeInput;
    type Writer = ProbeWriter;

    fn describe(&self) -> SinkDescription {
        SinkDescription::destination(
            "probe.destination",
            DeliveryMethod::Custom("probe".to_string()),
        )
        .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        self.calls
            .lock()
            .expect("call log")
            .push("open".to_string());
        if matches!(self.mode, FailureMode::OpenFailed) {
            return Err(operation_error());
        }
        Ok(ProbeWriter {
            mode: self.mode,
            calls: Arc::clone(&self.calls),
            writes: 0,
            retained: Vec::new(),
            fan_in_coordination: self.fan_in_coordination.clone(),
        })
    }
}

struct ProbeWriter {
    mode: FailureMode,
    calls: Arc<Mutex<Vec<String>>>,
    writes: usize,
    retained: Vec<PendingSinkInput>,
    fan_in_coordination: Option<FanInCoordination>,
}

impl std::fmt::Debug for ProbeWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeWriter")
            .field("mode", &self.mode)
            .field("writes", &self.writes)
            .field("retained", &self.retained.len())
            .finish()
    }
}

impl Drop for ProbeWriter {
    fn drop(&mut self) {
        self.calls
            .lock()
            .expect("call log")
            .push("drop".to_string());
    }
}

fn operation_error() -> SinkOperationError {
    SinkOperationError::remote("redacted destination failure").with_destination_error_code(
        SinkDestinationErrorCode::try_new("http.status", "503").expect("valid test code"),
    )
}

fn terminal_report() -> SinkWriteReport {
    SinkWriteReport::terminal(SinkTerminalOutcome::success(None).with_items(1))
}

#[async_trait]
impl SinkWriter for ProbeWriter {
    type Input = ProbeInput;

    async fn write(&mut self, input: ProbeInput, context: SinkWriteContext) -> SinkWriteResult {
        self.writes += 1;
        self.calls
            .lock()
            .expect("call log")
            .push(format!("write:{}", input.value));
        if let Some(coordination) = &self.fan_in_coordination {
            coordination.record_write(self.writes);
        }

        match (self.mode, self.writes) {
            (FailureMode::CurrentOnlyFirst, 1) => Err(SinkWriteFailure::current_only(
                SinkWritePhase::Execute,
                operation_error(),
            )),
            (FailureMode::PoisonedFirst, 1) => Err(SinkWriteFailure::poisoned(
                SinkWritePhase::Commit,
                operation_error(),
            )),
            (FailureMode::ConfirmedRollbackSecond, 1) => {
                self.retained.push(context.defer());
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                ))
            }
            (FailureMode::ConfirmedRollbackSecond, 2) => Err(SinkWriteFailure::confirmed_rollback(
                SinkWritePhase::Execute,
                operation_error(),
            )),
            (FailureMode::ConfirmedRollbackSecond, 3) => {
                let current = context.defer();
                let earlier = self.retained.pop().expect("first input remains retained");
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                )
                .with_commit_receipts([
                    SinkCommitReceipt::new(
                        earlier,
                        SinkTerminalOutcome::success(None).with_items(1),
                    ),
                    SinkCommitReceipt::new(
                        current,
                        SinkTerminalOutcome::success(None).with_items(1),
                    ),
                ]))
            }
            (FailureMode::DeferredOriginPoisonSecond, 1) => {
                self.retained.push(context.defer());
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                ))
            }
            (FailureMode::DeferredOriginPoisonSecond, 2) => {
                drop(context.defer());
                Err(SinkWriteFailure::poisoned_by_deferred(
                    self.retained.first().expect("first input remains retained"),
                    SinkWritePhase::Execute,
                    operation_error(),
                ))
            }
            (FailureMode::ConfirmedRollbackThirdFanIn, 1 | 2) => {
                self.retained.push(context.defer());
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                ))
            }
            (FailureMode::ConfirmedRollbackThirdFanIn, 3) => Err(
                SinkWriteFailure::confirmed_rollback(SinkWritePhase::Execute, operation_error()),
            ),
            (FailureMode::ConfirmedRollbackThirdFanIn, 4) => {
                let current = context.defer();
                let mut receipts = self
                    .retained
                    .drain(..)
                    .map(|pending| {
                        SinkCommitReceipt::new(
                            pending,
                            SinkTerminalOutcome::success(None).with_items(1),
                        )
                    })
                    .collect::<Vec<_>>();
                receipts.push(SinkCommitReceipt::new(
                    current,
                    SinkTerminalOutcome::success(None).with_items(1),
                ));
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                )
                .with_commit_receipts(receipts))
            }
            (FailureMode::PoisonedThirdFanIn, 1 | 2) => {
                self.retained.push(context.defer());
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                ))
            }
            (FailureMode::PoisonedThirdFanIn, 3) => {
                drop(context.defer());
                Err(SinkWriteFailure::poisoned_by_deferred(
                    self.retained
                        .get(1)
                        .expect("second fan-in parent remains retained"),
                    SinkWritePhase::Execute,
                    operation_error(),
                ))
            }
            (FailureMode::FlushDeferredOrigin, _) => {
                self.retained.push(context.defer());
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                ))
            }
            _ => Ok(terminal_report()),
        }
    }

    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.calls
            .lock()
            .expect("call log")
            .push("flush".to_string());
        if matches!(self.mode, FailureMode::FlushFailed) {
            return Err(operation_error());
        }
        if matches!(self.mode, FailureMode::FlushDeferredOrigin) {
            return Err(operation_error().with_deferred_operation_subject(
                self.retained
                    .first()
                    .expect("flush retains at least one deferred input"),
            ));
        }
        Ok(SinkWriterLifecycleReport::default())
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.calls
            .lock()
            .expect("call log")
            .push("drain".to_string());
        if matches!(self.mode, FailureMode::DrainFailed) {
            return Err(operation_error());
        }
        Ok(SinkWriterLifecycleReport::default())
    }
}

fn build_flow(
    journal_root: PathBuf,
    mode: FailureMode,
    calls: Arc<Mutex<Vec<String>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let source = ProbeSource { next: 1, end: 3 };
        let probe = ProbeConnector {
            mode,
            calls: Arc::clone(&calls),
            fan_in_coordination: None,
        };
        Ok(flow! {
            name: "sink_operation_failure_chain",
            journals: disk_journals(journal_root),

            stages: {
                inputs = source!(ProbeInput => source);
                probe = sink!(ProbeInput => probe);
            },

            topology: {
                inputs |> probe;
            }
        })
    })
}

fn probed_disk_journals(
    base_path: PathBuf,
    probe: AppendProbe,
) -> impl Fn(FlowId) -> Result<ProbedDiskJournalFactory, JournalError> {
    move |flow_id| {
        Ok(ProbedDiskJournalFactory {
            inner: DiskJournalFactory::new(base_path.clone(), flow_id)?,
            probe: probe.clone(),
        })
    }
}

fn build_append_probed_flow(
    journal_root: PathBuf,
    calls: Arc<Mutex<Vec<String>>>,
    append_probe: AppendProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let source = ProbeSource { next: 1, end: 3 };
        let probe = ProbeConnector {
            mode: FailureMode::PoisonedFirst,
            calls,
            fan_in_coordination: None,
        };
        Ok(flow! {
            name: "sink_operation_append_sequence",
            journals: probed_disk_journals(journal_root, append_probe),

            stages: {
                inputs = source!(ProbeInput => source);
                probe = sink!(ProbeInput => probe);
            },

            topology: {
                inputs |> probe;
            }
        })
    })
}

fn build_fan_in_flow(
    journal_root: PathBuf,
    mode: FailureMode,
    calls: Arc<Mutex<Vec<String>>>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let coordination = FanInCoordination::new();
        let left = CoordinatedAsyncSource {
            schedule: vec![
                CoordinatedPoll::AfterWrites {
                    required: 0,
                    value: 1,
                },
                CoordinatedPoll::AfterWrites {
                    required: 2,
                    value: 3,
                },
            ],
            next: 0,
            coordination: coordination.clone(),
        };
        let right = CoordinatedAsyncSource {
            schedule: vec![
                CoordinatedPoll::AfterWrites {
                    required: 1,
                    value: 2,
                },
                match mode {
                    FailureMode::ConfirmedRollbackThirdFanIn => CoordinatedPoll::AfterWrites {
                        required: 3,
                        value: 4,
                    },
                    FailureMode::PoisonedThirdFanIn => CoordinatedPoll::Pending,
                    _ => unreachable!("fan-in flow requires a fan-in failure mode"),
                },
            ],
            next: 0,
            coordination: coordination.clone(),
        };
        let probe = ProbeConnector {
            mode,
            calls: Arc::clone(&calls),
            fan_in_coordination: Some(coordination),
        };
        Ok(flow! {
            name: "sink_operation_failure_fan_in",
            journals: disk_journals(journal_root),

            stages: {
                left = async_source!(ProbeInput => left);
                right = async_source!(ProbeInput => right);
                probe = sink!(ProbeInput => probe);
            },

            topology: {
                left |> probe;
                right |> probe;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(base.join("flows"))
        .expect("flows directory")
        .map(|entry| entry.expect("run entry").path())
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("flow run archive")
}

fn manifest(run: &Path) -> serde_json::Value {
    serde_json::from_slice(
        &std::fs::read(run.join("run_manifest.json")).expect("manifest is readable"),
    )
    .expect("manifest is valid JSON")
}

async fn read_stage_journal(
    run: &Path,
    stage: &str,
    field: &str,
) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = manifest(run);
    let file = manifest["stages"][stage][field]
        .as_str()
        .expect("manifest stage journal");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(run.join(file), JournalOwner::stage(StageId::new()))
            .expect("stage journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("stage journal reads")
}

async fn read_system_journal(run: &Path) -> Vec<EventEnvelope<SystemEvent>> {
    let manifest = manifest(run);
    let file = manifest["system_journal_file"]
        .as_str()
        .expect("manifest system journal");
    let journal = DiskJournal::<SystemEvent>::with_owner(
        run.join(file),
        JournalOwner::system(SystemId::new()),
    )
    .expect("system journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("system journal reads")
}

fn direct_parent(event: &ChainEvent) -> Option<EventId> {
    event.causality.parent_ids.first().copied()
}

struct FailureChain<'a> {
    receipt: &'a ChainEvent,
    operation_event: &'a ChainEvent,
    operation: SinkOperationFailed,
    route: &'a ChainEvent,
}

fn failure_chain<'a>(
    data: &'a [EventEnvelope<ChainEvent>],
    errors: &'a [EventEnvelope<ChainEvent>],
) -> FailureChain<'a> {
    let (operation_envelope, operation) = errors
        .iter()
        .find_map(|envelope| {
            SinkOperationFailed::from_event(&envelope.event).map(|operation| (envelope, operation))
        })
        .expect("one typed operation failure");
    assert_eq!(
        errors
            .iter()
            .filter(|envelope| SinkOperationFailed::from_event(&envelope.event).is_some())
            .count(),
        1,
        "one authored error creates exactly one operation fact"
    );
    let receipt_id = operation
        .failed_delivery_event_id
        .expect("write failure names its receipt");
    let receipt = data
        .iter()
        .map(|envelope| &envelope.event)
        .find(|event| event.id == receipt_id)
        .expect("failed receipt exists");
    let routes = errors
        .iter()
        .map(|envelope| &envelope.event)
        .filter(|event| direct_parent(event) == Some(operation_envelope.event.id))
        .collect::<Vec<_>>();
    assert_eq!(
        routes.len(),
        1,
        "O has exactly one fresh error-route successor"
    );
    let route = routes[0];

    assert_eq!(direct_parent(&operation_envelope.event), Some(receipt.id));
    assert_ne!(
        route.id,
        operation.causal_event_id.expect("current input id")
    );
    assert_eq!(route.event_type(), ProbeInput::versioned_event_type());
    assert!(matches!(
        route.processing_info.status,
        ProcessingStatus::Error { .. }
    ));
    assert!(
        operation_envelope.event.admission_seq < route.admission_seq,
        "journal-stamped O must precede X"
    );

    FailureChain {
        receipt,
        operation_event: &operation_envelope.event,
        operation,
        route,
    }
}

fn failed_receipt_type(event: &ChainEvent) -> Option<&str> {
    let ChainEventContent::Delivery(payload) = &event.content else {
        return None;
    };
    let DeliveryResult::Failed { error_type, .. } = &payload.result else {
        return None;
    };
    Some(error_type)
}

async fn run_case(
    mode: FailureMode,
) -> (
    tempfile::TempDir,
    PathBuf,
    Result<(), ApplicationError>,
    Arc<Mutex<Vec<String>>>,
) {
    let temp = tempfile::tempdir().expect("temporary directory");
    let journals = temp.path().join("journals");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_flow(journals.clone(), mode, Arc::clone(&calls))),
    )
    .await
    .expect("flow terminates promptly");
    (temp, latest_run_dir(&journals), result, calls)
}

async fn run_fan_in_case(
    mode: FailureMode,
) -> (
    tempfile::TempDir,
    PathBuf,
    Result<(), ApplicationError>,
    Arc<Mutex<Vec<String>>>,
) {
    let temp = tempfile::tempdir().expect("temporary directory");
    let journals = temp.path().join("journals");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(60),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_fan_in_flow(
                journals.clone(),
                mode,
                Arc::clone(&calls),
            )),
    )
    .await
    .expect("fan-in flow terminates promptly");
    (temp, latest_run_dir(&journals), result, calls)
}

#[tokio::test(flavor = "multi_thread")]
async fn current_only_failure_is_accounted_linked_and_continuable() {
    let (_temp, run, result, calls) = run_case(FailureMode::CurrentOnlyFirst).await;
    result.expect("CurrentOnly remains usable");

    let data = read_stage_journal(&run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);
    assert_eq!(
        failed_receipt_type(chain.receipt),
        Some("sink_write_current_only_failed")
    );
    assert_eq!(
        chain.operation.phase,
        SinkOperationPhase::Write(SinkWritePhase::Execute)
    );
    assert_eq!(chain.operation.kind, ErrorKind::Remote);
    assert_eq!(
        chain
            .operation
            .destination_error_code
            .as_ref()
            .map(|code| (code.namespace(), code.value())),
        Some(("http.status", "503"))
    );
    assert_eq!(
        chain.operation.failed_delivery_event_id,
        Some(chain.receipt.id)
    );
    assert_eq!(direct_parent(chain.operation_event), Some(chain.receipt.id));
    assert_eq!(direct_parent(chain.route), Some(chain.operation_event.id));

    let outcomes = data
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Delivery(payload) => Some(&payload.result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 3);
    assert!(matches!(outcomes[0], DeliveryResult::Failed { .. }));
    assert!(matches!(outcomes[1], DeliveryResult::Success { .. }));
    assert!(matches!(outcomes[2], DeliveryResult::Success { .. }));
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "write:3", "flush", "drain", "drop"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn confirmed_rollback_retains_every_earlier_capability_and_never_settles_failed_current() {
    let (_temp, run, result, calls) = run_case(FailureMode::ConfirmedRollbackSecond).await;
    result.expect("confirmed rollback keeps the writer usable");

    let source = read_stage_journal(&run, "inputs", "data_journal_file").await;
    let input_ids = source
        .iter()
        .filter_map(|envelope| {
            ProbeInput::from_event(&envelope.event).map(|input| (input.value, envelope.event.id))
        })
        .collect::<std::collections::HashMap<_, _>>();
    let data = read_stage_journal(&run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);
    assert_eq!(
        failed_receipt_type(chain.receipt),
        Some("sink_batch_confirmed_rollback")
    );
    assert_eq!(chain.operation.causal_event_id, input_ids.get(&2).copied());

    let terminal_parents = data
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Delivery(payload)
                if matches!(payload.result, DeliveryResult::Success { .. }) =>
            {
                direct_parent(&envelope.event)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(terminal_parents, vec![input_ids[&1], input_ids[&3]]);
    assert!(
        !terminal_parents.contains(&input_ids[&2]),
        "the failed current capability is permanently revoked"
    );
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "write:3", "flush", "drain", "drop"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn deferred_origin_poison_names_the_buffered_subject_and_stops_reentry() {
    let (_temp, run, result, calls) = run_case(FailureMode::DeferredOriginPoisonSecond).await;
    result.expect_err("deferred-origin poison terminates the materialisation");

    let source = read_stage_journal(&run, "inputs", "data_journal_file").await;
    let input_ids = source
        .iter()
        .filter_map(|envelope| {
            ProbeInput::from_event(&envelope.event).map(|input| (input.value, envelope.event.id))
        })
        .collect::<std::collections::HashMap<_, _>>();
    let data = read_stage_journal(&run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);

    assert_eq!(
        failed_receipt_type(chain.receipt),
        Some("sink_materialisation_poisoned")
    );
    assert_eq!(chain.operation.causal_event_id, Some(input_ids[&2]));
    assert_eq!(
        chain.operation.operation_subject_event_id,
        Some(input_ids[&1])
    );
    assert_eq!(direct_parent(chain.receipt), Some(input_ids[&2]));
    assert_ne!(
        chain.operation.operation_subject_event_id,
        chain.operation.causal_event_id
    );

    let terminal_parents = data
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Delivery(payload)
                if matches!(payload.result, DeliveryResult::Success { .. }) =>
            {
                direct_parent(&envelope.event)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        terminal_parents.is_empty(),
        "the deferred subject stays unresolved"
    );
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "drop"],
        "poison prevents every later write and lifecycle call"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_reproduces_the_unresolved_deferred_origin_failure() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let journals = temp.path().join("journals");
    let live_calls = Arc::new(Mutex::new(Vec::new()));
    let live_result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(
            journals.clone(),
            FailureMode::DeferredOriginPoisonSecond,
            Arc::clone(&live_calls),
        ))
        .await;
    live_result.expect_err("live deferred-origin poison fails");
    let live_run = latest_run_dir(&journals);

    let replay_calls = Arc::new(Mutex::new(Vec::new()));
    let replay_result = FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_run.as_os_str().to_os_string(),
            OsString::from("--allow-incomplete-archive"),
        ])
        .run_async(build_flow(
            journals.clone(),
            FailureMode::DeferredOriginPoisonSecond,
            Arc::clone(&replay_calls),
        ))
        .await;
    replay_result.expect_err("replay encounters the unresolved deferred input again");
    let replay_run = latest_run_dir(&journals);
    assert_ne!(replay_run, live_run);

    let source = read_stage_journal(&replay_run, "inputs", "data_journal_file").await;
    let input_ids = source
        .iter()
        .filter_map(|envelope| {
            ProbeInput::from_event(&envelope.event).map(|input| (input.value, envelope.event.id))
        })
        .collect::<std::collections::HashMap<_, _>>();
    let data = read_stage_journal(&replay_run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&replay_run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);
    assert_eq!(chain.operation.causal_event_id, Some(input_ids[&2]));
    assert_eq!(
        chain.operation.operation_subject_event_id,
        Some(input_ids[&1])
    );
    assert_eq!(
        replay_calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "drop"],
        "replay neither skips nor silently settles the deferred input"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_in_confirmed_rollback_retains_and_settles_exact_cross_upstream_parents() {
    let (_temp, run, result, calls) =
        run_fan_in_case(FailureMode::ConfirmedRollbackThirdFanIn).await;
    result.expect("confirmed rollback keeps the fan-in writer usable");

    let left = read_stage_journal(&run, "left", "data_journal_file").await;
    let right = read_stage_journal(&run, "right", "data_journal_file").await;
    let input_ids = left
        .iter()
        .chain(&right)
        .filter_map(|envelope| {
            ProbeInput::from_event(&envelope.event).map(|input| (input.value, envelope.event.id))
        })
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(input_ids.len(), 4);
    assert!(left.iter().any(|event| event.event.id == input_ids[&1]));
    assert!(right.iter().any(|event| event.event.id == input_ids[&2]));

    let data = read_stage_journal(&run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);
    assert_eq!(chain.operation.causal_event_id, Some(input_ids[&3]));
    assert_eq!(chain.operation.operation_subject_event_id, None);
    assert_eq!(direct_parent(chain.receipt), Some(input_ids[&3]));

    let terminal_parents = data
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Delivery(payload)
                if matches!(payload.result, DeliveryResult::Success { .. }) =>
            {
                direct_parent(&envelope.event)
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        terminal_parents,
        vec![input_ids[&1], input_ids[&2], input_ids[&4]],
        "the later distinct invocation settles both retained upstream parents once"
    );
    assert!(!terminal_parents.contains(&input_ids[&3]));
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "write:3", "write:4", "flush", "drain", "drop"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn poisoned_failure_links_lifecycle_and_performs_drop_only_teardown() {
    let (_temp, run, result, calls) = run_case(FailureMode::PoisonedFirst).await;
    result.expect_err("Poisoned terminates the materialisation");

    let data = read_stage_journal(&run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);
    assert_eq!(
        failed_receipt_type(chain.receipt),
        Some("sink_materialisation_poisoned")
    );
    assert_eq!(
        chain.operation.phase,
        SinkOperationPhase::Write(SinkWritePhase::Commit)
    );

    let stage_id = chain.operation.stage_id;
    let system_events = read_system_journal(&run).await;
    let completed = system_events.iter().filter(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::StageLifecycle {
                stage_id: completed_stage,
                event: StageLifecycleEvent::Completed { .. },
            } if *completed_stage == stage_id
        )
    });
    assert_eq!(
        completed.count(),
        0,
        "a failed sink lifecycle must never retain completion evidence"
    );

    let tied_failures = system_events
        .into_iter()
        .filter_map(|envelope| match envelope.event.event {
            SystemEventType::StageLifecycle {
                stage_id: failed_stage,
                event:
                    StageLifecycleEvent::Failed {
                        causal_event_id, ..
                    },
            } if failed_stage == stage_id => causal_event_id,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tied_failures, vec![chain.route.id]);
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "drop"],
        "poison teardown must never write, flush, or drain again"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn poisoned_failure_awaits_each_unstamped_r_o_x_p_append() {
    let temp = tempfile::tempdir().expect("temporary directory");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let append_probe = Arc::new(Mutex::new(Vec::new()));
    let result = FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_append_probed_flow(
            temp.path().join("journals"),
            calls,
            append_probe.clone(),
        ))
        .await;
    result.expect_err("poisoned writer terminates the materialisation");

    let observations = append_probe.lock().expect("append probe").clone();
    assert_eq!(
        observations
            .iter()
            .map(|observation| (observation.fact, observation.boundary))
            .collect::<Vec<_>>(),
        vec![
            ("R", AppendBoundary::Begin),
            ("R", AppendBoundary::Returned),
            ("O", AppendBoundary::Begin),
            ("O", AppendBoundary::Returned),
            ("X", AppendBoundary::Begin),
            ("X", AppendBoundary::Returned),
            ("P", AppendBoundary::Begin),
            ("P", AppendBoundary::Returned),
        ],
        "each successor append must begin only after its predecessor returned"
    );

    for observation in observations.iter().filter(|observation| {
        observation.boundary == AppendBoundary::Begin && observation.fact != "P"
    }) {
        assert_eq!(
            observation.admission_seq, None,
            "{} must arrive at append without a pre-filled admission sequence",
            observation.fact
        );
    }
    let stamped = observations
        .iter()
        .filter(|observation| {
            observation.boundary == AppendBoundary::Returned && observation.fact != "P"
        })
        .map(|observation| {
            observation
                .admission_seq
                .expect("journal stamps sequence")
                .0
        })
        .collect::<Vec<_>>();
    assert!(
        stamped.windows(2).all(|window| window[0] < window[1]),
        "journal-stamped R -> O -> X admission order must increase: {stamped:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_in_poison_stops_before_a_later_parent_can_accumulate_receipt_progress() {
    let (_temp, run, result, calls) = run_fan_in_case(FailureMode::PoisonedThirdFanIn).await;
    result.expect_err("poison terminates the fan-in materialisation");

    let left = read_stage_journal(&run, "left", "data_journal_file").await;
    let right = read_stage_journal(&run, "right", "data_journal_file").await;
    let input_ids = left
        .iter()
        .chain(&right)
        .filter_map(|envelope| {
            ProbeInput::from_event(&envelope.event).map(|input| (input.value, envelope.event.id))
        })
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(
        input_ids.len(),
        3,
        "poison cancels the other upstream before its later input is committed"
    );
    assert!(input_ids.contains_key(&1));
    assert!(input_ids.contains_key(&2));
    assert!(input_ids.contains_key(&3));

    let data = read_stage_journal(&run, "probe", "data_journal_file").await;
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let chain = failure_chain(&data, &errors);
    assert_eq!(chain.operation.causal_event_id, Some(input_ids[&3]));
    assert_eq!(
        chain.operation.operation_subject_event_id,
        Some(input_ids[&2]),
        "the typed subject preserves the exact parent from the other fan-in feed"
    );
    assert_eq!(direct_parent(chain.receipt), Some(input_ids[&3]));
    assert_eq!(
        data.iter()
            .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Delivery(_)))
            .count(),
        3,
        "no later delivery receipt accumulates after poison"
    );
    assert_eq!(
        data.iter()
            .filter(|envelope| {
                matches!(
                    &envelope.event.content,
                    ChainEventContent::Delivery(payload)
                        if matches!(payload.result, DeliveryResult::Buffered { .. })
                )
            })
            .count(),
        2,
        "only the two exact pre-poison parents have provisional receipts"
    );
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "write:3", "drop"]
    );
}

async fn assert_lifecycle_failure(
    mode: FailureMode,
    expected_phase: SinkOperationPhase,
    expected_calls: &[&str],
) {
    let (_temp, run, result, calls) = run_case(mode).await;
    result.expect_err("lifecycle operation failure terminates the materialisation");

    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let operations = errors
        .iter()
        .filter_map(|envelope| {
            SinkOperationFailed::from_event(&envelope.event)
                .map(|operation| (&envelope.event, operation))
        })
        .collect::<Vec<_>>();
    assert_eq!(operations.len(), 1);
    let (operation_event, operation) = &operations[0];
    assert_eq!(operation.phase, expected_phase);
    assert_eq!(operation.kind, ErrorKind::Remote);
    assert_eq!(operation.causal_event_id, None);
    assert_eq!(operation.input_position, None);
    assert_eq!(operation.failed_delivery_event_id, None);
    assert_eq!(direct_parent(operation_event), None);

    let system_events = read_system_journal(&run).await;
    let completed = system_events.iter().filter(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { .. },
            } if *stage_id == operation.stage_id
        )
    });
    assert_eq!(
        completed.count(),
        0,
        "a failed sink lifecycle must never retain completion evidence"
    );

    let tied_failures = system_events
        .into_iter()
        .filter_map(|envelope| match envelope.event.event {
            SystemEventType::StageLifecycle {
                stage_id,
                event:
                    StageLifecycleEvent::Failed {
                        causal_event_id, ..
                    },
            } if stage_id == operation.stage_id => causal_event_id,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tied_failures, vec![operation_event.id]);
    assert_eq!(
        calls
            .lock()
            .expect("call log")
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        expected_calls
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn open_failure_is_framework_stamped_and_links_directly_to_lifecycle() {
    assert_lifecycle_failure(FailureMode::OpenFailed, SinkOperationPhase::Open, &["open"]).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn flush_failure_stops_before_drain_and_links_directly_to_lifecycle() {
    assert_lifecycle_failure(
        FailureMode::FlushFailed,
        SinkOperationPhase::Flush,
        &["open", "write:1", "write:2", "write:3", "flush", "drop"],
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn eof_flush_failure_names_the_deferred_subject_and_skips_drain() {
    let (_temp, run, result, calls) = run_case(FailureMode::FlushDeferredOrigin).await;
    result.expect_err("deferred-origin flush failure terminates the materialisation");

    let source = read_stage_journal(&run, "inputs", "data_journal_file").await;
    let first_id = source
        .iter()
        .find_map(|envelope| {
            ProbeInput::from_event(&envelope.event)
                .filter(|input| input.value == 1)
                .map(|_| envelope.event.id)
        })
        .expect("first deferred input exists");
    let errors = read_stage_journal(&run, "probe", "error_journal_file").await;
    let operations = errors
        .iter()
        .filter_map(|envelope| SinkOperationFailed::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].phase, SinkOperationPhase::Flush);
    assert_eq!(operations[0].causal_event_id, None);
    assert_eq!(operations[0].failed_delivery_event_id, None);
    assert_eq!(operations[0].operation_subject_event_id, Some(first_id));
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "write:3", "flush", "drop"],
        "a failed flush must prevent drain"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_failure_is_stamped_once_without_failed_teardown_reentry() {
    assert_lifecycle_failure(
        FailureMode::DrainFailed,
        SinkOperationPhase::Drain,
        &[
            "open", "write:1", "write:2", "write:3", "flush", "drain", "drop",
        ],
    )
    .await;
}
