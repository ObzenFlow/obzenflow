// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122a retained-outcome and admission-only boundary proofs.

use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareFactoryResult, MiddlewareMaterializationContext, MiddlewareOverrideKey,
    MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, SinkAdmission,
    SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, SinkOperationFailed, StageFatalCode, StageFatalRecorded,
    StageLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventEnvelope, EventId, StageId, SystemId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{
    PendingSinkInput, SinkCommitReceipt, SinkConnector, SinkDescription, SinkOperationError,
    SinkOperationResult, SinkTerminalOutcome, SinkWriteContext, SinkWriteFailure, SinkWritePhase,
    SinkWriteReport, SinkWriteResult, SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
};
use obzenflow_runtime::stages::{SourceError, TypedFiniteSourceHandler};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RetainedInput {
    value: u64,
}

impl TypedPayload for RetainedInput {
    const EVENT_TYPE: &'static str = "flowip_122a.retained_input";
}

#[derive(Clone, Debug)]
struct TwoInputs {
    next: u64,
}

impl TypedFiniteSourceHandler for TwoInputs {
    type Output = RetainedInput;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > 2 {
            return Ok(None);
        }
        let value = self.next;
        self.next += 1;
        Ok(Some(vec![RetainedInput { value }]))
    }
}

#[derive(Clone, Copy, Debug)]
enum WriterMode {
    BufferedBatch,
    Poisoned,
}

#[derive(Clone, Debug)]
struct RetainedConnector {
    mode: WriterMode,
    calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl SinkConnector for RetainedConnector {
    type Input = RetainedInput;
    type Writer = RetainedWriter;

    fn describe(&self) -> SinkDescription {
        SinkDescription::destination(
            "retention.destination",
            DeliveryMethod::Custom("retention".to_string()),
        )
        .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        self.calls
            .lock()
            .expect("call log")
            .push("open".to_string());
        Ok(RetainedWriter {
            mode: self.mode,
            calls: Arc::clone(&self.calls),
            pending: None,
        })
    }
}

struct RetainedWriter {
    mode: WriterMode,
    calls: Arc<Mutex<Vec<String>>>,
    pending: Option<PendingSinkInput>,
}

impl std::fmt::Debug for RetainedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetainedWriter")
            .field("mode", &self.mode)
            .field("pending", &self.pending.is_some())
            .finish()
    }
}

impl Drop for RetainedWriter {
    fn drop(&mut self) {
        self.calls
            .lock()
            .expect("call log")
            .push("drop".to_string());
    }
}

#[async_trait]
impl SinkWriter for RetainedWriter {
    type Input = RetainedInput;

    async fn write(&mut self, input: RetainedInput, context: SinkWriteContext) -> SinkWriteResult {
        self.calls
            .lock()
            .expect("call log")
            .push(format!("write:{}", input.value));
        match self.mode {
            WriterMode::Poisoned => Err(SinkWriteFailure::poisoned(
                SinkWritePhase::Commit,
                SinkOperationError::remote("ambiguous commit"),
            )),
            WriterMode::BufferedBatch => {
                let current = context.defer();
                let Some(first) = self.pending.take() else {
                    self.pending = Some(current);
                    return Ok(SinkWriteReport::buffered(
                        obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                    ));
                };
                Ok(SinkWriteReport::buffered(
                    obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
                )
                .with_commit_receipts([
                    SinkCommitReceipt::new(first, SinkTerminalOutcome::success(None).with_items(1)),
                    SinkCommitReceipt::new(
                        current,
                        SinkTerminalOutcome::success(None).with_items(1),
                    ),
                ]))
            }
        }
    }

    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.calls
            .lock()
            .expect("call log")
            .push("flush".to_string());
        Ok(SinkWriterLifecycleReport::default())
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.calls
            .lock()
            .expect("call log")
            .push("drain".to_string());
        Ok(SinkWriterLifecycleReport::default())
    }
}

#[derive(Clone, Copy, Debug)]
enum PolicyMode {
    PanicOnObservation(usize),
    Reject,
    PanicAdmission,
    EmitEvidence,
}

#[derive(Clone, Debug)]
struct TestPolicyFactory {
    mode: PolicyMode,
    observations: Arc<AtomicUsize>,
}

struct TestPolicyFamily;

impl MiddlewareFactory for TestPolicyFactory {
    fn label(&self) -> &'static str {
        "flowip_122a_test_policy"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<TestPolicyFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SinkDelivery])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        _ctx: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        match request.surface {
            MiddlewareSurface::SinkDelivery(_) => Ok(MiddlewareSurfaceAttachment::sink_delivery(
                Arc::new(TestPolicy {
                    mode: self.mode,
                    observations: Arc::clone(&self.observations),
                }),
            )),
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                "sink_observation_retention_test",
                std::io::Error::other(format!("unexpected surface {:?}", other.kind())),
            )),
        }
    }
}

struct TestPolicy {
    mode: PolicyMode,
    observations: Arc<AtomicUsize>,
}

#[async_trait]
impl SinkPolicy for TestPolicy {
    fn label(&self) -> &'static str {
        "flowip_122a_test_policy"
    }

    async fn admit(&self, _ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        match self.mode {
            PolicyMode::Reject => SinkAdmission::Reject {
                reason: "intentional rejection".to_string(),
            },
            PolicyMode::PanicAdmission => panic!("raw admission panic sentinel must be redacted"),
            PolicyMode::PanicOnObservation(_) | PolicyMode::EmitEvidence => {
                SinkAdmission::Admit(None)
            }
        }
    }

    fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {
        let observation = self.observations.fetch_add(1, Ordering::SeqCst) + 1;
        if matches!(self.mode, PolicyMode::PanicOnObservation(target) if observation == target) {
            panic!("raw observation panic sentinel must be redacted");
        }
        if matches!(self.mode, PolicyMode::EmitEvidence) {
            _ctx.try_push_evidence(
                obzenflow_runtime::stages::sink::journal_sink::SinkPolicyEvidence::circuit_breaker(
                    CircuitBreakerEvent::Closed {
                        success_count: observation as u64,
                        recovery_duration_ms: 1,
                    },
                )
                .expect("closed breaker state is allowed sink evidence"),
            )
            .expect("test evidence batch has capacity");
        }
    }
}

fn build_flow(
    journal_root: PathBuf,
    writer_mode: WriterMode,
    policy_mode: PolicyMode,
    calls: Arc<Mutex<Vec<String>>>,
    observations: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let input = TwoInputs { next: 1 };
        let output = RetainedConnector {
            mode: writer_mode,
            calls: Arc::clone(&calls),
        };
        let policy = TestPolicyFactory {
            mode: policy_mode,
            observations: Arc::clone(&observations),
        };
        Ok(flow! {
            name: "sink_observation_retention",
            journals: disk_journals(journal_root),

            stages: {
                input = source!(RetainedInput => input);
                output = sink!(RetainedInput => output with [policy]);
            },

            topology: {
                input |> output;
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

async fn read_stage(run: &Path, stage: &str, field: &str) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = manifest(run);
    let file = manifest["stages"][stage][field]
        .as_str()
        .expect("stage journal file");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(run.join(file), JournalOwner::stage(StageId::new()))
            .expect("stage journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("stage journal reads")
}

async fn read_system(run: &Path) -> Vec<EventEnvelope<SystemEvent>> {
    let manifest = manifest(run);
    let file = manifest["system_journal_file"]
        .as_str()
        .expect("system journal file");
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

fn parent(event: &ChainEvent) -> Option<EventId> {
    event.causality.parent_ids.first().copied()
}

async fn run_case(
    writer_mode: WriterMode,
    policy_mode: PolicyMode,
) -> (
    tempfile::TempDir,
    PathBuf,
    Result<(), obzenflow_infra::application::ApplicationError>,
    Arc<Mutex<Vec<String>>>,
    Arc<AtomicUsize>,
) {
    let temp = tempfile::tempdir().expect("temporary directory");
    let journals = temp.path().join("journals");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observations = Arc::new(AtomicUsize::new(0));
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(build_flow(
                journals.clone(),
                writer_mode,
                policy_mode,
                Arc::clone(&calls),
                Arc::clone(&observations),
            )),
    )
    .await
    .expect("flow terminates promptly");
    (temp, latest_run_dir(&journals), result, calls, observations)
}

#[tokio::test(flavor = "multi_thread")]
async fn observation_panic_cannot_truncate_a_multi_receipt_report() {
    let (_temp, run, result, calls, observations) =
        run_case(WriterMode::BufferedBatch, PolicyMode::PanicOnObservation(2)).await;
    result.expect_err("observation panic is a protocol fatality");
    assert_eq!(observations.load(Ordering::SeqCst), 2);

    let data = read_stage(&run, "output", "data_journal_file").await;
    let receipts = data
        .iter()
        .filter(|envelope| matches!(envelope.event.content, ChainEventContent::Delivery(_)))
        .collect::<Vec<_>>();
    assert_eq!(receipts.len(), 4);
    let result_names = receipts
        .iter()
        .map(|envelope| match &envelope.event.content {
            ChainEventContent::Delivery(payload) => match payload.result {
                DeliveryResult::Buffered { .. } => "buffered",
                DeliveryResult::Success { .. } => "success",
                _ => "unexpected",
            },
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();
    assert_eq!(result_names, ["buffered", "buffered", "success", "success"]);

    let errors = read_stage(&run, "output", "error_journal_file").await;
    assert!(errors
        .iter()
        .all(|envelope| SinkOperationFailed::from_event(&envelope.event).is_none()));
    let fatals = errors
        .iter()
        .filter_map(|envelope| {
            StageFatalRecorded::from_event(&envelope.event).map(|fatal| (&envelope.event, fatal))
        })
        .collect::<Vec<_>>();
    assert_eq!(fatals.len(), 1);
    assert_eq!(fatals[0].1.code, StageFatalCode::Protocol);
    assert_eq!(
        parent(fatals[0].0),
        Some(receipts.last().expect("last receipt").event.id)
    );
    assert!(!fatals[0]
        .1
        .detail
        .contains("raw observation panic sentinel"));
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "drop"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn poisoned_cause_remains_primary_when_observation_also_panics() {
    let (_temp, run, result, calls, observations) =
        run_case(WriterMode::Poisoned, PolicyMode::PanicOnObservation(1)).await;
    result.expect_err("poison remains terminal");
    assert_eq!(observations.load(Ordering::SeqCst), 1);

    let data = read_stage(&run, "output", "data_journal_file").await;
    let receipt = data
        .iter()
        .find(|envelope| matches!(envelope.event.content, ChainEventContent::Delivery(_)))
        .expect("failed receipt");
    let errors = read_stage(&run, "output", "error_journal_file").await;
    let operation = errors
        .iter()
        .find_map(|envelope| {
            SinkOperationFailed::from_event(&envelope.event)
                .map(|operation| (&envelope.event, operation))
        })
        .expect("operation failure");
    assert_eq!(operation.1.failed_delivery_event_id, Some(receipt.event.id));
    let route = errors
        .iter()
        .map(|envelope| &envelope.event)
        .find(|event| parent(event) == Some(operation.0.id))
        .expect("fresh error route");

    let stage_id = operation.1.stage_id;
    let lifecycle = read_system(&run)
        .await
        .into_iter()
        .find_map(|envelope| match envelope.event.event {
            SystemEventType::StageLifecycle {
                stage_id: failed_stage,
                event:
                    StageLifecycleEvent::Failed {
                        causal_event_id, ..
                    },
            } if failed_stage == stage_id => Some((envelope.event.id, causal_event_id)),
            _ => None,
        })
        .expect("poisoned lifecycle failure");
    assert_eq!(lifecycle.1, Some(route.id));

    let fatal = errors
        .iter()
        .find_map(|envelope| {
            StageFatalRecorded::from_event(&envelope.event).map(|fatal| (&envelope.event, fatal))
        })
        .expect("secondary observation fatal");
    assert_eq!(fatal.1.primary_cause_event_id, Some(lifecycle.0));
    assert_eq!(parent(fatal.0), Some(route.id));
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "drop"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejection_makes_zero_writer_calls_and_authors_no_operation_failure() {
    let (_temp, run, result, calls, observations) =
        run_case(WriterMode::BufferedBatch, PolicyMode::Reject).await;
    result.expect("policy rejection is an accounted per-input outcome");
    assert_eq!(observations.load(Ordering::SeqCst), 0);
    {
        let calls = calls.lock().expect("call log");
        assert!(calls.iter().all(|call| !call.starts_with("write:")));
    }

    let data = read_stage(&run, "output", "data_journal_file").await;
    assert_eq!(
        data.iter()
            .filter_map(|envelope| match &envelope.event.content {
                ChainEventContent::Delivery(payload) => match &payload.result {
                    DeliveryResult::Failed { error_type, .. } => Some(error_type.as_str()),
                    _ => None,
                },
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["sink_policy_rejected", "sink_policy_rejected"]
    );
    let errors = read_stage(&run, "output", "error_journal_file").await;
    assert!(errors
        .iter()
        .all(|envelope| SinkOperationFailed::from_event(&envelope.event).is_none()));
}

#[tokio::test(flavor = "multi_thread")]
async fn admission_panic_is_redacted_and_creates_no_receipt_or_writer_call() {
    let (_temp, run, result, calls, observations) =
        run_case(WriterMode::BufferedBatch, PolicyMode::PanicAdmission).await;
    result.expect_err("admission panic is a protocol fatality");
    assert_eq!(observations.load(Ordering::SeqCst), 0);
    assert_eq!(calls.lock().expect("call log").as_slice(), ["open", "drop"]);

    let data = read_stage(&run, "output", "data_journal_file").await;
    assert!(data
        .iter()
        .all(|envelope| !matches!(envelope.event.content, ChainEventContent::Delivery(_))));
    let errors = read_stage(&run, "output", "error_journal_file").await;
    let fatal = errors
        .iter()
        .filter_map(|envelope| StageFatalRecorded::from_event(&envelope.event))
        .collect::<Vec<_>>();
    assert_eq!(fatal.len(), 1);
    assert_eq!(fatal[0].code, StageFatalCode::Protocol);
    assert!(!fatal[0].detail.contains("raw admission panic sentinel"));
}

#[tokio::test(flavor = "multi_thread")]
async fn policy_evidence_is_runtime_stamped_parented_and_mirrored() {
    let (_temp, run, result, calls, observations) =
        run_case(WriterMode::BufferedBatch, PolicyMode::EmitEvidence).await;
    result.expect("allowed evidence does not affect settlement");
    assert_eq!(observations.load(Ordering::SeqCst), 2);

    let source = read_stage(&run, "input", "data_journal_file").await;
    let inputs = source
        .iter()
        .filter(|envelope| RetainedInput::from_event(&envelope.event).is_some())
        .map(|envelope| &envelope.event)
        .collect::<Vec<_>>();
    let sink = read_stage(&run, "output", "data_journal_file").await;
    let evidence = sink
        .iter()
        .filter(|envelope| {
            matches!(
                &envelope.event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::Closed { .. })
                ))
            )
        })
        .map(|envelope| &envelope.event)
        .collect::<Vec<_>>();
    assert_eq!(evidence.len(), 2);
    for (input, evidence) in inputs.iter().zip(&evidence) {
        assert_ne!(evidence.id, input.id);
        assert_eq!(parent(evidence), Some(input.id));
        assert_eq!(evidence.flow_context.stage_name, "output");
        assert_eq!(
            evidence.writer_id.as_stage(),
            Some(&evidence.flow_context.stage_id)
        );
        assert_eq!(evidence.correlation, input.correlation);
        assert_eq!(evidence.cycle_depth, input.cycle_depth);
        assert_eq!(evidence.cycle_scc_id, input.cycle_scc_id);
        assert_eq!(
            evidence.composite_activations(),
            input.composite_activations()
        );
        assert!(evidence.runtime_context.is_some());
        assert!(evidence.replay_context.is_none());
        assert!(evidence.ingress_context.is_none());
        assert!(evidence.effect_provenance.is_none());
        assert!(input.admission_seq < evidence.admission_seq);
    }

    let mirrored = read_system(&run)
        .await
        .into_iter()
        .filter_map(|envelope| match envelope.event.event {
            SystemEventType::MiddlewareLifecycle { origin, .. } => Some(origin.event_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        mirrored,
        evidence.iter().map(|event| event.id).collect::<Vec<_>>()
    );
    assert_eq!(
        calls.lock().expect("call log").as_slice(),
        ["open", "write:1", "write:2", "flush", "drain", "drop"]
    );
}
