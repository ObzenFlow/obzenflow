// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Application certification for production sink connectors.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryPayload, DeliveryResult};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, SinkOperationFailed, SinkOperationPhase, StageFatalRecorded,
    StageLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::{Journal, RunManifest, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION};
use obzenflow_core::metrics::SinkOperationFailureMetric;
use obzenflow_core::{AdmissionSeq, EventId, StageId, TypedPayload};
use obzenflow_dsl::FlowDefinition;
use obzenflow_infra::application::{CurrentRunLocator, FlowApplication};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::stages::sink::{SinkDestinationErrorCode, SinkWriteFailureDisposition};
use obzenflow_runtime::testing::sink::{
    SinkConformanceProfile, SinkExternalCallSnapshot, SinkFixtureError,
    SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use std::collections::HashMap;
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkApplicationTreatment {
    Live,
    ArchiveRedelivery,
    ArchiveRedeliveryOverride,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDestinationClass {
    SafeToRepeat,
    DuplicateSensitive,
    Unspecified,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkApplicationTopology {
    Single,
    FanIn,
    FanOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkApplicationScenario {
    treatment: SinkApplicationTreatment,
    destination_class: SinkDestinationClass,
    topology: SinkApplicationTopology,
    eof_kind: EofKind,
}

impl SinkApplicationScenario {
    pub fn treatment(&self) -> SinkApplicationTreatment {
        self.treatment
    }

    pub fn destination_class(&self) -> SinkDestinationClass {
        self.destination_class
    }

    pub fn topology(&self) -> SinkApplicationTopology {
        self.topology
    }

    pub fn eof_kind(&self) -> EofKind {
        self.eof_kind
    }

    fn name(&self) -> String {
        format!(
            "{:?}-{:?}-{:?}-{:?}",
            self.treatment, self.destination_class, self.topology, self.eof_kind
        )
    }

    fn expects_refusal(&self) -> bool {
        self.treatment == SinkApplicationTreatment::ArchiveRedelivery
            && matches!(
                self.destination_class,
                SinkDestinationClass::DuplicateSensitive | SinkDestinationClass::Unspecified
            )
    }
}

fn required_scenarios() -> Vec<SinkApplicationScenario> {
    use SinkApplicationTopology::{FanIn, FanOut, Single};
    use SinkApplicationTreatment::{ArchiveRedelivery, ArchiveRedeliveryOverride, Live};
    use SinkDestinationClass::{DuplicateSensitive, SafeToRepeat, Unspecified};
    vec![
        SinkApplicationScenario {
            treatment: Live,
            destination_class: SafeToRepeat,
            topology: Single,
            eof_kind: EofKind::Natural,
        },
        SinkApplicationScenario {
            treatment: Live,
            destination_class: SafeToRepeat,
            topology: FanIn,
            eof_kind: EofKind::Natural,
        },
        SinkApplicationScenario {
            treatment: Live,
            destination_class: SafeToRepeat,
            topology: FanOut,
            eof_kind: EofKind::Natural,
        },
        SinkApplicationScenario {
            treatment: ArchiveRedelivery,
            destination_class: SafeToRepeat,
            topology: Single,
            eof_kind: EofKind::Truncated,
        },
        SinkApplicationScenario {
            treatment: Live,
            destination_class: SafeToRepeat,
            topology: Single,
            eof_kind: EofKind::Poison,
        },
        SinkApplicationScenario {
            treatment: ArchiveRedelivery,
            destination_class: SafeToRepeat,
            topology: Single,
            eof_kind: EofKind::Natural,
        },
        SinkApplicationScenario {
            treatment: ArchiveRedelivery,
            destination_class: DuplicateSensitive,
            topology: Single,
            eof_kind: EofKind::Natural,
        },
        SinkApplicationScenario {
            treatment: ArchiveRedelivery,
            destination_class: Unspecified,
            topology: Single,
            eof_kind: EofKind::Natural,
        },
        SinkApplicationScenario {
            treatment: ArchiveRedeliveryOverride,
            destination_class: DuplicateSensitive,
            topology: Single,
            eof_kind: EofKind::Natural,
        },
    ]
}

pub struct SinkApplicationBuildCase {
    flow: Option<FlowDefinition>,
    archive_root: PathBuf,
    cli_args: Vec<OsString>,
}

impl fmt::Debug for SinkApplicationBuildCase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkApplicationBuildCase")
            .field("archive_root", &self.archive_root)
            .field("cli_arg_count", &self.cli_args.len())
            .finish_non_exhaustive()
    }
}

impl SinkApplicationBuildCase {
    pub fn new(flow: FlowDefinition, archive_root: impl Into<PathBuf>) -> Self {
        Self {
            flow: Some(flow),
            archive_root: archive_root.into(),
            cli_args: vec![OsString::from("sink-conformance")],
        }
    }

    pub fn with_cli_args<I, S>(mut self, args: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<OsString>,
    {
        self.cli_args = args.into_iter().map(Into::into).collect();
        self
    }

    fn into_parts(mut self) -> (FlowDefinition, PathBuf, Vec<OsString>) {
        (
            self.flow
                .take()
                .expect("application build case is consumed once"),
            self.archive_root,
            self.cli_args,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDestinationVerdict {
    Committed,
    Converged,
    Refused,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkDestinationExpectation {
    scenario: SinkApplicationScenario,
    verdict: SinkDestinationVerdict,
}

impl SinkDestinationExpectation {
    pub fn scenario(&self) -> SinkApplicationScenario {
        self.scenario
    }

    pub fn verdict(&self) -> SinkDestinationVerdict {
        self.verdict
    }
}

#[async_trait]
pub trait SinkDestinationVerifier: Send + Sync {
    type Snapshot: fmt::Debug + Eq + Send + Sync;

    async fn snapshot(&self) -> Result<Self::Snapshot, SinkFixtureError>;
    async fn verify(
        &self,
        expectation: SinkDestinationExpectation,
        before: &Self::Snapshot,
        after: &Self::Snapshot,
    ) -> Result<(), SinkFixtureError>;
    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError>;
}

#[async_trait]
pub trait SinkApplicationConformanceFixture: Send {
    type Verifier: SinkDestinationVerifier;

    fn profile(&self) -> SinkConformanceProfile;
    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError>;
    fn build_case(
        &mut self,
        scenario: SinkApplicationScenario,
    ) -> Result<SinkApplicationBuildCase, SinkFixtureError>;
    fn verifier(&self) -> &Self::Verifier;
}

#[derive(Debug, Clone)]
pub struct ProjectedSinkOperationFailure {
    event_id: EventId,
    stage_id: StageId,
    stage_key: String,
    logical_destination: String,
    causal_event_id: Option<EventId>,
    input_position: Option<u64>,
    phase: SinkOperationPhase,
    failed_delivery_event_id: Option<EventId>,
    kind: ErrorKind,
    destination_error_code: Option<SinkDestinationErrorCode>,
}

impl ProjectedSinkOperationFailure {
    pub fn event_id(&self) -> EventId {
        self.event_id
    }

    pub fn stage_id(&self) -> StageId {
        self.stage_id
    }

    pub fn stage_key(&self) -> &str {
        &self.stage_key
    }

    pub fn logical_destination(&self) -> &str {
        &self.logical_destination
    }

    pub fn causal_event_id(&self) -> Option<EventId> {
        self.causal_event_id
    }

    pub fn input_position(&self) -> Option<u64> {
        self.input_position
    }

    pub fn phase(&self) -> SinkOperationPhase {
        self.phase
    }

    pub fn failed_delivery_event_id(&self) -> Option<EventId> {
        self.failed_delivery_event_id
    }

    pub fn kind(&self) -> &ErrorKind {
        &self.kind
    }

    pub fn destination_error_code(&self) -> Option<&SinkDestinationErrorCode> {
        self.destination_error_code.as_ref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkFailureChainVerdict {
    operation_event_id: EventId,
    disposition: Option<SinkWriteFailureDisposition>,
    receipt_to_operation: bool,
    operation_to_route: bool,
    route_to_lifecycle: bool,
}

impl SinkFailureChainVerdict {
    pub fn operation_event_id(&self) -> EventId {
        self.operation_event_id
    }

    pub fn disposition(&self) -> Option<SinkWriteFailureDisposition> {
        self.disposition
    }

    pub fn receipt_to_operation(&self) -> bool {
        self.receipt_to_operation
    }

    pub fn operation_to_route(&self) -> bool {
        self.operation_to_route
    }

    pub fn route_to_lifecycle(&self) -> bool {
        self.route_to_lifecycle
    }
}

#[derive(Debug, Clone)]
pub struct SinkRunEvidence {
    locator: CurrentRunLocator,
    eof_kinds: Vec<EofKind>,
    operation_failures: Vec<ProjectedSinkOperationFailure>,
    operation_failure_metrics: Vec<SinkOperationFailureMetric>,
    failure_chains: Vec<SinkFailureChainVerdict>,
}

impl SinkRunEvidence {
    pub fn locator(&self) -> &CurrentRunLocator {
        &self.locator
    }

    pub fn eof_kinds(&self) -> &[EofKind] {
        &self.eof_kinds
    }

    pub fn operation_failures(&self) -> &[ProjectedSinkOperationFailure] {
        &self.operation_failures
    }

    pub fn operation_failure_metrics(&self) -> &[SinkOperationFailureMetric] {
        &self.operation_failure_metrics
    }

    pub fn failure_chains(&self) -> &[SinkFailureChainVerdict] {
        &self.failure_chains
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConformanceCaseResult {
    scenario: SinkApplicationScenario,
    archive: Option<PathBuf>,
}

impl SinkConformanceCaseResult {
    pub fn scenario(&self) -> SinkApplicationScenario {
        self.scenario
    }

    pub fn archive(&self) -> Option<&Path> {
        self.archive.as_deref()
    }
}

#[derive(Debug, Clone)]
pub struct SinkConformanceReport {
    protocol_version: u16,
    cases: Vec<SinkConformanceCaseResult>,
    runs: Vec<SinkRunEvidence>,
}

impl SinkConformanceReport {
    pub fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    pub fn cases(&self) -> &[SinkConformanceCaseResult] {
        &self.cases
    }

    pub fn runs(&self) -> &[SinkRunEvidence] {
        &self.runs
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConformanceFailure {
    suite: &'static str,
    case: String,
    detail: String,
}

impl SinkConformanceFailure {
    pub fn suite(&self) -> &'static str {
        self.suite
    }

    pub fn case(&self) -> &str {
        &self.case
    }

    pub fn detail(&self) -> &str {
        &self.detail
    }
}

impl fmt::Display for SinkConformanceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "sink application conformance {}/{}: {}",
            self.suite, self.case, self.detail
        )
    }
}

impl std::error::Error for SinkConformanceFailure {}

fn failure(
    suite: &'static str,
    case: impl Into<String>,
    detail: impl Into<String>,
) -> SinkConformanceFailure {
    SinkConformanceFailure {
        suite,
        case: case.into(),
        detail: detail.into(),
    }
}

fn latest_run_dir(root: &Path) -> Option<PathBuf> {
    let flows = root.join("flows");
    let mut runs = std::fs::read_dir(flows)
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.join(RUN_MANIFEST_FILENAME).is_file())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop()
}

async fn read_chain_journal(path: &Path) -> Result<Vec<ChainEvent>, SinkConformanceFailure> {
    let journal = DiskJournal::<ChainEvent>::with_owner(
        path.to_path_buf(),
        JournalOwner::stage(StageId::new()),
    )
    .map_err(|error| failure("journal", path.display().to_string(), error.to_string()))?;
    let mut reader = journal
        .reader()
        .await
        .map_err(|error| failure("journal", path.display().to_string(), error.to_string()))?;
    let mut events = Vec::new();
    while let Some(envelope) = reader
        .next()
        .await
        .map_err(|error| failure("journal", path.display().to_string(), error.to_string()))?
    {
        events.push(envelope.event);
    }
    Ok(events)
}

async fn read_system_journal(path: &Path) -> Result<Vec<SystemEvent>, SinkConformanceFailure> {
    let journal = DiskJournal::<SystemEvent>::with_owner(
        path.to_path_buf(),
        JournalOwner::system(obzenflow_core::SystemId::new()),
    )
    .map_err(|error| failure("journal", path.display().to_string(), error.to_string()))?;
    let mut reader = journal
        .reader()
        .await
        .map_err(|error| failure("journal", path.display().to_string(), error.to_string()))?;
    let mut events = Vec::new();
    while let Some(envelope) = reader
        .next()
        .await
        .map_err(|error| failure("journal", path.display().to_string(), error.to_string()))?
    {
        events.push(envelope.event);
    }
    Ok(events)
}

fn is_failed_receipt(event: &ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Delivery(DeliveryPayload {
            result: DeliveryResult::Failed { .. },
            ..
        })
    )
}

fn direct_parent(event: &ChainEvent) -> Option<EventId> {
    (event.causality.parent_ids.len() == 1).then(|| event.causality.parent_ids[0])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SinkJournalRoute {
    Data,
    Error,
}

fn expected_error_route(kind: &ErrorKind) -> SinkJournalRoute {
    match kind {
        ErrorKind::Validation | ErrorKind::Domain => SinkJournalRoute::Data,
        ErrorKind::Timeout
        | ErrorKind::Remote
        | ErrorKind::RateLimited
        | ErrorKind::PermanentFailure
        | ErrorKind::Deserialization
        | ErrorKind::Unknown => SinkJournalRoute::Error,
    }
}

fn event_error_kind(event: &ChainEvent) -> Option<&ErrorKind> {
    match &event.processing_info.status {
        ProcessingStatus::Error {
            kind: Some(kind), ..
        } => Some(kind),
        _ => None,
    }
}

fn failed_receipt_disposition(event: &ChainEvent) -> Option<SinkWriteFailureDisposition> {
    let ChainEventContent::Delivery(DeliveryPayload {
        result: DeliveryResult::Failed { error_type, .. },
        ..
    }) = &event.content
    else {
        return None;
    };
    match error_type.as_str() {
        "sink_write_current_only_failed" => Some(SinkWriteFailureDisposition::CurrentOnly),
        "sink_batch_confirmed_rollback" => Some(SinkWriteFailureDisposition::ConfirmedRollback),
        "sink_materialisation_poisoned" => Some(SinkWriteFailureDisposition::Poisoned),
        _ => None,
    }
}

fn same_data_payload(left: &ChainEvent, right: &ChainEvent) -> bool {
    matches!(
        (&left.content, &right.content),
        (
            ChainEventContent::Data {
                event_type: left_type,
                payload: left_payload,
            },
            ChainEventContent::Data {
                event_type: right_type,
                payload: right_payload,
            }
        ) if left_type == right_type && left_payload == right_payload
    )
}

fn inherited_route_context_matches(input: &ChainEvent, route: &ChainEvent) -> bool {
    input.correlation == route.correlation
        && input.cycle_depth == route.cycle_depth
        && input.cycle_scc_id == route.cycle_scc_id
        && input.composite_activations() == route.composite_activations()
        && input
            .replay_context
            .as_ref()
            .map(|value| format!("{value:?}"))
            == route
                .replay_context
                .as_ref()
                .map(|value| format!("{value:?}"))
        && input
            .ingress_context
            .as_ref()
            .map(|value| format!("{value:?}"))
            == route
                .ingress_context
                .as_ref()
                .map(|value| format!("{value:?}"))
}

fn require_admission_order(
    earlier: &ChainEvent,
    later: &ChainEvent,
    edge: &str,
) -> Result<(), SinkConformanceFailure> {
    match (earlier.admission_seq, later.admission_seq) {
        (Some(AdmissionSeq(before)), Some(AdmissionSeq(after))) if before < after => Ok(()),
        _ => Err(failure(
            "journal-truth",
            edge,
            "causal successors must have strictly increasing journal admission sequence",
        )),
    }
}

fn fold_operation_failure_metrics(
    operations: &[ProjectedSinkOperationFailure],
) -> Vec<SinkOperationFailureMetric> {
    let mut metrics: Vec<SinkOperationFailureMetric> = Vec::new();
    for operation in operations {
        if let Some(metric) = metrics.iter_mut().find(|metric| {
            metric.stage_id == operation.stage_id
                && metric.phase == operation.phase
                && metric.error_kind == operation.kind
        }) {
            metric.count += 1;
        } else {
            metrics.push(SinkOperationFailureMetric {
                stage_id: operation.stage_id,
                phase: operation.phase,
                error_kind: operation.kind.clone(),
                count: 1,
            });
        }
    }
    metrics.sort_by_key(|metric| {
        (
            metric.stage_id.to_string(),
            format!("{:?}", metric.phase),
            format!("{:?}", metric.error_kind),
        )
    });
    metrics
}

fn validate_write_operation_cardinality(
    failed_receipt_ids: &[EventId],
    operation_receipt_ids: &[EventId],
) -> Result<(), SinkConformanceFailure> {
    let mut counts = HashMap::<EventId, usize>::new();
    for receipt_id in operation_receipt_ids {
        *counts.entry(*receipt_id).or_default() += 1;
    }
    for receipt_id in failed_receipt_ids {
        match counts.remove(receipt_id).unwrap_or_default() {
            1 => {}
            0 => {
                return Err(failure(
                    "journal-truth",
                    receipt_id.to_string(),
                    "failed sink receipt has no SinkOperationFailed fact",
                ));
            }
            count => {
                return Err(failure(
                    "journal-truth",
                    receipt_id.to_string(),
                    format!("failed sink receipt has {count} SinkOperationFailed facts"),
                ));
            }
        }
    }
    if let Some((receipt_id, count)) = counts.into_iter().next() {
        return Err(failure(
            "journal-truth",
            receipt_id.to_string(),
            format!("{count} write operation failure facts name no recognised failed sink receipt"),
        ));
    }
    Ok(())
}

fn parse_current_manifest(raw: &str) -> Result<RunManifest, SinkConformanceFailure> {
    let raw_value: serde_json::Value = serde_json::from_str(raw)
        .map_err(|error| failure("archive", "manifest-json", error.to_string()))?;
    let version = raw_value.get("manifest_version");
    if version != Some(&serde_json::Value::String(RUN_MANIFEST_VERSION.to_string())) {
        let found = version
            .map(serde_json::Value::to_string)
            .unwrap_or_else(|| "<missing>".to_string());
        return Err(failure(
            "archive",
            "manifest-version",
            format!("unsupported manifest version {found}"),
        ));
    }
    serde_json::from_value(raw_value)
        .map_err(|error| failure("archive", "manifest-shape", error.to_string()))
}

async fn project_run(run_dir: &Path) -> Result<SinkRunEvidence, SinkConformanceFailure> {
    let manifest_path = run_dir.join(RUN_MANIFEST_FILENAME);
    let raw = std::fs::read_to_string(&manifest_path)
        .map_err(|error| failure("archive", "manifest-read", error.to_string()))?;
    let manifest = parse_current_manifest(&raw)?;

    let mut chain_events = Vec::new();
    let mut journal_routes = HashMap::new();
    for stage in manifest.stages.values() {
        let data_events = read_chain_journal(&run_dir.join(&stage.data_journal_file)).await?;
        for event in &data_events {
            if journal_routes
                .insert(event.id, SinkJournalRoute::Data)
                .is_some()
            {
                return Err(failure(
                    "journal-truth",
                    "event-identity",
                    "duplicate ChainEvent id across journal projections",
                ));
            }
        }
        chain_events.extend(data_events);
        let error_events = read_chain_journal(&run_dir.join(&stage.error_journal_file)).await?;
        for event in &error_events {
            if journal_routes
                .insert(event.id, SinkJournalRoute::Error)
                .is_some()
            {
                return Err(failure(
                    "journal-truth",
                    "event-identity",
                    "duplicate ChainEvent id across journal projections",
                ));
            }
        }
        chain_events.extend(error_events);
    }
    let system_events = read_system_journal(&run_dir.join(&manifest.system_journal_file)).await?;

    let by_id = chain_events
        .iter()
        .map(|event| (event.id, event))
        .collect::<HashMap<_, _>>();
    if by_id.len() != chain_events.len() {
        return Err(failure(
            "journal-truth",
            "event-identity",
            "duplicate ChainEvent id",
        ));
    }

    let lifecycle_causes = system_events
        .iter()
        .filter_map(|event| match &event.event {
            SystemEventType::StageLifecycle {
                stage_id,
                event:
                    StageLifecycleEvent::Failed {
                        causal_event_id: Some(cause),
                        ..
                    },
            } => Some((event.id, *stage_id, *cause)),
            _ => None,
        })
        .collect::<Vec<_>>();

    for event in &chain_events {
        let Some(fatal) = StageFatalRecorded::from_event(event) else {
            continue;
        };
        if fatal.stage_id != event.flow_context.stage_id
            || fatal.stage_key != event.flow_context.stage_name
            || fatal.causal_event_id != direct_parent(event)
            || journal_routes.get(&event.id) != Some(&SinkJournalRoute::Error)
        {
            return Err(failure(
                "journal-truth",
                event.id.to_string(),
                "stage-fatal tail has inconsistent stage, route, or direct parent",
            ));
        }
        match fatal.primary_cause_event_id {
            Some(primary) => {
                if !lifecycle_causes
                    .iter()
                    .any(|(id, stage, _)| *id == primary && *stage == fatal.stage_id)
                {
                    return Err(failure(
                        "journal-truth",
                        event.id.to_string(),
                        "secondary stage fatal does not name an existing primary lifecycle failure",
                    ));
                }
            }
            None => {
                if !lifecycle_causes
                    .iter()
                    .any(|(_, stage, cause)| *stage == fatal.stage_id && *cause == event.id)
                {
                    return Err(failure(
                        "journal-truth",
                        event.id.to_string(),
                        "primary stage-fatal tail is not named by lifecycle failure evidence",
                    ));
                }
            }
        }
    }

    let failed_receipt_ids = chain_events
        .iter()
        .filter(|event| failed_receipt_disposition(event).is_some())
        .map(|event| event.id)
        .collect::<Vec<_>>();
    let operation_receipt_ids = chain_events
        .iter()
        .filter_map(SinkOperationFailed::from_event)
        .filter(|operation| matches!(operation.phase, SinkOperationPhase::Write(_)))
        .filter_map(|operation| operation.failed_delivery_event_id)
        .collect::<Vec<_>>();
    validate_write_operation_cardinality(&failed_receipt_ids, &operation_receipt_ids)?;

    let mut operation_failures = Vec::new();
    let mut failure_chains = Vec::new();
    for event in &chain_events {
        let Some(operation) = SinkOperationFailed::from_event(event) else {
            continue;
        };
        let projected = ProjectedSinkOperationFailure {
            event_id: event.id,
            stage_id: operation.stage_id,
            stage_key: operation.stage_key.clone(),
            logical_destination: operation.logical_destination.clone(),
            causal_event_id: operation.causal_event_id,
            input_position: operation.input_position,
            phase: operation.phase,
            failed_delivery_event_id: operation.failed_delivery_event_id,
            kind: operation.kind.clone(),
            destination_error_code: operation.destination_error_code.clone(),
        };

        if operation.stage_id != event.flow_context.stage_id
            || operation.stage_key != event.flow_context.stage_name
            || operation.logical_destination.is_empty()
            || event_error_kind(event) != Some(&operation.kind)
            || journal_routes.get(&event.id) != Some(&SinkJournalRoute::Error)
            || direct_parent(event) == Some(event.id)
        {
            return Err(failure(
                "journal-truth",
                event.id.to_string(),
                "operation failure fact has inconsistent stage, status, route, or identity",
            ));
        }

        let (disposition, receipt_to_operation, operation_to_route, route_to_lifecycle) = if matches!(
            operation.phase,
            SinkOperationPhase::Write(_)
        ) {
            let receipt_id = operation.failed_delivery_event_id.ok_or_else(|| {
                failure(
                    "journal-truth",
                    event.id.to_string(),
                    "write operation failure is missing its failed receipt id",
                )
            })?;
            let input_id = operation.causal_event_id.ok_or_else(|| {
                failure(
                    "journal-truth",
                    event.id.to_string(),
                    "write operation failure is missing its input id",
                )
            })?;
            if operation.input_position.is_none() {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "write operation failure is missing its input position",
                ));
            }
            let receipt = by_id.get(&receipt_id).ok_or_else(|| {
                failure(
                    "journal-truth",
                    event.id.to_string(),
                    "failed receipt is missing",
                )
            })?;
            let input = by_id.get(&input_id).ok_or_else(|| {
                failure(
                    "journal-truth",
                    event.id.to_string(),
                    "causal input is missing",
                )
            })?;
            let disposition = failed_receipt_disposition(receipt).ok_or_else(|| {
                failure(
                    "journal-truth",
                    event.id.to_string(),
                    "failed receipt has no recognised sink write disposition",
                )
            })?;
            let receipt_ok = is_failed_receipt(receipt)
                && journal_routes.get(&receipt.id) == Some(&SinkJournalRoute::Data)
                && direct_parent(receipt) == Some(input.id)
                && direct_parent(event) == Some(receipt.id)
                && receipt.flow_context.stage_id == operation.stage_id
                && matches!(
                    &receipt.content,
                    ChainEventContent::Delivery(payload)
                        if payload.destination == operation.logical_destination
                );
            if !receipt_ok {
                return Err(failure(
                        "journal-truth",
                        event.id.to_string(),
                        "write failure does not reconstruct I -> R -> O with the exact stage and destination",
                    ));
            }
            require_admission_order(receipt, event, "R-to-O")?;

            let routes = chain_events
                .iter()
                .filter(|candidate| direct_parent(candidate) == Some(event.id))
                .collect::<Vec<_>>();
            if routes.len() != 1 {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    format!(
                        "operation failure has {} direct route successors",
                        routes.len()
                    ),
                ));
            }
            let route = routes[0];
            let route_ok = route.id != input.id
                && route.id != receipt.id
                && route.id != event.id
                && same_data_payload(input, route)
                && inherited_route_context_matches(input, route)
                && route.flow_context.stage_id == operation.stage_id
                && event_error_kind(route) == Some(&operation.kind)
                && journal_routes.get(&route.id) == Some(&expected_error_route(&operation.kind));
            if !route_ok {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "operation successor is not a fresh, correctly routed error copy of the input",
                ));
            }
            require_admission_order(event, route, "O-to-X")?;

            let lifecycle_matches = lifecycle_causes
                .iter()
                .filter(|(_, stage, cause)| *stage == operation.stage_id && *cause == route.id)
                .count();
            let lifecycle_ok = match disposition {
                SinkWriteFailureDisposition::Poisoned => lifecycle_matches == 1,
                SinkWriteFailureDisposition::CurrentOnly
                | SinkWriteFailureDisposition::ConfirmedRollback => lifecycle_matches == 0,
            };
            if !lifecycle_ok {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "write disposition and lifecycle failure edge disagree",
                ));
            }
            (Some(disposition), true, true, lifecycle_matches == 1)
        } else {
            if operation.failed_delivery_event_id.is_some()
                || operation.causal_event_id.is_some()
                || operation.input_position.is_some()
                || !event.causality.parent_ids.is_empty()
            {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "lifecycle operation failure manufactured input or receipt causality",
                ));
            }
            let lifecycle_matches = lifecycle_causes
                .iter()
                .filter(|(_, stage, cause)| *stage == operation.stage_id && *cause == event.id)
                .count();
            if lifecycle_matches != 1 {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "lifecycle operation failure does not reconstruct O -> P exactly once",
                ));
            }
            (None, true, true, true)
        };

        failure_chains.push(SinkFailureChainVerdict {
            operation_event_id: event.id,
            disposition,
            receipt_to_operation,
            operation_to_route,
            route_to_lifecycle,
        });
        operation_failures.push(projected);
    }

    let operation_failure_metrics = fold_operation_failure_metrics(&operation_failures);
    let eof_kinds = chain_events
        .iter()
        .filter_map(|event| match &event.content {
            ChainEventContent::FlowControl(FlowControlPayload::Eof { kind, .. }) => Some(*kind),
            _ => None,
        })
        .collect();
    Ok(SinkRunEvidence {
        locator: CurrentRunLocator::new(run_dir.to_path_buf()),
        eof_kinds,
        operation_failures,
        operation_failure_metrics,
        failure_chains,
    })
}

pub async fn run_application_conformance<F: SinkApplicationConformanceFixture>(
    fixture: &mut F,
) -> Result<SinkConformanceReport, SinkConformanceFailure> {
    let profile = fixture.profile();
    if profile.protocol_version() != SINK_CONFORMANCE_PROTOCOL_VERSION {
        return Err(failure(
            "profile",
            "protocol-version",
            format!(
                "fixture protocol {} does not match harness protocol {}",
                profile.protocol_version(),
                SINK_CONFORMANCE_PROTOCOL_VERSION
            ),
        ));
    }

    let mut cases = Vec::new();
    let mut runs = Vec::new();
    for scenario in required_scenarios() {
        fixture
            .reset_destination()
            .await
            .map_err(|error| failure("application", scenario.name(), error.to_string()))?;
        let before = fixture
            .verifier()
            .snapshot()
            .await
            .map_err(|error| failure("application", scenario.name(), error.to_string()))?;
        let calls_before = fixture
            .verifier()
            .external_calls()
            .map_err(|error| failure("application", scenario.name(), error.to_string()))?;
        let build_case = fixture
            .build_case(scenario)
            .map_err(|error| failure("application", scenario.name(), error.to_string()))?;
        let (flow, archive_root, cli_args) = build_case.into_parts();
        let result = FlowApplication::builder()
            .with_cli_args(cli_args)
            .run_async(flow)
            .await;

        if scenario.expects_refusal() {
            if result.is_ok() {
                return Err(failure(
                    "archive-gate",
                    scenario.name(),
                    "duplicate-sensitive or unspecified redelivery was not refused",
                ));
            }
        } else if scenario.eof_kind != EofKind::Poison && result.is_err() {
            return Err(failure(
                "application",
                scenario.name(),
                result.expect_err("checked error").to_string(),
            ));
        }

        let after = fixture
            .verifier()
            .snapshot()
            .await
            .map_err(|error| failure("application", scenario.name(), error.to_string()))?;
        let calls_after = fixture
            .verifier()
            .external_calls()
            .map_err(|error| failure("application", scenario.name(), error.to_string()))?;
        if scenario.expects_refusal() && calls_before != calls_after {
            return Err(failure(
                "archive-gate",
                scenario.name(),
                "refused redelivery made destination calls",
            ));
        } else if !scenario.expects_refusal() && calls_before == calls_after {
            return Err(failure(
                "application",
                scenario.name(),
                "executed application scenario made no connector or destination calls",
            ));
        }

        let verdict = if scenario.expects_refusal() {
            SinkDestinationVerdict::Refused
        } else if scenario.eof_kind == EofKind::Poison {
            SinkDestinationVerdict::Failed
        } else if scenario.treatment == SinkApplicationTreatment::Live {
            SinkDestinationVerdict::Committed
        } else {
            SinkDestinationVerdict::Converged
        };
        fixture
            .verifier()
            .verify(
                SinkDestinationExpectation { scenario, verdict },
                &before,
                &after,
            )
            .await
            .map_err(|error| failure("destination", scenario.name(), error.to_string()))?;

        let archive = if scenario.expects_refusal() {
            None
        } else {
            let run_dir = latest_run_dir(&archive_root).ok_or_else(|| {
                failure(
                    "archive",
                    scenario.name(),
                    "application produced no durable run manifest",
                )
            })?;
            let evidence = project_run(&run_dir).await?;
            if !evidence.eof_kinds().contains(&scenario.eof_kind()) {
                return Err(failure(
                    "journal-truth",
                    scenario.name(),
                    format!("archive contains no {:?} EOF evidence", scenario.eof_kind()),
                ));
            }
            runs.push(evidence);
            Some(run_dir)
        };
        cases.push(SinkConformanceCaseResult { scenario, archive });
    }

    Ok(SinkConformanceReport {
        protocol_version: SINK_CONFORMANCE_PROTOCOL_VERSION,
        cases,
        runs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outward_manifest_gate_rejects_every_non_exact_raw_shape() {
        for raw in [
            r#"{}"#,
            r#"{"manifest_version":3.0}"#,
            r#"{"manifest_version":"2.0"}"#,
            r#"{"manifest_version":"4.0"}"#,
            r#"{"manifest_version":null}"#,
        ] {
            let error = parse_current_manifest(raw).expect_err("non-exact epoch must fail");
            assert_eq!(error.suite(), "archive");
            assert_eq!(error.case(), "manifest-version");
        }
    }

    #[test]
    fn outward_projection_rejects_missing_duplicate_and_orphan_operation_facts() {
        let receipt = EventId::new();
        assert!(validate_write_operation_cardinality(&[receipt], &[receipt]).is_ok());

        let missing = validate_write_operation_cardinality(&[receipt], &[])
            .expect_err("missing operation fact must fail");
        assert!(missing.detail().contains("no SinkOperationFailed"));

        let duplicate = validate_write_operation_cardinality(&[receipt], &[receipt, receipt])
            .expect_err("duplicate operation fact must fail");
        assert!(duplicate.detail().contains("2 SinkOperationFailed"));

        let orphan = EventId::new();
        let orphan = validate_write_operation_cardinality(&[receipt], &[receipt, orphan])
            .expect_err("orphan operation fact must fail");
        assert!(orphan
            .detail()
            .contains("no recognised failed sink receipt"));
    }
}
