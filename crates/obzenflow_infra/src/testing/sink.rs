// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Outward application certification for production sink connectors.

use crate::application::{CurrentRunLocator, FlowApplication};
use crate::journal::DiskJournal;
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
use obzenflow_runtime::stages::sink::{SinkDestinationErrorCode, SinkWriteFailureDisposition};
use obzenflow_runtime::testing::sink::{
    SinkConformanceProfile, SinkDiagnosticSample, SinkExternalCallKind, SinkExternalCallSnapshot,
    SinkFixtureError, SINK_CONFORMANCE_PROTOCOL_VERSION,
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

    /// Additional connector-, error-, trace-, snapshot-, or verifier-shaped
    /// text that the fixture can expose to the harness's credential canary.
    fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
        Ok(Vec::new())
    }
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
    operation_subject_event_id: Option<EventId>,
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

    pub fn operation_subject_event_id(&self) -> Option<EventId> {
        self.operation_subject_event_id
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
    completed_sink_count: usize,
    failed_sink_count: usize,
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

    pub fn completed_sink_count(&self) -> usize {
        self.completed_sink_count
    }

    pub fn failed_sink_count(&self) -> usize {
        self.failed_sink_count
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

fn reject_credential_text(
    profile: &SinkConformanceProfile,
    case: impl Into<String>,
    text: &str,
) -> Result<(), SinkConformanceFailure> {
    let case = case.into();
    for (index, sentinel) in profile.credential_sentinels().iter().enumerate() {
        if !sentinel.is_empty() && text.contains(sentinel) {
            return Err(failure(
                "credential-redaction",
                &case,
                format!("credential sentinel #{index} appeared in captured text"),
            ));
        }
    }
    Ok(())
}

fn reject_error_diagnostics(
    profile: &SinkConformanceProfile,
    case: &str,
    error: &(dyn std::error::Error + 'static),
) -> Result<(), SinkConformanceFailure> {
    let mut source = Some(error);
    for depth in 0..64 {
        let Some(error) = source else {
            return Ok(());
        };
        reject_credential_text(
            profile,
            format!("{case}-error-source-{depth}-display"),
            &error.to_string(),
        )?;
        reject_credential_text(
            profile,
            format!("{case}-error-source-{depth}-debug"),
            &format!("{error:?}"),
        )?;
        source = error.source();
    }
    Err(failure(
        "credential-redaction",
        case,
        "error source chain exceeded the diagnostic scan limit",
    ))
}

fn failure_from_error(
    profile: &SinkConformanceProfile,
    suite: &'static str,
    case: impl Into<String>,
    error: &(dyn std::error::Error + 'static),
) -> SinkConformanceFailure {
    let case = case.into();
    match reject_error_diagnostics(profile, &case, error) {
        Ok(()) => failure(suite, case, error.to_string()),
        Err(redaction_failure) => redaction_failure,
    }
}

fn reject_credential_bytes(
    profile: &SinkConformanceProfile,
    case: impl Into<String>,
    bytes: &[u8],
) -> Result<(), SinkConformanceFailure> {
    let case = case.into();
    for (index, sentinel) in profile.credential_sentinels().iter().enumerate() {
        let needle = sentinel.as_bytes();
        if !needle.is_empty() && bytes.windows(needle.len()).any(|window| window == needle) {
            return Err(failure(
                "credential-redaction",
                &case,
                format!("credential sentinel #{index} appeared in a durable artifact"),
            ));
        }
    }
    Ok(())
}

fn scan_durable_tree_for_credentials(
    profile: &SinkConformanceProfile,
    root: &Path,
    case: &str,
) -> Result<(), SinkConformanceFailure> {
    if profile.credential_sentinels().is_empty() || !root.exists() {
        return Ok(());
    }
    let metadata = std::fs::metadata(root)
        .map_err(|error| failure_from_error(profile, "credential-redaction", case, &error))?;
    if metadata.is_file() {
        let bytes = std::fs::read(root)
            .map_err(|error| failure_from_error(profile, "credential-redaction", case, &error))?;
        return reject_credential_bytes(profile, case, &bytes);
    }
    for entry in std::fs::read_dir(root)
        .map_err(|error| failure_from_error(profile, "credential-redaction", case, &error))?
    {
        let path = entry
            .map_err(|error| failure_from_error(profile, "credential-redaction", case, &error))?
            .path();
        scan_durable_tree_for_credentials(profile, &path, case)?;
    }
    Ok(())
}

fn replay_archive_arg(args: &[OsString]) -> Option<PathBuf> {
    for (index, arg) in args.iter().enumerate() {
        if arg == "--replay-from" {
            return args.get(index + 1).map(PathBuf::from);
        }
        if let Some(value) = arg.to_string_lossy().strip_prefix("--replay-from=") {
            return Some(PathBuf::from(value));
        }
    }
    None
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

#[derive(Debug)]
struct ChainEventProjection {
    event_index: usize,
    route: SinkJournalRoute,
    stage_keys: Vec<String>,
}

fn normalised_forwarded_control(
    event: &ChainEvent,
) -> Result<Option<serde_json::Value>, SinkConformanceFailure> {
    if !matches!(event.content, ChainEventContent::FlowControl(_)) {
        return Ok(None);
    }

    let mut value = serde_json::to_value(event)
        .map_err(|error| failure("journal-truth", "event-identity", error.to_string()))?;
    let object = value.as_object_mut().ok_or_else(|| {
        failure(
            "journal-truth",
            "event-identity",
            "serialised ChainEvent is not an object",
        )
    })?;
    object.remove("runtime_context");
    let flow_context = object
        .get_mut("flow_context")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(|| {
            failure(
                "journal-truth",
                "event-identity",
                "serialised ChainEvent has no flow context",
            )
        })?;
    for field in ["stage_name", "stage_id", "stage_type"] {
        flow_context.remove(field);
    }
    Ok(Some(value))
}

fn record_chain_event_projection(
    chain_events: &mut Vec<ChainEvent>,
    projections: &mut HashMap<EventId, ChainEventProjection>,
    stage_key: &str,
    route: SinkJournalRoute,
    event: ChainEvent,
) -> Result<(), SinkConformanceFailure> {
    let Some(existing) = projections.get_mut(&event.id) else {
        let event_index = chain_events.len();
        projections.insert(
            event.id,
            ChainEventProjection {
                event_index,
                route,
                stage_keys: vec![stage_key.to_string()],
            },
        );
        chain_events.push(event);
        return Ok(());
    };

    if existing.stage_keys.iter().any(|key| key == stage_key) {
        return Err(failure(
            "journal-truth",
            "event-identity",
            format!(
                "duplicate ChainEvent {} ({}) within the {route:?} journal for stage {stage_key}",
                event.id,
                event.event_type()
            ),
        ));
    }

    let canonical = &chain_events[existing.event_index];
    let equivalent_forward = existing.route == SinkJournalRoute::Data
        && route == SinkJournalRoute::Data
        && normalised_forwarded_control(canonical)?
            .zip(normalised_forwarded_control(&event)?)
            .is_some_and(|(left, right)| left == right);
    if !equivalent_forward {
        return Err(failure(
            "journal-truth",
            "event-identity",
            format!(
                "conflicting reuse of ChainEvent {} ({}) between stages {} ({:?}) and {stage_key} ({route:?})",
                event.id,
                event.event_type(),
                existing.stage_keys.join(", "),
                existing.route
            ),
        ));
    }

    let canonical_is_authored_here =
        canonical.writer_id.as_stage() == Some(&canonical.flow_context.stage_id);
    let current_is_authored_here = event.writer_id.as_stage() == Some(&event.flow_context.stage_id);
    if current_is_authored_here && !canonical_is_authored_here {
        chain_events[existing.event_index] = event;
    }
    existing.stage_keys.push(stage_key.to_string());
    Ok(())
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

fn operation_failure_identity_matches(
    event: &ChainEvent,
    operation: &SinkOperationFailed,
    route: Option<&SinkJournalRoute>,
) -> bool {
    operation.stage_id == event.flow_context.stage_id
        && operation.stage_key == event.flow_context.stage_name
        && !operation.logical_destination.is_empty()
        && event_error_kind(event) == Some(&operation.kind)
        && route == Some(&SinkJournalRoute::Error)
        && !event.causality.parent_ids.contains(&event.id)
}

fn write_failure_receipt_matches(
    input: &ChainEvent,
    receipt: &ChainEvent,
    operation_event: &ChainEvent,
    operation: &SinkOperationFailed,
    receipt_route: Option<&SinkJournalRoute>,
) -> bool {
    is_failed_receipt(receipt)
        && receipt_route == Some(&SinkJournalRoute::Data)
        && direct_parent(receipt) == Some(input.id)
        && direct_parent(operation_event) == Some(receipt.id)
        && receipt.flow_context.stage_id == operation.stage_id
        && matches!(
            &receipt.content,
            ChainEventContent::Delivery(payload)
                if payload.destination == operation.logical_destination
        )
}

fn write_failure_route_matches(
    input: &ChainEvent,
    receipt: &ChainEvent,
    operation_event: &ChainEvent,
    operation: &SinkOperationFailed,
    route: &ChainEvent,
    journal_route: Option<&SinkJournalRoute>,
) -> bool {
    route.id != input.id
        && route.id != receipt.id
        && route.id != operation_event.id
        && !route.causality.parent_ids.contains(&route.id)
        && direct_parent(route) == Some(operation_event.id)
        && same_data_payload(input, route)
        && inherited_route_context_matches(input, route)
        && route.flow_context.stage_id == operation.stage_id
        && event_error_kind(route) == Some(&operation.kind)
        && journal_route == Some(&expected_error_route(&operation.kind))
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

fn validate_sink_lifecycle_projection(
    manifest: &RunManifest,
    system_events: &[SystemEvent],
) -> Result<(usize, usize), SinkConformanceFailure> {
    let mut completed_sink_count = 0;
    let mut failed_sink_count = 0;
    for (stage_key, stage) in manifest
        .stages
        .iter()
        .filter(|(_, stage)| stage.stage_type == obzenflow_core::event::context::StageType::Sink)
    {
        let lifecycle = system_events
            .iter()
            .enumerate()
            .filter_map(|(index, event)| match &event.event {
                SystemEventType::StageLifecycle {
                    stage_id,
                    event: lifecycle,
                } if stage_id.to_string() == stage.stage_id => Some((index, lifecycle)),
                _ => None,
            })
            .collect::<Vec<_>>();
        let running = lifecycle
            .iter()
            .filter(|(_, event)| matches!(event, StageLifecycleEvent::Running))
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let completed = lifecycle
            .iter()
            .filter(|(_, event)| matches!(event, StageLifecycleEvent::Completed { .. }))
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let failed = lifecycle
            .iter()
            .filter(|(_, event)| matches!(event, StageLifecycleEvent::Failed { .. }))
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        if running.len() != 1 {
            return Err(failure(
                "lifecycle",
                stage_key,
                format!("sink has {} Running lifecycle facts", running.len()),
            ));
        }
        if completed.len() + failed.len() != 1 {
            return Err(failure(
                "lifecycle",
                stage_key,
                format!(
                    "sink must have exactly one Completed-or-Failed terminal fact, observed {} Completed and {} Failed",
                    completed.len(),
                    failed.len()
                ),
            ));
        }
        let terminal = completed
            .first()
            .or_else(|| failed.first())
            .copied()
            .unwrap();
        if running[0] >= terminal {
            return Err(failure(
                "lifecycle",
                stage_key,
                "sink terminal lifecycle fact preceded Running",
            ));
        }
        completed_sink_count += completed.len();
        failed_sink_count += failed.len();
    }
    Ok((completed_sink_count, failed_sink_count))
}

fn validate_external_call_lifecycle(
    before: &SinkExternalCallSnapshot,
    after: &SinkExternalCallSnapshot,
    verdict: SinkDestinationVerdict,
    case: &str,
) -> Result<(), SinkConformanceFailure> {
    let Some(delta) = after.calls().strip_prefix(before.calls()) else {
        return Err(failure(
            "lifecycle",
            case,
            "external-call snapshot did not preserve its pre-launch prefix",
        ));
    };
    if verdict == SinkDestinationVerdict::Refused {
        if delta.is_empty() {
            return Ok(());
        }
        return Err(failure(
            "lifecycle",
            case,
            "refused application created a writer lifecycle",
        ));
    }
    if delta.is_empty() {
        return Err(failure(
            "lifecycle",
            case,
            "executed application captured no writer lifecycle",
        ));
    }
    if delta
        .windows(2)
        .any(|window| window[0].sequence() >= window[1].sequence())
    {
        return Err(failure(
            "lifecycle",
            case,
            "external-call sequence is not strictly increasing",
        ));
    }

    let mut by_writer = HashMap::<u64, Vec<SinkExternalCallKind>>::new();
    for call in delta {
        by_writer
            .entry(call.writer())
            .or_default()
            .push(call.kind());
    }
    for (writer, calls) in by_writer {
        if calls.first() != Some(&SinkExternalCallKind::Open)
            || calls.last() != Some(&SinkExternalCallKind::Drop)
        {
            return Err(failure(
                "lifecycle",
                case,
                format!("writer {writer} is not bounded by Open and Drop: {calls:?}"),
            ));
        }
        let flush = calls
            .iter()
            .position(|kind| *kind == SinkExternalCallKind::Flush);
        let drain = calls
            .iter()
            .position(|kind| *kind == SinkExternalCallKind::Drain);
        match verdict {
            SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged => {
                if flush.zip(drain).is_none_or(|(flush, drain)| flush >= drain) {
                    return Err(failure(
                        "lifecycle",
                        case,
                        format!(
                            "writer {writer} did not complete Flush -> Drain -> Drop: {calls:?}"
                        ),
                    ));
                }
            }
            SinkDestinationVerdict::Failed => {
                if flush.is_some() || drain.is_some() {
                    return Err(failure(
                        "lifecycle",
                        case,
                        format!(
                            "failed writer {writer} performed lifecycle I/O before Drop: {calls:?}"
                        ),
                    ));
                }
            }
            SinkDestinationVerdict::Refused => unreachable!("handled before writer grouping"),
        }
    }
    Ok(())
}

async fn project_run(run_dir: &Path) -> Result<SinkRunEvidence, SinkConformanceFailure> {
    let manifest_path = run_dir.join(RUN_MANIFEST_FILENAME);
    let raw = std::fs::read_to_string(&manifest_path)
        .map_err(|error| failure("archive", "manifest-read", error.to_string()))?;
    let manifest = parse_current_manifest(&raw)?;

    let mut chain_events = Vec::new();
    let mut projections = HashMap::new();
    for (stage_key, stage) in &manifest.stages {
        let data_events = read_chain_journal(&run_dir.join(&stage.data_journal_file)).await?;
        for event in data_events {
            record_chain_event_projection(
                &mut chain_events,
                &mut projections,
                stage_key,
                SinkJournalRoute::Data,
                event,
            )?;
        }
        let error_events = read_chain_journal(&run_dir.join(&stage.error_journal_file)).await?;
        for event in error_events {
            record_chain_event_projection(
                &mut chain_events,
                &mut projections,
                stage_key,
                SinkJournalRoute::Error,
                event,
            )?;
        }
    }
    let system_events = read_system_journal(&run_dir.join(&manifest.system_journal_file)).await?;
    let (completed_sink_count, failed_sink_count) =
        validate_sink_lifecycle_projection(&manifest, &system_events)?;

    let journal_routes = projections
        .iter()
        .map(|(event_id, projection)| (*event_id, projection.route))
        .collect::<HashMap<_, _>>();

    let by_id = chain_events
        .iter()
        .map(|event| (event.id, event))
        .collect::<HashMap<_, _>>();
    debug_assert_eq!(by_id.len(), chain_events.len());

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
            operation_subject_event_id: operation.operation_subject_event_id,
            kind: operation.kind.clone(),
            destination_error_code: operation.destination_error_code.clone(),
        };

        if !operation_failure_identity_matches(event, &operation, journal_routes.get(&event.id)) {
            return Err(failure(
                "journal-truth",
                event.id.to_string(),
                "operation failure fact has inconsistent stage, status, route, or identity",
            ));
        }

        if let Some(subject_id) = operation.operation_subject_event_id {
            if subject_id == event.id
                || operation.causal_event_id == Some(subject_id)
                || operation.failed_delivery_event_id == Some(subject_id)
                || !by_id.contains_key(&subject_id)
            {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "operation subject is missing, current, self-referential, or a receipt",
                ));
            }
            let buffered_receipts = chain_events
                .iter()
                .filter(|candidate| {
                    candidate.flow_context.stage_id == operation.stage_id
                        && direct_parent(candidate) == Some(subject_id)
                        && matches!(
                            &candidate.content,
                            ChainEventContent::Delivery(payload)
                                if matches!(payload.result, DeliveryResult::Buffered { .. })
                        )
                })
                .count();
            let terminal_receipts = chain_events
                .iter()
                .filter(|candidate| {
                    candidate.flow_context.stage_id == operation.stage_id
                        && direct_parent(candidate) == Some(subject_id)
                        && matches!(
                            &candidate.content,
                            ChainEventContent::Delivery(payload)
                                if matches!(
                                    payload.result,
                                    DeliveryResult::Success { .. }
                                        | DeliveryResult::Partial { .. }
                                        | DeliveryResult::Failed { .. }
                                )
                        )
                })
                .count();
            if buffered_receipts != 1 || terminal_receipts != 0 {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "operation subject is not one unresolved deferred input",
                ));
            }
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
            if operation.operation_subject_event_id.is_some()
                && disposition != SinkWriteFailureDisposition::Poisoned
            {
                return Err(failure(
                    "journal-truth",
                    event.id.to_string(),
                    "a deferred operation subject requires a poisoned write receipt",
                ));
            }
            let receipt_ok = write_failure_receipt_matches(
                input,
                receipt,
                event,
                &operation,
                journal_routes.get(&receipt.id),
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
            let route_ok = write_failure_route_matches(
                input,
                receipt,
                event,
                &operation,
                route,
                journal_routes.get(&route.id),
            );
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
        completed_sink_count,
        failed_sink_count,
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
    for sample in fixture.diagnostic_samples().map_err(|error| {
        failure_from_error(
            &profile,
            "credential-redaction",
            "diagnostic-samples",
            &error,
        )
    })? {
        reject_credential_text(
            &profile,
            format!("diagnostic-{:?}", sample.surface()),
            sample.text(),
        )?;
    }

    let mut cases = Vec::new();
    let mut runs = Vec::new();
    for scenario in required_scenarios() {
        let scenario_name = scenario.name();
        fixture
            .reset_destination()
            .await
            .map_err(|error| failure_from_error(&profile, "application", &scenario_name, &error))?;
        let before =
            fixture.verifier().snapshot().await.map_err(|error| {
                failure_from_error(&profile, "application", &scenario_name, &error)
            })?;
        let calls_before = fixture
            .verifier()
            .external_calls()
            .map_err(|error| failure_from_error(&profile, "application", &scenario_name, &error))?;
        reject_credential_text(
            &profile,
            format!("{scenario_name}-before-snapshot"),
            &format!("{before:?}"),
        )?;
        reject_credential_text(
            &profile,
            format!("{scenario_name}-before-calls"),
            &format!("{calls_before:?}"),
        )?;
        let build_case = fixture
            .build_case(scenario)
            .map_err(|error| failure_from_error(&profile, "application", &scenario_name, &error))?;
        let (flow, archive_root, cli_args) = build_case.into_parts();
        if let Some(source_archive) = replay_archive_arg(&cli_args) {
            scan_durable_tree_for_credentials(
                &profile,
                &source_archive,
                &format!("{scenario_name}-source-archive"),
            )?;
        }
        let result = FlowApplication::builder()
            .with_cli_args(cli_args)
            .run_async(flow)
            .await;
        reject_credential_text(
            &profile,
            format!("{scenario_name}-application-result"),
            &format!("{result:?}"),
        )?;
        if let Err(error) = &result {
            reject_error_diagnostics(
                &profile,
                &format!("{scenario_name}-application-result"),
                error,
            )?;
        }

        if scenario.expects_refusal() {
            if result.is_ok() {
                return Err(failure(
                    "archive-gate",
                    &scenario_name,
                    "duplicate-sensitive or unspecified redelivery was not refused",
                ));
            }
        } else if scenario.eof_kind != EofKind::Poison {
            if let Err(error) = &result {
                return Err(failure_from_error(
                    &profile,
                    "application",
                    &scenario_name,
                    error,
                ));
            }
        }

        let after =
            fixture.verifier().snapshot().await.map_err(|error| {
                failure_from_error(&profile, "application", &scenario_name, &error)
            })?;
        let calls_after = fixture
            .verifier()
            .external_calls()
            .map_err(|error| failure_from_error(&profile, "application", &scenario_name, &error))?;
        reject_credential_text(
            &profile,
            format!("{scenario_name}-after-snapshot"),
            &format!("{after:?}"),
        )?;
        reject_credential_text(
            &profile,
            format!("{scenario_name}-after-calls"),
            &format!("{calls_after:?}"),
        )?;
        if scenario.expects_refusal() && calls_before != calls_after {
            return Err(failure(
                "archive-gate",
                &scenario_name,
                "refused redelivery made destination calls",
            ));
        } else if !scenario.expects_refusal() && calls_before == calls_after {
            return Err(failure(
                "application",
                &scenario_name,
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
        validate_external_call_lifecycle(&calls_before, &calls_after, verdict, &scenario_name)?;
        fixture
            .verifier()
            .verify(
                SinkDestinationExpectation { scenario, verdict },
                &before,
                &after,
            )
            .await
            .map_err(|error| failure_from_error(&profile, "destination", &scenario_name, &error))?;

        let archive = if scenario.expects_refusal() {
            None
        } else {
            let run_dir = latest_run_dir(&archive_root).ok_or_else(|| {
                failure(
                    "archive",
                    &scenario_name,
                    "application produced no durable run manifest",
                )
            })?;
            scan_durable_tree_for_credentials(
                &profile,
                &run_dir,
                &format!("{scenario_name}-output-archive"),
            )?;
            let evidence = project_run(&run_dir).await?;
            reject_credential_text(
                &profile,
                format!("{scenario_name}-projected-evidence"),
                &format!("{evidence:?}"),
            )?;
            if !evidence.eof_kinds().contains(&scenario.eof_kind()) {
                return Err(failure(
                    "journal-truth",
                    &scenario_name,
                    format!("archive contains no {:?} EOF evidence", scenario.eof_kind()),
                ));
            }
            match verdict {
                SinkDestinationVerdict::Failed
                    if evidence.failed_sink_count() == 0
                        || evidence.completed_sink_count() != 0 =>
                {
                    return Err(failure(
                        "lifecycle",
                        &scenario_name,
                        "failed treatment did not retain Failed-only sink lifecycle evidence",
                    ));
                }
                SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged
                    if evidence.completed_sink_count() == 0
                        || evidence.failed_sink_count() != 0 =>
                {
                    return Err(failure(
                        "lifecycle",
                        &scenario_name,
                        "successful treatment did not retain Completed-only sink lifecycle evidence",
                    ));
                }
                _ => {}
            }
            runs.push(evidence);
            Some(run_dir)
        };
        cases.push(SinkConformanceCaseResult { scenario, archive });
    }

    let report = SinkConformanceReport {
        protocol_version: SINK_CONFORMANCE_PROTOCOL_VERSION,
        cases,
        runs,
    };
    reject_credential_text(&profile, "conformance-report", &format!("{report:?}"))?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::disk_journals;
    use obzenflow_adapters::sources;
    use obzenflow_core::event::context::causality_context::CausalityContext;
    use obzenflow_core::event::context::{FlowContext, StageType};
    use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::WriterId;
    use obzenflow_dsl::{flow, sink, source};
    use obzenflow_runtime::stages::sink::SinkTyped;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ProjectionInput(u64);

    impl TypedPayload for ProjectionInput {
        const EVENT_TYPE: &'static str = "sink_conformance.projection_input";
    }

    #[test]
    fn outward_redaction_rejects_canaries_without_reflecting_them() {
        let sentinel = "sink-conformance-secret-canary";
        let profile = SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            obzenflow_runtime::testing::sink::SinkSettlementMode::Terminal,
        )
        .with_credential_sentinel(sentinel);

        let error = crate::application::ApplicationError::Other(Box::new(SinkFixtureError::new(
            format!("nested source contains {sentinel}"),
        )));
        let failure = failure_from_error(&profile, "application", "nested-error", &error);
        assert_eq!(failure.suite(), "credential-redaction");
        assert!(!failure.to_string().contains(sentinel));

        let temp = tempfile::tempdir().expect("temporary credential scan root");
        let artifact = temp.path().join("artifact");
        std::fs::write(&artifact, format!("durable {sentinel}"))
            .expect("write credential canary artifact");
        let failure = scan_durable_tree_for_credentials(&profile, &artifact, "durable-artifact")
            .expect_err("durable credential canary must be rejected");
        assert_eq!(failure.suite(), "credential-redaction");
        assert!(!failure.to_string().contains(sentinel));
    }

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

    struct FailureProjectionFixture {
        input: ChainEvent,
        receipt: ChainEvent,
        operation_event: ChainEvent,
        operation: SinkOperationFailed,
        route: ChainEvent,
    }

    fn failure_projection_fixture() -> FailureProjectionFixture {
        let source_stage = StageId::new();
        let sink_stage = StageId::new();
        let context = |stage_name: &str, stage_id, stage_type| FlowContext {
            flow_name: "failure-projection".to_string(),
            flow_id: "flow_failure_projection".to_string(),
            stage_name: stage_name.to_string(),
            stage_id,
            stage_type,
        };
        let mut input = ChainEventFactory::data_event(
            WriterId::from(source_stage),
            "failure.projection.input.v1",
            serde_json::json!({"id": 7}),
        )
        .with_flow_context(context("inputs", source_stage, StageType::FiniteSource));
        input.admission_seq = Some(AdmissionSeq(1));

        let mut receipt_payload = DeliveryPayload::failed(
            DeliveryMethod::Noop,
            "sink_materialisation_poisoned",
            "redacted failure",
        );
        receipt_payload.destination = "projection-output".to_string();
        let mut receipt =
            ChainEventFactory::delivery_event(WriterId::from(sink_stage), receipt_payload)
                .with_flow_context(context("output", sink_stage, StageType::Sink))
                .with_causality(CausalityContext::with_parent(input.id));
        receipt.admission_seq = Some(AdmissionSeq(2));

        let operation = SinkOperationFailed {
            stage_id: sink_stage,
            stage_key: "output".to_string(),
            logical_destination: "projection-output".to_string(),
            causal_event_id: Some(input.id),
            input_position: Some(7),
            failed_delivery_event_id: Some(receipt.id),
            operation_subject_event_id: None,
            phase: SinkOperationPhase::Write(obzenflow_core::event::SinkWritePhase::Commit),
            kind: ErrorKind::Remote,
            destination_error_code: None,
            detail: "redacted failure".to_string(),
        };
        let mut operation_event = ChainEventFactory::data_event(
            WriterId::from(sink_stage),
            SinkOperationFailed::versioned_event_type(),
            serde_json::to_value(&operation).expect("operation serialises"),
        )
        .with_flow_context(context("output", sink_stage, StageType::Sink))
        .with_causality(CausalityContext::with_parent(receipt.id))
        .mark_as_error("redacted failure", ErrorKind::Remote);
        operation_event.admission_seq = Some(AdmissionSeq(3));

        let mut route = ChainEventFactory::data_event(
            WriterId::from(sink_stage),
            "failure.projection.input.v1",
            serde_json::json!({"id": 7}),
        )
        .with_flow_context(context("output", sink_stage, StageType::Sink))
        .with_causality(CausalityContext::with_parent(operation_event.id))
        .mark_as_error("redacted failure", ErrorKind::Remote);
        route.admission_seq = Some(AdmissionSeq(4));

        FailureProjectionFixture {
            input,
            receipt,
            operation_event,
            operation,
            route,
        }
    }

    #[test]
    fn outward_projection_rejects_wrong_stage_and_self_cycles() {
        let fixture = failure_projection_fixture();
        assert!(operation_failure_identity_matches(
            &fixture.operation_event,
            &fixture.operation,
            Some(&SinkJournalRoute::Error),
        ));

        let mut wrong_stage = fixture.operation.clone();
        wrong_stage.stage_id = StageId::new();
        assert!(!operation_failure_identity_matches(
            &fixture.operation_event,
            &wrong_stage,
            Some(&SinkJournalRoute::Error),
        ));

        let mut self_cycle = fixture.operation_event.clone();
        self_cycle.causality.parent_ids.push(self_cycle.id);
        assert!(!operation_failure_identity_matches(
            &self_cycle,
            &fixture.operation,
            Some(&SinkJournalRoute::Error),
        ));
    }

    #[test]
    fn outward_projection_rejects_wrong_type_reuse_and_route_corruption() {
        let fixture = failure_projection_fixture();
        assert!(write_failure_receipt_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            Some(&SinkJournalRoute::Data),
        ));
        assert!(write_failure_route_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            &fixture.route,
            Some(&SinkJournalRoute::Error),
        ));

        let mut wrong_type = fixture.route.clone();
        if let ChainEventContent::Data { event_type, .. } = &mut wrong_type.content {
            *event_type = "failure.projection.wrong.v1".to_string();
        }
        assert!(!write_failure_route_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            &wrong_type,
            Some(&SinkJournalRoute::Error),
        ));

        let mut reused_input = fixture.route.clone();
        reused_input.id = fixture.input.id;
        assert!(!write_failure_route_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            &reused_input,
            Some(&SinkJournalRoute::Error),
        ));

        let mut wrong_stage = fixture.route.clone();
        wrong_stage.flow_context.stage_id = StageId::new();
        assert!(!write_failure_route_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            &wrong_stage,
            Some(&SinkJournalRoute::Error),
        ));
        assert!(!write_failure_route_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            &fixture.route,
            Some(&SinkJournalRoute::Data),
        ));

        let mut self_cycle = fixture.route.clone();
        self_cycle.causality.parent_ids.push(self_cycle.id);
        assert!(!write_failure_route_matches(
            &fixture.input,
            &fixture.receipt,
            &fixture.operation_event,
            &fixture.operation,
            &self_cycle,
            Some(&SinkJournalRoute::Error),
        ));
    }

    #[test]
    fn outward_projection_rejects_reversed_or_missing_admission_order() {
        let mut fixture = failure_projection_fixture();
        require_admission_order(&fixture.receipt, &fixture.operation_event, "R-to-O")
            .expect("canonical receipt-to-operation order");
        require_admission_order(&fixture.operation_event, &fixture.route, "O-to-X")
            .expect("canonical operation-to-route order");

        fixture.operation_event.admission_seq = Some(AdmissionSeq(1));
        assert!(
            require_admission_order(&fixture.receipt, &fixture.operation_event, "R-to-O").is_err()
        );
        fixture.route.admission_seq = None;
        assert!(
            require_admission_order(&fixture.operation_event, &fixture.route, "O-to-X").is_err()
        );
    }

    #[test]
    fn outward_projection_coalesces_only_equivalent_forwarded_controls() {
        let source_stage = StageId::new();
        let sink_stage = StageId::new();
        let flow_context = |stage_name: &str, stage_id, stage_type| FlowContext {
            flow_name: "projection-test".to_string(),
            flow_id: "flow_projection_test".to_string(),
            stage_name: stage_name.to_string(),
            stage_id,
            stage_type,
        };
        let authored =
            ChainEventFactory::drain_event(WriterId::from(source_stage)).with_flow_context(
                flow_context("inputs", source_stage, StageType::FiniteSource),
            );
        let mut forwarded =
            authored
                .clone()
                .with_flow_context(flow_context("output", sink_stage, StageType::Sink));
        forwarded.runtime_context = None;

        let mut events = Vec::new();
        let mut projections = HashMap::new();
        record_chain_event_projection(
            &mut events,
            &mut projections,
            "output",
            SinkJournalRoute::Data,
            forwarded.clone(),
        )
        .expect("forwarded projection is recorded first");
        record_chain_event_projection(
            &mut events,
            &mut projections,
            "inputs",
            SinkJournalRoute::Data,
            authored.clone(),
        )
        .expect("equivalent authored projection coalesces");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].flow_context.stage_id, source_stage);

        let same_stage = record_chain_event_projection(
            &mut events,
            &mut projections,
            "inputs",
            SinkJournalRoute::Data,
            authored.clone(),
        )
        .expect_err("one physical stage projection cannot repeat an id");
        assert!(same_stage.detail().contains("within the Data journal"));

        let mut conflicting = forwarded;
        conflicting.causality.parent_ids.push(EventId::new());
        let conflict = record_chain_event_projection(
            &mut events,
            &mut projections,
            "other",
            SinkJournalRoute::Data,
            conflicting,
        )
        .expect_err("same-id control with different durable content must fail");
        assert!(conflict.detail().contains("conflicting reuse"));

        let route_conflict = record_chain_event_projection(
            &mut events,
            &mut projections,
            "errors",
            SinkJournalRoute::Error,
            authored,
        )
        .expect_err("a data projection cannot reappear on the error route");
        assert!(route_conflict.detail().contains("conflicting reuse"));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn outward_projection_accepts_an_ordinary_source_to_sink_archive() {
        let temp = tempfile::tempdir().expect("temporary journal root");
        let root = temp.path().join("journals");
        let flow = FlowDefinition::materialize({
            let root = root.clone();
            move |_runtime_config| {
                let inputs = sources::finite(vec![ProjectionInput(1), ProjectionInput(2)]);
                let output = SinkTyped::new(|_input: ProjectionInput| async move {}).idempotent();
                Ok(flow! {
                    name: "sink_conformance_projection",
                    journals: disk_journals(root),

                    stages: {
                        inputs = source!(ProjectionInput => inputs);
                        output = sink!(ProjectionInput => output);
                    },

                    topology: {
                        inputs |> output;
                    }
                })
            }
        });

        FlowApplication::builder()
            .with_cli_args(["sink-conformance-projection"])
            .run_async(flow)
            .await
            .expect("ordinary source-to-sink flow completes");
        let run = latest_run_dir(&root).expect("flow produced an archive");
        let evidence = project_run(&run)
            .await
            .expect("ordinary source-to-sink archive projects");
        assert_eq!(evidence.eof_kinds(), &[EofKind::Natural]);
    }
}
