// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Connector-author conformance kit for the production sink protocol.
//!
//! This module is available only through `test-support`. It deliberately drives
//! writers through the production typed-to-erased adapter so opaque pending
//! capabilities, report validation, and method lowering are exercised exactly
//! as they are by a running sink stage.

use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::sink::{
    SinkConnector, SinkDescription, SinkHandler, SinkWriteFailureDisposition, SinkWriterAdapter,
    SinkWriterInitContext,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
use obzenflow_core::event::{ChainEventFactory, SinkOperationPhase};
use obzenflow_core::{StageId, TypedPayload, WriterId};
use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

pub const SINK_CONFORMANCE_PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkSettlementMode {
    Terminal,
    Buffered { batch_size: usize },
}

impl SinkSettlementMode {
    fn batch_size(self) -> usize {
        match self {
            Self::Terminal => 1,
            Self::Buffered { batch_size } => batch_size,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkFault {
    Open,
    Encode,
    Acquire,
    BeforeDeferral,
    AfterDeferral,
    DestinationExecution,
    MidBatchMutation,
    PreCommit,
    Rollback,
    PostCommitPreAcknowledgement,
    Flush,
    Drain,
}

impl SinkFault {
    pub fn expected_phase(self) -> SinkOperationPhase {
        use obzenflow_core::event::SinkWritePhase;
        match self {
            Self::Open => SinkOperationPhase::Open,
            Self::Encode => SinkOperationPhase::Write(SinkWritePhase::Encode),
            Self::Acquire => SinkOperationPhase::Write(SinkWritePhase::Acquire),
            Self::BeforeDeferral
            | Self::AfterDeferral
            | Self::DestinationExecution
            | Self::MidBatchMutation => SinkOperationPhase::Write(SinkWritePhase::Execute),
            Self::PreCommit | Self::Rollback | Self::PostCommitPreAcknowledgement => {
                SinkOperationPhase::Write(SinkWritePhase::Commit)
            }
            Self::Flush => SinkOperationPhase::Flush,
            Self::Drain => SinkOperationPhase::Drain,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SinkFaultCase {
    fault: SinkFault,
    disposition: Option<SinkWriteFailureDisposition>,
}

impl SinkFaultCase {
    pub fn operation(fault: SinkFault) -> Self {
        assert!(matches!(
            fault,
            SinkFault::Open | SinkFault::Flush | SinkFault::Drain
        ));
        Self {
            fault,
            disposition: None,
        }
    }

    pub fn write(fault: SinkFault, disposition: SinkWriteFailureDisposition) -> Self {
        assert!(!matches!(
            fault,
            SinkFault::Open | SinkFault::Flush | SinkFault::Drain
        ));
        Self {
            fault,
            disposition: Some(disposition),
        }
    }

    pub fn fault(&self) -> SinkFault {
        self.fault
    }

    pub fn expected_phase(&self) -> SinkOperationPhase {
        self.fault.expected_phase()
    }

    pub fn expected_disposition(&self) -> Option<SinkWriteFailureDisposition> {
        self.disposition
    }
}

#[derive(Debug, Clone)]
pub struct SinkConformanceProfile {
    protocol_version: u16,
    settlement: SinkSettlementMode,
    faults: Vec<SinkFaultCase>,
    credential_sentinels: Vec<String>,
}

impl SinkConformanceProfile {
    pub fn new(protocol_version: u16, settlement: SinkSettlementMode) -> Self {
        Self {
            protocol_version,
            settlement,
            faults: Vec::new(),
            credential_sentinels: Vec::new(),
        }
    }

    pub fn with_fault(mut self, fault: SinkFaultCase) -> Self {
        self.faults.push(fault);
        self
    }

    pub fn with_credential_sentinel(mut self, sentinel: impl Into<String>) -> Self {
        self.credential_sentinels.push(sentinel.into());
        self
    }

    pub fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    pub fn settlement(&self) -> SinkSettlementMode {
        self.settlement
    }

    pub fn faults(&self) -> &[SinkFaultCase] {
        &self.faults
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BuildExpectation {
    Valid,
    Invalid,
}

pub struct SinkBuildCase<C> {
    name: String,
    expectation: BuildExpectation,
    build: Arc<dyn Fn() -> Result<C, SinkFixtureError> + Send + Sync>,
}

impl<C> fmt::Debug for SinkBuildCase<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkBuildCase")
            .field("name", &self.name)
            .field("expectation", &self.expectation)
            .finish_non_exhaustive()
    }
}

impl<C> SinkBuildCase<C> {
    pub fn valid(
        name: impl Into<String>,
        build: impl Fn() -> Result<C, SinkFixtureError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            expectation: BuildExpectation::Valid,
            build: Arc::new(build),
        }
    }

    pub fn invalid(
        name: impl Into<String>,
        build: impl Fn() -> Result<C, SinkFixtureError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            expectation: BuildExpectation::Invalid,
            build: Arc::new(build),
        }
    }
}

pub struct SinkFixtureInputs<T> {
    inputs: Vec<T>,
}

impl<T> SinkFixtureInputs<T> {
    pub fn new(inputs: impl IntoIterator<Item = T>) -> Self {
        Self {
            inputs: inputs.into_iter().collect(),
        }
    }

    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    fn into_inputs(self) -> std::vec::IntoIter<T> {
        self.inputs.into_iter()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SinkExternalCallKind {
    Open,
    Write,
    Flush,
    Drain,
    Begin,
    Execute,
    Commit,
    Rollback,
    Drop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkExternalCall {
    writer: u64,
    sequence: u64,
    kind: SinkExternalCallKind,
}

impl SinkExternalCall {
    pub fn new(writer: u64, sequence: u64, kind: SinkExternalCallKind) -> Self {
        Self {
            writer,
            sequence,
            kind,
        }
    }

    pub fn writer(&self) -> u64 {
        self.writer
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn kind(&self) -> SinkExternalCallKind {
        self.kind
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SinkExternalCallSnapshot {
    calls: Vec<SinkExternalCall>,
}

impl SinkExternalCallSnapshot {
    pub fn new(calls: impl IntoIterator<Item = SinkExternalCall>) -> Self {
        Self {
            calls: calls.into_iter().collect(),
        }
    }

    pub fn calls(&self) -> &[SinkExternalCall] {
        &self.calls
    }

    pub fn count(&self, kind: SinkExternalCallKind) -> usize {
        self.calls.iter().filter(|call| call.kind == kind).count()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkDiagnosticSurface {
    Debug,
    Display,
    ErrorSource,
    Trace,
    Snapshot,
    Verifier,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkDiagnosticSample {
    surface: SinkDiagnosticSurface,
    text: String,
}

impl SinkDiagnosticSample {
    pub fn new(surface: SinkDiagnosticSurface, text: impl Into<String>) -> Self {
        Self {
            surface,
            text: text.into(),
        }
    }

    pub fn surface(&self) -> SinkDiagnosticSurface {
        self.surface
    }

    pub fn text(&self) -> &str {
        &self.text
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkFixtureError {
    detail: String,
}

impl SinkFixtureError {
    pub fn new(detail: impl Into<String>) -> Self {
        Self {
            detail: detail.into(),
        }
    }
}

impl fmt::Display for SinkFixtureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.detail)
    }
}

impl std::error::Error for SinkFixtureError {}

#[async_trait]
pub trait SinkWriterConformanceFixture: Send {
    type Connector: SinkConnector;
    type DestinationSnapshot: fmt::Debug + Eq + Send + Sync;

    fn profile(&self) -> SinkConformanceProfile;
    fn build_cases(&self) -> Vec<SinkBuildCase<Self::Connector>>;
    fn fresh_inputs(
        &mut self,
    ) -> Result<SinkFixtureInputs<<Self::Connector as SinkConnector>::Input>, SinkFixtureError>;
    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError>;
    async fn arm_fault(&mut self, fault: SinkFault) -> Result<(), SinkFixtureError>;
    async fn destination_snapshot(&self) -> Result<Self::DestinationSnapshot, SinkFixtureError>;
    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError>;
    fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkConformanceCaseResult {
    suite: &'static str,
    case: String,
}

impl SinkConformanceCaseResult {
    pub fn suite(&self) -> &'static str {
        self.suite
    }

    pub fn case(&self) -> &str {
        &self.case
    }
}

#[derive(Debug, Clone)]
pub struct SinkConformanceReport {
    protocol_version: u16,
    cases: Vec<SinkConformanceCaseResult>,
}

impl SinkConformanceReport {
    pub fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    pub fn cases(&self) -> &[SinkConformanceCaseResult] {
        &self.cases
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
            "sink conformance {}/{}: {}",
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

fn passed(
    cases: &mut Vec<SinkConformanceCaseResult>,
    suite: &'static str,
    case: impl Into<String>,
) {
    cases.push(SinkConformanceCaseResult {
        suite,
        case: case.into(),
    });
}

fn validate_call_order(snapshot: &SinkExternalCallSnapshot) -> Result<(), SinkConformanceFailure> {
    let mut last_global = None;
    let mut last_by_writer = BTreeMap::new();
    let mut opened = HashSet::new();
    let mut dropped = HashSet::new();
    for call in snapshot.calls() {
        if last_global.is_some_and(|previous| call.sequence() <= previous) {
            return Err(failure(
                "call-order",
                "global",
                "external call snapshot is not in one strict global sequence",
            ));
        }
        last_global = Some(call.sequence());
        if let Some(previous) = last_by_writer.insert(call.writer(), call.sequence()) {
            if call.sequence() <= previous {
                return Err(failure(
                    "call-order",
                    format!("writer-{}", call.writer()),
                    "external call sequence is not strictly increasing",
                ));
            }
        }
        match call.kind() {
            SinkExternalCallKind::Open => {
                if !opened.insert(call.writer()) {
                    return Err(failure(
                        "call-order",
                        format!("writer-{}", call.writer()),
                        "writer has more than one open call",
                    ));
                }
            }
            _ if !opened.contains(&call.writer()) => {
                return Err(failure(
                    "call-order",
                    format!("writer-{}", call.writer()),
                    "writer call appeared before its open call",
                ));
            }
            _ if dropped.contains(&call.writer()) => {
                return Err(failure(
                    "call-order",
                    format!("writer-{}", call.writer()),
                    "writer call appeared after drop",
                ));
            }
            SinkExternalCallKind::Drop => {
                dropped.insert(call.writer());
            }
            _ => {}
        }
    }
    Ok(())
}

fn appended_calls(
    before: &SinkExternalCallSnapshot,
    after: &SinkExternalCallSnapshot,
    suite: &'static str,
    case: &str,
) -> Result<Vec<SinkExternalCall>, SinkConformanceFailure> {
    if after.calls().len() < before.calls().len() || !after.calls().starts_with(before.calls()) {
        return Err(failure(
            suite,
            case,
            "external call snapshot was reordered or replaced during one harness action",
        ));
    }
    validate_call_order(after)?;
    Ok(after.calls()[before.calls().len()..].to_vec())
}

fn validate_invocation(
    before: &SinkExternalCallSnapshot,
    after: &SinkExternalCallSnapshot,
    expected: SinkExternalCallKind,
    expected_writer: Option<u64>,
    suite: &'static str,
    case: &str,
) -> Result<u64, SinkConformanceFailure> {
    let appended = appended_calls(before, after, suite, case)?;
    let method_calls = appended
        .iter()
        .filter(|call| call.kind() == expected)
        .collect::<Vec<_>>();
    if method_calls.len() != 1 {
        return Err(failure(
            suite,
            case,
            format!(
                "one harness invocation produced {} {:?} call records",
                method_calls.len(),
                expected
            ),
        ));
    }
    let writer = method_calls[0].writer();
    if expected_writer.is_some_and(|expected_writer| expected_writer != writer)
        || appended.iter().any(|call| call.writer() != writer)
    {
        return Err(failure(
            suite,
            case,
            "one writer invocation produced a cross-writer call record",
        ));
    }
    Ok(writer)
}

fn validate_drop_only(
    before: &SinkExternalCallSnapshot,
    after: &SinkExternalCallSnapshot,
    expected_writers: &[u64],
    suite: &'static str,
    case: &str,
) -> Result<(), SinkConformanceFailure> {
    let appended = appended_calls(before, after, suite, case)?;
    let mut dropped = appended
        .iter()
        .filter_map(|call| (call.kind() == SinkExternalCallKind::Drop).then_some(call.writer()))
        .collect::<Vec<_>>();
    dropped.sort_unstable();
    let mut expected = expected_writers.to_vec();
    expected.sort_unstable();
    if appended
        .iter()
        .any(|call| call.kind() != SinkExternalCallKind::Drop)
        || dropped != expected
    {
        return Err(failure(
            suite,
            case,
            "dropping a writer performed I/O or did not record exactly one terminal drop",
        ));
    }
    Ok(())
}

fn validate_profile(profile: &SinkConformanceProfile) -> Result<(), SinkConformanceFailure> {
    if profile.protocol_version != SINK_CONFORMANCE_PROTOCOL_VERSION {
        return Err(failure(
            "profile",
            "protocol-version",
            format!(
                "fixture protocol {} does not match harness protocol {}",
                profile.protocol_version, SINK_CONFORMANCE_PROTOCOL_VERSION
            ),
        ));
    }
    if profile.settlement.batch_size() == 0 {
        return Err(failure(
            "profile",
            "batch-size",
            "buffered batch size must be greater than zero",
        ));
    }
    let mut faults = HashSet::new();
    for fault in &profile.faults {
        if !faults.insert(fault.fault) {
            return Err(failure(
                "profile",
                "duplicate-fault",
                format!("fault {:?} appears more than once", fault.fault),
            ));
        }
    }
    Ok(())
}

struct ProtocolDriver<W> {
    adapter: SinkWriterAdapter<W>,
    writer_id: WriterId,
}

enum DriverFailure {
    Write(crate::stages::sink::SinkWriteFailure),
    Operation(crate::stages::sink::SinkOperationError),
    Protocol(String),
}

impl<W> ProtocolDriver<W>
where
    W: crate::stages::sink::SinkWriter,
{
    fn new(writer: W, stage_id: StageId, description: &SinkDescription) -> Self {
        Self {
            adapter: SinkWriterAdapter::with_default_method(
                writer,
                stage_id,
                description.default_method().cloned(),
            ),
            writer_id: WriterId::from(stage_id),
        }
    }

    async fn write(
        &mut self,
        input: W::Input,
    ) -> Result<crate::stages::common::handlers::sink::SinkConsumeReport, DriverFailure> {
        let event = ChainEventFactory::data_event_from(
            self.writer_id,
            W::Input::versioned_event_type(),
            &input,
        )
        .map_err(|error| DriverFailure::Protocol(error.to_string()))?;
        let mut report = self
            .adapter
            .consume_report(event)
            .await
            .map_err(map_handler_error)?;
        report.commit_settlements().map_err(map_handler_error)?;
        Ok(report)
    }

    async fn flush(
        &mut self,
    ) -> Result<crate::stages::common::handlers::sink::SinkLifecycleReport, DriverFailure> {
        let mut report = self
            .adapter
            .flush_report()
            .await
            .map_err(map_handler_error)?;
        report.commit_settlements().map_err(map_handler_error)?;
        Ok(report)
    }

    async fn drain(
        &mut self,
    ) -> Result<crate::stages::common::handlers::sink::SinkLifecycleReport, DriverFailure> {
        let mut report = self
            .adapter
            .drain_report()
            .await
            .map_err(map_handler_error)?;
        report.commit_settlements().map_err(map_handler_error)?;
        Ok(report)
    }
}

fn map_handler_error(error: HandlerError) -> DriverFailure {
    match error {
        HandlerError::SinkWrite(failure) => DriverFailure::Write(*failure),
        HandlerError::SinkOperation(error) => DriverFailure::Operation(*error),
        other => DriverFailure::Protocol(other.to_string()),
    }
}

fn primary_is_buffered(report: &crate::stages::common::handlers::sink::SinkConsumeReport) -> bool {
    matches!(report.primary.result, DeliveryResult::Buffered { .. })
}

fn primary_is_terminal(report: &crate::stages::common::handlers::sink::SinkConsumeReport) -> bool {
    matches!(
        report.primary.result,
        DeliveryResult::Success { .. } | DeliveryResult::Partial { .. }
    )
}

fn driver_detail(error: DriverFailure) -> String {
    match error {
        DriverFailure::Write(error) => error.to_string(),
        DriverFailure::Operation(error) => error.to_string(),
        DriverFailure::Protocol(detail) => detail,
    }
}

fn build_connector<C>(cases: &[SinkBuildCase<C>]) -> Result<C, SinkConformanceFailure> {
    let case = cases
        .iter()
        .find(|case| case.expectation == BuildExpectation::Valid)
        .ok_or_else(|| {
            failure(
                "configuration",
                "valid-case",
                "no valid build case supplied",
            )
        })?;
    (case.build)().map_err(|error| failure("configuration", &case.name, error.to_string()))
}

async fn open_driver<C: SinkConnector>(
    connector: &C,
    stage_name: &str,
) -> Result<ProtocolDriver<C::Writer>, SinkConformanceFailure> {
    let stage_id = StageId::new();
    let writer = connector
        .open(SinkWriterInitContext::new(
            stage_id,
            stage_name.to_string(),
            "sink-conformance".to_string(),
        ))
        .await
        .map_err(|error| failure("opening", stage_name, error.to_string()))?;
    Ok(ProtocolDriver::new(writer, stage_id, &connector.describe()))
}

fn take_input<T>(
    inputs: &mut impl Iterator<Item = T>,
    suite: &'static str,
    case: &str,
) -> Result<T, SinkConformanceFailure> {
    inputs
        .next()
        .ok_or_else(|| failure(suite, case, "fixture supplied too few fresh inputs"))
}

pub async fn run_writer_conformance<F: SinkWriterConformanceFixture>(
    fixture: &mut F,
) -> Result<SinkConformanceReport, SinkConformanceFailure> {
    let profile = fixture.profile();
    validate_profile(&profile)?;

    let mut passed_cases = Vec::new();
    let build_cases = fixture.build_cases();
    if build_cases.is_empty() {
        return Err(failure(
            "configuration",
            "build-cases",
            "no build cases supplied",
        ));
    }
    let calls_before_build = fixture
        .external_calls()
        .map_err(|error| failure("configuration", "calls-before", error.to_string()))?;
    let mut saw_valid = false;
    let mut saw_invalid = false;
    for case in &build_cases {
        let first = (case.build)();
        let second = (case.build)();
        match case.expectation {
            BuildExpectation::Valid => {
                saw_valid = true;
                if first.is_err() || second.is_err() {
                    return Err(failure(
                        "configuration",
                        &case.name,
                        "valid build case was not repeatably valid",
                    ));
                }
            }
            BuildExpectation::Invalid => {
                saw_invalid = true;
                if first.is_ok() || second.is_ok() {
                    return Err(failure(
                        "configuration",
                        &case.name,
                        "invalid build case did not fail repeatably",
                    ));
                }
            }
        }
        passed(&mut passed_cases, "configuration", &case.name);
    }
    if !saw_valid || !saw_invalid {
        return Err(failure(
            "configuration",
            "coverage",
            "at least one valid and one invalid build case are required",
        ));
    }
    let calls_after_build = fixture
        .external_calls()
        .map_err(|error| failure("configuration", "calls-after", error.to_string()))?;
    if calls_before_build != calls_after_build {
        return Err(failure(
            "configuration",
            "io-purity",
            "connector build changed the external-call snapshot",
        ));
    }

    for sample in fixture
        .diagnostic_samples()
        .map_err(|error| failure("diagnostics", "samples", error.to_string()))?
    {
        for sentinel in &profile.credential_sentinels {
            if !sentinel.is_empty() && sample.text().contains(sentinel) {
                return Err(failure(
                    "diagnostics",
                    format!("{:?}", sample.surface()),
                    "credential sentinel appeared in diagnostic output",
                ));
            }
        }
    }
    passed(&mut passed_cases, "diagnostics", "credential-sentinels");

    fixture
        .reset_destination()
        .await
        .map_err(|error| failure("opening", "reset", error.to_string()))?;
    let destination_before = fixture
        .destination_snapshot()
        .await
        .map_err(|error| failure("opening", "snapshot-before", error.to_string()))?;
    let connector = build_connector(&build_cases)?;
    let description_text = format!("{:?}", connector.describe());
    for sentinel in &profile.credential_sentinels {
        if !sentinel.is_empty() && description_text.contains(sentinel) {
            return Err(failure(
                "diagnostics",
                "description",
                "credential sentinel appeared in SinkDescription",
            ));
        }
    }

    let calls_before_open = fixture
        .external_calls()
        .map_err(|error| failure("opening", "calls-before-open", error.to_string()))?;
    let mut writer_a = open_driver(&connector, "writer-a").await?;
    let calls_after_open_a = fixture
        .external_calls()
        .map_err(|error| failure("opening", "calls-after-open-a", error.to_string()))?;
    let writer_a_id = validate_invocation(
        &calls_before_open,
        &calls_after_open_a,
        SinkExternalCallKind::Open,
        None,
        "opening",
        "writer-a",
    )?;
    let mut writer_b = open_driver(&connector, "writer-b").await?;
    let calls_after_open_b = fixture
        .external_calls()
        .map_err(|error| failure("opening", "calls-after-open-b", error.to_string()))?;
    let writer_b_id = validate_invocation(
        &calls_after_open_a,
        &calls_after_open_b,
        SinkExternalCallKind::Open,
        None,
        "opening",
        "writer-b",
    )?;
    if writer_a_id == writer_b_id {
        return Err(failure(
            "opening",
            "isolated-double-open",
            "two opens exposed the same writer identity",
        ));
    }
    passed(&mut passed_cases, "opening", "isolated-double-open");

    let batch_size = profile.settlement.batch_size();
    let required_inputs = batch_size.saturating_mul(2).saturating_add(2);
    let fresh = fixture
        .fresh_inputs()
        .map_err(|error| failure("delivery", "fresh-inputs", error.to_string()))?;
    if fresh.len() < required_inputs {
        return Err(failure(
            "delivery",
            "fresh-input-count",
            format!("requires at least {required_inputs} fresh inputs"),
        ));
    }
    let mut inputs = fresh.into_inputs();
    for index in 0..batch_size {
        for (name, driver) in [("writer-a", &mut writer_a), ("writer-b", &mut writer_b)] {
            let input = take_input(&mut inputs, "delivery", name)?;
            let calls_before = fixture
                .external_calls()
                .map_err(|error| failure("delivery", name, error.to_string()))?;
            let report = driver
                .write(input)
                .await
                .map_err(|error| failure("delivery", name, driver_detail(error)))?;
            let calls_after = fixture
                .external_calls()
                .map_err(|error| failure("delivery", name, error.to_string()))?;
            validate_invocation(
                &calls_before,
                &calls_after,
                SinkExternalCallKind::Write,
                Some(if name == "writer-a" {
                    writer_a_id
                } else {
                    writer_b_id
                }),
                "delivery",
                name,
            )?;
            match profile.settlement {
                SinkSettlementMode::Terminal => {
                    if !primary_is_terminal(&report) || !report.commit_receipts.is_empty() {
                        return Err(failure(
                            "delivery",
                            name,
                            "terminal writer returned a buffered or multi-receipt report",
                        ));
                    }
                }
                SinkSettlementMode::Buffered { .. } if index + 1 < batch_size => {
                    if !primary_is_buffered(&report) || !report.commit_receipts.is_empty() {
                        return Err(failure(
                            "delivery",
                            name,
                            "pre-threshold write did not remain solely buffered",
                        ));
                    }
                }
                SinkSettlementMode::Buffered { .. } => {
                    if !primary_is_buffered(&report) || report.commit_receipts.len() != batch_size {
                        return Err(failure(
                            "delivery",
                            name,
                            "threshold write did not settle the complete batch",
                        ));
                    }
                }
            }
        }
    }
    passed(&mut passed_cases, "delivery", "interleaved-writers");

    let calls_before_flush_a = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "calls-before-flush-a", error.to_string()))?;
    let empty_flush_a = writer_a
        .flush()
        .await
        .map_err(|error| failure("lifecycle", "empty-flush-a", driver_detail(error)))?;
    let calls_after_flush_a = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "calls-after-flush-a", error.to_string()))?;
    validate_invocation(
        &calls_before_flush_a,
        &calls_after_flush_a,
        SinkExternalCallKind::Flush,
        Some(writer_a_id),
        "lifecycle",
        "empty-flush-a",
    )?;
    let empty_flush_b = writer_b
        .flush()
        .await
        .map_err(|error| failure("lifecycle", "empty-flush-b", driver_detail(error)))?;
    let calls_after_flush_b = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "calls-after-flush-b", error.to_string()))?;
    validate_invocation(
        &calls_after_flush_a,
        &calls_after_flush_b,
        SinkExternalCallKind::Flush,
        Some(writer_b_id),
        "lifecycle",
        "empty-flush-b",
    )?;
    if !empty_flush_a.commit_receipts.is_empty() || !empty_flush_b.commit_receipts.is_empty() {
        return Err(failure(
            "lifecycle",
            "empty-flush",
            "empty flush returned settlement receipts",
        ));
    }
    let after_flush = take_input(&mut inputs, "lifecycle", "post-flush-write")?;
    let calls_before_post_flush = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "post-flush-write", error.to_string()))?;
    let post_flush_report = writer_a
        .write(after_flush)
        .await
        .map_err(|error| failure("lifecycle", "post-flush-write", driver_detail(error)))?;
    let calls_after_post_flush = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "post-flush-write", error.to_string()))?;
    validate_invocation(
        &calls_before_post_flush,
        &calls_after_post_flush,
        SinkExternalCallKind::Write,
        Some(writer_a_id),
        "lifecycle",
        "post-flush-write",
    )?;
    let flush_report = writer_a
        .flush()
        .await
        .map_err(|error| failure("lifecycle", "settling-flush", driver_detail(error)))?;
    let calls_after_settling_flush = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "settling-flush", error.to_string()))?;
    validate_invocation(
        &calls_after_post_flush,
        &calls_after_settling_flush,
        SinkExternalCallKind::Flush,
        Some(writer_a_id),
        "lifecycle",
        "settling-flush",
    )?;
    if matches!(profile.settlement, SinkSettlementMode::Buffered { .. })
        && (!primary_is_buffered(&post_flush_report) || flush_report.commit_receipts.len() != 1)
    {
        return Err(failure(
            "lifecycle",
            "settling-flush",
            "flush did not settle exactly the pending buffered input",
        ));
    }
    let before_drain = take_input(&mut inputs, "lifecycle", "pre-drain-write")?;
    let calls_before_pre_drain = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "pre-drain-write", error.to_string()))?;
    let pre_drain_report = writer_b
        .write(before_drain)
        .await
        .map_err(|error| failure("lifecycle", "pre-drain-write", driver_detail(error)))?;
    let calls_after_pre_drain = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "pre-drain-write", error.to_string()))?;
    validate_invocation(
        &calls_before_pre_drain,
        &calls_after_pre_drain,
        SinkExternalCallKind::Write,
        Some(writer_b_id),
        "lifecycle",
        "pre-drain-write",
    )?;
    let drain_report = writer_b
        .drain()
        .await
        .map_err(|error| failure("lifecycle", "drain", driver_detail(error)))?;
    let calls_after_drain_b = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "drain-b", error.to_string()))?;
    validate_invocation(
        &calls_after_pre_drain,
        &calls_after_drain_b,
        SinkExternalCallKind::Drain,
        Some(writer_b_id),
        "lifecycle",
        "drain-b",
    )?;
    if matches!(profile.settlement, SinkSettlementMode::Buffered { .. })
        && (!primary_is_buffered(&pre_drain_report) || drain_report.commit_receipts.len() != 1)
    {
        return Err(failure(
            "lifecycle",
            "drain",
            "drain did not settle exactly the pending buffered input",
        ));
    }
    let drain_a = writer_a
        .drain()
        .await
        .map_err(|error| failure("lifecycle", "drain-a", driver_detail(error)))?;
    let calls_after_drain_a = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "drain-a", error.to_string()))?;
    validate_invocation(
        &calls_after_drain_b,
        &calls_after_drain_a,
        SinkExternalCallKind::Drain,
        Some(writer_a_id),
        "lifecycle",
        "drain-a",
    )?;
    if !drain_a.commit_receipts.is_empty() {
        return Err(failure(
            "lifecycle",
            "drain-a",
            "drain after settling flush returned duplicate receipts",
        ));
    }
    passed(
        &mut passed_cases,
        "lifecycle",
        "flush-usable-and-drain-empty",
    );

    let destination_after = fixture
        .destination_snapshot()
        .await
        .map_err(|error| failure("delivery", "snapshot-after", error.to_string()))?;
    if destination_before == destination_after {
        return Err(failure(
            "delivery",
            "destination-mutation",
            "successful baseline did not change the destination snapshot",
        ));
    }
    let calls_before_drop = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "calls-before-drop", error.to_string()))?;
    drop(writer_a);
    drop(writer_b);
    let calls_after_drop = fixture
        .external_calls()
        .map_err(|error| failure("lifecycle", "calls-after-drop", error.to_string()))?;
    validate_drop_only(
        &calls_before_drop,
        &calls_after_drop,
        &[writer_a_id, writer_b_id],
        "lifecycle",
        "drop-is-io-free",
    )?;
    let destination_after_drop = fixture
        .destination_snapshot()
        .await
        .map_err(|error| failure("lifecycle", "snapshot-after-drop", error.to_string()))?;
    if destination_after_drop != destination_after {
        return Err(failure(
            "lifecycle",
            "drop-is-io-free",
            "writer destruction changed committed destination state",
        ));
    }

    for fault_case in profile.faults() {
        fixture.reset_destination().await.map_err(|error| {
            failure(
                "failure",
                format!("{:?}-reset", fault_case.fault),
                error.to_string(),
            )
        })?;
        fixture.arm_fault(fault_case.fault).await.map_err(|error| {
            failure(
                "failure",
                format!("{:?}-arm", fault_case.fault),
                error.to_string(),
            )
        })?;
        let destination_before_fault = fixture.destination_snapshot().await.map_err(|error| {
            failure(
                "failure",
                format!("{:?}-snapshot-before", fault_case.fault),
                error.to_string(),
            )
        })?;
        let connector = build_connector(&build_cases)?;
        if fault_case.fault == SinkFault::Open {
            let stage_id = StageId::new();
            let calls_before = fixture
                .external_calls()
                .map_err(|error| failure("failure", "Open-calls-before", error.to_string()))?;
            let open_result = connector
                .open(SinkWriterInitContext::new(
                    stage_id,
                    "fault-open".to_string(),
                    "sink-conformance".to_string(),
                ))
                .await;
            let calls_after = fixture
                .external_calls()
                .map_err(|error| failure("failure", "Open-calls-after", error.to_string()))?;
            validate_invocation(
                &calls_before,
                &calls_after,
                SinkExternalCallKind::Open,
                None,
                "failure",
                "Open",
            )?;
            match open_result {
                Err(_) => {
                    let after = fixture.destination_snapshot().await.map_err(|error| {
                        failure("failure", "Open-snapshot-after", error.to_string())
                    })?;
                    if after != destination_before_fault {
                        return Err(failure(
                            "failure",
                            "Open",
                            "open failure mutated the destination",
                        ));
                    }
                    passed(&mut passed_cases, "failure", "Open")
                }
                Ok(_) => return Err(failure("failure", "Open", "armed open fault succeeded")),
            }
            continue;
        }

        let calls_before_fault_writer = fixture.external_calls().map_err(|error| {
            failure(
                "failure",
                format!("{:?}-calls-before-open", fault_case.fault),
                error.to_string(),
            )
        })?;
        let mut driver = open_driver(&connector, "fault-writer").await?;
        let calls_after_fault_writer = fixture.external_calls().map_err(|error| {
            failure(
                "failure",
                format!("{:?}-calls-after-open", fault_case.fault),
                error.to_string(),
            )
        })?;
        let fault_writer_id = validate_invocation(
            &calls_before_fault_writer,
            &calls_after_fault_writer,
            SinkExternalCallKind::Open,
            None,
            "failure",
            &format!("{:?}-open", fault_case.fault),
        )?;
        let fresh = fixture
            .fresh_inputs()
            .map_err(|error| failure("failure", "fresh-inputs", error.to_string()))?;
        let mut inputs = fresh.into_inputs();
        match fault_case.fault {
            SinkFault::Flush | SinkFault::Drain => {
                if matches!(profile.settlement, SinkSettlementMode::Buffered { .. }) {
                    let input = take_input(&mut inputs, "failure", "lifecycle-buffer")?;
                    let calls_before = fixture.external_calls().map_err(|error| {
                        failure("failure", "lifecycle-buffer", error.to_string())
                    })?;
                    let _ = driver.write(input).await.map_err(|error| {
                        failure("failure", "lifecycle-buffer", driver_detail(error))
                    })?;
                    let calls_after = fixture.external_calls().map_err(|error| {
                        failure("failure", "lifecycle-buffer", error.to_string())
                    })?;
                    validate_invocation(
                        &calls_before,
                        &calls_after,
                        SinkExternalCallKind::Write,
                        Some(fault_writer_id),
                        "failure",
                        "lifecycle-buffer",
                    )?;
                }
                let calls_before = fixture.external_calls().map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-calls-before", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                let result = if fault_case.fault == SinkFault::Flush {
                    driver.flush().await
                } else {
                    driver.drain().await
                };
                let calls_after = fixture.external_calls().map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-calls-after", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                validate_invocation(
                    &calls_before,
                    &calls_after,
                    if fault_case.fault == SinkFault::Flush {
                        SinkExternalCallKind::Flush
                    } else {
                        SinkExternalCallKind::Drain
                    },
                    Some(fault_writer_id),
                    "failure",
                    &format!("{:?}", fault_case.fault),
                )?;
                match result {
                    Err(DriverFailure::Operation(_)) => {
                        let after = fixture.destination_snapshot().await.map_err(|error| {
                            failure(
                                "failure",
                                format!("{:?}-snapshot-after", fault_case.fault),
                                error.to_string(),
                            )
                        })?;
                        if after != destination_before_fault {
                            return Err(failure(
                                "failure",
                                format!("{:?}", fault_case.fault),
                                "lifecycle failure committed buffered destination state",
                            ));
                        }
                        passed(
                            &mut passed_cases,
                            "failure",
                            format!("{:?}", fault_case.fault),
                        );
                    }
                    Err(other) => {
                        return Err(failure(
                            "failure",
                            format!("{:?}", fault_case.fault),
                            format!("wrong failure channel: {}", driver_detail(other)),
                        ))
                    }
                    Ok(_) => {
                        return Err(failure(
                            "failure",
                            format!("{:?}", fault_case.fault),
                            "armed lifecycle fault succeeded",
                        ))
                    }
                }
                let calls_before_drop = fixture.external_calls().map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-calls-before-drop", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                drop(driver);
                let calls_after_drop = fixture.external_calls().map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-calls-after-drop", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                validate_drop_only(
                    &calls_before_drop,
                    &calls_after_drop,
                    &[fault_writer_id],
                    "failure",
                    &format!("{:?}-failed-drop", fault_case.fault),
                )?;
                let after_drop = fixture.destination_snapshot().await.map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-snapshot-after-drop", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                if after_drop != destination_before_fault {
                    return Err(failure(
                        "failure",
                        format!("{:?}-failed-drop", fault_case.fault),
                        "failed lifecycle teardown or destruction mutated the destination",
                    ));
                }
            }
            SinkFault::Encode
            | SinkFault::Acquire
            | SinkFault::BeforeDeferral
            | SinkFault::AfterDeferral
            | SinkFault::DestinationExecution
            | SinkFault::MidBatchMutation
            | SinkFault::PreCommit
            | SinkFault::Rollback
            | SinkFault::PostCommitPreAcknowledgement => {
                let attempts = if matches!(profile.settlement, SinkSettlementMode::Buffered { .. })
                {
                    batch_size
                } else {
                    1
                };
                let mut observed = None;
                for _ in 0..attempts {
                    let input = take_input(&mut inputs, "failure", "write-fault")?;
                    let calls_before = fixture.external_calls().map_err(|error| {
                        failure("failure", "write-fault-calls-before", error.to_string())
                    })?;
                    let result = driver.write(input).await;
                    let calls_after = fixture.external_calls().map_err(|error| {
                        failure("failure", "write-fault-calls-after", error.to_string())
                    })?;
                    validate_invocation(
                        &calls_before,
                        &calls_after,
                        SinkExternalCallKind::Write,
                        Some(fault_writer_id),
                        "failure",
                        &format!("{:?}-write", fault_case.fault),
                    )?;
                    match result {
                        Ok(_) => {}
                        Err(DriverFailure::Write(write_failure)) => {
                            observed = Some(write_failure);
                            break;
                        }
                        Err(other) => {
                            return Err(failure(
                                "failure",
                                format!("{:?}", fault_case.fault),
                                format!("wrong failure channel: {}", driver_detail(other)),
                            ))
                        }
                    }
                }
                let observed = observed.ok_or_else(|| {
                    failure(
                        "failure",
                        format!("{:?}", fault_case.fault),
                        "armed write fault did not fail",
                    )
                })?;
                let SinkOperationPhase::Write(expected_phase) = fault_case.expected_phase() else {
                    unreachable!("write fault has write phase")
                };
                if observed.phase() != expected_phase
                    || Some(observed.disposition()) != fault_case.expected_disposition()
                {
                    return Err(failure(
                        "failure",
                        format!("{:?}", fault_case.fault),
                        format!(
                            "expected {:?}/{:?}, got {:?}/{:?}",
                            expected_phase,
                            fault_case.expected_disposition(),
                            observed.phase(),
                            observed.disposition()
                        ),
                    ));
                }
                let after_failure = fixture.destination_snapshot().await.map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-snapshot-after", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                if matches!(
                    observed.disposition(),
                    SinkWriteFailureDisposition::CurrentOnly
                        | SinkWriteFailureDisposition::ConfirmedRollback
                ) && after_failure != destination_before_fault
                {
                    return Err(failure(
                        "failure",
                        format!("{:?}", fault_case.fault),
                        "continuable write disposition mutated committed destination state",
                    ));
                }

                if matches!(
                    observed.disposition(),
                    SinkWriteFailureDisposition::CurrentOnly
                        | SinkWriteFailureDisposition::ConfirmedRollback
                ) {
                    let mut recovered = None;
                    for _ in 0..=batch_size {
                        let input = take_input(&mut inputs, "failure", "recovery-write")?;
                        let calls_before = fixture.external_calls().map_err(|error| {
                            failure("failure", "recovery-calls-before", error.to_string())
                        })?;
                        let report = driver.write(input).await.map_err(|error| {
                            failure(
                                "failure",
                                format!("{:?}-recovery", fault_case.fault),
                                driver_detail(error),
                            )
                        })?;
                        let calls_after = fixture.external_calls().map_err(|error| {
                            failure("failure", "recovery-calls-after", error.to_string())
                        })?;
                        validate_invocation(
                            &calls_before,
                            &calls_after,
                            SinkExternalCallKind::Write,
                            Some(fault_writer_id),
                            "failure",
                            &format!("{:?}-recovery", fault_case.fault),
                        )?;
                        if primary_is_terminal(&report) || !report.commit_receipts.is_empty() {
                            recovered = Some(report);
                            break;
                        }
                    }
                    let recovered = recovered.ok_or_else(|| {
                        failure(
                            "failure",
                            format!("{:?}-recovery", fault_case.fault),
                            "continuable writer did not settle after a distinct later invocation",
                        )
                    })?;
                    if matches!(profile.settlement, SinkSettlementMode::Buffered { .. })
                        && (!primary_is_buffered(&recovered)
                            || recovered.commit_receipts.len() != batch_size)
                    {
                        return Err(failure(
                            "failure",
                            format!("{:?}-recovery", fault_case.fault),
                            "recovery did not settle the exact retained batch once",
                        ));
                    }
                    let after_recovery = fixture.destination_snapshot().await.map_err(|error| {
                        failure(
                            "failure",
                            format!("{:?}-recovery-snapshot", fault_case.fault),
                            error.to_string(),
                        )
                    })?;
                    if after_recovery == destination_before_fault {
                        return Err(failure(
                            "failure",
                            format!("{:?}-recovery", fault_case.fault),
                            "later recovery invocation did not commit destination state",
                        ));
                    }
                }
                passed(
                    &mut passed_cases,
                    "failure",
                    format!("{:?}", fault_case.fault),
                );
                let destination_before_drop =
                    fixture.destination_snapshot().await.map_err(|error| {
                        failure(
                            "failure",
                            format!("{:?}-snapshot-before-drop", fault_case.fault),
                            error.to_string(),
                        )
                    })?;
                let calls_before_drop = fixture.external_calls().map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-calls-before-drop", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                drop(driver);
                let calls_after_drop = fixture.external_calls().map_err(|error| {
                    failure(
                        "failure",
                        format!("{:?}-calls-after-drop", fault_case.fault),
                        error.to_string(),
                    )
                })?;
                validate_drop_only(
                    &calls_before_drop,
                    &calls_after_drop,
                    &[fault_writer_id],
                    "failure",
                    &format!("{:?}-drop", fault_case.fault),
                )?;
                let destination_after_drop =
                    fixture.destination_snapshot().await.map_err(|error| {
                        failure(
                            "failure",
                            format!("{:?}-snapshot-after-drop", fault_case.fault),
                            error.to_string(),
                        )
                    })?;
                if destination_after_drop != destination_before_drop {
                    return Err(failure(
                        "failure",
                        format!("{:?}-drop", fault_case.fault),
                        "writer destruction changed committed destination state",
                    ));
                }
            }
            SinkFault::Open => unreachable!(),
        }
    }

    let calls = fixture
        .external_calls()
        .map_err(|error| failure("call-order", "snapshot", error.to_string()))?;
    validate_call_order(&calls)?;
    passed(&mut passed_cases, "call-order", "per-writer-strict");

    Ok(SinkConformanceReport {
        protocol_version: SINK_CONFORMANCE_PROTOCOL_VERSION,
        cases: passed_cases,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::sink::{
        PendingSinkInput, SinkBufferedOutcome, SinkCommitReceipt, SinkOperationError,
        SinkOperationResult, SinkTerminalOutcome, SinkWriteContext, SinkWriteFailure,
        SinkWritePhase, SinkWriteReport, SinkWriteResult, SinkWriter, SinkWriterLifecycleReport,
    };
    use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
    use serde::{Deserialize, Serialize};
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    #[derive(Debug, Serialize, Deserialize)]
    struct Input(u64);

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "sink.conformance.self-test";
    }

    #[derive(Default)]
    struct Shared {
        destination: AtomicUsize,
        next_writer: AtomicU64,
        next_sequence: AtomicU64,
        calls: Mutex<Vec<SinkExternalCall>>,
    }

    impl Shared {
        fn call(&self, writer: u64, kind: SinkExternalCallKind) {
            let sequence = self.next_sequence.fetch_add(1, Ordering::SeqCst);
            self.calls
                .lock()
                .expect("call probe lock")
                .push(SinkExternalCall::new(writer, sequence, kind));
        }
    }

    struct Connector {
        shared: Arc<Shared>,
    }

    struct Writer {
        shared: Arc<Shared>,
        id: u64,
    }

    impl Drop for Writer {
        fn drop(&mut self) {
            self.shared.call(self.id, SinkExternalCallKind::Drop);
        }
    }

    #[async_trait]
    impl SinkConnector for Connector {
        type Input = Input;
        type Writer = Writer;

        fn describe(&self) -> SinkDescription {
            SinkDescription::destination("self-test", DeliveryMethod::Noop)
        }

        async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
            let id = self.shared.next_writer.fetch_add(1, Ordering::SeqCst);
            self.shared.call(id, SinkExternalCallKind::Open);
            Ok(Writer {
                shared: Arc::clone(&self.shared),
                id,
            })
        }
    }

    #[async_trait]
    impl SinkWriter for Writer {
        type Input = Input;

        async fn write(&mut self, _input: Input, _context: SinkWriteContext) -> SinkWriteResult {
            self.shared.call(self.id, SinkExternalCallKind::Write);
            self.shared.destination.fetch_add(1, Ordering::SeqCst);
            Ok(SinkWriteReport::terminal(
                SinkTerminalOutcome::success(None).with_items(1),
            ))
        }

        async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
            self.shared.call(self.id, SinkExternalCallKind::Flush);
            Ok(SinkWriterLifecycleReport::default())
        }

        async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
            self.shared.call(self.id, SinkExternalCallKind::Drain);
            Ok(SinkWriterLifecycleReport::default())
        }
    }

    struct Fixture {
        shared: Arc<Shared>,
    }

    #[async_trait]
    impl SinkWriterConformanceFixture for Fixture {
        type Connector = Connector;
        type DestinationSnapshot = usize;

        fn profile(&self) -> SinkConformanceProfile {
            SinkConformanceProfile::new(
                SINK_CONFORMANCE_PROTOCOL_VERSION,
                SinkSettlementMode::Terminal,
            )
        }

        fn build_cases(&self) -> Vec<SinkBuildCase<Self::Connector>> {
            let shared = Arc::clone(&self.shared);
            vec![
                SinkBuildCase::valid("valid", move || {
                    Ok(Connector {
                        shared: Arc::clone(&shared),
                    })
                }),
                SinkBuildCase::invalid("invalid", || {
                    Err(SinkFixtureError::new("intentional invalid configuration"))
                }),
            ]
        }

        fn fresh_inputs(&mut self) -> Result<SinkFixtureInputs<Input>, SinkFixtureError> {
            Ok(SinkFixtureInputs::new((0..8).map(Input)))
        }

        async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
            self.shared.destination.store(0, Ordering::SeqCst);
            Ok(())
        }

        async fn arm_fault(&mut self, _fault: SinkFault) -> Result<(), SinkFixtureError> {
            Err(SinkFixtureError::new(
                "self-test profile declares no faults",
            ))
        }

        async fn destination_snapshot(&self) -> Result<usize, SinkFixtureError> {
            Ok(self.shared.destination.load(Ordering::SeqCst))
        }

        fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
            Ok(SinkExternalCallSnapshot::new(
                self.shared.calls.lock().expect("call probe lock").clone(),
            ))
        }

        fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
            Ok(vec![SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Debug,
                "redacted self-test connector",
            )])
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum BrokenMode {
        SharedWriter,
        PrematureSuccess,
        CapabilityLoss,
        HiddenRepetition,
        DestructorIo,
        MisorderedSnapshot,
        CrossWriterSnapshot,
        WrongPhase,
    }

    struct BrokenShared {
        mode: BrokenMode,
        destination: AtomicUsize,
        next_writer: AtomicU64,
        next_sequence: AtomicU64,
        calls: Mutex<Vec<SinkExternalCall>>,
        armed: Mutex<Option<SinkFault>>,
    }

    impl BrokenShared {
        fn new(mode: BrokenMode) -> Self {
            Self {
                mode,
                destination: AtomicUsize::new(0),
                next_writer: AtomicU64::new(0),
                next_sequence: AtomicU64::new(0),
                calls: Mutex::new(Vec::new()),
                armed: Mutex::new(None),
            }
        }

        fn record(&self, writer: u64, kind: SinkExternalCallKind) {
            let sequence = if self.mode == BrokenMode::MisorderedSnapshot {
                0
            } else {
                self.next_sequence.fetch_add(1, Ordering::SeqCst)
            };
            self.calls
                .lock()
                .expect("broken call probe")
                .push(SinkExternalCall::new(writer, sequence, kind));
        }

        fn reset(&self) {
            self.destination.store(0, Ordering::SeqCst);
            self.next_sequence.store(0, Ordering::SeqCst);
            self.calls.lock().expect("broken call probe").clear();
            *self.armed.lock().expect("broken fault probe") = None;
        }

        fn take_fault(&self, fault: SinkFault) -> bool {
            let mut armed = self.armed.lock().expect("broken fault probe");
            if *armed == Some(fault) {
                *armed = None;
                true
            } else {
                false
            }
        }
    }

    struct BrokenConnector(Arc<BrokenShared>);

    struct BrokenWriter {
        shared: Arc<BrokenShared>,
        id: u64,
        pending: Vec<PendingSinkInput>,
    }

    impl Drop for BrokenWriter {
        fn drop(&mut self) {
            if self.shared.mode == BrokenMode::DestructorIo {
                self.shared.record(self.id, SinkExternalCallKind::Flush);
                self.shared.destination.fetch_add(1, Ordering::SeqCst);
            }
            self.shared.record(self.id, SinkExternalCallKind::Drop);
        }
    }

    #[async_trait]
    impl SinkConnector for BrokenConnector {
        type Input = Input;
        type Writer = BrokenWriter;

        fn describe(&self) -> SinkDescription {
            SinkDescription::destination("broken", DeliveryMethod::Noop)
        }

        async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
            let id = if self.0.mode == BrokenMode::SharedWriter {
                0
            } else {
                self.0.next_writer.fetch_add(1, Ordering::SeqCst)
            };
            self.0.record(id, SinkExternalCallKind::Open);
            Ok(BrokenWriter {
                shared: Arc::clone(&self.0),
                id,
                pending: Vec::new(),
            })
        }
    }

    #[async_trait]
    impl SinkWriter for BrokenWriter {
        type Input = Input;

        async fn write(&mut self, _input: Input, context: SinkWriteContext) -> SinkWriteResult {
            self.shared.record(self.id, SinkExternalCallKind::Write);
            if self.shared.mode == BrokenMode::HiddenRepetition {
                self.shared.record(self.id, SinkExternalCallKind::Write);
            }
            if self.shared.mode == BrokenMode::CrossWriterSnapshot {
                self.shared
                    .record(self.id.saturating_add(1000), SinkExternalCallKind::Execute);
            }
            if self.shared.mode == BrokenMode::WrongPhase
                && self.shared.take_fault(SinkFault::Encode)
            {
                return Err(SinkWriteFailure::current_only(
                    SinkWritePhase::Execute,
                    SinkOperationError::other("deliberately wrong phase"),
                ));
            }
            if self.shared.mode == BrokenMode::CapabilityLoss {
                let current = context.defer();
                if self.pending.is_empty() {
                    self.pending.push(current);
                    return Ok(SinkWriteReport::buffered(SinkBufferedOutcome::accepted(
                        None,
                    )));
                }
                let prior = self.pending.remove(0);
                drop(current);
                self.shared.destination.fetch_add(1, Ordering::SeqCst);
                return Ok(
                    SinkWriteReport::buffered(SinkBufferedOutcome::accepted(None))
                        .with_commit_receipts(vec![SinkCommitReceipt::new(
                            prior,
                            SinkTerminalOutcome::success(None),
                        )]),
                );
            }
            if self.shared.mode != BrokenMode::PrematureSuccess {
                self.shared.destination.fetch_add(1, Ordering::SeqCst);
            }
            Ok(SinkWriteReport::terminal(
                SinkTerminalOutcome::success(None).with_items(1),
            ))
        }

        async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
            self.shared.record(self.id, SinkExternalCallKind::Flush);
            Ok(SinkWriterLifecycleReport::default())
        }

        async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
            self.shared.record(self.id, SinkExternalCallKind::Drain);
            let receipts = self
                .pending
                .drain(..)
                .map(|pending| SinkCommitReceipt::new(pending, SinkTerminalOutcome::success(None)))
                .collect::<Vec<_>>();
            Ok(SinkWriterLifecycleReport::default().with_commit_receipts(receipts))
        }
    }

    struct BrokenFixture(Arc<BrokenShared>);

    #[async_trait]
    impl SinkWriterConformanceFixture for BrokenFixture {
        type Connector = BrokenConnector;
        type DestinationSnapshot = usize;

        fn profile(&self) -> SinkConformanceProfile {
            let settlement = if self.0.mode == BrokenMode::CapabilityLoss {
                SinkSettlementMode::Buffered { batch_size: 2 }
            } else {
                SinkSettlementMode::Terminal
            };
            let profile =
                SinkConformanceProfile::new(SINK_CONFORMANCE_PROTOCOL_VERSION, settlement);
            if self.0.mode == BrokenMode::WrongPhase {
                profile.with_fault(SinkFaultCase::write(
                    SinkFault::Encode,
                    SinkWriteFailureDisposition::CurrentOnly,
                ))
            } else {
                profile
            }
        }

        fn build_cases(&self) -> Vec<SinkBuildCase<Self::Connector>> {
            let shared = Arc::clone(&self.0);
            vec![
                SinkBuildCase::valid("broken-valid", move || {
                    Ok(BrokenConnector(Arc::clone(&shared)))
                }),
                SinkBuildCase::invalid("broken-invalid", || {
                    Err(SinkFixtureError::new("intentional invalid configuration"))
                }),
            ]
        }

        fn fresh_inputs(&mut self) -> Result<SinkFixtureInputs<Input>, SinkFixtureError> {
            Ok(SinkFixtureInputs::new((0..16).map(Input)))
        }

        async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
            self.0.reset();
            Ok(())
        }

        async fn arm_fault(&mut self, fault: SinkFault) -> Result<(), SinkFixtureError> {
            *self.0.armed.lock().expect("broken fault probe") = Some(fault);
            Ok(())
        }

        async fn destination_snapshot(&self) -> Result<usize, SinkFixtureError> {
            Ok(self.0.destination.load(Ordering::SeqCst))
        }

        fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
            Ok(SinkExternalCallSnapshot::new(
                self.0.calls.lock().expect("broken call probe").clone(),
            ))
        }

        fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
            Ok(vec![SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Debug,
                "redacted broken connector",
            )])
        }
    }

    #[tokio::test]
    async fn harness_owns_and_executes_the_protocol_sequence() {
        let mut fixture = Fixture {
            shared: Arc::new(Shared::default()),
        };
        let report = run_writer_conformance(&mut fixture)
            .await
            .expect("honest terminal connector conforms");
        assert_eq!(report.protocol_version(), SINK_CONFORMANCE_PROTOCOL_VERSION);
        assert!(report.cases().len() >= 7);
    }

    #[tokio::test]
    async fn protocol_mismatch_fails_before_fixture_activity() {
        let fixture = Fixture {
            shared: Arc::new(Shared::default()),
        };
        let profile = SinkConformanceProfile::new(99, SinkSettlementMode::Terminal);
        assert!(validate_profile(&profile).is_err());
        assert!(fixture.external_calls().unwrap().calls().is_empty());
    }

    #[tokio::test]
    async fn broken_connectors_fail_every_universal_writer_oracle() {
        for mode in [
            BrokenMode::SharedWriter,
            BrokenMode::PrematureSuccess,
            BrokenMode::CapabilityLoss,
            BrokenMode::HiddenRepetition,
            BrokenMode::DestructorIo,
            BrokenMode::MisorderedSnapshot,
            BrokenMode::CrossWriterSnapshot,
            BrokenMode::WrongPhase,
        ] {
            let mut fixture = BrokenFixture(Arc::new(BrokenShared::new(mode)));
            let error = run_writer_conformance(&mut fixture)
                .await
                .expect_err("deliberately broken connector must fail conformance");
            assert!(
                !error.suite().is_empty() && !error.case().is_empty(),
                "broken mode {mode:?} must produce a harness-owned verdict"
            );
        }
    }
}
