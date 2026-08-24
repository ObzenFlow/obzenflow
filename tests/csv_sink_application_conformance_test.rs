// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "test-support")]

use async_trait::async_trait;
use obzenflow::sinks::{CsvProjection, CsvSink};
use obzenflow::sources;
use obzenflow::testing::sink::{
    run_application_conformance, SinkApplicationBuildCase, SinkApplicationConformanceFixture,
    SinkApplicationScenario, SinkApplicationTopology, SinkApplicationTreatment,
    SinkDestinationClass, SinkDestinationExpectation, SinkDestinationVerdict,
    SinkDestinationVerifier,
};
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SourceAdmission, SourcePolicy, SourcePolicyCtx, SourcePollAttachment,
    SourcePollOutcome,
};
use obzenflow_adapters::sinks::csv::testing::CsvTestProbe;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::HandlerError;
use obzenflow_runtime::stages::source::strategies::{
    CompletionContext, CompletionDecision, CompletionGate,
};
use obzenflow_runtime::stages::{SourceError, TypedFiniteSourceHandler};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use obzenflow_runtime::testing::sink::{
    SinkConformanceProfile, SinkExternalCallKind, SinkExternalCallSnapshot, SinkFault,
    SinkFixtureError, SinkSettlementMode, SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Row {
    id: u64,
    amount_cents: u64,
}

impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "flowip_122a.csv.application.row";
}

#[derive(Clone, Debug)]
struct RowProjection;

impl CsvProjection for RowProjection {
    type Input = Row;
    type Row = Row;

    fn project(&self, input: Self::Input) -> Result<Self::Row, HandlerError> {
        Ok(input)
    }
}

fn rows() -> Vec<Row> {
    (1..=4)
        .map(|id| Row {
            id,
            amount_cents: id * 1_000,
        })
        .collect()
}

#[derive(Debug)]
struct PoisonCompletion;

impl CompletionGate for PoisonCompletion {
    fn on_natural_completion(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::PoisonEof
    }

    fn on_begin_drain(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        CompletionDecision::PoisonEof
    }
}

struct AdmitPoll;

#[async_trait]
impl SourcePolicy for AdmitPoll {
    fn label(&self) -> &'static str {
        "flowip_122a_csv_poison"
    }

    async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        SourceAdmission::Admit(None)
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {}
}

struct PoisonFamily;
struct PoisonFactory;

impl MiddlewareFactory for PoisonFactory {
    fn label(&self) -> &'static str {
        "flowip_122a_csv_poison"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<PoisonFamily>(self.label())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SourcePoll])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        Ok(MiddlewareSurfaceAttachment::source_poll(
            SourcePollAttachment {
                policy: Arc::new(AdmitPoll),
                completion_gate: Some(Arc::new(PoisonCompletion)),
            },
        ))
    }
}

#[derive(Clone, Debug)]
struct StallingRows {
    next: u64,
}

impl TypedFiniteSourceHandler for StallingRows {
    type Output = Row;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next <= 2 {
            let id = self.next;
            self.next += 1;
            return Ok(Some(vec![Row {
                id,
                amount_cents: id * 1_000,
            }]));
        }
        std::thread::sleep(Duration::from_millis(10));
        Ok(Some(Vec::new()))
    }
}

fn flow_error(error: impl std::fmt::Display) -> Box<FlowBuildError> {
    Box::new(FlowBuildError::StageResourcesFailed(format!(
        "failed to build CSV sink: {error}"
    )))
}

fn build_sink(
    path: PathBuf,
    probe: CsvTestProbe,
    class: SinkDestinationClass,
) -> Result<CsvSink<RowProjection>, Box<FlowBuildError>> {
    let builder = CsvSink::builder(RowProjection)
        .path(path)
        .columns(["id", "amount_cents"])
        .buffer_size(2)
        .auto_flush(false)
        .test_probe(probe);
    let builder = match class {
        SinkDestinationClass::SafeToRepeat => builder.append(false),
        SinkDestinationClass::DuplicateSensitive => builder.append(true),
        SinkDestinationClass::Unspecified => builder.test_redelivery_unspecified(),
    };
    builder.build().map_err(flow_error)
}

fn single_flow(
    journal_root: PathBuf,
    output: PathBuf,
    probe: CsvTestProbe,
    class: SinkDestinationClass,
    poison_eof: bool,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let output = build_sink(output, probe, class).map_err(|error| *error)?;
        let inputs = sources::finite(rows());
        if poison_eof {
            Ok(flow! {
                name: "csv_sink_application",
                journals: disk_journals(journal_root),

                stages: {
                    inputs = source!(Row => inputs with [PoisonFactory]);
                    output = sink!(Row => output);
                },

                topology: {
                    inputs |> output;
                }
            })
        } else {
            Ok(flow! {
                name: "csv_sink_application",
                journals: disk_journals(journal_root),

                stages: {
                    inputs = source!(Row => inputs);
                    output = sink!(Row => output);
                },

                topology: {
                    inputs |> output;
                }
            })
        }
    })
}

fn fan_in_flow(journal_root: PathBuf, output: PathBuf, probe: CsvTestProbe) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let output = build_sink(output, probe, SinkDestinationClass::SafeToRepeat)
            .map_err(|error| *error)?;
        let left = sources::finite(vec![rows()[0].clone(), rows()[2].clone()]);
        let right = sources::finite(vec![rows()[1].clone(), rows()[3].clone()]);
        Ok(flow! {
            name: "csv_sink_application_fan_in",
            journals: disk_journals(journal_root),

            stages: {
                left = source!(Row => left);
                right = source!(Row => right);
                output = sink!(Row => output);
            },

            topology: {
                left |> output;
                right |> output;
            }
        })
    })
}

fn fan_out_flow(
    journal_root: PathBuf,
    primary: PathBuf,
    secondary: PathBuf,
    probe: CsvTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let primary = build_sink(primary, probe.clone(), SinkDestinationClass::SafeToRepeat)
            .map_err(|error| *error)?;
        let secondary = build_sink(secondary, probe, SinkDestinationClass::SafeToRepeat)
            .map_err(|error| *error)?;
        let inputs = sources::finite(rows());
        Ok(flow! {
            name: "csv_sink_application_fan_out",
            journals: disk_journals(journal_root),

            stages: {
                inputs = source!(Row => inputs);
                primary = sink!(Row => primary);
                secondary = sink!(Row => secondary);
            },

            topology: {
                inputs |> primary;
                inputs |> secondary;
            }
        })
    })
}

fn stalling_flow(journal_root: PathBuf, output: PathBuf, probe: CsvTestProbe) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let output = build_sink(output, probe, SinkDestinationClass::SafeToRepeat)
            .map_err(|error| *error)?;
        let inputs = StallingRows { next: 1 };
        Ok(flow! {
            name: "csv_sink_application",
            journals: disk_journals(journal_root),

            stages: {
                inputs = source!(Row => inputs);
                output = sink!(Row => output);
            },

            topology: {
                inputs |> output;
            }
        })
    })
}

fn latest_run_dir(root: &Path) -> PathBuf {
    let mut runs = std::fs::read_dir(root.join("flows"))
        .expect("flows directory")
        .map(|entry| entry.expect("run entry").path())
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().expect("durable run archive")
}

fn remove_output(path: &Path) -> Result<(), SinkFixtureError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SinkFixtureError::new(error.to_string())),
    }
}

async fn seed_live_archive(
    root: PathBuf,
    output: PathBuf,
    probe: CsvTestProbe,
    class: SinkDestinationClass,
) -> Result<PathBuf, SinkFixtureError> {
    probe.clear();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(single_flow(root.clone(), output, probe, class, false))
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    Ok(latest_run_dir(&root))
}

async fn seed_truncated_archive(
    root: PathBuf,
    output: PathBuf,
    probe: CsvTestProbe,
) -> Result<PathBuf, SinkFixtureError> {
    probe.clear();
    let handle = stalling_flow(root.clone(), output, probe.clone())
        .build(FlowBuildContext::for_tests())
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while probe.snapshot().count(SinkExternalCallKind::Commit) < 1 {
        if std::time::Instant::now() >= deadline {
            return Err(SinkFixtureError::new(
                "truncated seed did not commit its first CSV batch",
            ));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let archive = latest_run_dir(&root);
    handle
        .stop()
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .map_err(|_| SinkFixtureError::new("truncated seed did not stop"))?
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    Ok(archive)
}

#[derive(Debug, Eq, PartialEq)]
struct CsvSnapshot {
    primary: Vec<(u64, u64)>,
    secondary: Vec<(u64, u64)>,
}

fn read_rows(path: &Path) -> Result<Vec<(u64, u64)>, SinkFixtureError> {
    if !path.is_file()
        || std::fs::metadata(path)
            .map(|value| value.len())
            .unwrap_or(0)
            == 0
    {
        return Ok(Vec::new());
    }
    let mut reader =
        csv::Reader::from_path(path).map_err(|error| SinkFixtureError::new(error.to_string()))?;
    let mut rows = reader
        .deserialize::<Row>()
        .map(|row| {
            row.map(|row| (row.id, row.amount_cents))
                .map_err(|error| SinkFixtureError::new(error.to_string()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    rows.sort_unstable();
    Ok(rows)
}

struct CsvVerifier {
    primary: PathBuf,
    secondary: PathBuf,
    probe: CsvTestProbe,
}

#[async_trait]
impl SinkDestinationVerifier for CsvVerifier {
    type Snapshot = CsvSnapshot;

    async fn snapshot(&self) -> Result<Self::Snapshot, SinkFixtureError> {
        Ok(CsvSnapshot {
            primary: read_rows(&self.primary)?,
            secondary: read_rows(&self.secondary)?,
        })
    }

    async fn verify(
        &self,
        expectation: SinkDestinationExpectation,
        before: &Self::Snapshot,
        after: &Self::Snapshot,
    ) -> Result<(), SinkFixtureError> {
        let empty = CsvSnapshot {
            primary: Vec::new(),
            secondary: Vec::new(),
        };
        if before != &empty {
            return Err(SinkFixtureError::new(
                "scenario destination was not reset before launch",
            ));
        }
        let complete = vec![(1, 1_000), (2, 2_000), (3, 3_000), (4, 4_000)];
        let expected = match expectation.verdict() {
            SinkDestinationVerdict::Refused | SinkDestinationVerdict::Failed => empty,
            SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged
                if expectation.scenario().eof_kind() == EofKind::Truncated =>
            {
                CsvSnapshot {
                    primary: vec![(1, 1_000), (2, 2_000)],
                    secondary: Vec::new(),
                }
            }
            SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged
                if expectation.scenario().topology() == SinkApplicationTopology::FanOut =>
            {
                CsvSnapshot {
                    primary: complete.clone(),
                    secondary: complete,
                }
            }
            SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged => CsvSnapshot {
                primary: complete,
                secondary: Vec::new(),
            },
        };
        if after == &expected {
            Ok(())
        } else {
            Err(SinkFixtureError::new(format!(
                "expected CSV destination {expected:?}, observed {after:?}"
            )))
        }
    }

    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
        Ok(self.probe.snapshot())
    }
}

struct CsvApplicationFixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    verifier: CsvVerifier,
    duplicate_archive: PathBuf,
    unspecified_archive: PathBuf,
    truncated_archive: PathBuf,
    live_safe_root: Option<PathBuf>,
    next_case: usize,
}

impl CsvApplicationFixture {
    async fn new() -> Result<Self, SinkFixtureError> {
        let temp = tempfile::tempdir().map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let root = temp.path().join("journals");
        let primary = temp.path().join("primary.csv");
        let secondary = temp.path().join("secondary.csv");
        let probe = CsvTestProbe::default();
        let duplicate_archive = seed_live_archive(
            root.join("seed-duplicate"),
            primary.clone(),
            probe.clone(),
            SinkDestinationClass::DuplicateSensitive,
        )
        .await?;
        remove_output(&primary)?;
        let unspecified_archive = seed_live_archive(
            root.join("seed-unspecified"),
            primary.clone(),
            probe.clone(),
            SinkDestinationClass::Unspecified,
        )
        .await?;
        remove_output(&primary)?;
        let truncated_archive =
            seed_truncated_archive(root.join("seed-truncated"), primary.clone(), probe.clone())
                .await?;
        remove_output(&primary)?;
        probe.clear();

        Ok(Self {
            _temp: temp,
            root,
            verifier: CsvVerifier {
                primary,
                secondary,
                probe,
            },
            duplicate_archive,
            unspecified_archive,
            truncated_archive,
            live_safe_root: None,
            next_case: 0,
        })
    }
}

#[async_trait]
impl SinkApplicationConformanceFixture for CsvApplicationFixture {
    type Verifier = CsvVerifier;

    fn profile(&self) -> SinkConformanceProfile {
        SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            SinkSettlementMode::Buffered { batch_size: 2 },
        )
    }

    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
        self.verifier.probe.clear();
        remove_output(&self.verifier.primary)?;
        remove_output(&self.verifier.secondary)
    }

    fn build_case(
        &mut self,
        scenario: SinkApplicationScenario,
    ) -> Result<SinkApplicationBuildCase, SinkFixtureError> {
        let case_root = self.root.join(format!("case-{}", self.next_case));
        self.next_case += 1;
        if scenario.treatment() == SinkApplicationTreatment::Live
            && scenario.destination_class() == SinkDestinationClass::SafeToRepeat
            && scenario.topology() == SinkApplicationTopology::Single
            && scenario.eof_kind() == EofKind::Natural
        {
            self.live_safe_root = Some(case_root.clone());
        }
        if scenario.eof_kind() == EofKind::Poison {
            self.verifier.probe.arm(SinkFault::PreCommit);
        }

        let flow = match scenario.topology() {
            SinkApplicationTopology::Single => single_flow(
                case_root.clone(),
                self.verifier.primary.clone(),
                self.verifier.probe.clone(),
                scenario.destination_class(),
                scenario.eof_kind() == EofKind::Poison,
            ),
            SinkApplicationTopology::FanIn => fan_in_flow(
                case_root.clone(),
                self.verifier.primary.clone(),
                self.verifier.probe.clone(),
            ),
            SinkApplicationTopology::FanOut => fan_out_flow(
                case_root.clone(),
                self.verifier.primary.clone(),
                self.verifier.secondary.clone(),
                self.verifier.probe.clone(),
            ),
        };

        let mut args = vec![OsString::from("obzenflow")];
        match scenario.treatment() {
            SinkApplicationTreatment::Live => {}
            SinkApplicationTreatment::ArchiveRedelivery
            | SinkApplicationTreatment::ArchiveRedeliveryOverride => {
                let archive = if scenario.eof_kind() == EofKind::Truncated {
                    self.truncated_archive.clone()
                } else {
                    match scenario.destination_class() {
                        SinkDestinationClass::SafeToRepeat => {
                            latest_run_dir(self.live_safe_root.as_ref().ok_or_else(|| {
                                SinkFixtureError::new(
                                    "safe live archive was not produced before redelivery",
                                )
                            })?)
                        }
                        SinkDestinationClass::DuplicateSensitive => self.duplicate_archive.clone(),
                        SinkDestinationClass::Unspecified => self.unspecified_archive.clone(),
                    }
                };
                args.push(OsString::from("--replay-from"));
                args.push(archive.into_os_string());
                if scenario.treatment() == SinkApplicationTreatment::ArchiveRedeliveryOverride {
                    args.push(OsString::from("--allow-duplicate-sink-delivery"));
                }
            }
        }
        Ok(SinkApplicationBuildCase::new(flow, case_root).with_cli_args(args))
    }

    fn verifier(&self) -> &Self::Verifier {
        &self.verifier
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_passes_outward_application_conformance() {
    let mut fixture = CsvApplicationFixture::new()
        .await
        .expect("CSV application fixture initialises");
    let report = run_application_conformance(&mut fixture)
        .await
        .expect("CSV passes outward application conformance");
    assert_eq!(report.protocol_version(), SINK_CONFORMANCE_PROTOCOL_VERSION);
    assert_eq!(report.cases().len(), 9);
    assert_eq!(report.runs().len(), 7);
}
