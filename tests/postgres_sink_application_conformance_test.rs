// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(all(feature = "postgres", feature = "test-support"))]

use async_trait::async_trait;
use obzenflow::sinks::postgres::sqlx::postgres::PgArguments;
use obzenflow::sinks::postgres::sqlx::query::Query;
use obzenflow::sinks::postgres::sqlx::{PgPool, Postgres, Row};
use obzenflow::sinks::postgres::testing::PostgresTestProbe;
use obzenflow::sinks::postgres::{PostgresBind, PostgresConnection, PostgresSink};
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
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::{SinkOperationPhase, SinkWritePhase};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::run_context::FlowBuildContext;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Payment {
    id: i64,
    amount_cents: i64,
}

impl TypedPayload for Payment {
    const EVENT_TYPE: &'static str = "flowip_122a.postgres.application.payment";
}

#[derive(Clone, Debug)]
struct PaymentBinder;

impl PostgresBind<Payment> for PaymentBinder {
    fn bind<'q>(
        &self,
        query: Query<'q, Postgres, PgArguments>,
        input: &'q Payment,
    ) -> Query<'q, Postgres, PgArguments> {
        query.bind(input.id).bind(input.amount_cents)
    }
}

type PaymentSink = PostgresSink<Payment, PaymentBinder>;

fn payments() -> Vec<Payment> {
    (1..=4)
        .map(|id| Payment {
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
        "flowip_122a_postgres_poison"
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
        "flowip_122a_postgres_poison"
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
struct StallingPayments {
    next: i64,
}

impl TypedFiniteSourceHandler for StallingPayments {
    type Output = Payment;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next <= 2 {
            let id = self.next;
            self.next += 1;
            return Ok(Some(vec![Payment {
                id,
                amount_cents: id * 1_000,
            }]));
        }
        std::thread::sleep(Duration::from_millis(10));
        Ok(Some(Vec::new()))
    }
}

fn flow_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("failed to build PostgreSQL sink: {error}"))
}

fn build_sink(
    connection: PostgresConnection,
    schema: &str,
    probe: PostgresTestProbe,
    class: SinkDestinationClass,
) -> Result<PaymentSink, FlowBuildError> {
    let builder = PostgresSink::<Payment>::builder()
        .connection(connection)
        .table(schema, "payments")
        .map_err(flow_error)?
        .statement(format!(
            "INSERT INTO {schema}.payments (id, amount_cents) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents"
        ))
        .map_err(flow_error)?
        .batch_size(2)
        .map_err(flow_error)?
        .bind_with(PaymentBinder)
        .test_probe(probe);
    let builder = match class {
        SinkDestinationClass::SafeToRepeat => {
            builder.redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
        }
        SinkDestinationClass::DuplicateSensitive => {
            builder.redelivery_safety(SinkRedeliverySafety::DuplicateSensitive)
        }
        SinkDestinationClass::Unspecified => builder,
    };
    builder.build().map_err(flow_error)
}

fn single_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
    class: SinkDestinationClass,
    poison_eof: bool,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres = build_sink(connection, &schema, probe, class)?;
        let payments = sources::finite(payments());
        if poison_eof {
            Ok(flow! {
                name: "postgres_sink_application",
                journals: disk_journals(journal_root),

                stages: {
                    payments = source!(Payment => payments with [PoisonFactory]);
                    postgres = sink!(Payment => postgres);
                },

                topology: {
                    payments |> postgres;
                }
            })
        } else {
            Ok(flow! {
                name: "postgres_sink_application",
                journals: disk_journals(journal_root),

                stages: {
                    payments = source!(Payment => payments);
                    postgres = sink!(Payment => postgres);
                },

                topology: {
                    payments |> postgres;
                }
            })
        }
    })
}

fn fan_in_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )?;
        let left = sources::finite(vec![payments()[0].clone(), payments()[2].clone()]);
        let right = sources::finite(vec![payments()[1].clone(), payments()[3].clone()]);
        Ok(flow! {
            name: "postgres_sink_application_fan_in",
            journals: disk_journals(journal_root),

            stages: {
                left = source!(Payment => left);
                right = source!(Payment => right);
                postgres = sink!(Payment => postgres);
            },

            topology: {
                left |> postgres;
                right |> postgres;
            }
        })
    })
}

fn fan_out_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres_a = build_sink(
            connection.clone(),
            &schema,
            probe.clone(),
            SinkDestinationClass::SafeToRepeat,
        )?;
        let postgres_b = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )?;
        let payments = sources::finite(payments());
        Ok(flow! {
            name: "postgres_sink_application_fan_out",
            journals: disk_journals(journal_root),

            stages: {
                payments = source!(Payment => payments);
                postgres_a = sink!(Payment => postgres_a);
                postgres_b = sink!(Payment => postgres_b);
            },

            topology: {
                payments |> postgres_a;
                payments |> postgres_b;
            }
        })
    })
}

fn stalling_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )?;
        let payments = StallingPayments { next: 1 };
        Ok(flow! {
            name: "postgres_sink_application",
            journals: disk_journals(journal_root),

            stages: {
                payments = source!(Payment => payments);
                postgres = sink!(Payment => postgres);
            },

            topology: {
                payments |> postgres;
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

async fn truncate(pool: &PgPool, schema: &str) -> Result<(), SinkFixtureError> {
    obzenflow::sinks::postgres::sqlx::query(&format!("TRUNCATE TABLE {schema}.payments"))
        .execute(pool)
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    Ok(())
}

async fn seed_live_archive(
    root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
    class: SinkDestinationClass,
) -> Result<PathBuf, SinkFixtureError> {
    probe.clear();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(single_flow(
            root.clone(),
            connection,
            schema,
            probe,
            class,
            false,
        ))
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    Ok(latest_run_dir(&root))
}

async fn seed_truncated_archive(
    root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
) -> Result<PathBuf, SinkFixtureError> {
    probe.clear();
    let handle = stalling_flow(root.clone(), connection, schema, probe.clone())
        .build(FlowBuildContext::for_tests())
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while probe.snapshot().count(SinkExternalCallKind::Commit) < 1 {
        if std::time::Instant::now() >= deadline {
            return Err(SinkFixtureError::new(
                "truncated seed did not commit its first PostgreSQL batch",
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

struct PostgresVerifier {
    pool: PgPool,
    schema: String,
    probe: PostgresTestProbe,
}

#[async_trait]
impl SinkDestinationVerifier for PostgresVerifier {
    type Snapshot = Vec<(i64, i64)>;

    async fn snapshot(&self) -> Result<Self::Snapshot, SinkFixtureError> {
        obzenflow::sinks::postgres::sqlx::query(&format!(
            "SELECT id, amount_cents FROM {}.payments ORDER BY id",
            self.schema
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))
        .map(|rows| {
            rows.into_iter()
                .map(|row| (row.get("id"), row.get("amount_cents")))
                .collect()
        })
    }

    async fn verify(
        &self,
        expectation: SinkDestinationExpectation,
        before: &Self::Snapshot,
        after: &Self::Snapshot,
    ) -> Result<(), SinkFixtureError> {
        if !before.is_empty() {
            return Err(SinkFixtureError::new(
                "scenario destination was not reset before launch",
            ));
        }
        let expected_complete = vec![(1, 1_000), (2, 2_000), (3, 3_000), (4, 4_000)];
        match expectation.verdict() {
            SinkDestinationVerdict::Refused if after == before => Ok(()),
            SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged
                if expectation.scenario().eof_kind() == EofKind::Truncated
                    && after == &vec![(1, 1_000), (2, 2_000)] =>
            {
                Ok(())
            }
            SinkDestinationVerdict::Committed | SinkDestinationVerdict::Converged
                if after == &expected_complete =>
            {
                Ok(())
            }
            SinkDestinationVerdict::Failed if after == &vec![(1, 1_000), (2, 2_000)] => Ok(()),
            verdict => Err(SinkFixtureError::new(format!(
                "destination verdict {verdict:?} observed rows {after:?}"
            ))),
        }
    }

    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
        Ok(self.probe.snapshot())
    }
}

struct PostgresApplicationFixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
    connection: PostgresConnection,
    verifier: PostgresVerifier,
    duplicate_archive: PathBuf,
    unspecified_archive: PathBuf,
    truncated_archive: PathBuf,
    live_safe_root: Option<PathBuf>,
    next_case: usize,
}

impl PostgresApplicationFixture {
    async fn connect(url: &str) -> Result<Self, SinkFixtureError> {
        let temp = tempfile::tempdir().map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let root = temp.path().join("journals");
        let connection = PostgresConnection::from_url(url)
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let pool = PgPool::connect(url)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let schema = format!("obz122a_app_{}", std::process::id());
        obzenflow::sinks::postgres::sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&pool)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        obzenflow::sinks::postgres::sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&pool)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        obzenflow::sinks::postgres::sqlx::query(&format!(
            "CREATE TABLE {schema}.payments (id BIGINT PRIMARY KEY, amount_cents BIGINT NOT NULL)"
        ))
        .execute(&pool)
        .await
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;

        let probe = PostgresTestProbe::default();
        let duplicate_archive = seed_live_archive(
            root.join("seed-duplicate"),
            connection.clone(),
            schema.clone(),
            probe.clone(),
            SinkDestinationClass::DuplicateSensitive,
        )
        .await?;
        truncate(&pool, &schema).await?;
        let unspecified_archive = seed_live_archive(
            root.join("seed-unspecified"),
            connection.clone(),
            schema.clone(),
            probe.clone(),
            SinkDestinationClass::Unspecified,
        )
        .await?;
        truncate(&pool, &schema).await?;
        let truncated_archive = seed_truncated_archive(
            root.join("seed-truncated"),
            connection.clone(),
            schema.clone(),
            probe.clone(),
        )
        .await?;
        truncate(&pool, &schema).await?;
        probe.clear();

        Ok(Self {
            _temp: temp,
            root,
            connection,
            verifier: PostgresVerifier {
                pool,
                schema,
                probe,
            },
            duplicate_archive,
            unspecified_archive,
            truncated_archive,
            live_safe_root: None,
            next_case: 0,
        })
    }

    async fn cleanup(&self) {
        let _ = obzenflow::sinks::postgres::sqlx::query(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.verifier.schema
        ))
        .execute(&self.verifier.pool)
        .await;
    }
}

#[async_trait]
impl SinkApplicationConformanceFixture for PostgresApplicationFixture {
    type Verifier = PostgresVerifier;

    fn profile(&self) -> SinkConformanceProfile {
        SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            SinkSettlementMode::Buffered { batch_size: 2 },
        )
    }

    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
        self.verifier.probe.clear();
        truncate(&self.verifier.pool, &self.verifier.schema).await
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
            self.verifier
                .probe
                .arm(SinkFault::PostCommitPreAcknowledgement);
        }

        let flow = match scenario.topology() {
            SinkApplicationTopology::Single => single_flow(
                case_root.clone(),
                self.connection.clone(),
                self.verifier.schema.clone(),
                self.verifier.probe.clone(),
                scenario.destination_class(),
                scenario.eof_kind() == EofKind::Poison,
            ),
            SinkApplicationTopology::FanIn => fan_in_flow(
                case_root.clone(),
                self.connection.clone(),
                self.verifier.schema.clone(),
                self.verifier.probe.clone(),
            ),
            SinkApplicationTopology::FanOut => fan_out_flow(
                case_root.clone(),
                self.connection.clone(),
                self.verifier.schema.clone(),
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
async fn postgres_passes_live_redelivery_gate_and_archived_failure_projection() {
    let Some(url) = std::env::var("OBZENFLOW_POSTGRES_TEST_URL").ok() else {
        eprintln!("skipping PostgreSQL application conformance: test URL is unset");
        return;
    };
    let mut fixture = PostgresApplicationFixture::connect(&url)
        .await
        .expect("PostgreSQL application fixture initialises");
    let report = run_application_conformance(&mut fixture)
        .await
        .expect("PostgreSQL passes outward application conformance");
    assert_eq!(report.protocol_version(), SINK_CONFORMANCE_PROTOCOL_VERSION);
    assert_eq!(report.cases().len(), 9);
    assert_eq!(report.runs().len(), 7);

    let operation_runs = report
        .runs()
        .iter()
        .filter(|run| !run.operation_failures().is_empty())
        .collect::<Vec<_>>();
    assert_eq!(operation_runs.len(), 1);
    let operation = &operation_runs[0].operation_failures()[0];
    assert_eq!(
        operation.phase(),
        SinkOperationPhase::Write(SinkWritePhase::Commit)
    );
    assert_eq!(
        operation
            .destination_error_code()
            .map(|code| (code.namespace(), code.value())),
        Some(("postgres.sqlstate", "08007"))
    );
    assert_eq!(operation_runs[0].operation_failure_metrics().len(), 1);
    let chain = &operation_runs[0].failure_chains()[0];
    assert!(chain.receipt_to_operation());
    assert!(chain.operation_to_route());
    assert!(chain.route_to_lifecycle());

    fixture.cleanup().await;
}
