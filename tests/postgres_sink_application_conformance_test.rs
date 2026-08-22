// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(all(feature = "postgres", feature = "test-support"))]

mod replay_testkit;

use async_trait::async_trait;
use obzenflow::sinks::postgres::testing::{PostgresTestProbe, POSTGRES_SQLSTATE_NAMESPACE};
use obzenflow::sinks::postgres::{
    PostgresBind, PostgresConnection, PostgresQuery, PostgresSink, PostgresTransport,
};
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
use obzenflow_core::event::{SinkOperationPhase, SinkWritePhase, StageActivity};
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, transform, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::source::strategies::{
    CompletionContext, CompletionDecision, CompletionGate,
};
use obzenflow_runtime::stages::{SourceError, TypedFiniteSourceHandler, TypedTransformHandler};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use obzenflow_runtime::testing::sink::{
    SinkConformanceProfile, SinkDiagnosticSample, SinkDiagnosticSurface, SinkExternalCallKind,
    SinkExternalCallSnapshot, SinkFault, SinkFixtureError, SinkSettlementMode,
    SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

static POSTGRES_APPLICATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    fn bind<'q>(&self, query: PostgresQuery<'q>, input: &'q Payment) -> PostgresQuery<'q> {
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

#[derive(Clone, Debug)]
struct DelayedPayments {
    payments: Vec<Payment>,
    next: usize,
    initial_delay: Duration,
}

impl DelayedPayments {
    fn new(payments: Vec<Payment>, initial_delay: Duration) -> Self {
        Self {
            payments,
            next: 0,
            initial_delay,
        }
    }
}

impl TypedFiniteSourceHandler for DelayedPayments {
    type Output = Payment;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next >= self.payments.len() {
            return Ok(None);
        }
        if self.next == 0 && !self.initial_delay.is_zero() {
            std::thread::sleep(self.initial_delay);
        }
        let payment = self.payments[self.next].clone();
        self.next += 1;
        Ok(Some(vec![payment]))
    }
}

#[derive(Clone, Debug)]
struct DelayedPaymentTap {
    delay: Duration,
}

impl TypedTransformHandler for DelayedPaymentTap {
    type Input = Payment;
    type Output = Payment;

    fn process(&self, input: Payment) -> Result<Payment, HandlerError> {
        std::thread::sleep(self.delay);
        Ok(input)
    }
}

#[derive(Clone, Debug)]
struct IdentityPayment;

impl TypedTransformHandler for IdentityPayment {
    type Input = Payment;
    type Output = Payment;

    fn process(&self, input: Payment) -> Result<Payment, HandlerError> {
        Ok(input)
    }
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

fn flow_error(error: impl std::fmt::Display) -> Box<FlowBuildError> {
    Box::new(FlowBuildError::StageResourcesFailed(format!(
        "failed to build PostgreSQL sink: {error}"
    )))
}

fn build_sink(
    connection: PostgresConnection,
    schema: &str,
    probe: PostgresTestProbe,
    class: SinkDestinationClass,
) -> Result<PaymentSink, Box<FlowBuildError>> {
    let builder = PostgresSink::<Payment>::builder()
        .connection(connection)
        .insert_into(
            schema,
            "payments",
            "(id, amount_cents) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents",
        )
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
        SinkDestinationClass::Unspecified => builder.test_redelivery_unspecified(),
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
        let postgres = build_sink(connection, &schema, probe, class).map_err(|error| *error)?;
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
        )
        .map_err(|error| *error)?;
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

fn ordered_source_fan_in_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
    right_initial_delay: Duration,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )
        .map_err(|error| *error)?;
        let left = DelayedPayments::new(
            vec![
                Payment {
                    id: 9_101,
                    amount_cents: 100,
                },
                Payment {
                    id: 9_101,
                    amount_cents: 300,
                },
            ],
            Duration::ZERO,
        );
        let right = DelayedPayments::new(
            vec![
                Payment {
                    id: 9_101,
                    amount_cents: 200,
                },
                Payment {
                    id: 9_101,
                    amount_cents: 400,
                },
            ],
            right_initial_delay,
        );
        Ok(flow! {
            name: "postgres_order_sensitive_source_fan_in",
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

fn ordered_derived_fan_in_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
    delay: Duration,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let payments = sources::finite([Payment {
            id: 9_102,
            amount_cents: 1_250,
        }]);
        let delayed = DelayedPaymentTap { delay };
        let postgres = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )
        .map_err(|error| *error)?;
        Ok(flow! {
            name: "postgres_order_sensitive_derived_fan_in",
            journals: disk_journals(journal_root),

            stages: {
                payments = source!(Payment => payments);
                delayed = transform!(Payment -> Payment => delayed);
                postgres = sink!(Payment => postgres);
            },

            topology: {
                payments |> delayed;
                payments |> postgres;
                delayed |> postgres;
            }
        })
    })
}

fn ordered_cycle_fan_in_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let payments = sources::finite([Payment {
            id: 9_103,
            amount_cents: 500,
        }]);
        let cycle_a = IdentityPayment;
        let cycle_b = IdentityPayment;
        let postgres = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )
        .map_err(|error| *error)?;
        Ok(flow! {
            name: "postgres_order_sensitive_cycle_fan_in",
            journals: disk_journals(journal_root),

            stages: {
                payments = source!(Payment => payments);
                cycle_a = transform!(Payment -> Payment => cycle_a);
                cycle_b = transform!(Payment -> Payment => cycle_b);
                postgres = sink!(Payment => postgres);
            },

            topology: {
                payments |> cycle_a;
                cycle_a |> cycle_b;
                cycle_a <| cycle_b;
                cycle_b |> postgres;
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
        )
        .map_err(|error| *error)?;
        let postgres_b = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )
        .map_err(|error| *error)?;
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
        )
        .map_err(|error| *error)?;
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

struct OrderingDatabase {
    pool: PgPool,
    connection: PostgresConnection,
    schema: String,
}

impl OrderingDatabase {
    async fn connect(url: &str, label: &str) -> Self {
        let run_id = std::env::var("OBZENFLOW_POSTGRES_TEST_RUN_ID")
            .expect("OBZENFLOW_POSTGRES_TEST_RUN_ID comes from cargo xtask postgres test");
        assert!(
            run_id.len() == 32 && run_id.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "PostgreSQL ordering proof requires a canonical run token"
        );
        let schema = format!("obz083c_order_{label}_{run_id}");
        assert!(
            schema.len() <= 63,
            "ordering schema fits PostgreSQL's bound"
        );
        let pool = PgPool::connect(url)
            .await
            .expect("ordering proof connects to PostgreSQL");
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&pool)
            .await
            .expect("reset ordering schema");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&pool)
            .await
            .expect("create ordering schema");
        sqlx::query(&format!(
            "CREATE TABLE {schema}.payments (id BIGINT PRIMARY KEY, amount_cents BIGINT NOT NULL)"
        ))
        .execute(&pool)
        .await
        .expect("create ordering destination");
        let connection =
            PostgresConnection::from_url(url, PostgresTransport::ExternallyProtectedPlaintext)
                .expect("build ordering connection");
        Self {
            pool,
            connection,
            schema,
        }
    }

    async fn amount(&self, id: i64) -> Option<i64> {
        sqlx::query_scalar(&format!(
            "SELECT amount_cents FROM {}.payments WHERE id = $1",
            self.schema
        ))
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .expect("read ordering destination")
    }

    async fn cleanup(&self) {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .execute(&self.pool)
            .await
            .expect("drop ordering schema");
    }
}

fn same_key_payment_word(sequence: &[(String, u64)]) -> Vec<i64> {
    sequence
        .iter()
        .map(|(stage, ordinal)| match (stage.as_str(), *ordinal) {
            ("left", 1) => 100,
            ("left", 2) => 300,
            ("right", 1) => 200,
            ("right", 2) => 400,
            other => panic!("unexpected same-key input coordinate: {other:?}"),
        })
        .collect()
}

fn assert_postgres_ordered_delivery(run: &Path, expected_inbound: &[&str]) {
    let manifest = replay_testkit::archive_manifest(run);
    assert_eq!(
        manifest["stages"]["postgres"]["ordered_delivery"],
        serde_json::Value::Bool(true),
        "the real PostgreSQL sink must record deterministic input delivery"
    );
    let inbound = manifest["stages"]["postgres"]["inbound"]
        .as_array()
        .expect("PostgreSQL manifest records inbound stages")
        .iter()
        .map(|value| value.as_str().expect("inbound stage key"))
        .collect::<Vec<_>>();
    assert_eq!(inbound, expected_inbound);
}

async fn truncate(pool: &PgPool, schema: &str) -> Result<(), SinkFixtureError> {
    sqlx::query(&format!("TRUNCATE TABLE {schema}.payments"))
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
        sqlx::query(&format!(
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
        let connection =
            PostgresConnection::from_url(url, PostgresTransport::ExternallyProtectedPlaintext)
                .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let pool = PgPool::connect(url)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let run_id = std::env::var("OBZENFLOW_POSTGRES_TEST_RUN_ID").map_err(|_| {
            SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_RUN_ID is required from `cargo xtask postgres test`",
            )
        })?;
        if run_id.len() != 32 || !run_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_RUN_ID is not a canonical run token",
            ));
        }
        let schema = format!("obz083c_application_{run_id}");
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&pool)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&pool)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        sqlx::query(&format!(
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
        let _ = sqlx::query(&format!(
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
        .with_credential_sentinel("obzenflow-secret-083c")
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

    fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
        Ok(vec![
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Debug,
                format!("{:?}", self.connection),
            ),
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Verifier,
                format!("schema={}", self.verifier.schema),
            ),
        ])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_order_sensitive_source_fan_in_replays_same_word_and_final_row() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let database = OrderingDatabase::connect(&url, "source").await;
    let temp = tempfile::tempdir().expect("source fan-in journal directory");
    let journal_root = temp.path().join("journals");
    let delay = Duration::from_secs(4);

    let handle = ordered_source_fan_in_flow(
        journal_root.clone(),
        database.connection.clone(),
        database.schema.clone(),
        PostgresTestProbe::default(),
        delay,
    )
    .build(FlowBuildContext::for_tests())
    .await
    .expect("source-fed PostgreSQL fan-in builds");

    let early_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        if database.amount(9_101).await == Some(300) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < early_deadline,
            "source-fed PostgreSQL fan-in waited on the quiet right source instead of committing the two admitted left inputs"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    tokio::time::timeout(Duration::from_secs(12), handle.wait_for_completion())
        .await
        .expect("source-fed PostgreSQL fan-in completes")
        .expect("source-fed PostgreSQL fan-in succeeds");
    let live_run = replay_testkit::latest_run_dir(&journal_root);
    assert_postgres_ordered_delivery(&live_run, &["left", "right"]);
    let live_projection =
        replay_testkit::project_delivered_order(&live_run, "postgres", &["left", "right"]).await;
    let live_sequence = live_projection.consumption_sequence();
    let live_word = same_key_payment_word(&live_sequence);
    assert_eq!(live_word.len(), 4, "all same-key inputs reach PostgreSQL");
    assert_eq!(
        database.amount(9_101).await,
        live_word.last().copied(),
        "the live destination row is the final delivered value"
    );

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            live_run.as_os_str().to_os_string(),
            OsString::from("--verify"),
        ])
        .run_async(ordered_source_fan_in_flow(
            journal_root.clone(),
            database.connection.clone(),
            database.schema.clone(),
            PostgresTestProbe::default(),
            delay,
        ))
        .await
        .expect("same-key PostgreSQL fan-in redelivers with verified journals");
    let replay_run = replay_testkit::latest_run_dir(&journal_root);
    assert_ne!(replay_run, live_run);
    assert_postgres_ordered_delivery(&replay_run, &["left", "right"]);
    replay_testkit::assert_same_delivered_order(
        &live_run,
        &replay_run,
        "postgres",
        &["left", "right"],
    )
    .await;
    let replay_word = same_key_payment_word(
        &replay_testkit::project_delivered_order(&replay_run, "postgres", &["left", "right"])
            .await
            .consumption_sequence(),
    );
    assert_eq!(replay_word, live_word, "replay reproduces the sink word");
    assert_eq!(
        database.amount(9_101).await,
        live_word.last().copied(),
        "archive redelivery converges to the same final destination row"
    );
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_order_sensitive_derived_fan_in_reports_named_quiet_input() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let database = OrderingDatabase::connect(&url, "derived").await;
    let temp = tempfile::tempdir().expect("derived fan-in journal directory");
    let journal_root = temp.path().join("journals");
    let handle = ordered_derived_fan_in_flow(
        journal_root.clone(),
        database.connection.clone(),
        database.schema.clone(),
        PostgresTestProbe::default(),
        Duration::from_secs(3),
    )
    .build(FlowBuildContext::for_tests())
    .await
    .expect("derived-fed PostgreSQL fan-in builds");
    let topology = handle.topology().expect("flow exposes its topology");
    let snapshots = handle
        .liveness_snapshots()
        .expect("flow exposes liveness snapshots");

    let quiet_deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let quiet_upstream = loop {
        if let Some(upstream) = snapshots.with_read(|all| {
            all.values().find_map(|snapshot| {
                if snapshot.stage_name != "postgres" {
                    return None;
                }
                match snapshot.activity {
                    StageActivity::WaitingOnQuietInput {
                        upstream: Some(upstream),
                    } => Some(upstream),
                    _ => None,
                }
            })
        }) {
            break upstream;
        }
        assert!(
            tokio::time::Instant::now() < quiet_deadline,
            "derived-fed PostgreSQL fan-in never reported its canonical Kahn quiet input"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        topology.stage_name(quiet_upstream.to_topology_id()),
        Some("delayed"),
        "the Kahn wait identifies the delayed derived upstream"
    );

    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("derived-fed PostgreSQL fan-in completes")
        .expect("derived-fed PostgreSQL fan-in succeeds");
    let run = replay_testkit::latest_run_dir(&journal_root);
    assert_postgres_ordered_delivery(&run, &["delayed", "payments"]);
    let sequence =
        replay_testkit::project_delivered_order(&run, "postgres", &["payments", "delayed"])
            .await
            .consumption_sequence();
    assert_eq!(
        sequence,
        vec![("payments".to_string(), 1), ("delayed".to_string(), 1)],
        "canonical Kahn delivery preserves the direct event before its derived sibling"
    );
    assert_eq!(database.amount(9_102).await, Some(1_250));
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_order_sensitive_cycle_fan_in_is_rejected_before_open() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let run_id = std::env::var("OBZENFLOW_POSTGRES_TEST_RUN_ID")
        .expect("OBZENFLOW_POSTGRES_TEST_RUN_ID comes from cargo xtask postgres test");
    let connection =
        PostgresConnection::from_url(&url, PostgresTransport::ExternallyProtectedPlaintext)
            .expect("build cycle proof connection");
    let probe = PostgresTestProbe::default();
    let temp = tempfile::tempdir().expect("cycle fan-in journal directory");
    let result = ordered_cycle_fan_in_flow(
        temp.path().join("journals"),
        connection,
        format!("obz083c_cycle_{run_id}"),
        probe.clone(),
    )
    .build(FlowBuildContext::for_tests())
    .await;
    let failure = match result {
        Err(failure) => failure,
        Ok(handle) => {
            let _ = handle.stop().await;
            panic!("cycle-fed order-sensitive PostgreSQL sink must fail construction");
        }
    };
    match failure.error {
        FlowBuildError::OrderObserverFanInRequiresDeterministicOrder { stage_name } => {
            assert_eq!(stage_name, "postgres");
        }
        other => panic!("unexpected cycle-fed PostgreSQL build failure: {other:?}"),
    }
    assert!(
        probe.snapshot().calls().is_empty(),
        "cycle rejection occurs before the PostgreSQL sink opens a writer"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_passes_live_redelivery_gate_and_archived_failure_projection() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL").expect(
        "OBZENFLOW_POSTGRES_TEST_URL is required: PostgreSQL application conformance must not pass without a real database",
    );
    let mut fixture = PostgresApplicationFixture::connect(&url)
        .await
        .expect("PostgreSQL application fixture initialises");
    let report = run_application_conformance(&mut fixture)
        .await
        .expect("PostgreSQL passes outward application conformance");
    let mut report_failures = Vec::new();
    if report.protocol_version() != SINK_CONFORMANCE_PROTOCOL_VERSION {
        report_failures.push(format!(
            "protocol version: expected {SINK_CONFORMANCE_PROTOCOL_VERSION}, got {}",
            report.protocol_version()
        ));
    }
    if report.cases().len() != 9 {
        report_failures.push(format!(
            "case count: expected 9, got {}",
            report.cases().len()
        ));
    }
    if report.runs().len() != 7 {
        report_failures.push(format!(
            "durable run count: expected 7, got {}",
            report.runs().len()
        ));
    }

    let operation_runs = report
        .runs()
        .iter()
        .filter(|run| !run.operation_failures().is_empty())
        .collect::<Vec<_>>();
    if operation_runs.len() != 1 {
        report_failures.push(format!(
            "operation-failure run count: expected 1, got {}",
            operation_runs.len()
        ));
    }
    if let Some(run) = operation_runs.first() {
        if run.operation_failures().len() != 1 {
            report_failures.push(format!(
                "operation failure count: expected 1, got {}",
                run.operation_failures().len()
            ));
        }
        if let Some(operation) = run.operation_failures().first() {
            let expected_phase = SinkOperationPhase::Write(SinkWritePhase::Commit);
            if operation.phase() != expected_phase {
                report_failures.push(format!(
                    "operation phase: expected {expected_phase:?}, got {:?}",
                    operation.phase()
                ));
            }
            let expected_code = Some((POSTGRES_SQLSTATE_NAMESPACE, "08007"));
            let actual_code = operation
                .destination_error_code()
                .map(|code| (code.namespace(), code.value()));
            if actual_code != expected_code {
                report_failures.push(format!(
                    "destination error code: expected {expected_code:?}, got {actual_code:?}"
                ));
            }
        }
        if run.operation_failure_metrics().len() != 1 {
            report_failures.push(format!(
                "operation failure metric count: expected 1, got {}",
                run.operation_failure_metrics().len()
            ));
        }
        if run.failure_chains().len() != 1 {
            report_failures.push(format!(
                "failure-chain count: expected 1, got {}",
                run.failure_chains().len()
            ));
        }
        if let Some(chain) = run.failure_chains().first() {
            if !chain.receipt_to_operation() {
                report_failures.push("failure chain is missing receipt -> operation".to_string());
            }
            if !chain.operation_to_route() {
                report_failures.push("failure chain is missing operation -> route".to_string());
            }
            if !chain.route_to_lifecycle() {
                report_failures.push("failure chain is missing route -> lifecycle".to_string());
            }
        }
    }

    fixture.cleanup().await;

    assert!(
        report_failures.is_empty(),
        "PostgreSQL application conformance report mismatches:\n{}",
        report_failures.join("\n")
    );
}
