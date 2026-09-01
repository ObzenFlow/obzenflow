// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "test-support")]

use async_trait::async_trait;
use obzenflow_adapters::sinks::postgres::testing::PostgresTestProbe;
use obzenflow_adapters::sinks::postgres::{
    PostgresBind, PostgresBindings, PostgresConnection, PostgresSink, PostgresTransport,
};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::sink::SinkWriteFailureDisposition;
use obzenflow_runtime::testing::sink::{
    run_writer_conformance, SinkBuildCase, SinkConformanceProfile, SinkDiagnosticSample,
    SinkDiagnosticSurface, SinkExternalCallSnapshot, SinkFault, SinkFaultCase, SinkFixtureError,
    SinkFixtureInputs, SinkSettlementMode, SinkWriterConformanceFixture,
    SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::{fs, path::PathBuf};

#[derive(Debug, Serialize, Deserialize)]
struct Payment {
    id: i64,
    amount_cents: i64,
}

impl TypedPayload for Payment {
    const EVENT_TYPE: &'static str = "flowip_122a.postgres.payment";
}

#[derive(Clone, Debug)]
struct PaymentBinder;

impl PostgresBind for PaymentBinder {
    type Input = Payment;

    fn bind(&self, bindings: &mut PostgresBindings, input: &Self::Input) {
        bindings.bind(input.id).bind(input.amount_cents);
    }
}

type PaymentSink = PostgresSink<PaymentBinder>;

struct PostgresFixture {
    connection: PostgresConnection,
    pool: PgPool,
    schema: String,
    probe: PostgresTestProbe,
}

impl PostgresFixture {
    async fn connect(url: &str) -> Result<Self, SinkFixtureError> {
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
        let schema = format!("obz083c_writer_{run_id}");
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
        Ok(Self {
            connection,
            pool,
            schema,
            probe: PostgresTestProbe::default(),
        })
    }

    async fn cleanup(&self) {
        let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema))
            .execute(&self.pool)
            .await;
    }
}

#[async_trait]
impl SinkWriterConformanceFixture for PostgresFixture {
    type Connector = PaymentSink;
    type DestinationSnapshot = Vec<(i64, i64)>;

    fn profile(&self) -> SinkConformanceProfile {
        use SinkWriteFailureDisposition::{ConfirmedRollback, CurrentOnly, Poisoned};
        SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            SinkSettlementMode::Buffered { batch_size: 2 },
        )
        .with_credential_sentinel(managed_postgres_secret())
        .with_fault(SinkFaultCase::operation(SinkFault::Open))
        .with_fault(SinkFaultCase::write(SinkFault::Encode, CurrentOnly))
        .with_fault(SinkFaultCase::write(SinkFault::Acquire, CurrentOnly))
        .with_fault(SinkFaultCase::write(SinkFault::BeforeDeferral, CurrentOnly))
        .with_fault(SinkFaultCase::write(SinkFault::AfterDeferral, CurrentOnly))
        .with_fault(
            SinkFaultCase::write(SinkFault::DestinationExecution, Poisoned)
                .with_deferred_operation_subject(),
        )
        .with_fault(SinkFaultCase::write(
            SinkFault::MidBatchMutation,
            ConfirmedRollback,
        ))
        .with_fault(SinkFaultCase::write(
            SinkFault::PreCommit,
            ConfirmedRollback,
        ))
        .with_fault(SinkFaultCase::write(SinkFault::Rollback, Poisoned))
        .with_fault(SinkFaultCase::write(
            SinkFault::PostCommitPreAcknowledgement,
            Poisoned,
        ))
        .with_fault(SinkFaultCase::operation(SinkFault::Flush))
        .with_fault(SinkFaultCase::operation(SinkFault::Drain))
    }

    fn build_cases(&self) -> Vec<SinkBuildCase<Self::Connector>> {
        let connection = self.connection.clone();
        let schema = self.schema.clone();
        let probe = self.probe.clone();
        vec![
            SinkBuildCase::valid("postgres-valid", move || {
                PostgresSink::builder(PaymentBinder)
                    .connection(connection.clone())
                    .insert_into(
                        &schema,
                        "payments",
                        "(id, amount_cents) VALUES ($1, $2) \
                         ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents",
                    )
                    .map_err(|error| SinkFixtureError::new(error.to_string()))?
                    .batch_size(2)
                    .map_err(|error| SinkFixtureError::new(error.to_string()))?
                    .test_probe(probe.clone())
                    .build()
                    .map_err(|error| SinkFixtureError::new(error.to_string()))
            }),
            SinkBuildCase::invalid("postgres-mismatched-target", || {
                Err(SinkFixtureError::new(
                    "intentional invalid statement destination",
                ))
            }),
        ]
    }

    fn fresh_inputs(&mut self) -> Result<SinkFixtureInputs<Payment>, SinkFixtureError> {
        Ok(SinkFixtureInputs::new((0..16).map(|id| Payment {
            id,
            amount_cents: 1000 + id,
        })))
    }

    async fn reset_destination(&mut self) -> Result<(), SinkFixtureError> {
        self.probe.clear();
        sqlx::query(&format!("TRUNCATE TABLE {}.payments", self.schema))
            .execute(&self.pool)
            .await
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        Ok(())
    }

    async fn arm_fault(&mut self, fault: SinkFault) -> Result<(), SinkFixtureError> {
        self.probe.arm(fault);
        Ok(())
    }

    async fn destination_snapshot(&self) -> Result<Vec<(i64, i64)>, SinkFixtureError> {
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

    fn external_calls(&self) -> Result<SinkExternalCallSnapshot, SinkFixtureError> {
        Ok(self.probe.snapshot())
    }

    fn diagnostic_samples(&self) -> Result<Vec<SinkDiagnosticSample>, SinkFixtureError> {
        Ok(vec![
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Debug,
                format!("{:?}", self.connection),
            ),
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Snapshot,
                format!("schema={}", self.schema),
            ),
        ])
    }
}

fn managed_postgres_secret() -> String {
    let path = std::env::var_os("PGPASSFILE")
        .map(PathBuf::from)
        .expect("PGPASSFILE is required from `cargo xtask postgres test`");
    let contents = fs::read_to_string(path).expect("read managed PostgreSQL pgpass file");
    let secret = contents
        .lines()
        .next()
        .and_then(|line| line.rsplit_once(':').map(|(_, secret)| secret.to_string()))
        .expect("managed pgpass has five fields");
    assert!(
        secret.len() == 64
            && secret
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f')),
        "managed PostgreSQL secret has the expected generated shape"
    );
    secret
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_passes_the_real_writer_protocol_and_fault_matrix() {
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL").expect(
        "OBZENFLOW_POSTGRES_TEST_URL is required: PostgreSQL conformance must not pass without a real database",
    );
    let mut fixture = PostgresFixture::connect(&url)
        .await
        .expect("PostgreSQL fixture initialises");
    let report = run_writer_conformance(&mut fixture)
        .await
        .expect("PostgreSQL connector conforms");
    assert_eq!(report.protocol_version(), SINK_CONFORMANCE_PROTOCOL_VERSION);
    assert!(report.cases().len() >= 20);
    fixture.cleanup().await;
}
