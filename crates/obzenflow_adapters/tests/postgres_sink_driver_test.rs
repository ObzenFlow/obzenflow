// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "test-support")]

use obzenflow_adapters::sinks::postgres::testing::{PostgresDelayPoint, PostgresTestProbe};
use obzenflow_adapters::sinks::postgres::{
    PostgresBind, PostgresConnection, PostgresQuery, PostgresSink, PostgresTransport,
    PostgresWriter,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_runtime::stages::common::handlers::sink::{SinkHandler, SinkWriterAdapter};
use obzenflow_runtime::stages::common::HandlerError;
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkOperationError, SinkWriteFailureDisposition, SinkWritePhase,
    SinkWriterInitContext,
};
use obzenflow_runtime::testing::sink::{SinkExternalCallKind, SinkFault};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::path::Path;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use url::Url;

static NEXT_SCHEMA: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Serialize, Deserialize)]
struct DriverInput {
    id: i64,
    value: String,
}

impl TypedPayload for DriverInput {
    const EVENT_TYPE: &'static str = "flowip_083c.postgres.driver_input";
}

#[derive(Clone, Debug)]
struct IdValueBinder;

impl PostgresBind<DriverInput> for IdValueBinder {
    fn bind<'q>(&self, query: PostgresQuery<'q>, input: &'q DriverInput) -> PostgresQuery<'q> {
        query.bind(input.id).bind(&input.value)
    }
}

#[derive(Clone, Debug)]
struct ValueBinder;

impl PostgresBind<DriverInput> for ValueBinder {
    fn bind<'q>(&self, query: PostgresQuery<'q>, input: &'q DriverInput) -> PostgresQuery<'q> {
        query.bind(&input.value)
    }
}

#[derive(Clone, Debug)]
struct IdOnlyBinder;

impl PostgresBind<DriverInput> for IdOnlyBinder {
    fn bind<'q>(&self, query: PostgresQuery<'q>, input: &'q DriverInput) -> PostgresQuery<'q> {
        query.bind(input.id)
    }
}

#[derive(Clone, Debug)]
struct NoBinder;

impl PostgresBind<DriverInput> for NoBinder {
    fn bind<'q>(&self, query: PostgresQuery<'q>, _input: &'q DriverInput) -> PostgresQuery<'q> {
        query
    }
}

struct Fixture {
    url: String,
    pool: PgPool,
    schema: String,
}

impl Fixture {
    async fn new(label: &str) -> Self {
        let url = required_env("OBZENFLOW_POSTGRES_TEST_URL");
        let run_id = required_run_id();
        let ordinal = NEXT_SCHEMA.fetch_add(1, Ordering::Relaxed);
        let schema = format!("obz083c_{label}_{run_id}_{ordinal}");
        assert!(schema.len() <= 63);
        let pool = PgPool::connect(&url)
            .await
            .expect("connect to repository PostgreSQL proof service");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&pool)
            .await
            .expect("create collision-isolated driver schema");
        Self { url, pool, schema }
    }

    fn connection(&self) -> PostgresConnection {
        PostgresConnection::from_url(&self.url, PostgresTransport::ExternallyProtectedPlaintext)
            .expect("repository plaintext URL matches its typed transport policy")
    }

    async fn cleanup(self) {
        sqlx::query(&format!("DROP SCHEMA {} CASCADE", self.schema))
            .execute(&self.pool)
            .await
            .expect("drop driver schema");
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| panic!("{name} is required from `cargo xtask postgres test`"))
}

fn required_run_id() -> String {
    let run_id = required_env("OBZENFLOW_POSTGRES_TEST_RUN_ID");
    assert!(
        run_id.len() == 32 && run_id.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "repository tooling must supply a canonical random run token"
    );
    run_id
}

fn sqlstate(error: &SinkOperationError) -> Option<&str> {
    error.destination_error_code().map(|code| code.value())
}

async fn open_writer<B>(
    sink: &PostgresSink<DriverInput, B>,
) -> Result<PostgresWriter<DriverInput, B>, SinkOperationError>
where
    B: PostgresBind<DriverInput>,
{
    sink.open(SinkWriterInitContext::new(
        StageId::new(),
        "postgres-driver".to_string(),
        "flowip-083c".to_string(),
    ))
    .await
}

async fn open_adapter<B>(
    sink: PostgresSink<DriverInput, B>,
) -> Result<(SinkWriterAdapter<PostgresWriter<DriverInput, B>>, WriterId), SinkOperationError>
where
    B: PostgresBind<DriverInput>,
{
    let description = sink.describe();
    let stage_id = StageId::new();
    let writer = sink
        .open(SinkWriterInitContext::new(
            stage_id,
            "postgres-driver".to_string(),
            "flowip-083c".to_string(),
        ))
        .await?;
    Ok((
        SinkWriterAdapter::with_default_method(
            writer,
            stage_id,
            description.default_method().cloned(),
        ),
        WriterId::from(stage_id),
    ))
}

async fn consume<B>(
    adapter: &mut SinkWriterAdapter<PostgresWriter<DriverInput, B>>,
    writer_id: WriterId,
    input: DriverInput,
) -> Result<obzenflow_core::event::payloads::delivery_payload::DeliveryPayload, HandlerError>
where
    B: PostgresBind<DriverInput>,
{
    let event =
        ChainEventFactory::data_event_from(writer_id, DriverInput::versioned_event_type(), &input)
            .expect("driver input serialises");
    adapter.consume(event).await
}

fn sink<B>(
    connection: PostgresConnection,
    schema: &str,
    table: &str,
    body: &str,
    binder: B,
    probe: PostgresTestProbe,
) -> PostgresSink<DriverInput, B>
where
    B: PostgresBind<DriverInput>,
{
    PostgresSink::<DriverInput>::builder()
        .connection(connection)
        .insert_into(schema, table, body)
        .expect("driver target and body pass local validation")
        .bind_with(binder)
        .test_probe(probe)
        .build()
        .expect("driver sink builds without I/O")
}

fn url_with_root(url: &str, root: &Path) -> String {
    let mut url = Url::parse(url).expect("test TLS URL parses");
    url.query_pairs_mut()
        .append_pair("sslrootcert", root.to_str().expect("CA path is Unicode"));
    url.into()
}

#[tokio::test(flavor = "multi_thread")]
async fn open_is_non_mutating_and_postgres_owns_statement_authority() {
    let fixture = Fixture::new("open").await;
    sqlx::query(&format!(
        "CREATE SEQUENCE {}.readiness_id_seq",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create readiness sequence");
    sqlx::query(&format!(
        "CREATE TABLE {}.readiness (\
         id BIGINT DEFAULT nextval('{}.readiness_id_seq'), value TEXT)",
        fixture.schema, fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create readiness table");
    sqlx::query(&format!(
        "CREATE TABLE {}.readiness_audit (value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create readiness audit");
    sqlx::query(&format!(
        "CREATE FUNCTION {}.audit_readiness() RETURNS trigger AS $$ BEGIN \
         INSERT INTO {}.readiness_audit(value) VALUES (NEW.value); RETURN NEW; END; \
         $$ LANGUAGE plpgsql",
        fixture.schema, fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create readiness trigger function");
    sqlx::query(&format!(
        "CREATE TRIGGER readiness_audit AFTER INSERT ON {}.readiness \
         FOR EACH ROW EXECUTE FUNCTION {}.audit_readiness()",
        fixture.schema, fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create readiness trigger");

    let sequence_before: (i64, bool) = sqlx::query_as(&format!(
        "SELECT last_value, is_called FROM {}.readiness_id_seq",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read sequence baseline");
    let valid = sink(
        fixture.connection(),
        &fixture.schema,
        "readiness",
        "(value) VALUES ($1)",
        ValueBinder,
        PostgresTestProbe::default(),
    );
    drop(open_writer(&valid).await.expect("valid INSERT prepares"));
    let literal_semicolon = sink(
        fixture.connection(),
        &fixture.schema,
        "readiness",
        "(value) VALUES ('a;b')",
        NoBinder,
        PostgresTestProbe::default(),
    );
    drop(
        open_writer(&literal_semicolon)
            .await
            .expect("a semicolon inside a literal prepares"),
    );

    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.readiness",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read readiness rows");
    let audit_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.readiness_audit",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read readiness audit");
    let sequence_after: (i64, bool) = sqlx::query_as(&format!(
        "SELECT last_value, is_called FROM {}.readiness_id_seq",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read sequence after preparation");
    assert_eq!((count, audit_count), (0, 0));
    assert_eq!(sequence_after, sequence_before);

    let second_command = sink(
        fixture.connection(),
        &fixture.schema,
        "readiness",
        "(value) VALUES ('never'); SELECT 1",
        NoBinder,
        PostgresTestProbe::default(),
    );
    let error = open_writer(&second_command)
        .await
        .expect_err("extended-protocol preparation rejects a second command");
    assert_eq!(sqlstate(&error), Some("42601"));

    sqlx::query(&format!(
        "CREATE TABLE {}.forms (id BIGINT PRIMARY KEY, value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create statement-form table");
    let invalid = [
        ("forms", "(value VALUES ($1)", "42601"),
        ("missing_relation", "(value) VALUES ($1)", "42P01"),
        ("forms", "(missing) VALUES ($1)", "42703"),
        (
            "forms",
            "(value) VALUES ('fixed') RETURNING $1 IS NULL",
            "42P18",
        ),
    ];
    for (table, body, expected) in invalid {
        let candidate = sink(
            fixture.connection(),
            &fixture.schema,
            table,
            body,
            ValueBinder,
            PostgresTestProbe::default(),
        );
        let error = open_writer(&candidate)
            .await
            .expect_err("server-invalid INSERT must fail at open");
        assert_eq!(sqlstate(&error), Some(expected), "body={body}");
    }

    let supported_forms = [
        "(id, value) VALUES ($1, $2)",
        "DEFAULT VALUES",
        "(id, value) SELECT $1, $2",
        "(id, value) WITH source(id, value) AS (VALUES ($1::bigint, $2::text)) SELECT * FROM source",
        "AS target (id, value) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value RETURNING target.id",
        "(value) VALUES ('fixed') RETURNING $1::text",
    ];
    for body in supported_forms {
        let candidate = sink(
            fixture.connection(),
            &fixture.schema,
            "forms",
            body,
            IdValueBinder,
            PostgresTestProbe::default(),
        );
        drop(
            open_writer(&candidate).await.unwrap_or_else(|error| {
                panic!("supported body failed to prepare: {body}: {error}")
            }),
        );
    }

    sqlx::query(&format!(
        "CREATE TABLE {}.\"Items\" (id BIGINT, value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create exact uppercase target");
    let uppercase = sink(
        fixture.connection(),
        &fixture.schema,
        "Items",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    assert_eq!(
        uppercase.describe().destination_name(),
        Some(format!("postgres.{}.Items", fixture.schema).as_str())
    );
    drop(
        open_writer(&uppercase)
            .await
            .expect("exact quoted-uppercase target prepares"),
    );

    let sixty_three = format!("a{}", "b".repeat(62));
    sqlx::query(&format!(
        "CREATE TABLE {}.\"{}\" (id BIGINT, value TEXT)",
        fixture.schema, sixty_three
    ))
    .execute(&fixture.pool)
    .await
    .expect("create 63-byte target");
    let boundary = sink(
        fixture.connection(),
        &fixture.schema,
        &sixty_three,
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    drop(
        open_writer(&boundary)
            .await
            .expect("63-byte identifier passes local and server limits"),
    );

    let final_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.readiness",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read unchanged readiness rows");
    assert_eq!(final_count, 0);
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn binding_is_parameterised_and_readiness_remains_point_in_time() {
    let fixture = Fixture::new("binding").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.values_table (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create binding table");

    let correct = sink(
        fixture.connection(),
        &fixture.schema,
        "values_table",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    let (mut adapter, writer_id) = open_adapter(correct).await.expect("correct binder opens");
    let adversarial = "x'); DROP TABLE values_table; --".to_string();
    let receipt = consume(
        &mut adapter,
        writer_id,
        DriverInput {
            id: 1,
            value: adversarial.clone(),
        },
    )
    .await
    .expect("parameterised input commits");
    assert!(matches!(receipt.result, DeliveryResult::Success { .. }));
    assert_eq!(receipt.items_delivered, Some(1));
    assert_eq!(
        receipt.delivery_method,
        DeliveryMethod::DatabaseInsert {
            table: format!("{}.values_table", fixture.schema)
        }
    );
    let stored: String = sqlx::query_scalar(&format!(
        "SELECT value FROM {}.values_table WHERE id = 1",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read adversarial value literally");
    assert_eq!(stored, adversarial);

    sqlx::query(&format!(
        "CREATE TABLE {}.arity_table (id BIGINT, value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create binder-arity table");
    let arity = sink(
        fixture.connection(),
        &fixture.schema,
        "arity_table",
        "(id, value) VALUES ($1, $2)",
        IdOnlyBinder,
        PostgresTestProbe::default(),
    );
    let (mut arity_adapter, arity_writer_id) = open_adapter(arity)
        .await
        .expect("preparation does not execute the binder");
    let error = consume(
        &mut arity_adapter,
        arity_writer_id,
        DriverInput {
            id: 2,
            value: "not-bound".to_string(),
        },
    )
    .await
    .expect_err("binder arity is a write-time fact");
    assert!(matches!(error, HandlerError::SinkWrite(_)));
    let arity_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.arity_table",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read arity table");
    assert_eq!(arity_count, 0);

    let drift = sink(
        fixture.connection(),
        &fixture.schema,
        "values_table",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    let (mut drift_adapter, drift_writer_id) = open_adapter(drift)
        .await
        .expect("statement is ready before schema drift");
    sqlx::query(&format!(
        "ALTER TABLE {}.values_table RENAME COLUMN value TO renamed_value",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("apply post-open schema drift");
    let error = consume(
        &mut drift_adapter,
        drift_writer_id,
        DriverInput {
            id: 3,
            value: "drift".to_string(),
        },
    )
    .await
    .expect_err("schema drift remains a write-time failure");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::ConfirmedRollback
            );
            assert_eq!(sqlstate(failure.error()), Some("42703"));
        }
        other => panic!("expected typed sink write failure, got {other:?}"),
    }
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn operation_deadlines_preserve_only_acknowledged_transaction_truth() {
    const OPERATION_TIMEOUT: Duration = Duration::from_millis(500);
    const INJECTED_DELAY: Duration = Duration::from_secs(2);
    let fixture = Fixture::new("deadlines").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.deadline_values (id BIGINT PRIMARY KEY, value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create deadline table");

    let prepare_probe = PostgresTestProbe::default();
    prepare_probe.delay_once(PostgresDelayPoint::Prepare, INJECTED_DELAY);
    let prepare = sink(
        fixture
            .connection()
            .with_operation_timeout(OPERATION_TIMEOUT),
        &fixture.schema,
        "deadline_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        prepare_probe,
    );
    let error = open_writer(&prepare)
        .await
        .expect_err("preparation is inside the absolute open deadline");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(sqlstate(&error), None);

    let begin_probe = PostgresTestProbe::default();
    let begin = sink(
        fixture
            .connection()
            .with_operation_timeout(OPERATION_TIMEOUT),
        &fixture.schema,
        "deadline_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        begin_probe.clone(),
    );
    let (mut begin_adapter, begin_writer_id) = open_adapter(begin)
        .await
        .expect("begin-timeout writer opens");
    begin_probe.delay_once(PostgresDelayPoint::Begin, INJECTED_DELAY);
    let error = consume(
        &mut begin_adapter,
        begin_writer_id,
        DriverInput {
            id: 10,
            value: "begin".to_string(),
        },
    )
    .await
    .expect_err("BEGIN is inside the transaction deadline");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Acquire);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::CurrentOnly
            );
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
        }
        other => panic!("expected typed begin timeout, got {other:?}"),
    }

    let execute_probe = PostgresTestProbe::default();
    let execute = sink(
        fixture
            .connection()
            .with_operation_timeout(OPERATION_TIMEOUT)
            .with_rollback_timeout(Duration::from_secs(1)),
        &fixture.schema,
        "deadline_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        execute_probe.clone(),
    );
    let (mut execute_adapter, execute_writer_id) = open_adapter(execute)
        .await
        .expect("execute-timeout writer opens");
    execute_probe.delay_once(PostgresDelayPoint::Execute, INJECTED_DELAY);
    let error = consume(
        &mut execute_adapter,
        execute_writer_id,
        DriverInput {
            id: 20,
            value: "timed-out".to_string(),
        },
    )
    .await
    .expect_err("execution crosses the absolute deadline");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::ConfirmedRollback
            );
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
        }
        other => panic!("expected typed execute timeout, got {other:?}"),
    }
    consume(
        &mut execute_adapter,
        execute_writer_id,
        DriverInput {
            id: 21,
            value: "writer-reused".to_string(),
        },
    )
    .await
    .expect("acknowledged rollback leaves the writer reusable");

    let rollback_probe = PostgresTestProbe::default();
    let rollback = sink(
        fixture
            .connection()
            .with_operation_timeout(Duration::from_secs(1))
            .with_rollback_timeout(Duration::from_millis(100)),
        &fixture.schema,
        "deadline_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        rollback_probe.clone(),
    );
    let (mut rollback_adapter, rollback_writer_id) = open_adapter(rollback)
        .await
        .expect("rollback-timeout writer opens");
    rollback_probe.arm(SinkFault::DestinationExecution);
    rollback_probe.delay_once(PostgresDelayPoint::Rollback, INJECTED_DELAY);
    let error = consume(
        &mut rollback_adapter,
        rollback_writer_id,
        DriverInput {
            id: 30,
            value: "rollback".to_string(),
        },
    )
    .await
    .expect_err("unacknowledged rollback poisons the writer");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Commit);
            assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
        }
        other => panic!("expected typed rollback timeout, got {other:?}"),
    }
    drop(rollback_adapter);

    let commit_probe = PostgresTestProbe::default();
    let commit = sink(
        fixture
            .connection()
            .with_operation_timeout(OPERATION_TIMEOUT),
        &fixture.schema,
        "deadline_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        commit_probe.clone(),
    );
    let (mut commit_adapter, commit_writer_id) = open_adapter(commit)
        .await
        .expect("commit-ambiguity writer opens");
    commit_probe.delay_once(PostgresDelayPoint::CommitAcknowledgement, INJECTED_DELAY);
    let error = consume(
        &mut commit_adapter,
        commit_writer_id,
        DriverInput {
            id: 40,
            value: "committed-but-unacknowledged".to_string(),
        },
    )
    .await
    .expect_err("commit acknowledgement timeout emits no success receipt");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Commit);
            assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
        }
        other => panic!("expected typed commit timeout, got {other:?}"),
    }
    let committed: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.deadline_values WHERE id = 40",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("inspect ambiguous commit destination state");
    assert_eq!(committed, 1, "the probe delays only local acknowledgement");
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_postgres_locks_bound_preparation_rollback_and_quarantine() {
    const OPERATION_TIMEOUT: Duration = Duration::from_millis(300);
    const ROLLBACK_TIMEOUT: Duration = Duration::from_secs(2);
    const SHORT_ROLLBACK_TIMEOUT: Duration = Duration::from_millis(150);
    const ACKNOWLEDGED_LOCK_RELEASE: Duration = Duration::from_millis(700);
    const LATE_LOCK_RELEASE: Duration = Duration::from_millis(900);

    let fixture = Fixture::new("locks").await;
    for table in [
        "preparation_lock_values",
        "rollback_lock_values",
        "poison_lock_values",
    ] {
        sqlx::query(&format!(
            "CREATE TABLE {}.{table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
            fixture.schema
        ))
        .execute(&fixture.pool)
        .await
        .unwrap_or_else(|error| panic!("create {table}: {error}"));
    }

    let preparation_probe = PostgresTestProbe::default();
    let preparation = sink(
        fixture
            .connection()
            .with_operation_timeout(OPERATION_TIMEOUT),
        &fixture.schema,
        "preparation_lock_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        preparation_probe.clone(),
    );
    let mut schema_lock = fixture
        .pool
        .acquire()
        .await
        .expect("acquire schema-lock connection");
    sqlx::query("BEGIN")
        .execute(&mut *schema_lock)
        .await
        .expect("begin schema-lock transaction");
    sqlx::query(&format!(
        "LOCK TABLE {}.preparation_lock_values IN ACCESS EXCLUSIVE MODE",
        fixture.schema
    ))
    .execute(&mut *schema_lock)
    .await
    .expect("hold conflicting schema lock");
    let error = open_writer(&preparation)
        .await
        .expect_err("preparation must respect the absolute open deadline");
    assert_eq!(error.kind(), ErrorKind::Timeout);
    assert_eq!(sqlstate(&error), None);
    assert_eq!(
        preparation_probe
            .snapshot()
            .count(SinkExternalCallKind::Open),
        1
    );
    assert_eq!(
        preparation_probe
            .snapshot()
            .count(SinkExternalCallKind::Write),
        0
    );
    sqlx::query("ROLLBACK")
        .execute(&mut *schema_lock)
        .await
        .expect("release schema lock");
    drop(schema_lock);

    sqlx::query(&format!(
        "INSERT INTO {}.rollback_lock_values (id, value) VALUES (2, 'baseline')",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("seed acknowledged-rollback target");
    let rollback_probe = PostgresTestProbe::default();
    let rollback = PostgresSink::<DriverInput>::builder()
        .connection(
            fixture
                .connection()
                .with_operation_timeout(OPERATION_TIMEOUT)
                .with_rollback_timeout(ROLLBACK_TIMEOUT),
        )
        .insert_into(
            &fixture.schema,
            "rollback_lock_values",
            "(id, value) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        )
        .expect("rollback target and body pass local validation")
        .batch_size(2)
        .expect("two-row batch is valid")
        .bind_with(IdValueBinder)
        .test_probe(rollback_probe)
        .build()
        .expect("rollback sink builds without I/O");
    let (mut rollback_adapter, rollback_writer_id) = open_adapter(rollback)
        .await
        .expect("acknowledged-rollback writer opens");
    consume(
        &mut rollback_adapter,
        rollback_writer_id,
        DriverInput {
            id: 1,
            value: "retained".to_string(),
        },
    )
    .await
    .expect("first row remains buffered");

    let mut row_lock = fixture
        .pool
        .acquire()
        .await
        .expect("acquire row-lock connection");
    sqlx::query("BEGIN")
        .execute(&mut *row_lock)
        .await
        .expect("begin row-lock transaction");
    sqlx::query(&format!(
        "SELECT id FROM {}.rollback_lock_values WHERE id = 2 FOR UPDATE",
        fixture.schema
    ))
    .execute(&mut *row_lock)
    .await
    .expect("hold conflicting row lock");
    let release_row_lock = async {
        tokio::time::sleep(ACKNOWLEDGED_LOCK_RELEASE).await;
        sqlx::query("ROLLBACK")
            .execute(&mut *row_lock)
            .await
            .expect("release row lock within rollback budget");
    };
    let blocked_write = consume(
        &mut rollback_adapter,
        rollback_writer_id,
        DriverInput {
            id: 2,
            value: "blocked".to_string(),
        },
    );
    let (result, ()) = tokio::join!(blocked_write, release_row_lock);
    match result.expect_err("blocked execution crosses its primary deadline") {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::ConfirmedRollback
            );
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
        }
        other => panic!("expected typed row-lock timeout, got {other:?}"),
    }
    drop(row_lock);
    let after_rollback: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.rollback_lock_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect acknowledged rollback");
    assert_eq!(after_rollback, vec![(2, "baseline".to_string())]);

    consume(
        &mut rollback_adapter,
        rollback_writer_id,
        DriverInput {
            id: 3,
            value: "writer-reused".to_string(),
        },
    )
    .await
    .expect("acknowledged rollback retains buffered work and permits reuse");
    let after_reuse: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.rollback_lock_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect reused writer");
    assert_eq!(
        after_reuse,
        vec![
            (1, "retained".to_string()),
            (2, "baseline".to_string()),
            (3, "writer-reused".to_string()),
        ]
    );
    drop(rollback_adapter);

    sqlx::query(&format!(
        "INSERT INTO {}.poison_lock_values (id, value) VALUES (20, 'baseline')",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("seed poisoned-rollback target");
    let poison_probe = PostgresTestProbe::default();
    let poison = sink(
        fixture
            .connection()
            .with_operation_timeout(OPERATION_TIMEOUT)
            .with_rollback_timeout(SHORT_ROLLBACK_TIMEOUT),
        &fixture.schema,
        "poison_lock_values",
        "(id, value) VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        IdValueBinder,
        poison_probe.clone(),
    );
    let (mut poison_adapter, poison_writer_id) = open_adapter(poison)
        .await
        .expect("poisoned-rollback writer opens");
    let mut late_row_lock = fixture
        .pool
        .acquire()
        .await
        .expect("acquire long row-lock connection");
    sqlx::query("BEGIN")
        .execute(&mut *late_row_lock)
        .await
        .expect("begin long row-lock transaction");
    sqlx::query(&format!(
        "SELECT id FROM {}.poison_lock_values WHERE id = 20 FOR UPDATE",
        fixture.schema
    ))
    .execute(&mut *late_row_lock)
    .await
    .expect("hold row lock beyond rollback budget");
    let release_late_row_lock = async {
        tokio::time::sleep(LATE_LOCK_RELEASE).await;
        sqlx::query("ROLLBACK")
            .execute(&mut *late_row_lock)
            .await
            .expect("release long row lock after writer quarantine");
    };
    let blocked_write = consume(
        &mut poison_adapter,
        poison_writer_id,
        DriverInput {
            id: 20,
            value: "must-not-commit".to_string(),
        },
    );
    let (result, ()) = tokio::join!(blocked_write, release_late_row_lock);
    match result.expect_err("unconfirmed rollback poisons the writer") {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Commit);
            assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
        }
        other => panic!("expected typed rollback-budget timeout, got {other:?}"),
    }
    drop(late_row_lock);
    let poisoned_rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.poison_lock_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect quarantined transaction");
    assert_eq!(poisoned_rows, vec![(20, "baseline".to_string())]);
    let before_drop = poison_probe.snapshot();
    assert_eq!(before_drop.count(SinkExternalCallKind::Execute), 1);
    assert_eq!(before_drop.count(SinkExternalCallKind::Rollback), 1);
    assert_eq!(before_drop.count(SinkExternalCallKind::Commit), 0);
    drop(poison_adapter);
    let after_drop = poison_probe.snapshot();
    assert_eq!(after_drop.calls().len(), before_drop.calls().len() + 1);
    assert_eq!(
        after_drop.calls().last().map(|call| call.kind()),
        Some(SinkExternalCallKind::Drop)
    );

    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn server_cancellation_remains_remote_postgres_evidence() {
    let fixture = Fixture::new("server_timeout").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.server_timeout_values (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create server-timeout target");
    sqlx::query(&format!(
        "INSERT INTO {}.server_timeout_values (id, value) VALUES (30, 'baseline')",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("seed server-timeout target");

    let mut server_timeout_url = Url::parse(&fixture.url).expect("fixture URL parses");
    server_timeout_url
        .query_pairs_mut()
        .append_pair("options", "-c statement_timeout=100ms");
    let connection = PostgresConnection::from_url(
        server_timeout_url.as_str(),
        PostgresTransport::ExternallyProtectedPlaintext,
    )
    .expect("caller-owned PostgreSQL timeout option is valid")
    .with_operation_timeout(Duration::from_secs(2))
    .with_rollback_timeout(Duration::from_secs(1));
    let server_timeout = sink(
        connection,
        &fixture.schema,
        "server_timeout_values",
        "(id, value) VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    let (mut adapter, writer_id) = open_adapter(server_timeout)
        .await
        .expect("server-timeout writer opens before the conflicting lock");

    let mut row_lock = fixture
        .pool
        .acquire()
        .await
        .expect("acquire server-timeout row-lock connection");
    sqlx::query("BEGIN")
        .execute(&mut *row_lock)
        .await
        .expect("begin server-timeout row-lock transaction");
    sqlx::query(&format!(
        "SELECT id FROM {}.server_timeout_values WHERE id = 30 FOR UPDATE",
        fixture.schema
    ))
    .execute(&mut *row_lock)
    .await
    .expect("hold row lock until PostgreSQL cancels the statement");

    let error = consume(
        &mut adapter,
        writer_id,
        DriverInput {
            id: 30,
            value: "server-cancelled".to_string(),
        },
    )
    .await
    .expect_err("PostgreSQL statement_timeout cancels the blocked statement");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::ConfirmedRollback
            );
            assert_eq!(failure.error().kind(), ErrorKind::Remote);
            assert_eq!(sqlstate(failure.error()), Some("57014"));
        }
        other => panic!("expected typed PostgreSQL cancellation, got {other:?}"),
    }
    sqlx::query("ROLLBACK")
        .execute(&mut *row_lock)
        .await
        .expect("release server-timeout row lock");
    drop(row_lock);

    let unchanged: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.server_timeout_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect server-cancelled transaction");
    assert_eq!(unchanged, vec![(30, "baseline".to_string())]);
    consume(
        &mut adapter,
        writer_id,
        DriverInput {
            id: 31,
            value: "writer-reused".to_string(),
        },
    )
    .await
    .expect("acknowledged server cancellation leaves the writer reusable");

    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn typed_transport_proves_plaintext_and_tls_failure_matrix() {
    let fixture = Fixture::new("transport").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.transport_values (id BIGINT, value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create transport target");

    let plaintext = sink(
        fixture.connection(),
        &fixture.schema,
        "transport_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    drop(
        open_writer(&plaintext)
            .await
            .expect("explicit loopback plaintext opens"),
    );

    let tls_url = required_env("OBZENFLOW_POSTGRES_TEST_TLS_URL");
    let ca = required_env("OBZENFLOW_POSTGRES_TEST_CA_CERT");
    let trusted_url = url_with_root(&tls_url, Path::new(&ca));
    let trusted = sink(
        PostgresConnection::from_url(&trusted_url, PostgresTransport::VerifiedTls)
            .expect("matching verify-full URL and custom root parse"),
        &fixture.schema,
        "transport_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    drop(
        open_writer(&trusted)
            .await
            .expect("additive private-root hostname-verified TLS opens"),
    );

    let wrong_host_url = required_env("OBZENFLOW_POSTGRES_TEST_WRONG_HOST_URL");
    let wrong_host_url = url_with_root(&wrong_host_url, Path::new(&ca));
    let wrong_host = sink(
        PostgresConnection::from_url(&wrong_host_url, PostgresTransport::VerifiedTls)
            .expect("wrong-host URL remains valid configuration"),
        &fixture.schema,
        "transport_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    let error = open_writer(&wrong_host)
        .await
        .expect_err("hostname verification must fail without plaintext fallback");
    assert_eq!(sqlstate(&error), None);
    assert!(!error.detail().contains(&ca));
    assert!(!error.detail().contains("127.0.0.1"));

    let untrusted_ca = required_env("OBZENFLOW_POSTGRES_TEST_UNTRUSTED_CA_CERT");
    let untrusted_url = url_with_root(&tls_url, Path::new(&untrusted_ca));
    let untrusted = sink(
        PostgresConnection::from_url(&untrusted_url, PostgresTransport::VerifiedTls)
            .expect("untrusted-root URL remains valid configuration"),
        &fixture.schema,
        "transport_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    let error = open_writer(&untrusted)
        .await
        .expect_err("untrusted CA must fail without plaintext fallback");
    assert_eq!(sqlstate(&error), None);
    assert!(!error.detail().contains(&untrusted_ca));

    let mut bad_password = Url::parse(&fixture.url).expect("plaintext fixture URL parses");
    bad_password
        .set_password(Some("sentinel-invalid-password"))
        .expect("PostgreSQL URL accepts a password");
    let authentication = sink(
        PostgresConnection::from_url(
            bad_password.as_str(),
            PostgresTransport::ExternallyProtectedPlaintext,
        )
        .expect("bad credentials are externally valid configuration"),
        &fixture.schema,
        "transport_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    let error = open_writer(&authentication)
        .await
        .expect_err("authentication is an open-time fact");
    assert!(!error.detail().contains("sentinel-invalid-password"));
    assert!(!error.detail().contains("postgres://"));
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_tls_uses_native_root_loader_in_an_isolated_process() {
    const CHILD: &str = "OBZENFLOW_POSTGRES_NATIVE_ROOT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let current = std::env::current_exe().expect("locate integration-test executable");
        let status = Command::new(current)
            .arg("postgres_tls_uses_native_root_loader_in_an_isolated_process")
            .arg("--exact")
            .arg("--nocapture")
            .env(CHILD, "1")
            .env(
                "SSL_CERT_FILE",
                required_env("OBZENFLOW_POSTGRES_TEST_CA_CERT"),
            )
            .status()
            .expect("launch isolated native-root proof process");
        assert!(status.success(), "isolated native-root proof failed");
        return;
    }

    let url = required_env("OBZENFLOW_POSTGRES_TEST_URL");
    let pool = PgPool::connect(&url)
        .await
        .expect("connect over explicit fixture plaintext for setup");
    let schema = format!("obz083c_native_{}", required_run_id());
    sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop prior native-root schema");
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(&pool)
        .await
        .expect("create native-root schema");
    sqlx::query(&format!(
        "CREATE TABLE {schema}.native_values (id BIGINT, value TEXT)"
    ))
    .execute(&pool)
    .await
    .expect("create native-root target");

    let tls_url = required_env("OBZENFLOW_POSTGRES_TEST_TLS_URL");
    let native = sink(
        PostgresConnection::from_url(&tls_url, PostgresTransport::VerifiedTls)
            .expect("native-root TLS URL parses"),
        &schema,
        "native_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        PostgresTestProbe::default(),
    );
    drop(
        open_writer(&native)
            .await
            .expect("SSL_CERT_FILE supplies the current process native roots"),
    );
    sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
        .execute(&pool)
        .await
        .expect("drop native-root schema");
}
