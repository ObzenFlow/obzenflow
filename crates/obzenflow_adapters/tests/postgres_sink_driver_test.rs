// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "test-support")]

use obzenflow_adapters::sinks::postgres::testing::{PostgresDelayPoint, PostgresTestProbe};
use obzenflow_adapters::sinks::postgres::{
    PostgresBind, PostgresBindings, PostgresConnection, PostgresSink, PostgresTransport,
    PostgresWriter,
};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{EventId, StageId, TypedPayload, WriterId};
use obzenflow_runtime::stages::common::handlers::sink::{SinkHandler, SinkWriterAdapter};
use obzenflow_runtime::stages::common::HandlerError;
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkOperationError, SinkWriteFailureDisposition, SinkWritePhase,
    SinkWriterInitContext,
};
use obzenflow_runtime::testing::sink::{SinkExternalCallKind, SinkFault};
use serde::{Deserialize, Serialize};
use sqlx::encode::IsNull;
use sqlx::error::BoxDynError;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo};
use sqlx::{Encode, PgPool, Postgres, Type};
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
    fn bind(&self, bindings: &mut PostgresBindings, input: &DriverInput) {
        bindings.bind(input.id).bind(&input.value);
    }
}

#[derive(Clone, Debug)]
struct ValueBinder;

impl PostgresBind<DriverInput> for ValueBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &DriverInput) {
        bindings.bind(&input.value);
    }
}

#[derive(Clone, Debug)]
struct IdOnlyBinder;

impl PostgresBind<DriverInput> for IdOnlyBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &DriverInput) {
        bindings.bind(input.id);
    }
}

#[derive(Clone, Debug)]
struct NoBinder;

impl PostgresBind<DriverInput> for NoBinder {
    fn bind(&self, _bindings: &mut PostgresBindings, _input: &DriverInput) {}
}

#[derive(Clone, Debug)]
struct ExtraBinder;

impl PostgresBind<DriverInput> for ExtraBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &DriverInput) {
        bindings.bind(input.id).bind(&input.value).bind(input.id);
    }
}

#[derive(Clone, Debug)]
struct WrongTypeBinder;

impl PostgresBind<DriverInput> for WrongTypeBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &DriverInput) {
        bindings.bind(&input.value).bind(&input.value);
    }
}

struct DriverEncodingFailure;

impl Type<Postgres> for DriverEncodingFailure {
    fn type_info() -> PgTypeInfo {
        <String as Type<Postgres>>::type_info()
    }
}

impl<'q> Encode<'q, Postgres> for DriverEncodingFailure {
    fn encode_by_ref(&self, _buffer: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
        Err(std::io::Error::other("injected PostgreSQL parameter encoding failure").into())
    }
}

#[derive(Clone, Debug)]
struct EncodingFailureBinder;

impl PostgresBind<DriverInput> for EncodingFailureBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &DriverInput) {
        bindings.bind(input.id).bind(DriverEncodingFailure);
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

fn latest_authority_backend(probe: &PostgresTestProbe) -> i32 {
    probe
        .authority_snapshot()
        .sessions()
        .last()
        .map(|(_, backend_pid)| *backend_pid)
        .expect("an authority check identified its physical PostgreSQL backend")
}

async fn terminate_backend(pool: &PgPool, backend_pid: i32) {
    let terminated: bool = sqlx::query_scalar("SELECT pg_terminate_backend($1)")
        .bind(backend_pid)
        .fetch_one(pool)
        .await
        .expect("terminate the writer's physical PostgreSQL backend");
    assert!(terminated, "the writer backend must still be terminable");
}

async fn assert_backend_closed(pool: &PgPool, backend_pid: i32) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let active: bool =
                sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1)")
                    .bind(backend_pid)
                    .fetch_one(pool)
                    .await
                    .expect("inspect PostgreSQL backend activity");
            if !active {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("rejected or unverified PostgreSQL backend closes promptly");
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

async fn consume_with_event_id<B>(
    adapter: &mut SinkWriterAdapter<PostgresWriter<DriverInput, B>>,
    writer_id: WriterId,
    input: DriverInput,
) -> (
    EventId,
    Result<obzenflow_core::event::payloads::delivery_payload::DeliveryPayload, HandlerError>,
)
where
    B: PostgresBind<DriverInput>,
{
    let event =
        ChainEventFactory::data_event_from(writer_id, DriverInput::versioned_event_type(), &input)
            .expect("driver input serialises");
    let event_id = event.id;
    (event_id, adapter.consume(event).await)
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

async fn assert_binding_failure<B>(
    fixture: &Fixture,
    table: &str,
    binder: B,
    expected_kind: ErrorKind,
) where
    B: PostgresBind<DriverInput>,
{
    let probe = PostgresTestProbe::default();
    let connector = sink(
        fixture.connection(),
        &fixture.schema,
        table,
        "(id, value) VALUES ($1, $2)",
        binder,
        probe.clone(),
    );
    let (mut adapter, writer_id) = open_adapter(connector)
        .await
        .expect("statement preparation does not execute the binder");
    let error = consume(
        &mut adapter,
        writer_id,
        DriverInput {
            id: 99,
            value: "binding-failure-sentinel".to_string(),
        },
    )
    .await
    .expect_err("invalid bindings fail during write");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::ConfirmedRollback
            );
            assert_eq!(failure.error().kind(), expected_kind);
            assert!(!failure
                .error()
                .detail()
                .contains("binding-failure-sentinel"));
        }
        other => panic!("expected typed sink write failure, got {other:?}"),
    }
    assert_eq!(probe.snapshot().count(SinkExternalCallKind::Commit), 0);
    let count: i64 =
        sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}.{table}", fixture.schema))
            .fetch_one(&fixture.pool)
            .await
            .expect("read binding failure table");
    assert_eq!(count, 0);
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
    sqlx::query(&format!(
        "CREATE TABLE {}.unrelated_sentinel (id BIGINT PRIMARY KEY, marker TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create unrelated authority sentinel table");
    sqlx::query(&format!(
        "INSERT INTO {}.unrelated_sentinel (id, marker) VALUES (1, 'unchanged')",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("seed unrelated authority sentinel table");

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
    let unrelated_marker: String = sqlx::query_scalar(&format!(
        "SELECT marker FROM {}.unrelated_sentinel WHERE id = 1",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("read unrelated authority sentinel");
    assert_eq!(unrelated_marker, "unchanged");

    sqlx::query(&format!(
        "CREATE TABLE {}.arity_table (id BIGINT, value TEXT)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create binder-arity table");
    assert_binding_failure(&fixture, "arity_table", IdOnlyBinder, ErrorKind::Remote).await;
    assert_binding_failure(&fixture, "arity_table", ExtraBinder, ErrorKind::Remote).await;
    assert_binding_failure(&fixture, "arity_table", WrongTypeBinder, ErrorKind::Remote).await;
    assert_binding_failure(
        &fixture,
        "arity_table",
        EncodingFailureBinder,
        ErrorKind::Deserialization,
    )
    .await;

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
async fn successful_do_nothing_settles_inputs_not_destination_rows() {
    let fixture = Fixture::new("do_nothing").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.command_result_values (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create command-result settlement target");

    let probe = PostgresTestProbe::default();
    let connector = sink(
        fixture.connection(),
        &fixture.schema,
        "command_result_values",
        "(id, value) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        IdValueBinder,
        probe.clone(),
    );
    let (mut adapter, writer_id) = open_adapter(connector)
        .await
        .expect("DO NOTHING writer opens");

    let first = consume(
        &mut adapter,
        writer_id,
        DriverInput {
            id: 1,
            value: "first".to_string(),
        },
    )
    .await
    .expect("first input inserts and commits");
    let duplicate = consume(
        &mut adapter,
        writer_id,
        DriverInput {
            id: 1,
            value: "duplicate".to_string(),
        },
    )
    .await
    .expect("duplicate input completes successfully with no row insertion");

    for receipt in [&first, &duplicate] {
        assert!(matches!(&receipt.result, DeliveryResult::Success { .. }));
        assert_eq!(receipt.items_delivered, Some(1));
        assert!(
            receipt.middleware_context.is_none(),
            "PostgreSQL command-tag counts are not delivery evidence"
        );
    }

    let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.command_result_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect DO NOTHING destination state");
    assert_eq!(rows, vec![(1, "first".to_string())]);

    let calls = probe.snapshot();
    assert_eq!(calls.count(SinkExternalCallKind::Execute), 2);
    assert_eq!(calls.count(SinkExternalCallKind::Commit), 2);
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deferred_origin_failures_poison_with_exact_subject_and_current_failures_remain_reusable() {
    const OPERATION_TIMEOUT: Duration = Duration::from_millis(300);
    const ROLLBACK_TIMEOUT: Duration = Duration::from_secs(2);
    const LOCK_RELEASE: Duration = Duration::from_millis(700);

    let fixture = Fixture::new("deferred_origin").await;
    for table in ["constraint_values", "encoding_values", "timeout_values"] {
        let constraint = if table == "constraint_values" {
            ", CHECK (id > 0)"
        } else {
            ""
        };
        sqlx::query(&format!(
            "CREATE TABLE {}.{table} (id BIGINT PRIMARY KEY, value TEXT NOT NULL{constraint})",
            fixture.schema
        ))
        .execute(&fixture.pool)
        .await
        .unwrap_or_else(|error| panic!("create {table}: {error}"));
    }

    let deferred_probe = PostgresTestProbe::default();
    let deferred_constraint = PostgresSink::<DriverInput>::builder()
        .connection(fixture.connection())
        .insert_into(
            &fixture.schema,
            "constraint_values",
            "(id, value) VALUES ($1, $2)",
        )
        .expect("deferred constraint target is valid")
        .batch_size(2)
        .expect("two-row batch is valid")
        .bind_with(IdValueBinder)
        .test_probe(deferred_probe.clone())
        .build()
        .expect("deferred constraint sink builds without I/O");
    let (mut deferred_adapter, deferred_writer_id) = open_adapter(deferred_constraint)
        .await
        .expect("deferred constraint writer opens");
    let (bad_deferred_id, buffered) = consume_with_event_id(
        &mut deferred_adapter,
        deferred_writer_id,
        DriverInput {
            id: -1,
            value: "bad-deferred".to_string(),
        },
    )
    .await;
    assert!(matches!(
        buffered
            .expect("bad row is deferred before threshold")
            .result,
        DeliveryResult::Buffered { .. }
    ));
    let (good_current_id, failure) = consume_with_event_id(
        &mut deferred_adapter,
        deferred_writer_id,
        DriverInput {
            id: 2,
            value: "good-current".to_string(),
        },
    )
    .await;
    match failure.expect_err("the earlier deferred constraint violation poisons") {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
            assert_eq!(sqlstate(failure.error()), Some("23514"));
            assert_eq!(
                failure.error().operation_subject_event_id(),
                Some(bad_deferred_id)
            );
            assert_ne!(
                failure.error().operation_subject_event_id(),
                Some(good_current_id)
            );
        }
        other => panic!("expected deferred-origin poison, got {other:?}"),
    }
    let constraint_count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.constraint_values",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("inspect rolled-back deferred constraint batch");
    assert_eq!(constraint_count, 0);
    drop(deferred_adapter);
    let deferred_calls = deferred_probe.snapshot();
    assert_eq!(deferred_calls.count(SinkExternalCallKind::Write), 2);
    assert_eq!(deferred_calls.count(SinkExternalCallKind::Rollback), 1);
    assert_eq!(deferred_calls.count(SinkExternalCallKind::Commit), 0);
    assert_eq!(deferred_calls.count(SinkExternalCallKind::Flush), 0);
    assert_eq!(deferred_calls.count(SinkExternalCallKind::Drain), 0);

    let current_probe = PostgresTestProbe::default();
    let current_constraint = PostgresSink::<DriverInput>::builder()
        .connection(fixture.connection())
        .insert_into(
            &fixture.schema,
            "constraint_values",
            "(id, value) VALUES ($1, $2)",
        )
        .expect("current constraint target is valid")
        .batch_size(2)
        .expect("two-row batch is valid")
        .bind_with(IdValueBinder)
        .test_probe(current_probe)
        .build()
        .expect("current constraint sink builds without I/O");
    let (mut current_adapter, current_writer_id) = open_adapter(current_constraint)
        .await
        .expect("current constraint writer opens");
    consume(
        &mut current_adapter,
        current_writer_id,
        DriverInput {
            id: 1,
            value: "good-deferred".to_string(),
        },
    )
    .await
    .expect("good earlier row is buffered");
    let error = consume(
        &mut current_adapter,
        current_writer_id,
        DriverInput {
            id: -2,
            value: "bad-current".to_string(),
        },
    )
    .await
    .expect_err("current constraint violation rolls the batch back");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::ConfirmedRollback
            );
            assert_eq!(sqlstate(failure.error()), Some("23514"));
            assert_eq!(failure.error().operation_subject_event_id(), None);
        }
        other => panic!("expected current-origin confirmed rollback, got {other:?}"),
    }
    consume(
        &mut current_adapter,
        current_writer_id,
        DriverInput {
            id: 3,
            value: "later-current".to_string(),
        },
    )
    .await
    .expect("later current commits with the retained good row");
    let recovered_rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.constraint_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect current-origin recovery");
    assert_eq!(
        recovered_rows,
        vec![
            (1, "good-deferred".to_string()),
            (3, "later-current".to_string()),
        ]
    );
    drop(current_adapter);

    let encoding_probe = PostgresTestProbe::default();
    let encoding = PostgresSink::<DriverInput>::builder()
        .connection(fixture.connection())
        .insert_into(
            &fixture.schema,
            "encoding_values",
            "(id, value) VALUES ($1, $2)",
        )
        .expect("encoding target is valid")
        .batch_size(2)
        .expect("two-row batch is valid")
        .bind_with(EncodingFailureBinder)
        .test_probe(encoding_probe)
        .build()
        .expect("encoding sink builds without I/O");
    let (mut encoding_adapter, encoding_writer_id) =
        open_adapter(encoding).await.expect("encoding writer opens");
    let (encoding_subject_id, buffered) = consume_with_event_id(
        &mut encoding_adapter,
        encoding_writer_id,
        DriverInput {
            id: 4,
            value: "encoding-deferred".to_string(),
        },
    )
    .await;
    buffered.expect("encoding remains deferred below threshold");
    let (_, failure) = consume_with_event_id(
        &mut encoding_adapter,
        encoding_writer_id,
        DriverInput {
            id: 5,
            value: "encoding-current".to_string(),
        },
    )
    .await;
    match failure.expect_err("deferred encoding failure poisons") {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
            assert_eq!(failure.error().kind(), ErrorKind::Deserialization);
            assert_eq!(
                failure.error().operation_subject_event_id(),
                Some(encoding_subject_id)
            );
        }
        other => panic!("expected deferred encoding poison, got {other:?}"),
    }
    drop(encoding_adapter);

    let flush_encoding = PostgresSink::<DriverInput>::builder()
        .connection(fixture.connection())
        .insert_into(
            &fixture.schema,
            "encoding_values",
            "(id, value) VALUES ($1, $2)",
        )
        .expect("flush encoding target is valid")
        .batch_size(3)
        .expect("three-row batch is valid")
        .bind_with(EncodingFailureBinder)
        .test_probe(PostgresTestProbe::default())
        .build()
        .expect("flush encoding sink builds without I/O");
    let (mut flush_adapter, flush_writer_id) = open_adapter(flush_encoding)
        .await
        .expect("flush encoding writer opens");
    let (flush_subject_id, buffered) = consume_with_event_id(
        &mut flush_adapter,
        flush_writer_id,
        DriverInput {
            id: 6,
            value: "flush-deferred".to_string(),
        },
    )
    .await;
    buffered.expect("flush subject remains buffered");
    match flush_adapter
        .flush_report()
        .await
        .expect_err("flush reports the deferred encoding subject")
    {
        HandlerError::SinkOperation(error) => {
            assert_eq!(error.kind(), ErrorKind::Deserialization);
            assert_eq!(error.operation_subject_event_id(), Some(flush_subject_id));
        }
        other => panic!("expected deferred-origin flush failure, got {other:?}"),
    }
    drop(flush_adapter);

    sqlx::query(&format!(
        "INSERT INTO {}.timeout_values (id, value) VALUES (10, 'baseline')",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("seed deferred timeout target");
    let timeout_probe = PostgresTestProbe::default();
    let timeout_sink = PostgresSink::<DriverInput>::builder()
        .connection(
            fixture
                .connection()
                .with_operation_timeout(OPERATION_TIMEOUT)
                .with_rollback_timeout(ROLLBACK_TIMEOUT),
        )
        .insert_into(
            &fixture.schema,
            "timeout_values",
            "(id, value) VALUES ($1, $2) \
             ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value",
        )
        .expect("timeout target is valid")
        .batch_size(2)
        .expect("two-row timeout batch is valid")
        .bind_with(IdValueBinder)
        .test_probe(timeout_probe)
        .build()
        .expect("timeout sink builds without I/O");
    let (mut timeout_adapter, timeout_writer_id) = open_adapter(timeout_sink)
        .await
        .expect("timeout writer opens");
    let (timeout_subject_id, buffered) = consume_with_event_id(
        &mut timeout_adapter,
        timeout_writer_id,
        DriverInput {
            id: 10,
            value: "blocked-deferred".to_string(),
        },
    )
    .await;
    buffered.expect("locked row remains deferred until threshold");

    let mut row_lock = fixture.pool.acquire().await.expect("acquire row lock");
    sqlx::query("BEGIN")
        .execute(&mut *row_lock)
        .await
        .expect("begin row-lock transaction");
    sqlx::query(&format!(
        "SELECT id FROM {}.timeout_values WHERE id = 10 FOR UPDATE",
        fixture.schema
    ))
    .execute(&mut *row_lock)
    .await
    .expect("lock the deferred operation subject");
    let release_lock = async {
        tokio::time::sleep(LOCK_RELEASE).await;
        sqlx::query("ROLLBACK")
            .execute(&mut *row_lock)
            .await
            .expect("release lock within rollback confirmation budget");
    };
    let blocked = consume(
        &mut timeout_adapter,
        timeout_writer_id,
        DriverInput {
            id: 11,
            value: "good-current".to_string(),
        },
    );
    let (failure, ()) = tokio::join!(blocked, release_lock);
    match failure.expect_err("deferred execution timeout poisons after rollback") {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Execute);
            assert_eq!(failure.disposition(), SinkWriteFailureDisposition::Poisoned);
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
            assert_eq!(
                failure.error().operation_subject_event_id(),
                Some(timeout_subject_id)
            );
        }
        other => panic!("expected deferred timeout poison, got {other:?}"),
    }
    drop(row_lock);
    let timeout_rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.timeout_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect acknowledged deferred timeout rollback");
    assert_eq!(timeout_rows, vec![(10, "baseline".to_string())]);
    drop(timeout_adapter);

    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replacement_sessions_reestablish_target_authority_before_begin() {
    let fixture = Fixture::new("session_authority").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.authority_values (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create physical-session authority target");

    let open_rejection_probe = PostgresTestProbe::default();
    open_rejection_probe.authority_limit_once(8);
    let open_rejection = sink(
        fixture.connection(),
        &fixture.schema,
        "authority_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        open_rejection_probe.clone(),
    );
    let error = open_writer(&open_rejection)
        .await
        .expect_err("initial physical-session rejection fails open");
    assert_eq!(error.kind(), ErrorKind::PermanentFailure);
    assert_eq!(sqlstate(&error), None);
    assert!(error
        .detail()
        .ends_with("PostgreSQL destination identifier exceeds the server limit"));
    let rejected_open_pid = latest_authority_backend(&open_rejection_probe);
    let open_snapshot = open_rejection_probe.authority_snapshot();
    assert_eq!(open_snapshot.hook_invocations(), 1);
    assert_eq!(open_snapshot.rejections(), 1);
    assert!(open_snapshot.preparations().is_empty());
    assert_backend_closed(&fixture.pool, rejected_open_pid).await;

    let probe = PostgresTestProbe::default();
    let primary = sink(
        fixture.connection(),
        &fixture.schema,
        "authority_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        probe.clone(),
    );
    let (mut primary, primary_writer_id) = open_adapter(primary)
        .await
        .expect("primary writer establishes initial authority");
    let initial_snapshot = probe.authority_snapshot();
    assert_eq!(initial_snapshot.hook_invocations(), 1);
    assert_eq!(initial_snapshot.sessions().len(), 1);
    let (primary_probe_writer, initial_pid) = initial_snapshot.sessions()[0];
    assert_eq!(initial_snapshot.preparations(), &[primary_probe_writer]);

    let sibling_probe = PostgresTestProbe::default();
    let sibling = sink(
        fixture.connection(),
        &fixture.schema,
        "authority_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        sibling_probe.clone(),
    );
    let (mut sibling, sibling_writer_id) = open_adapter(sibling)
        .await
        .expect("fan-out sibling establishes independent authority");
    let sibling_pid = latest_authority_backend(&sibling_probe);
    assert_ne!(initial_pid, sibling_pid);

    terminate_backend(&fixture.pool, initial_pid).await;
    probe.authority_limit_once(8);
    let error = consume(
        &mut primary,
        primary_writer_id,
        DriverInput {
            id: 1,
            value: "must-not-mutate".to_string(),
        },
    )
    .await
    .expect_err("replacement session with a smaller limit is rejected");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Acquire);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::CurrentOnly
            );
            assert_eq!(failure.error().kind(), ErrorKind::PermanentFailure);
            assert_eq!(sqlstate(failure.error()), None);
            assert!(failure
                .error()
                .detail()
                .ends_with("PostgreSQL destination identifier exceeds the server limit"));
        }
        other => panic!("expected typed replacement-authority failure, got {other:?}"),
    }
    let rejected_snapshot = probe.authority_snapshot();
    assert_eq!(rejected_snapshot.hook_invocations(), 2);
    assert_eq!(rejected_snapshot.sessions().len(), 2);
    assert_eq!(rejected_snapshot.rejections(), 1);
    assert_eq!(rejected_snapshot.preparations(), &[primary_probe_writer]);
    let (replacement_writer, rejected_pid) = rejected_snapshot.sessions()[1];
    assert_eq!(replacement_writer, primary_probe_writer);
    assert_ne!(rejected_pid, initial_pid);
    let calls = probe.snapshot();
    assert_eq!(calls.count(SinkExternalCallKind::Begin), 0);
    assert_eq!(calls.count(SinkExternalCallKind::Execute), 0);
    assert_eq!(calls.count(SinkExternalCallKind::Commit), 0);
    assert_backend_closed(&fixture.pool, rejected_pid).await;
    let unchanged: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.authority_values",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("inspect rejected authority destination");
    assert_eq!(unchanged, 0);

    let sibling_receipt = consume(
        &mut sibling,
        sibling_writer_id,
        DriverInput {
            id: 9,
            value: "fan-out-isolated".to_string(),
        },
    )
    .await
    .expect("one rejected writer cannot poison its fan-out sibling");
    assert!(matches!(
        sibling_receipt.result,
        DeliveryResult::Success { .. }
    ));
    assert_eq!(sibling_probe.authority_snapshot().hook_invocations(), 1);
    assert_eq!(sibling_probe.authority_snapshot().rejections(), 0);

    let rematerialized = sink(
        fixture.connection(),
        &fixture.schema,
        "authority_values",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        probe.clone(),
    );
    let (mut rematerialized, rematerialized_writer_id) = open_adapter(rematerialized)
        .await
        .expect("a replay-style rematerialisation creates fresh authority state");
    let rematerialized_snapshot = probe.authority_snapshot();
    assert_eq!(rematerialized_snapshot.hook_invocations(), 3);
    let (rematerialized_probe_writer, rematerialized_pid) =
        *rematerialized_snapshot.sessions().last().unwrap();
    assert_ne!(rematerialized_probe_writer, primary_probe_writer);
    assert_ne!(rematerialized_pid, rejected_pid);
    consume(
        &mut rematerialized,
        rematerialized_writer_id,
        DriverInput {
            id: 3,
            value: "fresh-authority".to_string(),
        },
    )
    .await
    .expect("fresh materialisation is not poisoned by a prior writer verdict");

    let receipt = consume(
        &mut primary,
        primary_writer_id,
        DriverInput {
            id: 2,
            value: "compatible-replacement".to_string(),
        },
    )
    .await
    .expect("a later compatible replacement can mutate and settle");
    assert!(matches!(receipt.result, DeliveryResult::Success { .. }));
    let final_snapshot = probe.authority_snapshot();
    assert_eq!(final_snapshot.hook_invocations(), 4);
    assert_eq!(final_snapshot.sessions().len(), 4);
    assert_eq!(final_snapshot.rejections(), 1);
    let (compatible_writer, compatible_pid) = *final_snapshot.sessions().last().unwrap();
    assert_eq!(compatible_writer, primary_probe_writer);
    assert_ne!(compatible_pid, rejected_pid);
    assert_eq!(
        final_snapshot
            .preparations()
            .iter()
            .filter(|writer| **writer == primary_probe_writer)
            .count(),
        1,
        "replacement sessions must not repeat eager open preparation"
    );

    let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.authority_values ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect authorized session mutations");
    assert_eq!(
        rows,
        vec![
            (2, "compatible-replacement".to_string()),
            (3, "fresh-authority".to_string()),
            (9, "fan-out-isolated".to_string()),
        ]
    );
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replacement_authority_query_failures_and_timeouts_close_unverified_sessions() {
    let fixture = Fixture::new("authority_failure").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.authority_failures (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create authority-failure target");

    let failure_probe = PostgresTestProbe::default();
    let query_failure = sink(
        fixture.connection(),
        &fixture.schema,
        "authority_failures",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        failure_probe.clone(),
    );
    let (mut query_failure, query_failure_writer_id) = open_adapter(query_failure)
        .await
        .expect("authority-query failure writer opens initially");
    terminate_backend(&fixture.pool, latest_authority_backend(&failure_probe)).await;
    failure_probe.fail_authority_query_once();
    let error = consume(
        &mut query_failure,
        query_failure_writer_id,
        DriverInput {
            id: 10,
            value: "query-failure".to_string(),
        },
    )
    .await
    .expect_err("a failed replacement authority query rejects the session");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Acquire);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::CurrentOnly
            );
            assert_eq!(failure.error().kind(), ErrorKind::Remote);
            assert_eq!(sqlstate(failure.error()), None);
            assert!(!failure.error().detail().contains("current_setting"));
            assert!(!failure.error().detail().contains("authority-query"));
        }
        other => panic!("expected typed authority-query failure, got {other:?}"),
    }
    let failure_snapshot = failure_probe.authority_snapshot();
    assert_eq!(failure_snapshot.hook_invocations(), 2);
    assert_eq!(failure_snapshot.rejections(), 1);
    let failed_query_pid = latest_authority_backend(&failure_probe);
    assert_backend_closed(&fixture.pool, failed_query_pid).await;
    assert_eq!(
        failure_probe.snapshot().count(SinkExternalCallKind::Begin),
        0
    );

    consume(
        &mut query_failure,
        query_failure_writer_id,
        DriverInput {
            id: 11,
            value: "query-recovered".to_string(),
        },
    )
    .await
    .expect("a later call may establish a new compatible session");
    assert_eq!(failure_probe.authority_snapshot().hook_invocations(), 3);

    let timeout_probe = PostgresTestProbe::default();
    let authority_timeout = sink(
        fixture
            .connection()
            .with_operation_timeout(Duration::from_secs(1)),
        &fixture.schema,
        "authority_failures",
        "(id, value) VALUES ($1, $2)",
        IdValueBinder,
        timeout_probe.clone(),
    );
    let (mut authority_timeout, authority_timeout_writer_id) = open_adapter(authority_timeout)
        .await
        .expect("authority-timeout writer opens initially");
    terminate_backend(&fixture.pool, latest_authority_backend(&timeout_probe)).await;
    timeout_probe.delay_once(PostgresDelayPoint::Authority, Duration::from_secs(3));
    let error = consume(
        &mut authority_timeout,
        authority_timeout_writer_id,
        DriverInput {
            id: 12,
            value: "authority-timeout".to_string(),
        },
    )
    .await
    .expect_err("an unfinalised replacement authority check times out closed");
    match error {
        HandlerError::SinkWrite(failure) => {
            assert_eq!(failure.phase(), SinkWritePhase::Acquire);
            assert_eq!(
                failure.disposition(),
                SinkWriteFailureDisposition::CurrentOnly
            );
            assert_eq!(failure.error().kind(), ErrorKind::Timeout);
            assert_eq!(sqlstate(failure.error()), None);
            assert!(!failure.error().detail().contains("current_setting"));
        }
        other => panic!("expected typed authority timeout, got {other:?}"),
    }
    let timeout_snapshot = timeout_probe.authority_snapshot();
    assert_eq!(timeout_snapshot.hook_invocations(), 2);
    assert_eq!(timeout_snapshot.sessions().len(), 2);
    assert_eq!(timeout_snapshot.rejections(), 0);
    let timed_out_pid = latest_authority_backend(&timeout_probe);
    assert_backend_closed(&fixture.pool, timed_out_pid).await;
    let timeout_calls = timeout_probe.snapshot();
    assert_eq!(timeout_calls.count(SinkExternalCallKind::Begin), 0);
    assert_eq!(timeout_calls.count(SinkExternalCallKind::Execute), 0);
    assert_eq!(timeout_calls.count(SinkExternalCallKind::Commit), 0);

    let rows: Vec<(i64, String)> = sqlx::query_as(&format!(
        "SELECT id, value FROM {}.authority_failures ORDER BY id",
        fixture.schema
    ))
    .fetch_all(&fixture.pool)
    .await
    .expect("inspect query-failure and timeout destination");
    assert_eq!(rows, vec![(11, "query-recovered".to_string())]);
    fixture.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn buffered_flush_and_drain_rejection_settle_nothing() {
    let fixture = Fixture::new("authority_lifecycle").await;
    sqlx::query(&format!(
        "CREATE TABLE {}.authority_lifecycle (id BIGINT PRIMARY KEY, value TEXT NOT NULL)",
        fixture.schema
    ))
    .execute(&fixture.pool)
    .await
    .expect("create authority lifecycle target");

    let flush_probe = PostgresTestProbe::default();
    let flush_sink = PostgresSink::<DriverInput>::builder()
        .connection(fixture.connection())
        .insert_into(
            &fixture.schema,
            "authority_lifecycle",
            "(id, value) VALUES ($1, $2)",
        )
        .expect("flush target and body pass local validation")
        .batch_size(3)
        .expect("flush batch size is valid")
        .bind_with(IdValueBinder)
        .test_probe(flush_probe.clone())
        .build()
        .expect("flush sink builds without I/O");
    let (mut flush_adapter, flush_writer_id) =
        open_adapter(flush_sink).await.expect("flush writer opens");
    let buffered = consume(
        &mut flush_adapter,
        flush_writer_id,
        DriverInput {
            id: 20,
            value: "flush-pending".to_string(),
        },
    )
    .await
    .expect("flush input remains buffered");
    assert!(matches!(buffered.result, DeliveryResult::Buffered { .. }));
    terminate_backend(&fixture.pool, latest_authority_backend(&flush_probe)).await;
    flush_probe.authority_limit_once(8);
    let error = flush_adapter
        .flush_report()
        .await
        .expect_err("flush rejects an unauthorized replacement session");
    match error {
        HandlerError::SinkOperation(error) => {
            assert_eq!(error.kind(), ErrorKind::PermanentFailure);
            assert_eq!(sqlstate(&error), None);
        }
        other => panic!("expected typed flush operation failure, got {other:?}"),
    }
    let flush_snapshot = flush_probe.authority_snapshot();
    assert_eq!(flush_snapshot.hook_invocations(), 2);
    assert_eq!(flush_snapshot.rejections(), 1);
    assert_backend_closed(&fixture.pool, latest_authority_backend(&flush_probe)).await;
    let flush_calls = flush_probe.snapshot();
    assert_eq!(flush_calls.count(SinkExternalCallKind::Flush), 1);
    assert_eq!(flush_calls.count(SinkExternalCallKind::Begin), 0);
    assert_eq!(flush_calls.count(SinkExternalCallKind::Execute), 0);
    assert_eq!(flush_calls.count(SinkExternalCallKind::Commit), 0);

    let drain_probe = PostgresTestProbe::default();
    let drain_sink = PostgresSink::<DriverInput>::builder()
        .connection(fixture.connection())
        .insert_into(
            &fixture.schema,
            "authority_lifecycle",
            "(id, value) VALUES ($1, $2)",
        )
        .expect("drain target and body pass local validation")
        .batch_size(3)
        .expect("drain batch size is valid")
        .bind_with(IdValueBinder)
        .test_probe(drain_probe.clone())
        .build()
        .expect("drain sink builds without I/O");
    let (mut drain_adapter, drain_writer_id) =
        open_adapter(drain_sink).await.expect("drain writer opens");
    let buffered = consume(
        &mut drain_adapter,
        drain_writer_id,
        DriverInput {
            id: 21,
            value: "drain-pending".to_string(),
        },
    )
    .await
    .expect("drain input remains buffered");
    assert!(matches!(buffered.result, DeliveryResult::Buffered { .. }));
    terminate_backend(&fixture.pool, latest_authority_backend(&drain_probe)).await;
    drain_probe.authority_limit_once(8);
    let error = drain_adapter
        .drain_report()
        .await
        .expect_err("drain rejects an unauthorized replacement session");
    match error {
        HandlerError::SinkOperation(error) => {
            assert_eq!(error.kind(), ErrorKind::PermanentFailure);
            assert_eq!(sqlstate(&error), None);
        }
        other => panic!("expected typed drain operation failure, got {other:?}"),
    }
    let drain_snapshot = drain_probe.authority_snapshot();
    assert_eq!(drain_snapshot.hook_invocations(), 2);
    assert_eq!(drain_snapshot.rejections(), 1);
    assert_backend_closed(&fixture.pool, latest_authority_backend(&drain_probe)).await;
    let drain_calls = drain_probe.snapshot();
    assert_eq!(drain_calls.count(SinkExternalCallKind::Drain), 1);
    assert_eq!(drain_calls.count(SinkExternalCallKind::Begin), 0);
    assert_eq!(drain_calls.count(SinkExternalCallKind::Execute), 0);
    assert_eq!(drain_calls.count(SinkExternalCallKind::Commit), 0);

    let rows: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.authority_lifecycle",
        fixture.schema
    ))
    .fetch_one(&fixture.pool)
    .await
    .expect("inspect lifecycle rejection destination");
    assert_eq!(rows, 0);
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
