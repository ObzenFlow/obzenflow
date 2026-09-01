// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(all(feature = "postgres", feature = "test-support"))]

mod replay_testkit;

use async_trait::async_trait;
use obzenflow::sinks::postgres::testing::{
    PostgresDelayPoint, PostgresTestProbe, POSTGRES_SQLSTATE_NAMESPACE,
};
use obzenflow::sinks::postgres::{
    PostgresBind, PostgresBindings, PostgresConnection, PostgresSink, PostgresTransport,
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
use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, SinkOperationFailed, SinkOperationPhase, SinkWritePhase,
    StageActivity, StageLifecycleEvent, SystemEvent, SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventEnvelope, StageId, SystemId, TypedPayload};
use obzenflow_dsl::{async_source, flow, sink, source, transform, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::{disk_journals, DiskJournal};
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::SinkConnector;
use obzenflow_runtime::stages::source::strategies::{
    CompletionContext, CompletionDecision, CompletionGate,
};
use obzenflow_runtime::stages::{
    SourceError, TypedAsyncFiniteSourceHandler, TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::supervised_base::SupervisorHandle;
use obzenflow_runtime::testing::sink::{
    SinkConformanceProfile, SinkDiagnosticSample, SinkDiagnosticSurface, SinkExternalCallKind,
    SinkExternalCallSnapshot, SinkFault, SinkFixtureError, SinkSettlementMode,
    SINK_CONFORMANCE_PROTOCOL_VERSION,
};
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgConnectOptions;
use sqlx::{ConnectOptions as _, PgPool, Row};
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

static POSTGRES_APPLICATION_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const POSTGRES_SQL_EVIDENCE_CANARY: &str = "obz083c_sql_body_canary_7f3c91a6";
const POSTGRES_DURABLE_ONLY_VALUES: &[&str] = &[
    "VerifiedTls",
    "ExternallyProtectedPlaintext",
    "sslrootcert",
    "max_identifier_length",
    "statement_fingerprint",
];
const POSTGRES_FORBIDDEN_EVIDENCE_KEYS: &[&str] = &[
    "certificate",
    "certificate_path",
    "compose_project",
    "connection_url",
    "container_id",
    "health",
    "identifier_limit",
    "max_identifier_length",
    "port",
    "postgres_url",
    "sql",
    "sslrootcert",
    "statement",
    "statement_fingerprint",
    "transport",
    "trust_store",
];

fn managed_postgres_secret() -> Result<String, SinkFixtureError> {
    let path = std::env::var_os("PGPASSFILE")
        .map(PathBuf::from)
        .ok_or_else(|| {
            SinkFixtureError::new("PGPASSFILE is required from `cargo xtask postgres test`")
        })?;
    let contents =
        std::fs::read_to_string(path).map_err(|error| SinkFixtureError::new(error.to_string()))?;
    let secret = contents
        .lines()
        .next()
        .and_then(|line| line.rsplit_once(':').map(|(_, secret)| secret.to_string()))
        .ok_or_else(|| SinkFixtureError::new("managed pgpass has five fields"))?;
    if secret.len() != 64 {
        return Err(SinkFixtureError::new(
            "managed PostgreSQL secret has an invalid generated shape",
        ));
    }
    Ok(secret)
}

#[derive(Clone, Default)]
struct TraceCapture {
    bytes: Arc<Mutex<Vec<u8>>>,
}

impl TraceCapture {
    fn clear(&self) {
        self.bytes.lock().expect("trace capture lock").clear();
    }

    fn text(&self) -> String {
        String::from_utf8_lossy(&self.bytes.lock().expect("trace capture lock")).into_owned()
    }
}

impl Write for TraceCapture {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes
            .lock()
            .map_err(|_| std::io::Error::other("trace capture lock poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn trace_capture() -> &'static TraceCapture {
    static CAPTURE: OnceLock<TraceCapture> = OnceLock::new();
    CAPTURE.get_or_init(|| {
        let capture = TraceCapture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_max_level(tracing::Level::TRACE)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("install PostgreSQL evidence trace capture");
        capture
    })
}

#[derive(Clone, Debug)]
struct PostgresEvidenceCanaries {
    forbidden_everywhere: Vec<String>,
    forbidden_port: u16,
}

impl PostgresEvidenceCanaries {
    fn apply_to_profile(&self, profile: SinkConformanceProfile) -> SinkConformanceProfile {
        self.forbidden_everywhere
            .iter()
            .cloned()
            .fold(profile, SinkConformanceProfile::with_credential_sentinel)
    }

    fn reject_text(
        &self,
        surface: &str,
        text: &str,
        include_durable_only: bool,
    ) -> Result<(), SinkFixtureError> {
        for (index, value) in self.forbidden_everywhere.iter().enumerate() {
            if !value.is_empty() && text.contains(value) {
                return Err(SinkFixtureError::new(format!(
                    "PostgreSQL evidence surface {surface} contains forbidden canary #{index}"
                )));
            }
        }
        if include_durable_only {
            for (index, value) in POSTGRES_DURABLE_ONLY_VALUES.iter().enumerate() {
                if text.contains(value) {
                    return Err(SinkFixtureError::new(format!(
                        "PostgreSQL durable evidence surface {surface} contains forbidden policy value #{index}"
                    )));
                }
            }
        }
        Ok(())
    }

    fn scan_tree(&self, root: &Path) -> Result<(), SinkFixtureError> {
        if !root.exists() {
            return Ok(());
        }
        let mut pending = vec![root.to_path_buf()];
        while let Some(path) = pending.pop() {
            if path.is_dir() {
                for entry in std::fs::read_dir(&path)
                    .map_err(|error| SinkFixtureError::new(error.to_string()))?
                {
                    pending.push(
                        entry
                            .map_err(|error| SinkFixtureError::new(error.to_string()))?
                            .path(),
                    );
                }
                continue;
            }
            let bytes =
                std::fs::read(&path).map_err(|error| SinkFixtureError::new(error.to_string()))?;
            self.reject_text(
                &path.display().to_string(),
                &String::from_utf8_lossy(&bytes),
                true,
            )?;
            if path.extension().and_then(|extension| extension.to_str()) == Some("json") {
                let value: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|error| SinkFixtureError::new(error.to_string()))?;
                self.reject_json(&path, &value)?;
            }
        }
        Ok(())
    }

    fn reject_json(&self, path: &Path, value: &serde_json::Value) -> Result<(), SinkFixtureError> {
        match value {
            serde_json::Value::Object(fields) => {
                for (key, value) in fields {
                    let normalized = key.to_ascii_lowercase().replace('-', "_");
                    if POSTGRES_FORBIDDEN_EVIDENCE_KEYS.contains(&normalized.as_str()) {
                        return Err(SinkFixtureError::new(format!(
                            "PostgreSQL durable JSON {} contains forbidden field `{key}`",
                            path.display()
                        )));
                    }
                    self.reject_json(path, value)?;
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    self.reject_json(path, value)?;
                }
            }
            serde_json::Value::Number(number)
                if number.as_u64() == Some(u64::from(self.forbidden_port)) =>
            {
                return Err(SinkFixtureError::new(format!(
                    "PostgreSQL durable JSON {} contains the proof-service port",
                    path.display()
                )));
            }
            serde_json::Value::String(value) if value == &self.forbidden_port.to_string() => {
                return Err(SinkFixtureError::new(format!(
                    "PostgreSQL durable JSON {} contains the proof-service port",
                    path.display()
                )));
            }
            _ => {}
        }
        Ok(())
    }
}

fn assert_postgres_evidence_is_confined(
    fixture: &PostgresApplicationFixture,
    report: &obzenflow::testing::sink::SinkConformanceReport,
    trace: &str,
) -> Result<(), SinkFixtureError> {
    fixture.evidence.reject_text("trace", trace, true)?;
    fixture
        .evidence
        .reject_text("conformance-report", &format!("{report:?}"), true)?;
    fixture.evidence.scan_tree(&fixture.root)
}

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

impl PostgresBind for PaymentBinder {
    type Input = Payment;

    fn bind(&self, bindings: &mut PostgresBindings, input: &Self::Input) {
        bindings.bind(input.id).bind(input.amount_cents);
    }
}

type PaymentSink = PostgresSink<PaymentBinder>;

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
struct GatedPayments {
    payments: Vec<Payment>,
    next: usize,
    initial_gate: Option<SourceGate>,
}

impl GatedPayments {
    fn new(payments: Vec<Payment>, initial_gate: Option<SourceGate>) -> Self {
        Self {
            payments,
            next: 0,
            initial_gate,
        }
    }
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for GatedPayments {
    type Output = Payment;

    async fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next >= self.payments.len() {
            return Ok(None);
        }
        if let (0, Some(gate)) = (self.next, &self.initial_gate) {
            gate.wait_until_released().await;
        }
        let payment = self.payments[self.next].clone();
        self.next += 1;
        Ok(Some(vec![payment]))
    }
}

#[derive(Default)]
struct SourceGateState {
    waiting: AtomicBool,
    released: AtomicBool,
    waiting_changed: tokio::sync::Notify,
    release_changed: tokio::sync::Notify,
}

#[derive(Clone, Default)]
struct SourceGate {
    state: Arc<SourceGateState>,
}

impl std::fmt::Debug for SourceGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SourceGate")
            .field("waiting", &self.state.waiting.load(Ordering::Acquire))
            .field("released", &self.state.released.load(Ordering::Acquire))
            .finish()
    }
}

impl SourceGate {
    async fn wait_until_released(&self) {
        self.state.waiting.store(true, Ordering::Release);
        self.state.waiting_changed.notify_waiters();
        loop {
            let released = self.state.release_changed.notified();
            if self.state.released.load(Ordering::Acquire) {
                return;
            }
            released.await;
        }
    }

    async fn wait_until_waiting(&self) {
        loop {
            let waiting = self.state.waiting_changed.notified();
            if self.state.waiting.load(Ordering::Acquire) {
                return;
            }
            waiting.await;
        }
    }

    fn release(&self) {
        self.state.released.store(true, Ordering::Release);
        self.state.release_changed.notify_waiters();
    }
}

struct ReleaseSourceGateOnDrop(SourceGate);

impl Drop for ReleaseSourceGateOnDrop {
    fn drop(&mut self) {
        self.0.release();
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
    let builder = PostgresSink::builder(PaymentBinder)
        .connection(connection)
        .insert_into(
            schema,
            "payments",
            format!(
                "(id, amount_cents) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents \
                 /* {POSTGRES_SQL_EVIDENCE_CANARY} */"
            ),
        )
        .map_err(flow_error)?
        .batch_size(2)
        .map_err(flow_error)?
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

fn build_custom_sink(
    connection: PostgresConnection,
    schema: &str,
    table: &str,
    body: &str,
    batch_size: usize,
    probe: PostgresTestProbe,
) -> Result<PaymentSink, Box<FlowBuildError>> {
    PostgresSink::builder(PaymentBinder)
        .connection(connection)
        .insert_into(schema, table, body)
        .map_err(flow_error)?
        .batch_size(batch_size)
        .map_err(flow_error)?
        .redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
        .test_probe(probe)
        .build()
        .map_err(flow_error)
}

struct CustomPostgresFlowSpec {
    table: String,
    body: String,
    batch_size: usize,
    payments: Vec<Payment>,
}

fn custom_postgres_flow(
    journal_root: PathBuf,
    connection: PostgresConnection,
    schema: String,
    probe: PostgresTestProbe,
    spec: CustomPostgresFlowSpec,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres = build_custom_sink(
            connection,
            &schema,
            &spec.table,
            &spec.body,
            spec.batch_size,
            probe,
        )
        .map_err(|error| *error)?;
        let payments = sources::finite(spec.payments);
        Ok(flow! {
            name: "postgres_full_flow_failure",
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
    right_gate: SourceGate,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let postgres = build_sink(
            connection,
            &schema,
            probe,
            SinkDestinationClass::SafeToRepeat,
        )
        .map_err(|error| *error)?;
        let left = GatedPayments::new(
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
            None,
        );
        let right = GatedPayments::new(
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
            Some(right_gate),
        );
        Ok(flow! {
            name: "postgres_order_sensitive_source_fan_in",
            journals: disk_journals(journal_root),

            stages: {
                left = async_source!(Payment => left);
                right = async_source!(Payment => right);
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
    delayed_gate: SourceGate,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let payment = Payment {
            id: 9_102,
            amount_cents: 1_250,
        };
        let payments = sources::finite([payment.clone()]);
        let delayed_input = GatedPayments::new(vec![payment], Some(delayed_gate));
        let delayed = IdentityPayment;
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
                delayed_input = async_source!(Payment => delayed_input);
                delayed = transform!(Payment -> Payment => delayed);
                postgres = sink!(Payment => postgres);
            },

            topology: {
                payments |> postgres;
                delayed_input |> delayed;
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

async fn read_stage_journal(
    run: &Path,
    stage: &str,
    field: &str,
) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = replay_testkit::archive_manifest(run);
    let file = manifest["stages"][stage][field]
        .as_str()
        .expect("manifest contains the PostgreSQL stage journal");
    let journal =
        DiskJournal::<ChainEvent>::with_owner(run.join(file), JournalOwner::stage(StageId::new()))
            .expect("PostgreSQL stage journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("PostgreSQL stage journal reads")
}

async fn read_system_journal(run: &Path) -> Vec<EventEnvelope<SystemEvent>> {
    let manifest = replay_testkit::archive_manifest(run);
    let file = manifest["system_journal_file"]
        .as_str()
        .expect("manifest contains the system journal");
    let journal = DiskJournal::<SystemEvent>::with_owner(
        run.join(file),
        JournalOwner::system(SystemId::new()),
    )
    .expect("PostgreSQL system journal opens");
    journal
        .read_causally_ordered()
        .await
        .expect("PostgreSQL system journal reads")
}

async fn assert_operation_failure_lifecycle(
    run: &Path,
    expected_phase: SinkOperationPhase,
    expected_kind: ErrorKind,
    expected_code: Option<&str>,
    expected_subject: Option<obzenflow_core::EventId>,
) -> SinkOperationFailed {
    let errors = read_stage_journal(run, "postgres", "error_journal_file").await;
    let operations = errors
        .iter()
        .filter_map(|envelope| {
            SinkOperationFailed::from_event(&envelope.event)
                .map(|operation| (envelope.event.id, operation))
        })
        .collect::<Vec<_>>();
    assert_eq!(
        operations.len(),
        1,
        "one connector failure authors one durable operation fact"
    );
    let (operation_event_id, operation) = &operations[0];
    assert_eq!(operation.phase, expected_phase);
    assert_eq!(operation.kind, expected_kind);
    assert_eq!(operation.operation_subject_event_id, expected_subject);
    assert_eq!(
        operation
            .destination_error_code
            .as_ref()
            .map(|code| (code.namespace(), code.value())),
        expected_code.map(|code| (POSTGRES_SQLSTATE_NAMESPACE, code))
    );

    let system = read_system_journal(run).await;
    let tied_failures = system
        .iter()
        .filter_map(|envelope| match &envelope.event.event {
            SystemEventType::StageLifecycle {
                stage_id,
                event:
                    StageLifecycleEvent::Failed {
                        causal_event_id, ..
                    },
            } if *stage_id == operation.stage_id => *causal_event_id,
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tied_failures,
        vec![*operation_event_id],
        "the real connector failure is the sink lifecycle's exact durable cause"
    );
    assert!(
        !system.iter().any(|envelope| matches!(
            &envelope.event.event,
            SystemEventType::StageLifecycle {
                stage_id,
                event: StageLifecycleEvent::Completed { .. },
            } if *stage_id == operation.stage_id
        )),
        "a failed PostgreSQL lifecycle cannot claim completion"
    );
    operation.clone()
}

async fn assert_eof_flush_failure_evidence(
    run: &Path,
    probe: &PostgresTestProbe,
    database: &OrderingDatabase,
) {
    let source = read_stage_journal(run, "payments", "data_journal_file").await;
    let subject = source
        .iter()
        .find_map(|envelope| Payment::from_event(&envelope.event).map(|_| envelope.event.id))
        .expect("the unresolved PostgreSQL input remains in the source archive");
    let operation = assert_operation_failure_lifecycle(
        run,
        SinkOperationPhase::Flush,
        ErrorKind::Timeout,
        None,
        Some(subject),
    )
    .await;
    assert_eq!(operation.causal_event_id, None);
    assert_eq!(operation.input_position, None);
    assert_eq!(operation.failed_delivery_event_id, None);

    let sink_data = read_stage_journal(run, "postgres", "data_journal_file").await;
    let outcomes = sink_data
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Delivery(payload) => Some(&payload.result),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(outcomes.len(), 1, "the deferred input is recorded once");
    assert!(matches!(outcomes[0], DeliveryResult::Buffered { .. }));
    assert!(
        !outcomes.iter().any(|outcome| matches!(
            outcome,
            DeliveryResult::Success { .. } | DeliveryResult::Failed { .. }
        )),
        "failed EOF flush must not manufacture terminal settlement"
    );

    let calls = probe.snapshot();
    assert_eq!(calls.count(SinkExternalCallKind::Open), 1);
    assert_eq!(calls.count(SinkExternalCallKind::Write), 1);
    assert_eq!(calls.count(SinkExternalCallKind::Flush), 1);
    assert_eq!(calls.count(SinkExternalCallKind::Drain), 0);
    assert_eq!(calls.count(SinkExternalCallKind::Begin), 1);
    assert_eq!(calls.count(SinkExternalCallKind::Execute), 0);
    assert_eq!(calls.count(SinkExternalCallKind::Commit), 0);
    assert_eq!(calls.count(SinkExternalCallKind::Rollback), 1);
    assert_eq!(
        database.amount(10_001).await,
        None,
        "the unresolved buffered input never reached PostgreSQL"
    );
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

    async fn wait_for_amount(&self, id: i64, expected: i64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if self.amount(id).await == Some(expected) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("expected value becomes visible in the ordering destination");
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
    evidence: PostgresEvidenceCanaries,
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
        let tls_url = std::env::var("OBZENFLOW_POSTGRES_TEST_TLS_URL").map_err(|_| {
            SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_TLS_URL is required from `cargo xtask postgres test`",
            )
        })?;
        let ca_cert = std::env::var("OBZENFLOW_POSTGRES_TEST_CA_CERT").map_err(|_| {
            SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_CA_CERT is required from `cargo xtask postgres test`",
            )
        })?;
        let project = std::env::var("OBZENFLOW_POSTGRES_TEST_PROJECT").map_err(|_| {
            SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_PROJECT is required from `cargo xtask postgres test`",
            )
        })?;
        let container_id = std::env::var("OBZENFLOW_POSTGRES_TEST_CONTAINER_ID").map_err(|_| {
            SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_CONTAINER_ID is required from `cargo xtask postgres test`",
            )
        })?;
        let tls_directory = std::env::var("OBZENFLOW_POSTGRES_TEST_TLS_DIR").map_err(|_| {
            SinkFixtureError::new(
                "OBZENFLOW_POSTGRES_TEST_TLS_DIR is required from `cargo xtask postgres test`",
            )
        })?;
        let port = std::env::var("OBZENFLOW_POSTGRES_TEST_PORT")
            .map_err(|_| {
                SinkFixtureError::new(
                    "OBZENFLOW_POSTGRES_TEST_PORT is required from `cargo xtask postgres test`",
                )
            })?
            .parse::<u16>()
            .map_err(|_| SinkFixtureError::new("invalid PostgreSQL proof-service port"))?;
        let options = PgConnectOptions::from_str(&tls_url)
            .map_err(|error| SinkFixtureError::new(error.to_string()))?
            .ssl_root_cert(&ca_cert)
            .log_statements("trace".parse().expect("trace log level parses"))
            .log_slow_statements(
                "trace".parse().expect("trace log level parses"),
                Duration::ZERO,
            );
        let connection = PostgresConnection::from_options(options, PostgresTransport::VerifiedTls)
            .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        let evidence = PostgresEvidenceCanaries {
            forbidden_everywhere: vec![
                managed_postgres_secret()?,
                url.to_string(),
                tls_url,
                ca_cert,
                project,
                container_id,
                tls_directory,
                POSTGRES_SQL_EVIDENCE_CANARY.to_string(),
            ],
            forbidden_port: port,
        };
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
            evidence,
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
        self.evidence.apply_to_profile(SinkConformanceProfile::new(
            SINK_CONFORMANCE_PROTOCOL_VERSION,
            SinkSettlementMode::Buffered { batch_size: 2 },
        ))
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
        let sink = build_sink(
            self.connection.clone(),
            &self.verifier.schema,
            self.verifier.probe.clone(),
            SinkDestinationClass::SafeToRepeat,
        )
        .map_err(|error| SinkFixtureError::new(error.to_string()))?;
        Ok(vec![
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Debug,
                format!("{:?}", self.connection),
            ),
            SinkDiagnosticSample::new(SinkDiagnosticSurface::Debug, format!("{sink:?}")),
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Snapshot,
                format!("{:?}", sink.describe()),
            ),
            SinkDiagnosticSample::new(
                SinkDiagnosticSurface::Verifier,
                format!("schema={}", self.verifier.schema),
            ),
        ])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_order_sensitive_source_fan_in_archive_redelivery_reproduces_same_word_and_final_row(
) {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    let _ = trace_capture();
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let database = OrderingDatabase::connect(&url, "source").await;
    let temp = tempfile::tempdir().expect("source fan-in journal directory");
    let journal_root = temp.path().join("journals");
    let right_gate = SourceGate::default();
    let _release_right_on_drop = ReleaseSourceGateOnDrop(right_gate.clone());
    let live_probe = PostgresTestProbe::default();

    let handle = ordered_source_fan_in_flow(
        journal_root.clone(),
        database.connection.clone(),
        database.schema.clone(),
        live_probe.clone(),
        right_gate.clone(),
    )
    .build(FlowBuildContext::for_tests())
    .await
    .expect("source-fed PostgreSQL fan-in builds");

    tokio::time::timeout(Duration::from_secs(10), right_gate.wait_until_waiting())
        .await
        .expect("right source reaches its deterministic asynchronous fan-in gate");
    tokio::time::timeout(
        Duration::from_secs(10),
        live_probe.wait_for_calls(SinkExternalCallKind::Commit, 1),
    )
    .await
    .expect("the left-only threshold transaction commits while the right source is gated");
    database.wait_for_amount(9_101, 300).await;
    right_gate.release();

    tokio::time::timeout(Duration::from_secs(12), handle.wait_for_completion())
        .await
        .expect("source-fed PostgreSQL fan-in completes")
        .expect("source-fed PostgreSQL fan-in succeeds");
    let live_authority = live_probe.authority_snapshot();
    assert_eq!(live_authority.hook_invocations(), 1);
    assert_eq!(live_authority.sessions().len(), 1);
    assert_eq!(live_authority.preparations().len(), 1);
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

    let replay_probe = PostgresTestProbe::default();
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
            replay_probe.clone(),
            right_gate.clone(),
        ))
        .await
        .expect("same-key PostgreSQL fan-in redelivers with verified journals");
    let replay_authority = replay_probe.authority_snapshot();
    assert_eq!(replay_authority.hook_invocations(), 1);
    assert_eq!(replay_authority.sessions().len(), 1);
    assert_eq!(replay_authority.preparations().len(), 1);
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
    assert_eq!(
        replay_word, live_word,
        "archive redelivery reproduces the sink word"
    );
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
    let _ = trace_capture();
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let database = OrderingDatabase::connect(&url, "derived").await;
    let temp = tempfile::tempdir().expect("derived fan-in journal directory");
    let journal_root = temp.path().join("journals");
    let delayed_gate = SourceGate::default();
    let _release_delayed_on_drop = ReleaseSourceGateOnDrop(delayed_gate.clone());
    let handle = ordered_derived_fan_in_flow(
        journal_root.clone(),
        database.connection.clone(),
        database.schema.clone(),
        PostgresTestProbe::default(),
        delayed_gate.clone(),
    )
    .build(FlowBuildContext::for_tests())
    .await
    .expect("derived-fed PostgreSQL fan-in builds");
    let topology = handle.topology().expect("flow exposes its topology");
    let snapshots = handle
        .liveness_snapshots()
        .expect("flow exposes liveness snapshots");

    tokio::time::timeout(Duration::from_secs(5), delayed_gate.wait_until_waiting())
        .await
        .expect("the derived input reaches its deterministic asynchronous gate");

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
    delayed_gate.release();

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
        vec![("delayed".to_string(), 1), ("payments".to_string(), 1)],
        "canonical Kahn delivery uses its stable reader tiebreak even though the derived input arrived later"
    );
    assert_eq!(database.amount(9_102).await, Some(1_250));
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_open_failures_traverse_full_application_lifecycle() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    trace_capture().clear();
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let database = OrderingDatabase::connect(&url, "open_failure").await;
    let managed_secret = managed_postgres_secret().expect("read managed PostgreSQL secret");
    sqlx::query(&format!(
        "CREATE TABLE {}.forms (id BIGINT PRIMARY KEY, value TEXT)",
        database.schema
    ))
    .execute(&database.pool)
    .await
    .expect("create full-flow open-failure target");

    let invalid = [
        ("syntax", "forms", "(value VALUES ($1)", "42601"),
        (
            "missing_relation",
            "missing_relation",
            "(value) VALUES ($1)",
            "42P01",
        ),
        ("missing_column", "forms", "(missing) VALUES ($1)", "42703"),
        (
            "indeterminate_parameter",
            "forms",
            "(value) VALUES ('fixed') RETURNING $1 IS NULL",
            "42P18",
        ),
    ];

    for (label, table, body, expected_code) in invalid {
        let temp = tempfile::tempdir().expect("open-failure journal directory");
        let journals = temp.path().join(label);
        let probe = PostgresTestProbe::default();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            FlowApplication::builder()
                .with_cli_args(["obzenflow"])
                .run_async(custom_postgres_flow(
                    journals.clone(),
                    database.connection.clone(),
                    database.schema.clone(),
                    probe.clone(),
                    CustomPostgresFlowSpec {
                        table: table.to_string(),
                        body: body.to_string(),
                        batch_size: 1,
                        payments: vec![Payment {
                            id: 10_000,
                            amount_cents: 500,
                        }],
                    },
                )),
        )
        .await
        .expect("real PostgreSQL open failure terminates promptly");
        result.expect_err("server-invalid PostgreSQL statement fails the application");

        let run = latest_run_dir(&journals);
        let operation = assert_operation_failure_lifecycle(
            &run,
            SinkOperationPhase::Open,
            ErrorKind::Remote,
            Some(expected_code),
            None,
        )
        .await;
        assert_eq!(operation.causal_event_id, None, "case={label}");
        assert_eq!(operation.input_position, None, "case={label}");
        assert_eq!(operation.failed_delivery_event_id, None, "case={label}");
        assert!(!operation.detail.contains(&url), "case={label}");
        assert!(!operation.detail.contains(body), "case={label}");
        assert!(!operation.detail.contains(&managed_secret));

        let sink_data = read_stage_journal(&run, "postgres", "data_journal_file").await;
        assert!(
            sink_data
                .iter()
                .all(|envelope| !matches!(envelope.event.content, ChainEventContent::Delivery(_))),
            "open failure creates no input receipt; case={label}"
        );
        let calls = probe.snapshot();
        assert_eq!(calls.count(SinkExternalCallKind::Open), 1, "case={label}");
        assert_eq!(calls.count(SinkExternalCallKind::Write), 0, "case={label}");
        assert_eq!(
            calls.count(SinkExternalCallKind::Execute),
            0,
            "case={label}"
        );
        assert_eq!(calls.count(SinkExternalCallKind::Commit), 0, "case={label}");
        let row_count: i64 =
            sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}.forms", database.schema))
                .fetch_one(&database.pool)
                .await
                .expect("open failure leaves destination readable");
        assert_eq!(row_count, 0, "open failure cannot mutate; case={label}");
    }

    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_eof_flush_failure_archive_redelivery_retries_unresolved_input_without_drain() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    trace_capture().clear();
    let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL")
        .expect("OBZENFLOW_POSTGRES_TEST_URL comes from cargo xtask postgres test");
    let database = OrderingDatabase::connect(&url, "flush_failure").await;
    let temp = tempfile::tempdir().expect("flush-failure journal directory");
    let journals = temp.path().join("journals");
    let body = format!(
        "(id, amount_cents) VALUES ($1, $2) \
         ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents \
         /* {POSTGRES_SQL_EVIDENCE_CANARY} */"
    );
    let input = Payment {
        id: 10_001,
        amount_cents: 750,
    };

    let live_probe = PostgresTestProbe::default();
    live_probe.arm(SinkFault::DestinationExecution);
    live_probe.delay_once(PostgresDelayPoint::Rollback, Duration::from_secs(1));
    let live_result = tokio::time::timeout(
        Duration::from_secs(30),
        FlowApplication::builder()
            .with_cli_args(["obzenflow"])
            .run_async(custom_postgres_flow(
                journals.clone(),
                database
                    .connection
                    .clone()
                    .with_rollback_timeout(Duration::from_millis(50)),
                database.schema.clone(),
                live_probe.clone(),
                CustomPostgresFlowSpec {
                    table: "payments".to_string(),
                    body: body.clone(),
                    batch_size: 2,
                    payments: vec![input.clone()],
                },
            )),
    )
    .await
    .expect("real PostgreSQL EOF-flush failure terminates promptly");
    live_result.expect_err("live EOF flush fault fails the application");
    let live_run = latest_run_dir(&journals);
    assert_eof_flush_failure_evidence(&live_run, &live_probe, &database).await;

    let replay_probe = PostgresTestProbe::default();
    replay_probe.arm(SinkFault::DestinationExecution);
    replay_probe.delay_once(PostgresDelayPoint::Rollback, Duration::from_secs(1));
    let replay_result = tokio::time::timeout(
        Duration::from_secs(30),
        FlowApplication::builder()
            .with_cli_args(vec![
                OsString::from("obzenflow"),
                OsString::from("--replay-from"),
                live_run.as_os_str().to_os_string(),
                OsString::from("--allow-incomplete-archive"),
            ])
            .run_async(custom_postgres_flow(
                journals.clone(),
                database
                    .connection
                    .clone()
                    .with_rollback_timeout(Duration::from_millis(50)),
                database.schema.clone(),
                replay_probe.clone(),
                CustomPostgresFlowSpec {
                    table: "payments".to_string(),
                    body,
                    batch_size: 2,
                    payments: vec![input],
                },
            )),
    )
    .await
    .expect("real PostgreSQL archive-redelivery flush failure terminates promptly");
    replay_result.expect_err("archive treatment encounters the unresolved flush fault again");
    let replay_run = latest_run_dir(&journals);
    assert_ne!(replay_run, live_run);
    assert_eof_flush_failure_evidence(&replay_run, &replay_probe, &database).await;

    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn postgres_order_sensitive_cycle_fan_in_is_rejected_before_open() {
    let _guard = POSTGRES_APPLICATION_TEST_LOCK.lock().await;
    let _ = trace_capture();
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
    let trace = trace_capture().clone();
    trace.clear();
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

    if let Err(error) = assert_postgres_evidence_is_confined(&fixture, &report, &trace.text()) {
        report_failures.push(error.to_string());
    }

    fixture.cleanup().await;

    assert!(
        report_failures.is_empty(),
        "PostgreSQL application conformance report mismatches:\n{}",
        report_failures.join("\n")
    );
}
