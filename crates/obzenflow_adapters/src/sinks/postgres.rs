// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A production PostgreSQL sink built on ObzenFlow's typed connector contract.
//!
//! Configuration and statement validation are synchronous and perform no I/O.
//! Each call to [`SinkConnector::open`](obzenflow_runtime::stages::sink::SinkConnector::open)
//! creates an isolated writer and verifies its pool. Values enter the fixed
//! INSERT statement only through [`PostgresBind`].

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::sink::{
    PendingSinkInput, SinkCommitReceipt, SinkConnector, SinkDescription, SinkDestinationErrorCode,
    SinkInputOrder, SinkOperationError, SinkOperationResult, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteFailure, SinkWritePhase, SinkWriteReport, SinkWriteResult, SinkWriter,
    SinkWriterInitContext, SinkWriterLifecycleReport,
};
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::query::Query;
use sqlx::{ConnectOptions as _, Executor as _, PgPool, Postgres};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;
use tokio::time::{timeout, timeout_at, Instant};
use url::Url;

/// Adapter-owned PostgreSQL query carrier accepted by [`PostgresBind`].
///
/// Connector authors do not need a direct SQLx dependency merely to bind
/// domain values to the configured statement.
pub type PostgresQuery<'q> = Query<'q, Postgres, PgArguments>;

const DEFAULT_BATCH_SIZE: usize = 1;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_ROLLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_SIZE: usize = 100_000;
const PORTABLE_IDENTIFIER_LIMIT: usize = 63;
const POSTGRES_SQLSTATE_NAMESPACE: &str = "postgresql.sqlstate";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PostgresConfigError {
    #[error("PostgreSQL connection configuration is missing")]
    MissingConnection,
    #[error("PostgreSQL environment variable '{0}' is missing or is not valid Unicode")]
    MissingEnvironment(String),
    #[error("PostgreSQL connection options are invalid")]
    InvalidConnection,
    #[error("PostgreSQL URL transport mode conflicts with the explicit transport policy")]
    ConflictingTransport,
    #[error("PostgreSQL schema or table identifier is invalid")]
    InvalidIdentifier,
    #[error("PostgreSQL INSERT body is missing")]
    MissingInsertBody,
    #[error("PostgreSQL INSERT body is invalid")]
    InvalidInsertBody,
    #[error("PostgreSQL batch size must be in 1..={MAX_BATCH_SIZE}")]
    InvalidBatchSize,
    #[error("PostgreSQL parameter binder is missing")]
    MissingBinder,
    #[error("PostgreSQL timeouts must be strictly positive")]
    InvalidTimeout,
}

/// Application-owned PostgreSQL transport assurance.
///
/// The policy is required independently of URL or SQLx option provenance. It
/// always normalises the retained driver mode to one of the two assurances the
/// connector can state honestly.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostgresTransport {
    /// Require certificate-chain and hostname verification.
    VerifiedTls,
    /// Use plaintext because the application asserts that another local
    /// boundary, such as loopback, a Unix socket, sidecar, or tunnel, protects
    /// the connection.
    ExternallyProtectedPlaintext,
}

impl PostgresTransport {
    fn ssl_mode(self) -> PgSslMode {
        match self {
            Self::VerifiedTls => PgSslMode::VerifyFull,
            Self::ExternallyProtectedPlaintext => PgSslMode::Disable,
        }
    }

    fn agrees_with(self, mode: PgSslMode) -> bool {
        matches!(
            (self, mode),
            (Self::VerifiedTls, PgSslMode::VerifyFull)
                | (Self::ExternallyProtectedPlaintext, PgSslMode::Disable)
        )
    }
}

/// Parsed connection options whose formatting never reveals the URL or password.
#[derive(Clone)]
pub struct PostgresConnection {
    options: PgConnectOptions,
    transport: PostgresTransport,
    acquire_timeout: Duration,
    operation_timeout: Duration,
    rollback_timeout: Duration,
}

impl PostgresConnection {
    pub fn from_env(
        name: impl AsRef<str>,
        transport: PostgresTransport,
    ) -> Result<Self, PostgresConfigError> {
        let name = name.as_ref();
        let value = std::env::var(name)
            .map_err(|_| PostgresConfigError::MissingEnvironment(name.to_string()))?;
        Self::from_url(&value, transport)
    }

    pub fn from_url(url: &str, transport: PostgresTransport) -> Result<Self, PostgresConfigError> {
        preflight_url(url, transport)?;
        let options =
            PgConnectOptions::from_str(url).map_err(|_| PostgresConfigError::InvalidConnection)?;
        Self::from_options(options, transport)
    }

    pub fn from_options(
        options: PgConnectOptions,
        transport: PostgresTransport,
    ) -> Result<Self, PostgresConfigError> {
        Ok(Self {
            options: options
                .ssl_mode(transport.ssl_mode())
                .disable_statement_logging(),
            transport,
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
            operation_timeout: DEFAULT_OPERATION_TIMEOUT,
            rollback_timeout: DEFAULT_ROLLBACK_TIMEOUT,
        })
    }

    /// Override how long a writer waits to acquire its sole connection.
    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }

    /// Override the absolute budget for one external open or transaction.
    pub fn with_operation_timeout(mut self, operation_timeout: Duration) -> Self {
        self.operation_timeout = operation_timeout;
        self
    }

    /// Override the fresh budget for explicit rollback confirmation.
    pub fn with_rollback_timeout(mut self, rollback_timeout: Duration) -> Self {
        self.rollback_timeout = rollback_timeout;
        self
    }

    fn validate(&self) -> Result<(), PostgresConfigError> {
        if self.acquire_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.rollback_timeout.is_zero()
        {
            return Err(PostgresConfigError::InvalidTimeout);
        }
        Ok(())
    }
}

impl fmt::Debug for PostgresConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnection")
            .field("configured", &true)
            .field("transport", &self.transport)
            .field("acquire_timeout", &self.acquire_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("rollback_timeout", &self.rollback_timeout)
            .finish()
    }
}

fn preflight_url(url: &str, transport: PostgresTransport) -> Result<(), PostgresConfigError> {
    let parsed = Url::parse(url).map_err(|_| PostgresConfigError::InvalidConnection)?;
    if !matches!(parsed.scheme(), "postgres" | "postgresql") {
        return Err(PostgresConfigError::InvalidConnection);
    }

    for (key, value) in parsed.query_pairs() {
        let supported = matches!(
            key.as_ref(),
            "sslmode"
                | "ssl-mode"
                | "sslrootcert"
                | "ssl-root-cert"
                | "ssl-ca"
                | "sslcert"
                | "ssl-cert"
                | "sslkey"
                | "ssl-key"
                | "statement-cache-capacity"
                | "host"
                | "hostaddr"
                | "port"
                | "dbname"
                | "user"
                | "password"
                | "application_name"
                | "options"
        ) || (key.starts_with("options[") && key.ends_with(']'));
        if !supported {
            return Err(PostgresConfigError::InvalidConnection);
        }

        if matches!(key.as_ref(), "sslmode" | "ssl-mode") {
            let mode = PgSslMode::from_str(value.as_ref())
                .map_err(|_| PostgresConfigError::InvalidConnection)?;
            if !transport.agrees_with(mode) {
                return Err(PostgresConfigError::ConflictingTransport);
            }
        }
    }
    Ok(())
}

#[derive(Clone, PartialEq, Eq)]
pub struct PostgresTable {
    schema: String,
    table: String,
}

impl PostgresTable {
    fn try_new(
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, PostgresConfigError> {
        let schema = schema.into();
        let table = table.into();
        if !valid_identifier(&schema) || !valid_identifier(&table) {
            return Err(PostgresConfigError::InvalidIdentifier);
        }
        Ok(Self { schema, table })
    }

    fn qualified(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }

    fn quoted_target(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.table)
        )
    }

    fn statement(&self, body: &str) -> String {
        format!("INSERT INTO {} {body}", self.quoted_target())
    }

    fn validate_server_limit(&self, limit: i32) -> SinkOperationResult<()> {
        let limit = usize::try_from(limit).unwrap_or_default();
        if limit == 0 || self.schema.len() > limit || self.table.len() > limit {
            return Err(SinkOperationError::permanent(
                "PostgreSQL destination identifier exceeds the server limit",
            ));
        }
        Ok(())
    }

    fn logical_destination(&self) -> String {
        format!("postgres.{}", self.qualified())
    }
}

impl fmt::Debug for PostgresTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.qualified())
    }
}

fn valid_identifier(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= PORTABLE_IDENTIFIER_LIMIT
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn validate_insert_body(value: &str) -> Result<String, PostgresConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(PostgresConfigError::MissingInsertBody);
    }
    if trimmed.contains('\0') {
        return Err(PostgresConfigError::InvalidInsertBody);
    }
    Ok(trimmed.to_string())
}

/// A separate, clonable mapping from one domain input to PostgreSQL parameters.
///
/// `validate` is the writer's explicit encode phase. `bind` receives a query
/// created from the fixed configuration statement, so payload data cannot alter
/// SQL structure.
pub trait PostgresBind<T>: Clone + Send + Sync + 'static {
    fn validate(&self, _input: &T) -> SinkOperationResult<()> {
        Ok(())
    }

    fn bind<'q>(&self, query: PostgresQuery<'q>, input: &'q T) -> PostgresQuery<'q>;
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct MissingPostgresBinder;

/// Reusable, I/O-free PostgreSQL sink configuration.
pub struct PostgresSink<T, B = MissingPostgresBinder> {
    connection: PostgresConnection,
    destination: PostgresTable,
    _insert_body: String,
    statement: String,
    binder: B,
    batch_size: usize,
    redelivery_safety: Option<SinkRedeliverySafety>,
    #[cfg(feature = "test-support")]
    test_probe: Option<testing::PostgresTestProbe>,
    _input: PhantomData<fn() -> T>,
}

impl<T, B> fmt::Debug for PostgresSink<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSink")
            .field("input", &std::any::type_name::<T>())
            .field("destination", &self.destination)
            .field("batch_size", &self.batch_size)
            .field("redelivery_safety", &self.redelivery_safety)
            .finish_non_exhaustive()
    }
}

impl<T> PostgresSink<T, MissingPostgresBinder> {
    pub fn builder() -> PostgresSinkBuilder<T, MissingPostgresBinder> {
        PostgresSinkBuilder {
            connection: None,
            destination: None,
            insert_body: None,
            binder: None,
            batch_size: DEFAULT_BATCH_SIZE,
            redelivery_safety: None,
            #[cfg(feature = "test-support")]
            test_probe: None,
            _input: PhantomData,
        }
    }
}

pub struct PostgresSinkBuilder<T, B> {
    connection: Option<PostgresConnection>,
    destination: Option<PostgresTable>,
    insert_body: Option<String>,
    binder: Option<B>,
    batch_size: usize,
    redelivery_safety: Option<SinkRedeliverySafety>,
    #[cfg(feature = "test-support")]
    test_probe: Option<testing::PostgresTestProbe>,
    _input: PhantomData<fn() -> T>,
}

impl<T, B> fmt::Debug for PostgresSinkBuilder<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresSinkBuilder")
            .field("connection_configured", &self.connection.is_some())
            .field("destination", &self.destination)
            .field("insert_configured", &self.insert_body.is_some())
            .field("binder_configured", &self.binder.is_some())
            .field("batch_size", &self.batch_size)
            .field("redelivery_safety", &self.redelivery_safety)
            .finish()
    }
}

impl<T, B> PostgresSinkBuilder<T, B> {
    pub fn connection(mut self, connection: PostgresConnection) -> Self {
        self.connection = Some(connection);
        self
    }

    pub fn insert_into(
        mut self,
        schema: impl Into<String>,
        table: impl Into<String>,
        body: impl AsRef<str>,
    ) -> Result<Self, PostgresConfigError> {
        self.destination = Some(PostgresTable::try_new(schema, table)?);
        self.insert_body = Some(validate_insert_body(body.as_ref())?);
        Ok(self)
    }

    pub fn batch_size(mut self, batch_size: usize) -> Result<Self, PostgresConfigError> {
        if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
            return Err(PostgresConfigError::InvalidBatchSize);
        }
        self.batch_size = batch_size;
        Ok(self)
    }

    /// Classify archive redelivery for this exact configured operation.
    ///
    /// `SafeToRepeat` claims convergence only while the compiled target, SQL
    /// body, binder behaviour, batch configuration, upstream flow, schema,
    /// and relevant secondary effects remain the same. It is not a
    /// cross-version compatibility promise or a pre-write migration gate.
    pub fn redelivery_safety(mut self, safety: SinkRedeliverySafety) -> Self {
        self.redelivery_safety = Some(safety);
        self
    }

    /// Test-only seam for the generic archive gate's undeclared-sink case.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_redelivery_unspecified(mut self) -> Self {
        self.redelivery_safety = None;
        self
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_probe(mut self, probe: testing::PostgresTestProbe) -> Self {
        self.test_probe = Some(probe);
        self
    }

    pub fn bind_with<B2>(self, binder: B2) -> PostgresSinkBuilder<T, B2> {
        PostgresSinkBuilder {
            connection: self.connection,
            destination: self.destination,
            insert_body: self.insert_body,
            binder: Some(binder),
            batch_size: self.batch_size,
            redelivery_safety: self.redelivery_safety,
            #[cfg(feature = "test-support")]
            test_probe: self.test_probe,
            _input: PhantomData,
        }
    }
}

impl<T, B> PostgresSinkBuilder<T, B>
where
    B: PostgresBind<T>,
{
    pub fn build(self) -> Result<PostgresSink<T, B>, PostgresConfigError> {
        let destination = self
            .destination
            .ok_or(PostgresConfigError::InvalidIdentifier)?;
        let insert_body = self
            .insert_body
            .ok_or(PostgresConfigError::MissingInsertBody)?;
        let statement = destination.statement(&insert_body);
        let connection = self
            .connection
            .ok_or(PostgresConfigError::MissingConnection)?;
        connection.validate()?;
        Ok(PostgresSink {
            connection,
            destination,
            _insert_body: insert_body,
            statement,
            binder: self.binder.ok_or(PostgresConfigError::MissingBinder)?,
            batch_size: self.batch_size,
            redelivery_safety: self.redelivery_safety,
            #[cfg(feature = "test-support")]
            test_probe: self.test_probe,
            _input: PhantomData,
        })
    }
}

#[cfg(feature = "test-support")]
pub mod testing {
    use obzenflow_runtime::testing::sink::{
        SinkExternalCall, SinkExternalCallKind, SinkExternalCallSnapshot, SinkFault,
    };
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::Duration;

    /// Connector-owned namespace for projected PostgreSQL SQLSTATE diagnostics.
    pub const POSTGRES_SQLSTATE_NAMESPACE: &str = super::POSTGRES_SQLSTATE_NAMESPACE;

    /// Connector-specific deterministic delay seams for deadline proof.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum PostgresDelayPoint {
        Prepare,
        Begin,
        Execute,
        Rollback,
        CommitAcknowledgement,
    }

    #[derive(Default)]
    struct ProbeState {
        armed: Option<SinkFault>,
        delay: Option<(PostgresDelayPoint, Duration)>,
        next_writer: u64,
        next_sequence: u64,
        calls: Vec<SinkExternalCall>,
    }

    /// Test-only, connector-owned call and one-shot fault probe.
    #[derive(Clone, Default)]
    pub struct PostgresTestProbe {
        state: Arc<Mutex<ProbeState>>,
    }

    impl std::fmt::Debug for PostgresTestProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("PostgresTestProbe")
                .field("configured", &true)
                .finish()
        }
    }

    impl PostgresTestProbe {
        fn state(&self) -> MutexGuard<'_, ProbeState> {
            match self.state.lock() {
                Ok(state) => state,
                Err(poisoned) => poisoned.into_inner(),
            }
        }

        pub fn arm(&self, fault: SinkFault) {
            self.state().armed = Some(fault);
        }

        pub fn clear(&self) {
            let mut state = self.state();
            state.armed = None;
            state.delay = None;
            state.calls.clear();
            state.next_sequence = 0;
        }

        pub fn snapshot(&self) -> SinkExternalCallSnapshot {
            SinkExternalCallSnapshot::new(self.state().calls.clone())
        }

        pub fn delay_once(&self, point: PostgresDelayPoint, duration: Duration) {
            self.state().delay = Some((point, duration));
        }

        pub(crate) fn new_writer(&self) -> u64 {
            let mut state = self.state();
            let writer = state.next_writer;
            state.next_writer += 1;
            writer
        }

        pub(crate) fn record(&self, writer: u64, kind: SinkExternalCallKind) {
            let mut state = self.state();
            let sequence = state.next_sequence;
            state.next_sequence += 1;
            state
                .calls
                .push(SinkExternalCall::new(writer, sequence, kind));
        }

        pub(crate) fn take(&self, fault: SinkFault) -> bool {
            let mut state = self.state();
            if state.armed == Some(fault) {
                state.armed = None;
                true
            } else {
                false
            }
        }

        pub(crate) fn is_armed(&self, fault: SinkFault) -> bool {
            self.state().armed == Some(fault)
        }

        pub(crate) fn take_delay(&self, point: PostgresDelayPoint) -> Option<Duration> {
            let mut state = self.state();
            if state.delay.is_some_and(|(candidate, _)| candidate == point) {
                state.delay.take().map(|(_, duration)| duration)
            } else {
                None
            }
        }
    }
}

#[derive(Clone, Default)]
struct WriterProbe {
    #[cfg(feature = "test-support")]
    probe: Option<testing::PostgresTestProbe>,
    #[cfg(feature = "test-support")]
    writer: u64,
}

macro_rules! writer_probe_fault_method {
    ($name:ident, $fault:ident) => {
        fn $name(&self) -> bool {
            #[cfg(feature = "test-support")]
            {
                return self.take(obzenflow_runtime::testing::sink::SinkFault::$fault);
            }
            #[cfg(not(feature = "test-support"))]
            {
                false
            }
        }
    };
}

impl WriterProbe {
    #[cfg(feature = "test-support")]
    fn new(probe: Option<testing::PostgresTestProbe>) -> Self {
        let writer = probe
            .as_ref()
            .map(testing::PostgresTestProbe::new_writer)
            .unwrap_or_default();
        Self { probe, writer }
    }

    #[cfg(not(feature = "test-support"))]
    fn new() -> Self {
        Self {}
    }

    #[cfg(feature = "test-support")]
    fn record(&self, kind: obzenflow_runtime::testing::sink::SinkExternalCallKind) {
        if let Some(probe) = &self.probe {
            probe.record(self.writer, kind);
        }
    }

    #[cfg(feature = "test-support")]
    fn take(&self, fault: obzenflow_runtime::testing::sink::SinkFault) -> bool {
        self.probe.as_ref().is_some_and(|probe| probe.take(fault))
    }

    fn rollback_fault_armed(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.probe.as_ref().is_some_and(|probe| {
                probe.is_armed(obzenflow_runtime::testing::sink::SinkFault::Rollback)
            })
        }
        #[cfg(not(feature = "test-support"))]
        {
            false
        }
    }

    #[cfg(feature = "test-support")]
    async fn delay(&self, point: testing::PostgresDelayPoint) {
        if let Some(duration) = self
            .probe
            .as_ref()
            .and_then(|probe| probe.take_delay(point))
        {
            tokio::time::sleep(duration).await;
        }
    }

    fn record_open(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Open);
    }

    fn record_write(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Write);
    }

    fn record_flush(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Flush);
    }

    fn record_drain(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Drain);
    }

    fn record_begin(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Begin);
    }

    fn record_execute(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Execute);
    }

    fn record_commit(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Commit);
    }

    fn record_rollback(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Rollback);
    }

    fn record_drop(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Drop);
    }

    writer_probe_fault_method!(fault_open, Open);
    writer_probe_fault_method!(fault_encode, Encode);
    writer_probe_fault_method!(fault_acquire, Acquire);
    writer_probe_fault_method!(fault_before_deferral, BeforeDeferral);
    writer_probe_fault_method!(fault_after_deferral, AfterDeferral);
    writer_probe_fault_method!(fault_destination_execution, DestinationExecution);
    writer_probe_fault_method!(fault_mid_batch_mutation, MidBatchMutation);
    writer_probe_fault_method!(fault_pre_commit, PreCommit);
    writer_probe_fault_method!(fault_rollback, Rollback);
    writer_probe_fault_method!(fault_post_commit, PostCommitPreAcknowledgement);
    writer_probe_fault_method!(fault_flush, Flush);
    writer_probe_fault_method!(fault_drain, Drain);
}

#[async_trait]
impl<T, B> SinkConnector for PostgresSink<T, B>
where
    T: TypedPayload + Send + Sync + 'static,
    B: PostgresBind<T>,
{
    type Input = T;
    type Writer = PostgresWriter<T, B>;

    fn describe(&self) -> SinkDescription {
        let description = SinkDescription::destination(
            self.destination.logical_destination(),
            DeliveryMethod::DatabaseInsert {
                table: self.destination.qualified(),
            },
        )
        .with_input_order(SinkInputOrder::OrderSensitive);
        match self.redelivery_safety {
            Some(safety) => description.with_redelivery_safety(safety),
            None => description,
        }
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        #[cfg(feature = "test-support")]
        let probe = WriterProbe::new(self.test_probe.clone());
        #[cfg(not(feature = "test-support"))]
        let probe = WriterProbe::new();
        probe.record_open();
        if probe.fault_open() {
            return Err(destination_operation_error(
                &self.destination,
                test_operation_error("injected PostgreSQL open failure", None),
            ));
        }
        let deadline = Instant::now() + self.connection.operation_timeout;
        let pool = timeout_at(
            deadline,
            PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(self.connection.acquire_timeout)
                .connect_with(self.connection.options.clone()),
        )
        .await
        .map_err(|_| {
            destination_operation_error(
                &self.destination,
                SinkOperationError::timeout("PostgreSQL open timed out"),
            )
        })?
        .map_err(operation_error)
        .map_err(|error| destination_operation_error(&self.destination, error))?;

        let mut connection = timeout_at(deadline, pool.acquire())
            .await
            .map_err(|_| {
                destination_operation_error(
                    &self.destination,
                    SinkOperationError::timeout("PostgreSQL open timed out"),
                )
            })?
            .map_err(operation_error)
            .map_err(|error| destination_operation_error(&self.destination, error))?;
        let preparation = timeout_at(deadline, async {
            let max_identifier_length: i32 =
                sqlx::query_scalar("SELECT current_setting('max_identifier_length')::integer")
                    .fetch_one(&mut *connection)
                    .await
                    .map_err(operation_error)?;
            self.destination
                .validate_server_limit(max_identifier_length)?;
            #[cfg(feature = "test-support")]
            probe.delay(testing::PostgresDelayPoint::Prepare).await;
            (&mut *connection)
                .prepare(&self.statement)
                .await
                .map_err(operation_error)?;
            Ok::<(), SinkOperationError>(())
        })
        .await;
        match preparation {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                connection.close_on_drop();
                return Err(destination_operation_error(&self.destination, error));
            }
            Err(_) => {
                connection.close_on_drop();
                return Err(destination_operation_error(
                    &self.destination,
                    SinkOperationError::timeout("PostgreSQL open timed out"),
                ));
            }
        }
        drop(connection);
        Ok(PostgresWriter {
            pool,
            destination: self.destination.clone(),
            statement: self.statement.clone(),
            binder: self.binder.clone(),
            batch_size: self.batch_size,
            operation_timeout: self.connection.operation_timeout,
            rollback_timeout: self.connection.rollback_timeout,
            pending: Vec::new(),
            probe,
        })
    }
}

struct BufferedRow<T> {
    input: T,
    pending: PendingSinkInput,
}

pub struct PostgresWriter<T, B> {
    pool: PgPool,
    destination: PostgresTable,
    statement: String,
    binder: B,
    batch_size: usize,
    operation_timeout: Duration,
    rollback_timeout: Duration,
    pending: Vec<BufferedRow<T>>,
    probe: WriterProbe,
}

impl<T, B> fmt::Debug for PostgresWriter<T, B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresWriter")
            .field("input", &std::any::type_name::<T>())
            .field("destination", &self.destination)
            .field("batch_size", &self.batch_size)
            .field("pending", &self.pending.len())
            .finish_non_exhaustive()
    }
}

impl<T, B> Drop for PostgresWriter<T, B> {
    fn drop(&mut self) {
        self.probe.record_drop();
    }
}

enum TransactionFailure {
    Acquire(SinkOperationError),
    Execute {
        operation: SinkOperationError,
        rollback: Option<SinkOperationError>,
    },
    PreCommit {
        operation: SinkOperationError,
        rollback: Option<SinkOperationError>,
    },
    Commit(SinkOperationError),
    PostCommit(SinkOperationError),
}

impl TransactionFailure {
    fn with_destination(self, destination: &PostgresTable) -> Self {
        match self {
            Self::Acquire(error) => Self::Acquire(destination_operation_error(destination, error)),
            Self::Execute {
                operation,
                rollback,
            } => Self::Execute {
                operation: destination_operation_error(destination, operation),
                rollback: rollback.map(|error| destination_operation_error(destination, error)),
            },
            Self::PreCommit {
                operation,
                rollback,
            } => Self::PreCommit {
                operation: destination_operation_error(destination, operation),
                rollback: rollback.map(|error| destination_operation_error(destination, error)),
            },
            Self::Commit(error) => Self::Commit(destination_operation_error(destination, error)),
            Self::PostCommit(error) => {
                Self::PostCommit(destination_operation_error(destination, error))
            }
        }
    }
}

struct TransactionConnection {
    connection: PoolConnection<Postgres>,
    reusable: bool,
}

impl TransactionConnection {
    fn new(connection: PoolConnection<Postgres>) -> Self {
        Self {
            connection,
            reusable: false,
        }
    }

    fn mark_reusable(&mut self) {
        self.reusable = true;
    }
}

impl Drop for TransactionConnection {
    fn drop(&mut self) {
        if !self.reusable {
            self.connection.close_on_drop();
        }
    }
}

async fn rollback_transaction(
    connection: &mut TransactionConnection,
    rollback_timeout: Duration,
    probe: &WriterProbe,
) -> Result<(), SinkOperationError> {
    probe.record_rollback();
    let injected_failure = probe.fault_rollback();
    let result = timeout(rollback_timeout, async {
        #[cfg(feature = "test-support")]
        probe.delay(testing::PostgresDelayPoint::Rollback).await;
        sqlx::query("ROLLBACK")
            .execute(&mut *connection.connection)
            .await
            .map(|_| ())
            .map_err(operation_error)
    })
    .await;

    if injected_failure {
        return Err(test_operation_error(
            "injected PostgreSQL rollback failure",
            Some("08007"),
        ));
    }
    match result {
        Ok(Ok(())) => {
            connection.mark_reusable();
            Ok(())
        }
        Ok(Err(error)) => Err(error),
        Err(_) => Err(SinkOperationError::timeout(
            "PostgreSQL rollback confirmation timed out",
        )),
    }
}

async fn execute_transaction<T, B>(
    writer: &PostgresWriter<T, B>,
    current: Option<&T>,
) -> Result<(), TransactionFailure>
where
    T: Send + Sync,
    B: PostgresBind<T>,
{
    let pool = &writer.pool;
    let statement = writer.statement.as_str();
    let binder = &writer.binder;
    let buffered = writer.pending.as_slice();
    let operation_timeout = writer.operation_timeout;
    let rollback_timeout = writer.rollback_timeout;
    let probe = &writer.probe;
    let deadline = Instant::now() + operation_timeout;
    probe.record_begin();
    if probe.fault_acquire() {
        return Err(TransactionFailure::Acquire(test_operation_error(
            "injected PostgreSQL acquisition failure",
            None,
        )));
    }
    let connection = match timeout_at(deadline, pool.acquire()).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            return Err(TransactionFailure::Acquire(operation_error(error)));
        }
        Err(_) => {
            return Err(TransactionFailure::Acquire(SinkOperationError::timeout(
                "PostgreSQL transaction acquisition timed out",
            )));
        }
    };

    let mut connection = TransactionConnection::new(connection);
    let begin = timeout_at(deadline, async {
        #[cfg(feature = "test-support")]
        probe.delay(testing::PostgresDelayPoint::Begin).await;
        sqlx::query("BEGIN")
            .execute(&mut *connection.connection)
            .await
            .map(|_| ())
            .map_err(operation_error)
    })
    .await;
    match begin {
        Ok(Ok(())) => {}
        Ok(Err(error)) => return Err(TransactionFailure::Acquire(error)),
        Err(_) => {
            return Err(TransactionFailure::Acquire(SinkOperationError::timeout(
                "PostgreSQL transaction begin timed out",
            )));
        }
    }

    for (index, input) in buffered
        .iter()
        .map(|row| &row.input)
        .chain(current)
        .enumerate()
    {
        if probe.fault_destination_execution() || (index > 0 && probe.fault_mid_batch_mutation()) {
            let operation =
                test_operation_error("injected PostgreSQL execution failure", Some("23505"));
            let rollback = rollback_transaction(&mut connection, rollback_timeout, probe)
                .await
                .err();
            return Err(TransactionFailure::Execute {
                operation,
                rollback,
            });
        }
        probe.record_execute();
        let query = binder.bind(sqlx::query(statement), input);
        let execution = timeout_at(deadline, async {
            #[cfg(feature = "test-support")]
            probe.delay(testing::PostgresDelayPoint::Execute).await;
            query
                .execute(&mut *connection.connection)
                .await
                .map(|_| ())
                .map_err(operation_error)
        })
        .await;
        let operation = match execution {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => error,
            Err(_) => SinkOperationError::timeout("PostgreSQL transaction execution timed out"),
        };
        let rollback = rollback_transaction(&mut connection, rollback_timeout, probe)
            .await
            .err();
        return Err(TransactionFailure::Execute {
            operation,
            rollback,
        });
    }

    let pre_commit_failure = if probe.rollback_fault_armed() {
        Some(test_operation_error(
            "injected PostgreSQL failure requiring rollback",
            Some("23505"),
        ))
    } else if probe.fault_pre_commit() {
        Some(test_operation_error(
            "injected PostgreSQL pre-commit failure",
            Some("23505"),
        ))
    } else if Instant::now() >= deadline {
        Some(SinkOperationError::timeout(
            "PostgreSQL transaction timed out before commit",
        ))
    } else {
        None
    };
    if let Some(operation) = pre_commit_failure {
        let rollback = rollback_transaction(&mut connection, rollback_timeout, probe)
            .await
            .err();
        return Err(TransactionFailure::PreCommit {
            operation,
            rollback,
        });
    }

    probe.record_commit();
    let commit = timeout_at(deadline, async {
        sqlx::query("COMMIT")
            .execute(&mut *connection.connection)
            .await
            .map_err(operation_error)?;
        #[cfg(feature = "test-support")]
        probe
            .delay(testing::PostgresDelayPoint::CommitAcknowledgement)
            .await;
        Ok::<(), SinkOperationError>(())
    })
    .await;
    match commit {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Err(TransactionFailure::Commit(error));
        }
        Err(_) => {
            return Err(TransactionFailure::Commit(SinkOperationError::timeout(
                "PostgreSQL commit acknowledgement timed out",
            )));
        }
    }
    if probe.fault_post_commit() {
        return Err(TransactionFailure::PostCommit(test_operation_error(
            "injected PostgreSQL acknowledgement ambiguity",
            Some("08007"),
        )));
    }
    connection.mark_reusable();
    Ok(())
}

fn terminal_outcome() -> SinkTerminalOutcome {
    SinkTerminalOutcome::success(None).with_items(1)
}

fn committed_receipts<T>(pending: &mut Vec<BufferedRow<T>>) -> Vec<SinkCommitReceipt> {
    pending
        .drain(..)
        .map(|row| SinkCommitReceipt::new(row.pending, terminal_outcome()))
        .collect()
}

fn map_write_failure(failure: TransactionFailure) -> SinkWriteFailure {
    match failure {
        TransactionFailure::Acquire(error) => {
            SinkWriteFailure::current_only(SinkWritePhase::Acquire, error)
        }
        TransactionFailure::Execute {
            operation,
            rollback: None,
        } => SinkWriteFailure::confirmed_rollback(SinkWritePhase::Execute, operation),
        TransactionFailure::Execute {
            rollback: Some(rollback),
            ..
        } => SinkWriteFailure::poisoned(SinkWritePhase::Commit, rollback),
        TransactionFailure::PreCommit {
            operation,
            rollback: None,
        } => SinkWriteFailure::confirmed_rollback(SinkWritePhase::Commit, operation),
        TransactionFailure::PreCommit {
            rollback: Some(rollback),
            ..
        } => SinkWriteFailure::poisoned(SinkWritePhase::Commit, rollback),
        TransactionFailure::Commit(error) => {
            SinkWriteFailure::poisoned(SinkWritePhase::Commit, error)
        }
        TransactionFailure::PostCommit(error) => {
            SinkWriteFailure::poisoned(SinkWritePhase::Commit, error)
        }
    }
}

fn map_lifecycle_failure(failure: TransactionFailure) -> SinkOperationError {
    match failure {
        TransactionFailure::Acquire(error)
        | TransactionFailure::Commit(error)
        | TransactionFailure::PostCommit(error) => error,
        TransactionFailure::Execute {
            operation,
            rollback: None,
        } => operation,
        TransactionFailure::Execute {
            rollback: Some(rollback),
            ..
        } => rollback,
        TransactionFailure::PreCommit {
            operation,
            rollback: None,
        } => operation,
        TransactionFailure::PreCommit {
            rollback: Some(rollback),
            ..
        } => rollback,
    }
}

#[async_trait]
impl<T, B> SinkWriter for PostgresWriter<T, B>
where
    T: TypedPayload + Send + Sync + 'static,
    B: PostgresBind<T>,
{
    type Input = T;

    async fn write(&mut self, input: T, context: SinkWriteContext) -> SinkWriteResult {
        self.probe.record_write();
        if self.probe.fault_encode() {
            return Err(SinkWriteFailure::current_only(
                SinkWritePhase::Encode,
                destination_operation_error(
                    &self.destination,
                    test_operation_error("injected PostgreSQL encode failure", None),
                ),
            ));
        }
        self.binder
            .validate(&input)
            .map_err(|error| destination_operation_error(&self.destination, error))
            .map_err(|error| SinkWriteFailure::current_only(SinkWritePhase::Encode, error))?;

        if self.batch_size == 1 {
            execute_transaction(self, Some(&input))
                .await
                .map_err(|failure| failure.with_destination(&self.destination))
                .map_err(map_write_failure)?;
            return Ok(SinkWriteReport::terminal(terminal_outcome()));
        }

        if self.batch_size > 1 && self.pending.len() + 1 < self.batch_size {
            if self.probe.fault_before_deferral() {
                return Err(SinkWriteFailure::current_only(
                    SinkWritePhase::Execute,
                    destination_operation_error(
                        &self.destination,
                        test_operation_error("injected PostgreSQL pre-deferral failure", None),
                    ),
                ));
            }
            let pending = context.defer();
            if self.probe.fault_after_deferral() {
                return Err(SinkWriteFailure::current_only(
                    SinkWritePhase::Execute,
                    destination_operation_error(
                        &self.destination,
                        test_operation_error("injected PostgreSQL post-deferral failure", None),
                    ),
                ));
            }
            self.pending.push(BufferedRow { input, pending });
            return Ok(SinkWriteReport::buffered(
                obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
            ));
        }

        let current_pending = context.defer();
        execute_transaction(self, Some(&input))
            .await
            .map_err(|failure| failure.with_destination(&self.destination))
            .map_err(map_write_failure)?;

        let mut receipts = committed_receipts(&mut self.pending);
        receipts.push(SinkCommitReceipt::new(current_pending, terminal_outcome()));
        Ok(SinkWriteReport::buffered(
            obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
        )
        .with_commit_receipts(receipts))
    }

    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.probe.record_flush();
        if self.probe.fault_flush() {
            return Err(destination_operation_error(
                &self.destination,
                test_operation_error("injected PostgreSQL flush failure", None),
            ));
        }
        self.settle_pending().await
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.probe.record_drain();
        if self.probe.fault_drain() {
            return Err(destination_operation_error(
                &self.destination,
                test_operation_error("injected PostgreSQL drain failure", None),
            ));
        }
        self.settle_pending().await
    }
}

impl<T, B> PostgresWriter<T, B>
where
    T: TypedPayload + Send + Sync + 'static,
    B: PostgresBind<T>,
{
    async fn settle_pending(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        if self.pending.is_empty() {
            return Ok(SinkWriterLifecycleReport::default());
        }
        execute_transaction(self, None)
            .await
            .map_err(|failure| failure.with_destination(&self.destination))
            .map_err(map_lifecycle_failure)?;
        Ok(SinkWriterLifecycleReport::default()
            .with_commit_receipts(committed_receipts(&mut self.pending)))
    }
}

fn operation_error(error: sqlx::Error) -> SinkOperationError {
    let destination_code = match &error {
        sqlx::Error::Database(database) => database
            .code()
            .and_then(|code| sqlstate_code(code.as_ref())),
        _ => None,
    };
    let operation = match error {
        sqlx::Error::PoolTimedOut => {
            SinkOperationError::timeout("PostgreSQL pool acquisition timed out")
        }
        sqlx::Error::Io(_) | sqlx::Error::Tls(_) | sqlx::Error::PoolClosed => {
            SinkOperationError::remote("PostgreSQL transport operation failed")
        }
        sqlx::Error::Database(_) => SinkOperationError::remote("PostgreSQL rejected the operation"),
        sqlx::Error::Configuration(_) => {
            SinkOperationError::permanent("PostgreSQL connection configuration was rejected")
        }
        sqlx::Error::Protocol(_) => {
            SinkOperationError::remote("PostgreSQL protocol operation failed")
        }
        sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_) => {
            SinkOperationError::deserialization("PostgreSQL value encoding or decoding failed")
        }
        _ => SinkOperationError::other("PostgreSQL operation failed"),
    };
    match destination_code {
        Some(code) => operation.with_destination_error_code(code),
        None => operation,
    }
}

fn destination_operation_error(
    destination: &PostgresTable,
    error: SinkOperationError,
) -> SinkOperationError {
    use obzenflow_core::event::status::processing_status::ErrorKind;

    let code = error.destination_error_code().cloned();
    let detail = format!(
        "PostgreSQL destination {}: {}",
        destination.logical_destination(),
        error.detail()
    );
    let contextual = match error.kind() {
        ErrorKind::Timeout => SinkOperationError::timeout(detail),
        ErrorKind::Remote => SinkOperationError::remote(detail),
        ErrorKind::RateLimited => SinkOperationError::rate_limited(detail, error.retry_after()),
        ErrorKind::PermanentFailure => SinkOperationError::permanent(detail),
        ErrorKind::Deserialization => SinkOperationError::deserialization(detail),
        ErrorKind::Validation => SinkOperationError::validation(detail),
        ErrorKind::Domain => SinkOperationError::domain(detail),
        ErrorKind::Unknown => SinkOperationError::other(detail),
    };
    match code {
        Some(code) => contextual.with_destination_error_code(code),
        None => contextual,
    }
}

fn test_operation_error(detail: &'static str, sqlstate: Option<&str>) -> SinkOperationError {
    let error = SinkOperationError::remote(detail);
    match sqlstate.and_then(sqlstate_code) {
        Some(code) => error.with_destination_error_code(code),
        None => error,
    }
}

fn sqlstate_code(value: &str) -> Option<SinkDestinationErrorCode> {
    SinkDestinationErrorCode::try_new(POSTGRES_SQLSTATE_NAMESPACE, value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Input {
        value: String,
    }

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "postgres.connector.test.input";
    }

    #[derive(Clone)]
    struct Binder;

    impl PostgresBind<Input> for Binder {
        fn bind<'q>(&self, query: PostgresQuery<'q>, input: &'q Input) -> PostgresQuery<'q> {
            query.bind(&input.value)
        }
    }

    fn connection() -> PostgresConnection {
        PostgresConnection::from_url(
            "postgres://sentinel-user:sentinel-password@localhost/test?sslmode=disable",
            PostgresTransport::ExternallyProtectedPlaintext,
        )
        .expect("test URL parses without I/O")
    }

    #[test]
    fn connector_input_witness_is_the_builder_payload() {
        fn assert_input<C: SinkConnector<Input = Input>>() {}

        assert_input::<PostgresSink<Input, Binder>>();
    }

    #[test]
    fn typed_transport_is_authoritative_over_urls_and_options() {
        let base = "postgres://sentinel-user:sentinel-password@localhost/test";

        let tls = PostgresConnection::from_url(base, PostgresTransport::VerifiedTls)
            .expect("omitted URL mode is normalised by typed policy");
        assert!(matches!(tls.options.get_ssl_mode(), PgSslMode::VerifyFull));
        let plaintext =
            PostgresConnection::from_url(base, PostgresTransport::ExternallyProtectedPlaintext)
                .expect("omitted URL mode is normalised by typed policy");
        assert!(matches!(
            plaintext.options.get_ssl_mode(),
            PgSslMode::Disable
        ));

        PostgresConnection::from_url(
            &format!("{base}?sslmode=disable"),
            PostgresTransport::ExternallyProtectedPlaintext,
        )
        .expect("matching explicit plaintext is accepted");
        PostgresConnection::from_url(
            &format!("{base}?sslmode=verify-full"),
            PostgresTransport::VerifiedTls,
        )
        .expect("matching hostname-verified TLS is accepted");
        for mode in ["allow", "prefer", "require", "verify-ca", "disable"] {
            assert!(matches!(
                PostgresConnection::from_url(
                    &format!("{base}?sslmode={mode}"),
                    PostgresTransport::VerifiedTls,
                ),
                Err(PostgresConfigError::ConflictingTransport)
            ));
        }

        let options = PostgresConnection::from_options(
            PgConnectOptions::new().ssl_mode(PgSslMode::Prefer),
            PostgresTransport::VerifiedTls,
        )
        .expect("typed policy overwrites provenance-unknown options");
        assert!(matches!(
            options.options.get_ssl_mode(),
            PgSslMode::VerifyFull
        ));
        let options = PostgresConnection::from_options(
            PgConnectOptions::new().ssl_mode(PgSslMode::VerifyFull),
            PostgresTransport::ExternallyProtectedPlaintext,
        )
        .expect("typed plaintext policy overwrites provenance-unknown options");
        assert!(matches!(options.options.get_ssl_mode(), PgSslMode::Disable));

        for incoming in [
            PgSslMode::Disable,
            PgSslMode::Allow,
            PgSslMode::Prefer,
            PgSslMode::Require,
            PgSslMode::VerifyCa,
            PgSslMode::VerifyFull,
        ] {
            let tls = PostgresConnection::from_options(
                PgConnectOptions::new().ssl_mode(incoming),
                PostgresTransport::VerifiedTls,
            )
            .expect("typed TLS policy normalises every incoming SQLx mode");
            assert!(matches!(tls.options.get_ssl_mode(), PgSslMode::VerifyFull));
            let plaintext = PostgresConnection::from_options(
                PgConnectOptions::new().ssl_mode(incoming),
                PostgresTransport::ExternallyProtectedPlaintext,
            )
            .expect("typed plaintext policy normalises every incoming SQLx mode");
            assert!(matches!(
                plaintext.options.get_ssl_mode(),
                PgSslMode::Disable
            ));
        }

        for mode in ["allow", "prefer", "require", "verify-ca", "verify-full"] {
            assert!(matches!(
                PostgresConnection::from_url(
                    &format!("{base}?sslmode={mode}"),
                    PostgresTransport::ExternallyProtectedPlaintext,
                ),
                Err(PostgresConfigError::ConflictingTransport)
            ));
        }

        assert!(matches!(
            PostgresConnection::from_url(
                &format!("{base}?unknown=sentinel-secret"),
                PostgresTransport::VerifiedTls,
            ),
            Err(PostgresConfigError::InvalidConnection)
        ));
    }

    #[test]
    fn typed_transport_ignores_ambient_pgsslmode() {
        const CHILD: &str = "OBZENFLOW_POSTGRES_PGSSLMODE_CHILD";
        if std::env::var_os(CHILD).is_some() {
            let base = "postgres://sentinel-user:sentinel-password@localhost/test";
            let tls = PostgresConnection::from_url(base, PostgresTransport::VerifiedTls)
                .expect("typed TLS policy overrides ambient PGSSLMODE");
            assert!(matches!(tls.options.get_ssl_mode(), PgSslMode::VerifyFull));
            let plaintext =
                PostgresConnection::from_url(base, PostgresTransport::ExternallyProtectedPlaintext)
                    .expect("typed plaintext policy overrides ambient PGSSLMODE");
            assert!(matches!(
                plaintext.options.get_ssl_mode(),
                PgSslMode::Disable
            ));
            return;
        }

        let test_name = "sinks::postgres::tests::typed_transport_ignores_ambient_pgsslmode";
        for mode in [None, Some("disable"), Some("verify-full"), Some("prefer")] {
            let mut command = std::process::Command::new(
                std::env::current_exe().expect("locate adapter unit-test executable"),
            );
            command
                .args(["--exact", test_name, "--nocapture"])
                .env(CHILD, "1");
            match mode {
                Some(mode) => {
                    command.env("PGSSLMODE", mode);
                }
                None => {
                    command.env_remove("PGSSLMODE");
                }
            }
            let output = command.output().expect("launch isolated PGSSLMODE proof");
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success() && stdout.contains("1 passed"),
                "isolated PGSSLMODE proof did not execute exactly one passing test: stdout={stdout} stderr={stderr}"
            );
            assert!(!stdout.contains("sentinel-password"));
            assert!(!stderr.contains("sentinel-password"));
        }
    }

    #[test]
    fn build_is_local_and_debug_is_redacted() {
        let sink = PostgresSink::<Input>::builder()
            .connection(connection())
            .insert_into("public", "items", "(value) VALUES ($1)")
            .unwrap()
            .bind_with(Binder)
            .batch_size(16)
            .unwrap()
            .redelivery_safety(SinkRedeliverySafety::DuplicateSensitive)
            .build()
            .unwrap();
        let formatted = format!("{sink:?}");
        assert!(!formatted.contains("sentinel-user"));
        assert!(!formatted.contains("sentinel-password"));
        assert!(!formatted.contains("INSERT"));
        assert_eq!(
            sink.describe().destination_name(),
            Some("postgres.public.items")
        );
        assert_eq!(
            sink.describe().redelivery_safety(),
            Some(SinkRedeliverySafety::DuplicateSensitive)
        );
        assert_eq!(
            sink.describe().input_order(),
            SinkInputOrder::OrderSensitive
        );
    }

    #[test]
    fn configuration_rejects_unsafe_shapes() {
        assert!(PostgresSink::<Input>::builder()
            .insert_into("public;drop", "items", "(value) VALUES ($1)")
            .is_err());
        assert!(PostgresSink::<Input>::builder()
            .insert_into("public", "items", "")
            .is_err());
        assert!(PostgresSink::<Input>::builder()
            .insert_into("public", "items", "(value) VALUES ('bad\0value')")
            .is_err());
        assert!(PostgresSink::<Input>::builder().batch_size(0).is_err());
        assert!(matches!(
            PostgresSink::<Input>::builder()
                .connection(connection().with_operation_timeout(Duration::ZERO))
                .insert_into("public", "items", "(value) VALUES ($1)")
                .unwrap()
                .bind_with(Binder)
                .build(),
            Err(PostgresConfigError::InvalidTimeout)
        ));
    }

    #[test]
    fn target_generation_is_exact_and_does_not_parse_the_body() {
        let sixty_three = format!("a{}", "b".repeat(62));
        assert!(valid_identifier(&sixty_three));
        assert!(!valid_identifier(&format!("a{}", "b".repeat(63))));
        assert_eq!(quote_identifier("a\"b"), "\"a\"\"b\"");

        let sink = PostgresSink::<Input>::builder()
            .connection(connection())
            .insert_into(
                "Public",
                "Items",
                "(value) VALUES ('a;b') ON CONFLICT DO NOTHING",
            )
            .unwrap()
            .bind_with(Binder)
            .build()
            .unwrap();
        assert_eq!(
            sink.statement,
            "INSERT INTO \"Public\".\"Items\" (value) VALUES ('a;b') ON CONFLICT DO NOTHING"
        );
        assert_eq!(
            sink._insert_body,
            "(value) VALUES ('a;b') ON CONFLICT DO NOTHING"
        );
        assert_eq!(
            sink.describe().destination_name(),
            Some("postgres.Public.Items")
        );
        assert_eq!(sink.describe().redelivery_safety(), None);

        let table = PostgresTable::try_new("public", "portable_name").unwrap();
        let error = table
            .validate_server_limit(8)
            .expect_err("a smaller custom server limit rejects before preparation");
        assert_eq!(
            error.kind(),
            obzenflow_core::event::status::processing_status::ErrorKind::PermanentFailure
        );
    }

    #[test]
    fn sqlstate_codes_use_the_typed_bounded_carrier() {
        assert_eq!(POSTGRES_SQLSTATE_NAMESPACE, "postgresql.sqlstate");
        for value in ["23505", "08007"] {
            let code = sqlstate_code(value).expect("SQLSTATE is valid");
            assert_eq!(code.namespace(), POSTGRES_SQLSTATE_NAMESPACE);
            assert_eq!(code.value(), value);
        }
        assert!(sqlstate_code("unsafe code with spaces").is_none());
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn real_driver_sqlstates_and_transport_absence_map_without_text_parsing() {
        let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL").expect(
            "OBZENFLOW_POSTGRES_TEST_URL is required: the real-driver SQLSTATE proof must not pass without PostgreSQL",
        );
        let pool = PgPool::connect(&url).await.expect("PostgreSQL test pool");
        let run_id = std::env::var("OBZENFLOW_POSTGRES_TEST_RUN_ID")
            .expect("OBZENFLOW_POSTGRES_TEST_RUN_ID is required from `cargo xtask postgres test`");
        assert!(run_id.len() == 32 && run_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        let schema = format!("obz083c_codes_{run_id}");
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&pool)
            .await
            .expect("old code-test schema drops");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&pool)
            .await
            .expect("code-test schema creates");
        sqlx::query(&format!(
            "CREATE TABLE {schema}.unique_values (id BIGINT PRIMARY KEY)"
        ))
        .execute(&pool)
        .await
        .expect("code-test table creates");
        sqlx::query(&format!(
            "INSERT INTO {schema}.unique_values (id) VALUES (1)"
        ))
        .execute(&pool)
        .await
        .expect("first unique row commits");

        let duplicate = sqlx::query(&format!(
            "INSERT INTO {schema}.unique_values (id) VALUES (1)"
        ))
        .execute(&pool)
        .await
        .expect_err("duplicate key returns a database error");
        let duplicate = operation_error(duplicate);
        assert_eq!(
            duplicate.destination_error_code().map(|code| code.value()),
            Some("23505")
        );

        let ambiguity = sqlx::query(
            "DO $$ BEGIN RAISE EXCEPTION USING ERRCODE = '08007', MESSAGE = 'redacted'; END $$",
        )
        .execute(&pool)
        .await
        .expect_err("server emits requested transaction-resolution SQLSTATE");
        let ambiguity = operation_error(ambiguity);
        assert_eq!(
            ambiguity.destination_error_code().map(|code| code.value()),
            Some("08007")
        );

        let transport = operation_error(sqlx::Error::PoolClosed);
        assert!(transport.destination_error_code().is_none());
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(&pool)
            .await
            .expect("code-test schema drops");
    }

    #[cfg(feature = "test-support")]
    #[tokio::test]
    async fn real_writers_own_distinct_one_slot_pools() {
        let url = std::env::var("OBZENFLOW_POSTGRES_TEST_URL").expect(
            "OBZENFLOW_POSTGRES_TEST_URL is required: pool isolation must use real PostgreSQL",
        );
        let run_id = std::env::var("OBZENFLOW_POSTGRES_TEST_RUN_ID")
            .expect("OBZENFLOW_POSTGRES_TEST_RUN_ID is required from `cargo xtask postgres test`");
        assert!(run_id.len() == 32 && run_id.bytes().all(|byte| byte.is_ascii_hexdigit()));
        let schema = format!("obz083c_pools_{run_id}");
        let setup = PgPool::connect(&url).await.expect("PostgreSQL setup pool");
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&setup)
            .await
            .expect("pool-test schema creates");
        sqlx::query(&format!("CREATE TABLE {schema}.values_table (value TEXT)"))
            .execute(&setup)
            .await
            .expect("pool-test table creates");

        let sink = PostgresSink::<Input>::builder()
            .connection(
                PostgresConnection::from_url(&url, PostgresTransport::ExternallyProtectedPlaintext)
                    .expect("test URL parses"),
            )
            .insert_into(&schema, "values_table", "(value) VALUES ($1)")
            .unwrap()
            .bind_with(Binder)
            .build()
            .unwrap();
        let writer_a = sink
            .open(SinkWriterInitContext::new(
                obzenflow_core::StageId::new(),
                "writer-a".to_string(),
                "pool-isolation".to_string(),
            ))
            .await
            .expect("first writer opens");
        let writer_b = sink
            .open(SinkWriterInitContext::new(
                obzenflow_core::StageId::new(),
                "writer-b".to_string(),
                "pool-isolation".to_string(),
            ))
            .await
            .expect("second writer opens");

        let mut lease_a = writer_a.pool.acquire().await.expect("writer A lease");
        let pid_a: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *lease_a)
            .await
            .expect("writer A backend id");
        assert!(
            timeout(Duration::from_millis(30), writer_a.pool.acquire())
                .await
                .is_err(),
            "one held lease must exhaust only writer A's one-slot pool"
        );
        let mut lease_b = timeout(Duration::from_secs(1), writer_b.pool.acquire())
            .await
            .expect("writer B is not blocked by writer A")
            .expect("writer B lease");
        let pid_b: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *lease_b)
            .await
            .expect("writer B backend id");
        assert_ne!(pid_a, pid_b, "writers must not share a physical session");
        drop(lease_a);
        drop(lease_b);
        drop(writer_a);
        drop(writer_b);
        sqlx::query(&format!("DROP SCHEMA {schema} CASCADE"))
            .execute(&setup)
            .await
            .expect("pool-test schema drops");
    }
}
