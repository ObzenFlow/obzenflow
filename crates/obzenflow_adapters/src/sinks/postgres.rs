// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A production PostgreSQL sink built on ObzenFlow's typed connector contract.
//!
//! Configuration and statement validation are synchronous and perform no I/O.
//! Each call to [`SinkConnector::open`](obzenflow_runtime::stages::sink::SinkConnector::open)
//! creates an isolated writer, verifies its initial session, and installs a
//! fail-closed authority check for every replacement session. Values enter the
//! fixed INSERT statement only through [`PostgresBind`].

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
use sqlx::__query_with_result as query_with_result;
use sqlx::error::BoxDynError;
use sqlx::pool::PoolConnection;
use sqlx::postgres::{PgArguments, PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::query::Query;
use sqlx::{Arguments as _, ConnectOptions as _, Encode, Executor as _, PgPool, Postgres, Type};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use thiserror::Error;
use tokio::time::{timeout, timeout_at, Instant};
use url::Url;

const DEFAULT_BATCH_SIZE: usize = 1;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_ROLLBACK_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_SIZE: usize = 100_000;
const PORTABLE_IDENTIFIER_LIMIT: usize = 63;
const POSTGRES_WRITER_POOL_SIZE: u32 = 1;
const POSTGRES_SQLSTATE_NAMESPACE: &str = "postgresql.sqlstate";

/// A locally detected PostgreSQL connector configuration error.
///
/// These errors are produced without opening a socket or consulting a
/// PostgreSQL server. Server and transport failures surface later through the
/// sink operation protocol when a writer is opened.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PostgresConfigError {
    /// No connection configuration was supplied to the builder.
    #[error("PostgreSQL connection configuration is missing")]
    MissingConnection,
    /// The named environment variable was absent or was not valid Unicode.
    #[error("PostgreSQL environment variable '{0}' is missing or is not valid Unicode")]
    MissingEnvironment(String),
    /// The connection URL or parsed driver options were invalid.
    #[error("PostgreSQL connection options are invalid")]
    InvalidConnection,
    /// URL options contradicted the application-selected transport policy.
    #[error("PostgreSQL URL transport mode conflicts with the explicit transport policy")]
    ConflictingTransport,
    /// A schema or table identifier was empty, malformed, or too long.
    #[error("PostgreSQL schema or table identifier is invalid")]
    InvalidIdentifier,
    /// No SQL body following the generated `INSERT INTO` target was supplied.
    #[error("PostgreSQL INSERT body is missing")]
    MissingInsertBody,
    /// The supplied INSERT body failed local safety validation.
    #[error("PostgreSQL INSERT body is invalid")]
    InvalidInsertBody,
    /// The configured batch size was outside the supported finite range.
    #[error("PostgreSQL batch size must be in 1..={MAX_BATCH_SIZE}")]
    InvalidBatchSize,
    /// No typed parameter binder was supplied.
    #[error("PostgreSQL parameter binder is missing")]
    MissingBinder,
    /// At least one configured timeout was zero.
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
    /// Read a PostgreSQL URL from `name` without opening a connection.
    ///
    /// The explicit `transport` policy remains authoritative over URL options
    /// and ambient PostgreSQL environment variables.
    pub fn from_env(
        name: impl AsRef<str>,
        transport: PostgresTransport,
    ) -> Result<Self, PostgresConfigError> {
        let name = name.as_ref();
        let value = std::env::var(name)
            .map_err(|_| PostgresConfigError::MissingEnvironment(name.to_string()))?;
        Self::from_url(&value, transport)
    }

    /// Parse a PostgreSQL URL without DNS, authentication, or socket I/O.
    ///
    /// Unsupported URL options and transport-policy conflicts fail locally.
    pub fn from_url(url: &str, transport: PostgresTransport) -> Result<Self, PostgresConfigError> {
        preflight_url(url, transport)?;
        let options =
            PgConnectOptions::from_str(url).map_err(|_| PostgresConfigError::InvalidConnection)?;
        Self::from_options(options, transport)
    }

    /// Retain parsed SQLx connection options under an explicit transport policy.
    ///
    /// Statement logging is disabled and the policy overwrites the driver SSL
    /// mode. No connection is opened until [`SinkConnector::open`].
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
struct PostgresTable {
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

    // Keep server-authority failures in the sink protocol's typed error.
    #[allow(clippy::result_large_err)]
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

/// An opaque accumulator for PostgreSQL parameter values.
///
/// Its fields, constructor, and finaliser are private so a binder can append
/// values without receiving the adapter-owned statement, query, transaction, or
/// raw SQLx arguments.
pub struct PostgresBindings {
    arguments: Result<PgArguments, BoxDynError>,
    parameter_count: usize,
}

impl PostgresBindings {
    fn new() -> Self {
        Self {
            arguments: Ok(PgArguments::default()),
            parameter_count: 0,
        }
    }

    /// Appends one value to the configured statement's PostgreSQL arguments.
    ///
    /// Encoding happens immediately. As with SQLx's query binder, the first
    /// encoding error is retained and surfaced when the writer executes the
    /// assembled query; later calls do not replace it.
    pub fn bind<'q, V>(&mut self, value: V) -> &mut Self
    where
        V: 'q + Encode<'q, Postgres> + Type<Postgres>,
    {
        let argument_number = self.parameter_count + 1;
        let result = match &mut self.arguments {
            Ok(arguments) => arguments.add(value),
            Err(_) => return self,
        };

        match result {
            Ok(()) => self.parameter_count = argument_number,
            Err(error) => {
                self.arguments =
                    Err(format!("Encoding argument ${argument_number} failed: {error}").into());
            }
        }
        self
    }

    fn finish(self) -> Result<PgArguments, BoxDynError> {
        self.arguments
    }
}

impl fmt::Debug for PostgresBindings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("PostgresBindings { .. }")
    }
}

/// A separate, clonable mapping from one domain input to PostgreSQL parameters.
///
/// `validate` performs optional input validation. `bind` receives only a
/// private-field value accumulator, so it cannot replace or execute the fixed
/// configuration statement through this API.
pub trait PostgresBind<T>: Clone + Send + Sync + 'static {
    /// Validate one input before any parameters are assembled or SQL executes.
    ///
    /// The default accepts every input. Returning an operational error fails
    /// the current delivery without granting retry authority.
    // Binder validation is part of the sink protocol and therefore returns
    // its by-value operational error rather than an adapter-local box.
    #[allow(clippy::result_large_err)]
    fn validate(&self, _input: &T) -> SinkOperationResult<()> {
        Ok(())
    }

    /// Append this input's parameter values in statement order.
    fn bind(&self, bindings: &mut PostgresBindings, input: &T);
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
    /// Begin an I/O-free builder for inputs of type `T`.
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

/// An I/O-free builder for a fixed, typed PostgreSQL INSERT or UPSERT sink.
///
/// The builder validates only local configuration. PostgreSQL owns statement
/// preparation and destination acceptance when the connector opens a writer.
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
    /// Set the cold, redacted connection configuration.
    pub fn connection(mut self, connection: PostgresConnection) -> Self {
        self.connection = Some(connection);
        self
    }

    /// Configure the sole generated INSERT target and its post-target SQL body.
    ///
    /// `schema` and `table` are validated and quoted by the adapter. `body`
    /// begins immediately after `INSERT INTO "schema"."table"`; callers
    /// cannot supply a second primary target through this method.
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

    /// Set the transaction threshold for committed delivery receipts.
    ///
    /// A value of one commits each input immediately. Larger values defer
    /// settlement until the threshold or a lifecycle flush is reached.
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

    /// Install the typed value-only parameter binder.
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
    /// Finish local validation and return immutable connector configuration.
    ///
    /// This method performs no DNS, authentication, schema, preparation, or
    /// destination I/O.
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
#[doc(hidden)]
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
        Authority,
        Prepare,
        Begin,
        Execute,
        Rollback,
        CommitAcknowledgement,
    }

    /// Test-only observation of physical-session authority checks.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct PostgresAuthoritySnapshot {
        hook_invocations: u64,
        sessions: Vec<(u64, i32)>,
        rejections: u64,
        preparations: Vec<u64>,
    }

    impl PostgresAuthoritySnapshot {
        pub fn hook_invocations(&self) -> u64 {
            self.hook_invocations
        }

        pub fn sessions(&self) -> &[(u64, i32)] {
            &self.sessions
        }

        pub fn rejections(&self) -> u64 {
            self.rejections
        }

        pub fn preparations(&self) -> &[u64] {
            &self.preparations
        }
    }

    #[derive(Default)]
    struct ProbeState {
        armed: Option<SinkFault>,
        delay: Option<(PostgresDelayPoint, Duration)>,
        authority_limit: Option<i32>,
        authority_query_failure: bool,
        authority_hook_invocations: u64,
        authority_sessions: Vec<(u64, i32)>,
        authority_rejections: u64,
        preparations: Vec<u64>,
        next_writer: u64,
        next_sequence: u64,
        calls: Vec<SinkExternalCall>,
    }

    /// Test-only, connector-owned call and one-shot fault probe.
    #[derive(Clone, Default)]
    pub struct PostgresTestProbe {
        state: Arc<Mutex<ProbeState>>,
        changed: Arc<tokio::sync::Notify>,
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
            state.authority_limit = None;
            state.authority_query_failure = false;
            state.authority_hook_invocations = 0;
            state.authority_sessions.clear();
            state.authority_rejections = 0;
            state.preparations.clear();
            state.calls.clear();
            state.next_sequence = 0;
            drop(state);
            self.changed.notify_waiters();
        }

        pub fn snapshot(&self) -> SinkExternalCallSnapshot {
            SinkExternalCallSnapshot::new(self.state().calls.clone())
        }

        /// Wait until at least `minimum` calls of `kind` have been observed.
        ///
        /// This is a test-only scheduling seam. Callers should place their own
        /// diagnostic timeout around the wait rather than use elapsed time as
        /// the observation oracle.
        pub async fn wait_for_calls(&self, kind: SinkExternalCallKind, minimum: usize) {
            loop {
                let changed = self.changed.notified();
                if self.snapshot().count(kind) >= minimum {
                    return;
                }
                changed.await;
            }
        }

        pub fn delay_once(&self, point: PostgresDelayPoint, duration: Duration) {
            self.state().delay = Some((point, duration));
        }

        /// Override the server-reported identifier limit for the next newly
        /// created physical session.
        pub fn authority_limit_once(&self, limit: i32) {
            self.state().authority_limit = Some(limit);
        }

        /// Replace the next successful authority query with a redacted
        /// protocol failure after the physical backend has been identified.
        pub fn fail_authority_query_once(&self) {
            self.state().authority_query_failure = true;
        }

        pub fn authority_snapshot(&self) -> PostgresAuthoritySnapshot {
            let state = self.state();
            PostgresAuthoritySnapshot {
                hook_invocations: state.authority_hook_invocations,
                sessions: state.authority_sessions.clone(),
                rejections: state.authority_rejections,
                preparations: state.preparations.clone(),
            }
        }

        pub(crate) fn new_writer(&self) -> u64 {
            let mut state = self.state();
            let writer = state.next_writer;
            state.next_writer += 1;
            writer
        }

        pub(crate) fn record(&self, writer: u64, kind: SinkExternalCallKind) {
            {
                let mut state = self.state();
                let sequence = state.next_sequence;
                state.next_sequence += 1;
                state
                    .calls
                    .push(SinkExternalCall::new(writer, sequence, kind));
            }
            self.changed.notify_one();
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

        pub(crate) fn record_authority_hook(&self) {
            self.state().authority_hook_invocations += 1;
        }

        pub(crate) fn record_authority_session(&self, writer: u64, backend_pid: i32) {
            self.state().authority_sessions.push((writer, backend_pid));
        }

        pub(crate) fn record_authority_rejection(&self) {
            self.state().authority_rejections += 1;
        }

        pub(crate) fn record_preparation(&self, writer: u64) {
            self.state().preparations.push(writer);
        }

        pub(crate) fn take_authority_limit(&self) -> Option<i32> {
            self.state().authority_limit.take()
        }

        pub(crate) fn take_authority_query_failure(&self) -> bool {
            let mut state = self.state();
            std::mem::take(&mut state.authority_query_failure)
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

    fn record_authority_hook(&self) {
        #[cfg(feature = "test-support")]
        if let Some(probe) = &self.probe {
            probe.record_authority_hook();
        }
    }

    fn record_authority_session(&self, backend_pid: i32) {
        #[cfg(feature = "test-support")]
        if let Some(probe) = &self.probe {
            probe.record_authority_session(self.writer, backend_pid);
        }
        #[cfg(not(feature = "test-support"))]
        let _ = backend_pid;
    }

    fn record_authority_rejection(&self) {
        #[cfg(feature = "test-support")]
        if let Some(probe) = &self.probe {
            probe.record_authority_rejection();
        }
    }

    fn record_preparation(&self) {
        #[cfg(feature = "test-support")]
        if let Some(probe) = &self.probe {
            probe.record_preparation(self.writer);
        }
    }

    fn authority_limit(&self, observed: i32) -> i32 {
        #[cfg(feature = "test-support")]
        {
            self.probe
                .as_ref()
                .and_then(testing::PostgresTestProbe::take_authority_limit)
                .unwrap_or(observed)
        }
        #[cfg(not(feature = "test-support"))]
        {
            observed
        }
    }

    fn fault_authority_query(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.probe.as_ref().is_some_and(|probe| {
                testing::PostgresTestProbe::take_authority_query_failure(probe)
            })
        }
        #[cfg(not(feature = "test-support"))]
        {
            false
        }
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

#[derive(Clone)]
struct PostgresSessionAuthority {
    verdict: Arc<Mutex<PostgresSessionAuthorityVerdict>>,
}

#[derive(Clone)]
enum PostgresSessionAuthorityVerdict {
    Unverified,
    Ready,
    Rejected(SinkOperationError),
}

impl PostgresSessionAuthority {
    fn new() -> Self {
        Self {
            verdict: Arc::new(Mutex::new(PostgresSessionAuthorityVerdict::Unverified)),
        }
    }

    fn verdict(&self) -> MutexGuard<'_, PostgresSessionAuthorityVerdict> {
        match self.verdict.lock() {
            Ok(verdict) => verdict,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn mark_unverified(&self) {
        *self.verdict() = PostgresSessionAuthorityVerdict::Unverified;
    }

    fn mark_ready(&self) {
        *self.verdict() = PostgresSessionAuthorityVerdict::Ready;
    }

    fn mark_rejected(&self, error: SinkOperationError) {
        *self.verdict() = PostgresSessionAuthorityVerdict::Rejected(error);
    }

    fn snapshot(&self) -> PostgresSessionAuthorityVerdict {
        self.verdict().clone()
    }
}

/// The only production path to a writer's physical PostgreSQL connection.
///
/// The shared verdict is sound only because this private pool is permanently
/// capped at one connection and the writer is serially borrowed. Every new
/// physical connection finalises the verdict before SQLx can expose it.
struct PostgresSessionPool {
    pool: PgPool,
    authority: PostgresSessionAuthority,
}

impl PostgresSessionPool {
    fn new(
        options: PgConnectOptions,
        acquire_timeout: Duration,
        destination: PostgresTable,
        probe: WriterProbe,
    ) -> Self {
        let authority = PostgresSessionAuthority::new();
        let hook_authority = authority.clone();
        let pool = PgPoolOptions::new()
            .max_connections(POSTGRES_WRITER_POOL_SIZE)
            .min_connections(0)
            .acquire_timeout(acquire_timeout)
            .after_connect(move |connection, _metadata| {
                let destination = destination.clone();
                let authority = hook_authority.clone();
                let probe = probe.clone();
                Box::pin(async move {
                    authority.mark_unverified();
                    probe.record_authority_hook();
                    let verdict = async {
                        let (max_identifier_length, backend_pid): (i32, i32) = sqlx::query_as(
                            "SELECT current_setting('max_identifier_length')::integer, \
                                 pg_backend_pid()",
                        )
                        .fetch_one(&mut *connection)
                        .await
                        .map_err(operation_error)?;
                        probe.record_authority_session(backend_pid);
                        #[cfg(feature = "test-support")]
                        probe.delay(testing::PostgresDelayPoint::Authority).await;
                        if probe.fault_authority_query() {
                            return Err(operation_error(sqlx::Error::Protocol(
                                "injected PostgreSQL authority-query failure".into(),
                            )));
                        }
                        destination
                            .validate_server_limit(probe.authority_limit(max_identifier_length))
                    }
                    .await;

                    match verdict {
                        Ok(()) => authority.mark_ready(),
                        Err(error) => {
                            probe.record_authority_rejection();
                            authority.mark_rejected(error);
                        }
                    }

                    // SQLx retries and logs after_connect errors. The wrapper
                    // must observe the exact rejection instead, so the hook
                    // deliberately exposes the connection for fail-closed
                    // inspection and never returns its adapter verdict here.
                    Ok(())
                })
            })
            // Pool construction remains inside open and the immediately
            // following authorized acquisition performs all external I/O.
            // With min_connections(0), no background connection can race the
            // single shared verdict.
            .connect_lazy_with(options);
        Self { pool, authority }
    }

    async fn acquire_authorized(&self) -> SinkOperationResult<PoolConnection<Postgres>> {
        let mut connection = self.pool.acquire().await.map_err(operation_error)?;
        match self.authority.snapshot() {
            PostgresSessionAuthorityVerdict::Ready => Ok(connection),
            PostgresSessionAuthorityVerdict::Rejected(error) => {
                connection.close_on_drop();
                Err(error)
            }
            PostgresSessionAuthorityVerdict::Unverified => {
                connection.close_on_drop();
                Err(SinkOperationError::other(
                    "PostgreSQL session target authority was not established",
                ))
            }
        }
    }
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
        let pool = PostgresSessionPool::new(
            self.connection.options.clone(),
            self.connection.acquire_timeout,
            self.destination.clone(),
            probe.clone(),
        );
        let mut connection = timeout_at(deadline, pool.acquire_authorized())
            .await
            .map_err(|_| {
                destination_operation_error(
                    &self.destination,
                    SinkOperationError::timeout("PostgreSQL open timed out"),
                )
            })?
            .map_err(|error| destination_operation_error(&self.destination, error))?;
        let preparation = timeout_at(deadline, async {
            #[cfg(feature = "test-support")]
            probe.delay(testing::PostgresDelayPoint::Prepare).await;
            probe.record_preparation();
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

/// A stage-local PostgreSQL writer created exclusively by [`PostgresSink`].
///
/// Each writer owns its one-slot session pool, transaction and batching state,
/// pending capabilities, deadlines, and failure lifecycle. Applications pass
/// the connector to `sink!`; they do not construct writers directly.
pub struct PostgresWriter<T, B> {
    pool: PostgresSessionPool,
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
        subject: TransactionInputSubject,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionInputSubject {
    Deferred(usize),
    Current,
}

impl TransactionFailure {
    fn with_destination(self, destination: &PostgresTable) -> Self {
        match self {
            Self::Acquire(error) => Self::Acquire(destination_operation_error(destination, error)),
            Self::Execute {
                subject,
                operation,
                rollback,
            } => Self::Execute {
                subject,
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

fn assemble_query<'q, T, B>(
    statement: &'q str,
    binder: &B,
    input: &'q T,
) -> Query<'q, Postgres, PgArguments>
where
    B: PostgresBind<T>,
{
    let mut bindings = PostgresBindings::new();
    binder.bind(&mut bindings, input);
    query_with_result(statement, bindings.finish())
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
    if probe.fault_acquire() {
        return Err(TransactionFailure::Acquire(test_operation_error(
            "injected PostgreSQL acquisition failure",
            None,
        )));
    }
    let connection = match timeout_at(deadline, pool.acquire_authorized()).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            return Err(TransactionFailure::Acquire(error));
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
        probe.record_begin();
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

    for (index, (subject, input)) in buffered
        .iter()
        .enumerate()
        .map(|(index, row)| (TransactionInputSubject::Deferred(index), &row.input))
        .chain(current.map(|input| (TransactionInputSubject::Current, input)))
        .enumerate()
    {
        if probe.fault_destination_execution() || (index > 0 && probe.fault_mid_batch_mutation()) {
            let operation =
                test_operation_error("injected PostgreSQL execution failure", Some("23505"));
            let rollback = rollback_transaction(&mut connection, rollback_timeout, probe)
                .await
                .err();
            return Err(TransactionFailure::Execute {
                subject,
                operation,
                rollback,
            });
        }
        probe.record_execute();
        let query = assemble_query(statement, binder, input);
        let execution = timeout_at(deadline, async {
            #[cfg(feature = "test-support")]
            probe.delay(testing::PostgresDelayPoint::Execute).await;
            query
                .execute(&mut *connection.connection)
                .await
                // PostgreSQL command-tag row counts describe physical row effects,
                // not ObzenFlow input settlement. A successful
                // `ON CONFLICT DO NOTHING` can report zero, so the connector
                // intentionally discards the count.
                .map(|_command_result| ())
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
            subject,
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

fn map_write_failure<T>(
    failure: TransactionFailure,
    pending: &[BufferedRow<T>],
) -> SinkWriteFailure {
    match failure {
        TransactionFailure::Acquire(error) => {
            SinkWriteFailure::current_only(SinkWritePhase::Acquire, error)
        }
        TransactionFailure::Execute {
            subject: TransactionInputSubject::Deferred(index),
            operation,
            rollback: None,
        } => match pending.get(index) {
            Some(row) => SinkWriteFailure::poisoned_by_deferred(
                &row.pending,
                SinkWritePhase::Execute,
                operation,
            ),
            None => SinkWriteFailure::poisoned(
                SinkWritePhase::Execute,
                SinkOperationError::other(
                    "PostgreSQL deferred operation subject index was invalid",
                ),
            ),
        },
        TransactionFailure::Execute {
            subject: TransactionInputSubject::Current,
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

fn map_lifecycle_failure<T>(
    failure: TransactionFailure,
    pending: &[BufferedRow<T>],
) -> SinkOperationError {
    match failure {
        TransactionFailure::Acquire(error)
        | TransactionFailure::Commit(error)
        | TransactionFailure::PostCommit(error) => error,
        TransactionFailure::Execute {
            subject: TransactionInputSubject::Deferred(index),
            operation,
            rollback: None,
        } => match pending.get(index) {
            Some(row) => operation.with_deferred_operation_subject(&row.pending),
            None => {
                SinkOperationError::other("PostgreSQL deferred operation subject index was invalid")
            }
        },
        TransactionFailure::Execute {
            subject: TransactionInputSubject::Current,
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

fn with_first_deferred_subject<T>(
    operation: SinkOperationError,
    pending: &[BufferedRow<T>],
) -> SinkOperationError {
    match pending.first() {
        Some(row) => operation.with_deferred_operation_subject(&row.pending),
        None => operation,
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
                .map_err(|failure| map_write_failure(failure, &self.pending))?;
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
            .map_err(|failure| map_write_failure(failure, &self.pending))?;

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
            let operation = destination_operation_error(
                &self.destination,
                test_operation_error("injected PostgreSQL flush failure", None),
            );
            return Err(with_first_deferred_subject(operation, &self.pending));
        }
        self.settle_pending().await
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.probe.record_drain();
        if self.probe.fault_drain() {
            let operation = destination_operation_error(
                &self.destination,
                test_operation_error("injected PostgreSQL drain failure", None),
            );
            return Err(with_first_deferred_subject(operation, &self.pending));
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
            .map_err(|failure| map_lifecycle_failure(failure, &self.pending))?;
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
        | sqlx::Error::Decode(_)
        | sqlx::Error::Encode(_) => {
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
    use sqlx::encode::IsNull;
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo};
    use sqlx::Execute as _;

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
        fn bind(&self, bindings: &mut PostgresBindings, input: &Input) {
            bindings.bind(&input.value);
        }
    }

    struct EncodingFailure(&'static str);

    impl Type<Postgres> for EncodingFailure {
        fn type_info() -> PgTypeInfo {
            <String as Type<Postgres>>::type_info()
        }
    }

    impl<'q> Encode<'q, Postgres> for EncodingFailure {
        fn encode_by_ref(&self, _buffer: &mut PgArgumentBuffer) -> Result<IsNull, BoxDynError> {
            Err(std::io::Error::other(self.0).into())
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
    fn private_assembler_retains_configured_statement_authority() {
        let input = Input {
            value: "binder-value-sentinel".to_string(),
        };
        let statement = "INSERT INTO \"public\".\"configured\" (value) VALUES ($1)";
        let query = assemble_query(statement, &Binder, &input);

        assert_eq!(query.sql(), statement);

        let mut bindings = PostgresBindings::new();
        bindings.bind(&input.value);
        assert_eq!(format!("{bindings:?}"), "PostgresBindings { .. }");
        assert!(!format!("{bindings:?}").contains("binder-value-sentinel"));
    }

    #[test]
    fn bindings_retain_only_the_first_encoding_error_for_query_execution() {
        let mut bindings = PostgresBindings::new();
        bindings
            .bind(EncodingFailure("first injected encoding failure"))
            .bind(EncodingFailure("second injected encoding failure"));
        let formatted = format!("{bindings:?}");
        assert_eq!(formatted, "PostgresBindings { .. }");
        assert!(!formatted.contains("injected encoding failure"));
        let mut query: Query<'_, Postgres, PgArguments> =
            query_with_result("SELECT $1::text", bindings.finish());

        assert_eq!(query.sql(), "SELECT $1::text");
        let error = query
            .take_arguments()
            .expect_err("encoding failure remains deferred on the query");
        let detail = error.to_string();
        assert!(detail.contains("Encoding argument $1 failed"));
        assert!(detail.contains("first injected encoding failure"));
        assert!(!detail.contains("second injected encoding failure"));

        let mapped = operation_error(sqlx::Error::Encode(error));
        assert_eq!(
            mapped.kind(),
            obzenflow_core::event::status::processing_status::ErrorKind::Deserialization
        );
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

        let mut lease_a = writer_a
            .pool
            .acquire_authorized()
            .await
            .expect("writer A lease");
        let pid_a: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *lease_a)
            .await
            .expect("writer A backend id");
        assert!(
            timeout(
                Duration::from_millis(30),
                writer_a.pool.acquire_authorized(),
            )
            .await
            .is_err(),
            "one held lease must exhaust only writer A's one-slot pool"
        );
        let mut lease_b = timeout(Duration::from_secs(1), writer_b.pool.acquire_authorized())
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
