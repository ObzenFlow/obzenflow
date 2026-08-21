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
use sqlx::postgres::{PgArguments, PgConnectOptions, PgPoolOptions, PgSslMode};
use sqlx::query::Query;
use sqlx::{ConnectOptions as _, PgPool, Postgres};
use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;
use std::time::Duration;
use thiserror::Error;

/// Adapter-owned PostgreSQL query carrier accepted by [`PostgresBind`].
///
/// Connector authors do not need a direct SQLx dependency merely to bind
/// domain values to the configured statement.
pub type PostgresQuery<'q> = Query<'q, Postgres, PgArguments>;

const DEFAULT_BATCH_SIZE: usize = 1;
const DEFAULT_ACQUIRE_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_BATCH_SIZE: usize = 100_000;
const POSTGRES_SQLSTATE_NAMESPACE: &str = "postgresql.sqlstate";

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PostgresConfigError {
    #[error("PostgreSQL connection configuration is missing")]
    MissingConnection,
    #[error("PostgreSQL environment variable '{0}' is missing or is not valid Unicode")]
    MissingEnvironment(String),
    #[error("PostgreSQL connection options are invalid")]
    InvalidConnection,
    #[error(
        "PostgreSQL sslmode must be explicitly set to 'verify-full' for authenticated TLS or 'disable' for a separately protected local transport"
    )]
    UnsafeSslMode,
    #[error("PostgreSQL schema or table identifier is invalid")]
    InvalidIdentifier,
    #[error("PostgreSQL INSERT statement is missing")]
    MissingStatement,
    #[error("PostgreSQL statement must be exactly one INSERT statement")]
    InvalidStatement,
    #[error("PostgreSQL INSERT target must match the configured schema and table")]
    StatementDestinationMismatch,
    #[error("PostgreSQL batch size must be in 1..={MAX_BATCH_SIZE}")]
    InvalidBatchSize,
    #[error("PostgreSQL parameter binder is missing")]
    MissingBinder,
}

/// Parsed connection options whose formatting never reveals the URL or password.
#[derive(Clone)]
pub struct PostgresConnection {
    options: PgConnectOptions,
    acquire_timeout: Duration,
}

impl PostgresConnection {
    pub fn from_env(name: impl AsRef<str>) -> Result<Self, PostgresConfigError> {
        let name = name.as_ref();
        let value = std::env::var(name)
            .map_err(|_| PostgresConfigError::MissingEnvironment(name.to_string()))?;
        Self::from_url(&value)
    }

    pub fn from_url(url: &str) -> Result<Self, PostgresConfigError> {
        let options =
            PgConnectOptions::from_str(url).map_err(|_| PostgresConfigError::InvalidConnection)?;
        Self::from_options(options)
    }

    pub fn from_options(options: PgConnectOptions) -> Result<Self, PostgresConfigError> {
        match options.get_ssl_mode() {
            PgSslMode::Disable | PgSslMode::VerifyFull => {}
            PgSslMode::Allow | PgSslMode::Prefer | PgSslMode::Require | PgSslMode::VerifyCa => {
                return Err(PostgresConfigError::UnsafeSslMode);
            }
        }
        Ok(Self {
            options: options.disable_statement_logging(),
            acquire_timeout: DEFAULT_ACQUIRE_TIMEOUT,
        })
    }

    /// Override how long a writer waits to acquire its sole connection.
    pub fn with_acquire_timeout(mut self, acquire_timeout: Duration) -> Self {
        self.acquire_timeout = acquire_timeout;
        self
    }
}

impl fmt::Debug for PostgresConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConnection")
            .field("configured", &true)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish()
    }
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
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$')
}

fn validate_statement(value: &str) -> Result<String, PostgresConfigError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(PostgresConfigError::MissingStatement);
    }
    let without_trailing = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    if without_trailing.contains(';')
        || without_trailing
            .split_ascii_whitespace()
            .next()
            .is_none_or(|keyword| !keyword.eq_ignore_ascii_case("insert"))
    {
        return Err(PostgresConfigError::InvalidStatement);
    }
    Ok(without_trailing.to_string())
}

fn statement_destination(value: &str) -> Option<&str> {
    let mut tokens = value.split_ascii_whitespace();
    let insert = tokens.next()?;
    let into = tokens.next()?;
    let destination = tokens.next()?;
    (insert.eq_ignore_ascii_case("insert") && into.eq_ignore_ascii_case("into"))
        .then_some(destination)
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
            statement: None,
            binder: None,
            batch_size: DEFAULT_BATCH_SIZE,
            redelivery_safety: Some(SinkRedeliverySafety::DuplicateSensitive),
            #[cfg(feature = "test-support")]
            test_probe: None,
            _input: PhantomData,
        }
    }
}

pub struct PostgresSinkBuilder<T, B> {
    connection: Option<PostgresConnection>,
    destination: Option<PostgresTable>,
    statement: Option<String>,
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
            .field("statement_configured", &self.statement.is_some())
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

    pub fn table(
        mut self,
        schema: impl Into<String>,
        table: impl Into<String>,
    ) -> Result<Self, PostgresConfigError> {
        self.destination = Some(PostgresTable::try_new(schema, table)?);
        Ok(self)
    }

    pub fn statement(mut self, statement: impl AsRef<str>) -> Result<Self, PostgresConfigError> {
        self.statement = Some(validate_statement(statement.as_ref())?);
        Ok(self)
    }

    pub fn batch_size(mut self, batch_size: usize) -> Result<Self, PostgresConfigError> {
        if !(1..=MAX_BATCH_SIZE).contains(&batch_size) {
            return Err(PostgresConfigError::InvalidBatchSize);
        }
        self.batch_size = batch_size;
        Ok(self)
    }

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
            statement: self.statement,
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
        let statement = self
            .statement
            .ok_or(PostgresConfigError::MissingStatement)?;
        if statement_destination(&statement)
            .is_none_or(|target| !target.eq_ignore_ascii_case(&destination.qualified()))
        {
            return Err(PostgresConfigError::StatementDestinationMismatch);
        }
        Ok(PostgresSink {
            connection: self
                .connection
                .ok_or(PostgresConfigError::MissingConnection)?,
            destination,
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

    /// Connector-owned namespace for projected PostgreSQL SQLSTATE diagnostics.
    pub const POSTGRES_SQLSTATE_NAMESPACE: &str = super::POSTGRES_SQLSTATE_NAMESPACE;

    #[derive(Default)]
    struct ProbeState {
        armed: Option<SinkFault>,
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
            state.calls.clear();
            state.next_sequence = 0;
        }

        pub fn snapshot(&self) -> SinkExternalCallSnapshot {
            SinkExternalCallSnapshot::new(self.state().calls.clone())
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
            return Err(test_operation_error(
                "injected PostgreSQL open failure",
                None,
            ));
        }
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(self.connection.acquire_timeout)
            .connect_with(self.connection.options.clone())
            .await
            .map_err(operation_error)?;
        pool.acquire().await.map_err(operation_error)?;
        Ok(PostgresWriter {
            pool,
            destination: self.destination.clone(),
            statement: self.statement.clone(),
            binder: self.binder.clone(),
            batch_size: self.batch_size,
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

async fn execute_transaction<T, B>(
    pool: &PgPool,
    statement: &str,
    binder: &B,
    buffered: &[BufferedRow<T>],
    current: Option<&T>,
    probe: &WriterProbe,
) -> Result<(), TransactionFailure>
where
    T: Send + Sync,
    B: PostgresBind<T>,
{
    probe.record_begin();
    if probe.fault_acquire() {
        return Err(TransactionFailure::Acquire(test_operation_error(
            "injected PostgreSQL acquisition failure",
            None,
        )));
    }
    let mut transaction = pool
        .begin()
        .await
        .map_err(|error| TransactionFailure::Acquire(operation_error(error)))?;

    for (index, input) in buffered
        .iter()
        .map(|row| &row.input)
        .chain(current)
        .enumerate()
    {
        if probe.fault_destination_execution() || (index > 0 && probe.fault_mid_batch_mutation()) {
            let operation =
                test_operation_error("injected PostgreSQL execution failure", Some("23505"));
            probe.record_rollback();
            let rollback = transaction.rollback().await.err().map(operation_error);
            return Err(TransactionFailure::Execute {
                operation,
                rollback,
            });
        }
        if probe.fault_rollback() {
            let operation =
                test_operation_error("injected PostgreSQL execution failure", Some("23505"));
            probe.record_rollback();
            let _ = transaction.rollback().await;
            return Err(TransactionFailure::Execute {
                operation,
                rollback: Some(test_operation_error(
                    "injected PostgreSQL rollback failure",
                    Some("08007"),
                )),
            });
        }
        probe.record_execute();
        let query = binder.bind(sqlx::query(statement), input);
        if let Err(error) = query.execute(&mut *transaction).await {
            let operation = operation_error(error);
            probe.record_rollback();
            let rollback = transaction.rollback().await.err().map(operation_error);
            return Err(TransactionFailure::Execute {
                operation,
                rollback,
            });
        }
    }

    if probe.fault_pre_commit() {
        let operation =
            test_operation_error("injected PostgreSQL pre-commit failure", Some("23505"));
        probe.record_rollback();
        let rollback = transaction.rollback().await.err().map(operation_error);
        return Err(TransactionFailure::PreCommit {
            operation,
            rollback,
        });
    }

    probe.record_commit();
    transaction
        .commit()
        .await
        .map_err(|error| TransactionFailure::Commit(operation_error(error)))?;
    if probe.fault_post_commit() {
        return Err(TransactionFailure::PostCommit(test_operation_error(
            "injected PostgreSQL acknowledgement ambiguity",
            Some("08007"),
        )));
    }
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
                test_operation_error("injected PostgreSQL encode failure", None),
            ));
        }
        self.binder
            .validate(&input)
            .map_err(|error| SinkWriteFailure::current_only(SinkWritePhase::Encode, error))?;

        if self.batch_size == 1 {
            execute_transaction(
                &self.pool,
                &self.statement,
                &self.binder,
                &self.pending,
                Some(&input),
                &self.probe,
            )
            .await
            .map_err(map_write_failure)?;
            return Ok(SinkWriteReport::terminal(terminal_outcome()));
        }

        if self.batch_size > 1 && self.pending.len() + 1 < self.batch_size {
            if self.probe.fault_before_deferral() {
                return Err(SinkWriteFailure::current_only(
                    SinkWritePhase::Execute,
                    test_operation_error("injected PostgreSQL pre-deferral failure", None),
                ));
            }
            let pending = context.defer();
            if self.probe.fault_after_deferral() {
                return Err(SinkWriteFailure::current_only(
                    SinkWritePhase::Execute,
                    test_operation_error("injected PostgreSQL post-deferral failure", None),
                ));
            }
            self.pending.push(BufferedRow { input, pending });
            return Ok(SinkWriteReport::buffered(
                obzenflow_runtime::stages::sink::SinkBufferedOutcome::accepted(None),
            ));
        }

        let current_pending = context.defer();
        execute_transaction(
            &self.pool,
            &self.statement,
            &self.binder,
            &self.pending,
            Some(&input),
            &self.probe,
        )
        .await
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
            return Err(test_operation_error(
                "injected PostgreSQL flush failure",
                None,
            ));
        }
        self.settle_pending().await
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.probe.record_drain();
        if self.probe.fault_drain() {
            return Err(test_operation_error(
                "injected PostgreSQL drain failure",
                None,
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
        execute_transaction(
            &self.pool,
            &self.statement,
            &self.binder,
            &self.pending,
            None,
            &self.probe,
        )
        .await
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
        )
        .expect("test URL parses without I/O")
    }

    #[test]
    fn connection_requires_an_explicit_safe_transport_mode() {
        let base = "postgres://sentinel-user:sentinel-password@localhost/test";

        assert!(matches!(
            PostgresConnection::from_url(base),
            Err(PostgresConfigError::UnsafeSslMode)
        ));
        assert!(matches!(
            PostgresConnection::from_options(PgConnectOptions::new()),
            Err(PostgresConfigError::UnsafeSslMode)
        ));
        for mode in ["allow", "prefer", "require", "verify-ca"] {
            assert!(matches!(
                PostgresConnection::from_url(&format!("{base}?sslmode={mode}")),
                Err(PostgresConfigError::UnsafeSslMode)
            ));
        }

        PostgresConnection::from_url(&format!("{base}?sslmode=disable"))
            .expect("explicit plaintext is permitted for a separately protected transport");
        PostgresConnection::from_url(&format!("{base}?sslmode=verify-full"))
            .expect("hostname-verified TLS is permitted");
        PostgresConnection::from_options(PgConnectOptions::new().ssl_mode(PgSslMode::VerifyFull))
            .expect("programmatic hostname-verified TLS is permitted");
        PostgresConnection::from_options(PgConnectOptions::new().ssl_mode(PgSslMode::Disable))
            .expect("programmatic explicit plaintext is permitted");
    }

    #[test]
    fn build_is_local_and_debug_is_redacted() {
        let sink = PostgresSink::<Input>::builder()
            .connection(connection())
            .table("public", "items")
            .unwrap()
            .statement("INSERT INTO public.items (value) VALUES ($1)")
            .unwrap()
            .bind_with(Binder)
            .batch_size(16)
            .unwrap()
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
            .table("public;drop", "items")
            .is_err());
        assert!(PostgresSink::<Input>::builder()
            .statement("DELETE FROM public.items")
            .is_err());
        assert!(PostgresSink::<Input>::builder()
            .statement("INSERT INTO a VALUES ($1); DROP TABLE a")
            .is_err());
        assert!(PostgresSink::<Input>::builder().batch_size(0).is_err());
        assert!(matches!(
            PostgresSink::<Input>::builder()
                .connection(connection())
                .table("public", "items")
                .unwrap()
                .statement("INSERT INTO public.other_items (value) VALUES ($1)")
                .unwrap()
                .bind_with(Binder)
                .build(),
            Err(PostgresConfigError::StatementDestinationMismatch)
        ));
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
        let schema = format!("obz122a_codes_{}", std::process::id());
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
}
