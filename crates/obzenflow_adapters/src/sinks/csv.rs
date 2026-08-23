// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file sink

use async_trait::async_trait;
use csv::{Writer, WriterBuilder};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    PendingSinkInput, SinkAuditOutcome, SinkBufferedOutcome, SinkCommitReceipt, SinkConnector,
    SinkDescription, SinkInputOrder, SinkOperationError, SinkOperationResult, SinkTerminalOutcome,
    SinkWriteContext, SinkWriteFailure, SinkWritePhase, SinkWriteReport, SinkWriteResult,
    SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct CsvIoGate(Arc<AtomicBool>);

impl CsvIoGate {
    fn new() -> Self {
        Self(Arc::new(AtomicBool::new(true)))
    }

    fn disable(&self) {
        self.0.store(false, Ordering::SeqCst);
    }

    fn enabled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

struct GatedCsvFile {
    file: File,
    gate: CsvIoGate,
}

impl Write for GatedCsvFile {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        if self.gate.enabled() {
            self.file.write(buffer)
        } else {
            Ok(buffer.len())
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.gate.enabled() {
            self.file.flush()
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "test-support")]
pub mod testing {
    use obzenflow_runtime::testing::sink::{
        SinkExternalCall, SinkExternalCallKind, SinkExternalCallSnapshot, SinkFault,
    };
    use std::sync::{Arc, Mutex, MutexGuard};

    #[derive(Default)]
    struct ProbeState {
        armed: Option<SinkFault>,
        next_writer: u64,
        next_sequence: u64,
        calls: Vec<SinkExternalCall>,
    }

    /// Test-only one-shot fault and physical-call probe for CSV conformance.
    #[derive(Clone, Default)]
    pub struct CsvTestProbe {
        state: Arc<Mutex<ProbeState>>,
    }

    impl std::fmt::Debug for CsvTestProbe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("CsvTestProbe")
                .field("configured", &true)
                .finish()
        }
    }

    impl CsvTestProbe {
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

        pub(super) fn new_writer(&self) -> u64 {
            let mut state = self.state();
            let writer = state.next_writer;
            state.next_writer += 1;
            writer
        }

        pub(super) fn record(&self, writer: u64, kind: SinkExternalCallKind) {
            let mut state = self.state();
            let sequence = state.next_sequence;
            state.next_sequence += 1;
            state
                .calls
                .push(SinkExternalCall::new(writer, sequence, kind));
        }

        pub(super) fn take(&self, fault: SinkFault) -> bool {
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
struct CsvWriterProbe {
    #[cfg(feature = "test-support")]
    probe: Option<testing::CsvTestProbe>,
    #[cfg(feature = "test-support")]
    writer: u64,
}

impl CsvWriterProbe {
    #[cfg(feature = "test-support")]
    fn new(probe: Option<testing::CsvTestProbe>) -> Self {
        let writer = probe
            .as_ref()
            .map(testing::CsvTestProbe::new_writer)
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

    fn record_execute(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Execute);
    }

    fn record_commit(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Commit);
    }

    fn record_drop(&self) {
        #[cfg(feature = "test-support")]
        self.record(obzenflow_runtime::testing::sink::SinkExternalCallKind::Drop);
    }

    fn fault_open(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.take(obzenflow_runtime::testing::sink::SinkFault::Open)
        }
        #[cfg(not(feature = "test-support"))]
        false
    }

    fn fault_encode(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.take(obzenflow_runtime::testing::sink::SinkFault::Encode)
        }
        #[cfg(not(feature = "test-support"))]
        false
    }

    fn fault_mid_batch_mutation(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.take(obzenflow_runtime::testing::sink::SinkFault::MidBatchMutation)
        }
        #[cfg(not(feature = "test-support"))]
        false
    }

    fn fault_pre_commit(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.take(obzenflow_runtime::testing::sink::SinkFault::PreCommit)
        }
        #[cfg(not(feature = "test-support"))]
        false
    }

    fn fault_flush(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.take(obzenflow_runtime::testing::sink::SinkFault::Flush)
        }
        #[cfg(not(feature = "test-support"))]
        false
    }

    fn fault_drain(&self) -> bool {
        #[cfg(feature = "test-support")]
        {
            self.take(obzenflow_runtime::testing::sink::SinkFault::Drain)
        }
        #[cfg(not(feature = "test-support"))]
        false
    }
}

/// Builder for a CSV projection whose accepted event type is `T`.
#[derive(Clone, Debug)]
pub struct CsvSinkBuilder<T> {
    path: Option<PathBuf>,
    columns: Option<Vec<String>>,
    headers: Option<Vec<String>>,
    delimiter: u8,
    buffer_size: usize,
    flush_every: Option<usize>,
    auto_flush: bool,
    append: bool,
    #[cfg(feature = "test-support")]
    test_redelivery_unspecified: bool,
    #[cfg(feature = "test-support")]
    test_probe: Option<testing::CsvTestProbe>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Default for CsvSinkBuilder<T> {
    fn default() -> Self {
        Self {
            path: None,
            columns: None,
            headers: None,
            delimiter: b',',
            buffer_size: 100,
            flush_every: None,
            auto_flush: false,
            append: false,
            #[cfg(feature = "test-support")]
            test_redelivery_unspecified: false,
            #[cfg(feature = "test-support")]
            test_probe: None,
            _phantom: PhantomData,
        }
    }
}

impl<T> CsvSinkBuilder<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    pub fn headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.headers = Some(headers.into_iter().map(Into::into).collect());
        self
    }

    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = delimiter;
        self
    }

    pub fn tab_delimited(mut self) -> Self {
        self.delimiter = b'\t';
        self
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub fn flush_every(mut self, flush_every: usize) -> Self {
        self.flush_every = Some(flush_every);
        self
    }

    pub fn auto_flush(mut self, auto_flush: bool) -> Self {
        self.auto_flush = auto_flush;
        self
    }

    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Conformance-only seam for exercising the generic undeclared-safety
    /// archive gate. Production CSV configurations are always classified from
    /// their append mode.
    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_redelivery_unspecified(mut self) -> Self {
        self.test_redelivery_unspecified = true;
        self
    }

    #[cfg(feature = "test-support")]
    #[doc(hidden)]
    pub fn test_probe(mut self, probe: testing::CsvTestProbe) -> Self {
        self.test_probe = Some(probe);
        self
    }

    pub fn build(self) -> Result<CsvSink<T>, anyhow::Error> {
        let path = self.path.ok_or_else(|| anyhow::anyhow!("path required"))?;

        if self.buffer_size == 0 {
            anyhow::bail!("buffer_size must be > 0");
        }

        if let Some(columns) = self.columns.as_ref() {
            if columns.is_empty() {
                anyhow::bail!("columns must be non-empty when provided");
            }
        }

        if let Some(headers) = self.headers.as_ref() {
            let Some(columns) = self.columns.as_ref() else {
                anyhow::bail!("headers requires columns (header names must map to columns)");
            };
            if headers.len() != columns.len() {
                anyhow::bail!("headers length must match columns length");
            }
        }

        Ok(CsvSink {
            path,
            columns: self.columns,
            headers: self.headers,
            delimiter: self.delimiter,
            buffer_size: self.buffer_size,
            flush_every: self.flush_every,
            auto_flush: self.auto_flush,
            append: self.append,
            #[cfg(feature = "test-support")]
            test_redelivery_unspecified: self.test_redelivery_unspecified,
            #[cfg(feature = "test-support")]
            test_probe: self.test_probe,
            _phantom: PhantomData,
        })
    }
}

/// A type-indexed CSV projection sink.
///
/// The type parameter is the connector-owned input witness used by `sink!`; the
/// writer and row-shaping state remain schema-agnostic internally.
pub struct CsvSink<T> {
    path: PathBuf,
    columns: Option<Vec<String>>,
    headers: Option<Vec<String>>,
    delimiter: u8,
    buffer_size: usize,
    flush_every: Option<usize>,
    auto_flush: bool,
    append: bool,
    #[cfg(feature = "test-support")]
    test_redelivery_unspecified: bool,
    #[cfg(feature = "test-support")]
    test_probe: Option<testing::CsvTestProbe>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> std::fmt::Debug for CsvSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink")
            .field("path", &self.path)
            .field("buffer_size", &self.buffer_size)
            .field("auto_flush", &self.auto_flush)
            .field("append", &self.append)
            .finish_non_exhaustive()
    }
}

impl<T> CsvSink<T> {
    pub fn builder() -> CsvSinkBuilder<T> {
        CsvSinkBuilder::new()
    }

    pub fn new(path: impl Into<PathBuf>) -> Result<Self, anyhow::Error> {
        Self::builder().path(path).build()
    }

    pub fn tsv(path: impl Into<PathBuf>) -> Result<Self, anyhow::Error> {
        Self::builder().path(path).tab_delimited().build()
    }
}

#[async_trait]
impl<T> SinkConnector for CsvSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;
    type Writer = CsvWriter<T>;

    fn describe(&self) -> SinkDescription {
        let description = SinkDescription::method(DeliveryMethod::FileWrite {
            path: self.path.clone(),
        })
        .with_input_order(SinkInputOrder::OrderSensitive);
        #[cfg(feature = "test-support")]
        if self.test_redelivery_unspecified {
            return description;
        }
        let safety = if self.append {
            SinkRedeliverySafety::DuplicateSensitive
        } else {
            SinkRedeliverySafety::SafeToRepeat
        };
        description.with_redelivery_safety(safety)
    }

    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        #[cfg(feature = "test-support")]
        let probe = CsvWriterProbe::new(self.test_probe.clone());
        #[cfg(not(feature = "test-support"))]
        let probe = CsvWriterProbe::new();
        probe.record_open();
        if probe.fault_open() {
            return Err(SinkOperationError::other("injected CSV open failure"));
        }
        let file_non_empty = self.append
            && std::fs::metadata(&self.path)
                .map(|metadata| metadata.len() > 0)
                .unwrap_or(false);

        if file_non_empty && self.columns.is_none() {
            return Err(SinkOperationError::validation(
                "CsvSink append=true requires explicit columns when appending to a non-empty file"
                    .to_string(),
            ));
        }

        let file = if self.append {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.path)
        } else {
            File::create(&self.path)
        }
        .map_err(|error| {
            SinkOperationError::other(format!(
                "CsvSink failed to open {}: {error}",
                self.path.display()
            ))
        })?;

        let io_gate = CsvIoGate::new();
        let writer = WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(GatedCsvFile {
                file,
                gate: io_gate.clone(),
            });
        Ok(CsvWriter {
            inner: Mutex::new(CsvSinkInner {
                writer,
                path: self.path.clone(),
                columns: self.columns.clone(),
                headers: self.headers.clone(),
                buffer: Vec::new(),
                buffer_size: self.buffer_size,
                flush_every: self.flush_every,
                auto_flush: self.auto_flush,
                headers_written: file_non_empty,
                row_count: 0,
                warned_column_drift: false,
                io_gate,
                #[cfg(test)]
                fail_next_buffer_flush: false,
            }),
            probe,
            _phantom: PhantomData,
        })
    }
}

/// Stage-local CSV writer opened from [`CsvSink`].
pub struct CsvWriter<T> {
    inner: Mutex<CsvSinkInner>,
    probe: CsvWriterProbe,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Drop for CsvWriter<T> {
    fn drop(&mut self) {
        self.probe.record_drop();
    }
}

impl<T> std::fmt::Debug for CsvWriter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvWriter")
            .field("payload_type", &std::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T> SinkWriter for CsvWriter<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    async fn write(&mut self, input: T, context: SinkWriteContext) -> SinkWriteResult {
        self.probe.record_write();
        if self.probe.fault_encode() {
            return Err(SinkWriteFailure::current_only(
                SinkWritePhase::Encode,
                SinkOperationError::other("injected CSV encode failure"),
            ));
        }
        let mut inner = self.inner.lock().map_err(|_| {
            SinkWriteFailure::poisoned(
                SinkWritePhase::Execute,
                SinkOperationError::other("CsvWriter mutex poisoned"),
            )
        })?;
        inner
            .consume_report(input, context, &self.probe)
            .map_err(CsvWriteError::into_failure)
    }

    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.probe.record_flush();
        if self.probe.fault_flush() {
            return Err(SinkOperationError::other("injected CSV flush failure"));
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SinkOperationError::other("CsvWriter mutex poisoned"))?;
        inner.flush_report(&self.probe)
    }

    async fn drain(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.probe.record_drain();
        if self.probe.fault_drain() {
            return Err(SinkOperationError::other("injected CSV drain failure"));
        }
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| SinkOperationError::other("CsvWriter mutex poisoned"))?;
        inner.flush_report(&self.probe)
    }
}

struct CsvWriteError {
    phase: SinkWritePhase,
    error: Box<SinkOperationError>,
    poisoned: bool,
}

impl CsvWriteError {
    fn current(phase: SinkWritePhase, error: SinkOperationError) -> Self {
        Self {
            phase,
            error: Box::new(error),
            poisoned: false,
        }
    }

    fn poisoned(phase: SinkWritePhase, error: SinkOperationError) -> Self {
        Self {
            phase,
            error: Box::new(error),
            poisoned: true,
        }
    }

    fn into_failure(self) -> SinkWriteFailure {
        if self.poisoned {
            SinkWriteFailure::poisoned(self.phase, *self.error)
        } else {
            SinkWriteFailure::current_only(self.phase, *self.error)
        }
    }

    fn into_operation_error(self) -> SinkOperationError {
        *self.error
    }
}

#[derive(Debug)]
struct BufferedCsvRow {
    pending: PendingSinkInput,
    row: Vec<String>,
}

struct CsvSinkInner {
    writer: Writer<GatedCsvFile>,
    path: PathBuf,
    columns: Option<Vec<String>>,
    headers: Option<Vec<String>>,
    buffer: Vec<BufferedCsvRow>,
    buffer_size: usize,
    flush_every: Option<usize>,
    auto_flush: bool,
    headers_written: bool,
    row_count: usize,
    warned_column_drift: bool,
    io_gate: CsvIoGate,
    #[cfg(test)]
    fail_next_buffer_flush: bool,
}

impl Drop for CsvSinkInner {
    fn drop(&mut self) {
        // `csv::Writer::drop` flushes by default. Disable its underlying
        // write gate first so failed teardown and ordinary destruction cannot
        // create destination evidence outside an invoked lifecycle method.
        self.io_gate.disable();
    }
}

impl CsvSinkInner {
    fn ensure_columns_locked(&mut self, payload: &serde_json::Map<String, Value>) {
        if self.columns.is_some() {
            return;
        }

        let mut columns: Vec<String> = payload.keys().cloned().collect();
        columns.sort();
        self.columns = Some(columns);
    }

    fn write_headers_if_needed(&mut self) -> Result<(), HandlerError> {
        if self.headers_written {
            return Ok(());
        }

        let Some(columns) = self.columns.as_ref() else {
            // Will be called again after the first event locks columns.
            return Ok(());
        };

        let headers = self.headers.as_ref().unwrap_or(columns);
        self.writer
            .write_record(headers)
            .map_err(|e| HandlerError::Other(format!("Failed to write CSV headers: {e}")))?;
        self.headers_written = true;
        Ok(())
    }

    fn payload_to_row(
        &mut self,
        payload: &serde_json::Map<String, Value>,
    ) -> Result<Vec<String>, HandlerError> {
        self.ensure_columns_locked(payload);
        let Some(columns) = self.columns.as_ref() else {
            return Err(HandlerError::Other(
                "CsvSink failed to lock columns".to_string(),
            ));
        };

        if !self.warned_column_drift {
            let column_set: HashSet<&str> = columns.iter().map(|c| c.as_str()).collect();
            let extra: Vec<&str> = payload
                .keys()
                .map(String::as_str)
                .filter(|k| !column_set.contains(k))
                .collect();
            let missing: Vec<&str> = columns
                .iter()
                .map(String::as_str)
                .filter(|k| !payload.contains_key(*k))
                .collect();

            if !extra.is_empty() || !missing.is_empty() {
                self.warned_column_drift = true;
                tracing::warn!(
                    file = %self.path.display(),
                    extra_keys = ?extra,
                    missing_keys = ?missing,
                    "CsvSink column drift detected; extra keys will be ignored and missing keys will be empty"
                );
            }
        }

        let row = columns
            .iter()
            .map(|col| match payload.get(col) {
                None | Some(Value::Null) => String::new(),
                Some(Value::String(s)) => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(v @ (Value::Array(_) | Value::Object(_))) => v.to_string(),
            })
            .collect();

        Ok(row)
    }

    fn terminal_outcome(&self) -> SinkTerminalOutcome {
        SinkTerminalOutcome::success(None)
    }

    fn buffered_outcome(&self) -> SinkBufferedOutcome {
        SinkBufferedOutcome::accepted(None)
    }

    fn flush_buffer(
        &mut self,
        probe: &CsvWriterProbe,
    ) -> Result<Vec<SinkCommitReceipt>, CsvWriteError> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_buffer_flush) {
            return Err(CsvWriteError::poisoned(
                SinkWritePhase::Commit,
                SinkOperationError::other("intentional buffered CSV flush failure"),
            ));
        }

        for (index, row) in self.buffer.iter().enumerate() {
            probe.record_execute();
            self.writer.write_record(&row.row).map_err(|error| {
                CsvWriteError::poisoned(
                    SinkWritePhase::Execute,
                    SinkOperationError::other(format!("Failed to write CSV row: {error}")),
                )
            })?;
            if index == 0 && probe.fault_mid_batch_mutation() {
                return Err(CsvWriteError::poisoned(
                    SinkWritePhase::Execute,
                    SinkOperationError::other("injected CSV mid-batch mutation failure"),
                ));
            }
        }

        if probe.fault_pre_commit() {
            return Err(CsvWriteError::poisoned(
                SinkWritePhase::Commit,
                SinkOperationError::other("injected CSV pre-commit failure"),
            ));
        }
        probe.record_commit();
        self.writer.flush().map_err(|error| {
            CsvWriteError::poisoned(
                SinkWritePhase::Commit,
                SinkOperationError::other(format!("Failed to flush CSV: {error}")),
            )
        })?;

        let committed = self
            .buffer
            .drain(..)
            .map(|row| SinkCommitReceipt::new(row.pending, SinkTerminalOutcome::success(None)))
            .collect();

        Ok(committed)
    }

    /// Flush triggered while accepting the current input.
    ///
    /// `flush_buffer` does not drain on failure, so the current row is still
    /// last. Remove that row before returning the handler error: the adapter
    /// will revoke the same input's settlement capability before the
    /// supervisor authors the failed delivery receipt, and the sink must not
    /// retain stale authority that a later flush could submit.
    fn flush_buffer_for_current_input(
        &mut self,
        probe: &CsvWriterProbe,
    ) -> Result<Vec<SinkCommitReceipt>, CsvWriteError> {
        match self.flush_buffer(probe) {
            Ok(receipts) => Ok(receipts),
            Err(error) => {
                let removed = self.buffer.pop();
                debug_assert!(removed.is_some());
                Err(error)
            }
        }
    }

    fn consume_report<T: TypedPayload>(
        &mut self,
        input: T,
        context: SinkWriteContext,
        probe: &CsvWriterProbe,
    ) -> Result<SinkWriteReport, CsvWriteError> {
        let payload = serde_json::to_value(input).map_err(|error| {
            CsvWriteError::current(
                SinkWritePhase::Encode,
                SinkOperationError::other(format!(
                    "CsvSink failed to serialize typed input: {error}"
                )),
            )
        })?;
        let Value::Object(obj) = payload else {
            return Err(CsvWriteError::current(
                SinkWritePhase::Encode,
                SinkOperationError::validation(format!(
                    "CsvSink requires object payloads, got {payload}"
                )),
            ));
        };

        let row = self.payload_to_row(&obj).map_err(|error| {
            CsvWriteError::current(
                SinkWritePhase::Encode,
                SinkOperationError::try_from(error)
                    .unwrap_or_else(|_| SinkOperationError::other("CSV row encoding failed")),
            )
        })?;

        // Ensure headers exist before writing any rows (unless append+non-empty).
        self.write_headers_if_needed().map_err(|error| {
            CsvWriteError::poisoned(
                SinkWritePhase::Execute,
                SinkOperationError::try_from(error)
                    .unwrap_or_else(|_| SinkOperationError::other("CSV header write failed")),
            )
        })?;

        let mut commit_receipts = Vec::new();
        if self.auto_flush {
            probe.record_execute();
            self.writer.write_record(&row).map_err(|error| {
                CsvWriteError::current(
                    SinkWritePhase::Execute,
                    SinkOperationError::other(format!("Failed to write CSV row: {error}")),
                )
            })?;
            if probe.fault_mid_batch_mutation() || probe.fault_pre_commit() {
                return Err(CsvWriteError::poisoned(
                    SinkWritePhase::Commit,
                    SinkOperationError::other("injected CSV commit ambiguity"),
                ));
            }
            probe.record_commit();
            self.writer.flush().map_err(|error| {
                CsvWriteError::poisoned(
                    SinkWritePhase::Commit,
                    SinkOperationError::other(format!("Failed to flush CSV: {error}")),
                )
            })?;
        } else {
            self.buffer.push(BufferedCsvRow {
                pending: context.defer(),
                row,
            });
            if self.buffer.len() >= self.buffer_size {
                commit_receipts.extend(self.flush_buffer_for_current_input(probe)?);
            }
        }

        self.row_count = self.row_count.saturating_add(1);

        if let Some(flush_every) = self.flush_every {
            if flush_every > 0 && self.row_count.is_multiple_of(flush_every) {
                commit_receipts.extend(self.flush_buffer_for_current_input(probe)?);
            }
        }

        let report = if self.auto_flush {
            SinkWriteReport::terminal(self.terminal_outcome())
        } else {
            let middleware_context = json!({
                "csv_sink": {
                    "buffered_rows": self.buffer.len(),
                }
            });
            SinkWriteReport::buffered(
                self.buffered_outcome()
                    .with_middleware_context(middleware_context),
            )
        };

        Ok(report.with_commit_receipts(commit_receipts))
    }

    // The sink protocol owns this by-value error shape. Boxing only this
    // synchronous helper would make it inconsistent with `SinkWriter`.
    #[allow(clippy::result_large_err)]
    fn flush_report(
        &mut self,
        probe: &CsvWriterProbe,
    ) -> SinkOperationResult<SinkWriterLifecycleReport> {
        self.write_headers_if_needed().map_err(|error| {
            SinkOperationError::try_from(error)
                .unwrap_or_else(|_| SinkOperationError::other("CSV header write failed"))
        })?;
        let commit_receipts = self
            .flush_buffer(probe)
            .map_err(CsvWriteError::into_operation_error)?;

        let audit = if commit_receipts.is_empty() {
            SinkAuditOutcome::success(None)
        } else {
            SinkAuditOutcome::success(None).with_middleware_context(json!({
                "csv_sink": {
                    "flush": true,
                    "committed_rows": commit_receipts.len(),
                }
            }))
        };

        Ok(SinkWriterLifecycleReport::audit(audit).with_commit_receipts(commit_receipts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, SinkWriterAdapter};
    use serde::{Deserialize, Serialize};
    use std::io::Read;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestRow {
        a: i32,
        b: i32,
    }

    impl TypedPayload for TestRow {
        const EVENT_TYPE: &'static str = "test.csv.row";
    }

    #[derive(Clone, Debug, Deserialize)]
    struct SerializationFails {
        value: u64,
    }

    impl Serialize for SerializationFails {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(format!(
                "intentional serialization failure for {}",
                self.value
            )))
        }
    }

    impl TypedPayload for SerializationFails {
        const EVENT_TYPE: &'static str = "test.csv.serialization_fails";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct ScalarRow(u64);

    impl TypedPayload for ScalarRow {
        const EVENT_TYPE: &'static str = "test.csv.scalar";
    }

    fn event(a: i32, b: i32) -> obzenflow_core::ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            TestRow::versioned_event_type(),
            json!({ "a": a, "b": b }),
        )
    }

    async fn adapted<T>(connector: CsvSink<T>) -> SinkWriterAdapter<CsvWriter<T>>
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        let stage_id = StageId::new();
        let description = connector.describe();
        let writer = connector
            .open(SinkWriterInitContext::new(
                stage_id,
                "csv".to_string(),
                "test".to_string(),
            ))
            .await
            .expect("CSV connector opens");
        SinkWriterAdapter::with_default_method(
            writer,
            stage_id,
            description.default_method().cloned(),
        )
    }

    #[test]
    fn csv_sink_describes_repeatable_redelivery() {
        let tmp = NamedTempFile::new().expect("temp file");
        let sink = CsvSink::<TestRow>::new(tmp.path()).unwrap();
        assert_eq!(
            sink.describe().redelivery_safety(),
            Some(SinkRedeliverySafety::SafeToRepeat)
        );
    }

    #[test]
    fn append_mode_describes_duplicate_sensitive_redelivery() {
        let tmp = NamedTempFile::new().expect("temp file");
        let sink = CsvSink::<TestRow>::builder()
            .path(tmp.path())
            .columns(["a", "b"])
            .append(true)
            .build()
            .expect("append connector");
        assert_eq!(
            sink.describe().redelivery_safety(),
            Some(SinkRedeliverySafety::DuplicateSensitive)
        );
    }

    #[tokio::test]
    async fn csv_sink_auto_detects_headers_and_writes_rows() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let sink = CsvSink::<TestRow>::builder()
            .path(&path)
            .auto_flush(true)
            .build()
            .unwrap();
        let mut sink = adapted(sink).await;
        sink.consume(event(1, 2)).await.unwrap();
        sink.flush().await.unwrap();

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();
        assert!(out.contains("a,b"));
        assert!(out.contains("1,2"));
    }

    #[tokio::test]
    async fn csv_sink_buffered_mode_emits_commit_receipts_on_flush() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let sink = CsvSink::<TestRow>::builder()
            .path(&path)
            .buffer_size(10)
            .auto_flush(false)
            .build()
            .unwrap();
        let mut sink = adapted(sink).await;
        let first = event(1, 2);
        let second = event(3, 4);

        let report = sink.consume_report(first.clone()).await.unwrap();
        assert!(matches!(
            report.primary.result,
            DeliveryResult::Buffered { .. }
        ));
        assert!(report.commit_receipts.is_empty());

        let report = sink.consume_report(second.clone()).await.unwrap();
        assert!(matches!(
            report.primary.result,
            DeliveryResult::Buffered { .. }
        ));
        assert!(report.commit_receipts.is_empty());

        let lifecycle = sink.flush_report().await.unwrap();
        assert_eq!(lifecycle.commit_receipts.len(), 2);
        assert_eq!(lifecycle.commit_receipts[0].parent_event_id, first.id);
        assert_eq!(lifecycle.commit_receipts[1].parent_event_id, second.id);
        assert!(matches!(
            lifecycle.audit_payload.expect("audit payload").result,
            DeliveryResult::Success { .. }
        ));

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();
        assert!(out.contains("a,b"));
        assert!(out.contains("1,2"));
        assert!(out.contains("3,4"));
    }

    #[tokio::test]
    async fn csv_sink_drain_flushes_pending_rows_with_exact_parent_receipts() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let sink = CsvSink::<TestRow>::builder()
            .path(&path)
            .buffer_size(10)
            .auto_flush(false)
            .build()
            .unwrap();
        let mut sink = adapted(sink).await;
        let input = event(5, 6);

        let consume = sink.consume_report(input.clone()).await.unwrap();
        assert!(matches!(
            consume.primary.result,
            DeliveryResult::Buffered { .. }
        ));

        let lifecycle = sink.drain_report().await.unwrap();
        assert_eq!(lifecycle.commit_receipts.len(), 1);
        assert_eq!(lifecycle.commit_receipts[0].parent_event_id, input.id);
        assert!(matches!(
            lifecycle.commit_receipts[0].payload.result,
            DeliveryResult::Success { .. }
        ));

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();
        assert_eq!(out.lines().collect::<Vec<_>>(), vec!["a,b", "5,6"]);
    }

    #[tokio::test]
    async fn csv_sink_buffer_threshold_emits_per_event_commit_receipts() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let sink = CsvSink::<TestRow>::builder()
            .path(&path)
            .buffer_size(2)
            .auto_flush(false)
            .build()
            .unwrap();
        let mut sink = adapted(sink).await;
        let first = event(1, 2);
        let second = event(3, 4);

        let first_report = sink.consume_report(first.clone()).await.unwrap();
        assert!(matches!(
            first_report.primary.result,
            DeliveryResult::Buffered { .. }
        ));
        assert!(first_report.commit_receipts.is_empty());

        let second_report = sink.consume_report(second.clone()).await.unwrap();
        assert!(matches!(
            second_report.primary.result,
            DeliveryResult::Buffered { .. }
        ));
        assert_eq!(second_report.commit_receipts.len(), 2);
        assert_eq!(second_report.commit_receipts[0].parent_event_id, first.id);
        assert_eq!(second_report.commit_receipts[1].parent_event_id, second.id);

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();
        assert!(out.contains("a,b"));
        assert!(out.contains("1,2"));
        assert!(out.contains("3,4"));
    }

    #[tokio::test]
    async fn failed_consume_flush_discards_only_the_current_settlement_capability() {
        let tmp = NamedTempFile::new().expect("temp file");
        let connector = CsvSink::<TestRow>::builder()
            .path(tmp.path())
            .buffer_size(2)
            .auto_flush(false)
            .build()
            .unwrap();
        let stage_id = StageId::new();
        let description = connector.describe();
        let mut writer = connector
            .open(SinkWriterInitContext::new(
                stage_id,
                "csv".to_string(),
                "test".to_string(),
            ))
            .await
            .expect("CSV connector opens");
        writer
            .inner
            .get_mut()
            .expect("CSV writer lock")
            .fail_next_buffer_flush = true;
        let mut sink = SinkWriterAdapter::with_default_method(
            writer,
            stage_id,
            description.default_method().cloned(),
        );
        let first = event(1, 2);
        let failed = event(3, 4);

        sink.consume_report(first.clone())
            .await
            .expect("first row buffers");
        let error = sink
            .consume_report(failed)
            .await
            .expect_err("threshold flush is forced to fail");
        assert!(matches!(
            error,
            HandlerError::SinkWrite(ref failure)
                if failure.phase() == SinkWritePhase::Commit
                    && failure.error().detail().contains("intentional buffered CSV flush failure")
        ));

        let lifecycle = sink
            .flush_report()
            .await
            .expect("the earlier buffered row remains settleable");
        assert_eq!(lifecycle.commit_receipts.len(), 1);
        assert_eq!(lifecycle.commit_receipts[0].parent_event_id, first.id);
    }

    #[tokio::test]
    async fn csv_sink_append_writes_headers_once() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let first = CsvSink::<TestRow>::builder()
            .path(&path)
            .append(true)
            .columns(["a", "b"])
            .auto_flush(true)
            .build()
            .unwrap();
        let mut first = adapted(first).await;
        first.consume(event(1, 2)).await.unwrap();
        first.flush().await.unwrap();
        drop(first);

        let second = CsvSink::<TestRow>::builder()
            .path(&path)
            .append(true)
            .columns(["a", "b"])
            .auto_flush(true)
            .build()
            .unwrap();
        let mut second = adapted(second).await;
        second.consume(event(3, 4)).await.unwrap();
        second.flush().await.unwrap();

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();

        assert_eq!(out.lines().filter(|l| *l == "a,b").count(), 1);
        assert!(out.contains("1,2"));
        assert!(out.contains("3,4"));
        assert_eq!(out.lines().count(), 3);
    }

    #[tokio::test]
    async fn csv_sink_append_non_empty_requires_explicit_columns() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "a,b").unwrap();
        writeln!(tmp, "1,2").unwrap();

        let connector = CsvSink::<TestRow>::builder()
            .path(tmp.path())
            .append(true)
            .build()
            .expect("configuration builds without opening the file");
        let err = connector
            .open(SinkWriterInitContext::new(
                StageId::new(),
                "csv".to_string(),
                "test".to_string(),
            ))
            .await
            .expect_err("non-empty append without columns must fail when opening");

        assert!(
            err.to_string()
                .contains("append=true requires explicit columns"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn csv_connector_build_is_io_free_and_open_creates_the_file() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let path = temp.path().join("opened-at-materialisation.csv");

        let connector = CsvSink::<TestRow>::builder()
            .path(&path)
            .build()
            .expect("local configuration is valid");
        assert!(!path.exists(), "build must not touch the destination");

        let _writer = connector
            .open(SinkWriterInitContext::new(
                StageId::new(),
                "csv".to_string(),
                "test".to_string(),
            ))
            .await
            .expect("open creates the CSV writer");

        assert!(path.exists(), "open owns destination creation");
    }

    #[tokio::test]
    async fn repeated_csv_opens_have_isolated_writer_buffers() {
        let temp = tempfile::tempdir().expect("temporary directory");
        let connector = CsvSink::<TestRow>::builder()
            .path(temp.path().join("isolated-writers.csv"))
            .columns(["a", "b"])
            .buffer_size(10)
            .auto_flush(false)
            .build()
            .expect("local configuration is valid");
        let method = connector.describe().default_method().cloned();
        let first_stage = StageId::new();
        let second_stage = StageId::new();
        let first_writer = connector
            .open(SinkWriterInitContext::new(
                first_stage,
                "first_csv".to_string(),
                "test".to_string(),
            ))
            .await
            .expect("first writer opens");
        let second_writer = connector
            .open(SinkWriterInitContext::new(
                second_stage,
                "second_csv".to_string(),
                "test".to_string(),
            ))
            .await
            .expect("second writer opens");
        let mut first =
            SinkWriterAdapter::with_default_method(first_writer, first_stage, method.clone());
        let mut second =
            SinkWriterAdapter::with_default_method(second_writer, second_stage, method);

        first
            .consume_report(event(1, 2))
            .await
            .expect("first writer buffers one input");
        let second_flush = second.flush_report().await.expect("second writer flushes");
        assert!(
            second_flush.commit_receipts.is_empty(),
            "the second writer cannot see the first writer's buffer"
        );
        let first_flush = first.flush_report().await.expect("first writer flushes");
        assert_eq!(first_flush.commit_receipts.len(), 1);
    }

    #[tokio::test]
    async fn csv_sink_tsv_writes_tab_delimited() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let sink = CsvSink::<TestRow>::builder()
            .path(&path)
            .tab_delimited()
            .auto_flush(true)
            .build()
            .unwrap();
        let mut sink = adapted(sink).await;
        sink.consume(event(1, 2)).await.unwrap();
        sink.flush().await.unwrap();

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();
        assert!(out.contains("a\tb"));
        assert!(out.contains("1\t2"));
    }

    #[tokio::test]
    async fn csv_sink_routes_typed_serialization_failures_before_deferral() {
        let tmp = NamedTempFile::new().expect("temp file");
        let sink = CsvSink::<SerializationFails>::new(tmp.path()).unwrap();
        let mut sink = adapted(sink).await;
        let input = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            SerializationFails::versioned_event_type(),
            json!({ "value": 7 }),
        );

        let error = sink
            .consume_report(input)
            .await
            .expect_err("serialization failure must use the handler error path");
        assert!(matches!(
            error,
            HandlerError::SinkWrite(ref failure)
                if failure.phase() == SinkWritePhase::Encode
                    && failure.error().detail().contains("intentional serialization failure for 7")
        ));
    }

    #[tokio::test]
    async fn csv_sink_rejects_non_object_typed_payloads_before_deferral() {
        let tmp = NamedTempFile::new().expect("temp file");
        let sink = CsvSink::<ScalarRow>::new(tmp.path()).unwrap();
        let mut sink = adapted(sink).await;
        let input = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            ScalarRow::versioned_event_type(),
            json!(7),
        );

        let error = sink
            .consume_report(input)
            .await
            .expect_err("CSV requires an object-shaped typed payload");
        assert!(matches!(
            error,
            HandlerError::SinkWrite(ref failure)
                if failure.phase() == SinkWritePhase::Encode
                    && failure.error().detail().contains("CsvSink requires object payloads, got 7")
        ));
    }
}
