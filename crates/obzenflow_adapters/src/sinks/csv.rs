// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file sink

use async_trait::async_trait;
use csv::{Writer, WriterBuilder};
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    PendingSinkInput, SinkAuditOutcome, SinkBufferedOutcome, SinkDeliveryDeclaration,
    SinkInputContext, SinkTerminalOutcome, TypedCommitReceipt, TypedSinkConsumeReport,
    TypedSinkHandler, TypedSinkLifecycleReport,
};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

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

        let file_non_empty = self.append
            && std::fs::metadata(&path)
                .map(|m| m.len() > 0)
                .unwrap_or(false);

        if file_non_empty && self.columns.is_none() {
            anyhow::bail!(
                "append=true requires explicit columns when appending to a non-empty file"
            );
        }

        let file = if self.append {
            OpenOptions::new().create(true).append(true).open(&path)?
        } else {
            File::create(&path)?
        };

        let writer = WriterBuilder::new()
            .delimiter(self.delimiter)
            .from_writer(file);

        let inner = CsvSinkInner {
            writer,
            path: path.clone(),
            columns: self.columns,
            headers: self.headers,
            buffer: Vec::new(),
            buffer_size: self.buffer_size,
            flush_every: self.flush_every,
            auto_flush: self.auto_flush,
            headers_written: file_non_empty,
            row_count: 0,
            warned_column_drift: false,
            #[cfg(test)]
            fail_next_buffer_flush: false,
        };

        Ok(CsvSink {
            inner: Arc::new(Mutex::new(inner)),
            _phantom: PhantomData,
        })
    }
}

pub struct CsvSink<T> {
    inner: Arc<Mutex<CsvSinkInner>>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Clone for CsvSink<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            _phantom: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for CsvSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink").finish()
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
impl<T> TypedSinkHandler for CsvSink<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    type Input = T;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        // CSV re-writes the same rows deterministically on catch-up
        // (FLOWIP-120n F16).
        SinkDeliveryDeclaration::safety_only(SinkDeliverySafety::IdempotentProjection)
    }

    async fn consume(
        &mut self,
        input: T,
        context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HandlerError::Other("CsvSink mutex poisoned".to_string()))?;
        inner.consume_report(input, context)
    }

    async fn flush(&mut self) -> Result<TypedSinkLifecycleReport, HandlerError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HandlerError::Other("CsvSink mutex poisoned".to_string()))?;
        inner.flush_report()
    }
}

#[derive(Debug)]
struct BufferedCsvRow {
    pending: PendingSinkInput,
    row: Vec<String>,
}

struct CsvSinkInner {
    writer: Writer<File>,
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
    #[cfg(test)]
    fail_next_buffer_flush: bool,
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
        SinkTerminalOutcome::success(
            DeliveryMethod::FileWrite {
                path: self.path.clone(),
            },
            None,
        )
    }

    fn buffered_outcome(&self) -> SinkBufferedOutcome {
        SinkBufferedOutcome::new(
            DeliveryMethod::FileWrite {
                path: self.path.clone(),
            },
            None,
        )
    }

    fn flush_buffer(&mut self) -> Result<Vec<TypedCommitReceipt>, HandlerError> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_buffer_flush) {
            return Err(HandlerError::Other(
                "intentional buffered CSV flush failure".to_string(),
            ));
        }

        for row in &self.buffer {
            self.writer
                .write_record(&row.row)
                .map_err(|e| HandlerError::Other(format!("Failed to write CSV row: {e}")))?;
        }

        self.writer
            .flush()
            .map_err(|e| HandlerError::Other(format!("Failed to flush CSV: {e}")))?;

        let path = self.path.clone();
        let committed = self
            .buffer
            .drain(..)
            .map(|row| {
                TypedCommitReceipt::new(
                    row.pending,
                    SinkTerminalOutcome::success(
                        DeliveryMethod::FileWrite { path: path.clone() },
                        None,
                    ),
                )
            })
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
    fn flush_buffer_for_current_input(&mut self) -> Result<Vec<TypedCommitReceipt>, HandlerError> {
        match self.flush_buffer() {
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
        context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        let payload = serde_json::to_value(input).map_err(|error| {
            HandlerError::Other(format!("CsvSink failed to serialize typed input: {error}"))
        })?;
        let Value::Object(obj) = payload else {
            return Err(HandlerError::Validation(format!(
                "CsvSink requires object payloads, got {payload}"
            )));
        };

        let row = self.payload_to_row(&obj)?;

        // Ensure headers exist before writing any rows (unless append+non-empty).
        self.write_headers_if_needed()?;

        let mut commit_receipts = Vec::new();
        if self.auto_flush {
            self.writer
                .write_record(&row)
                .map_err(|e| HandlerError::Other(format!("Failed to write CSV row: {e}")))?;
            self.writer
                .flush()
                .map_err(|e| HandlerError::Other(format!("Failed to flush CSV: {e}")))?;
        } else {
            self.buffer.push(BufferedCsvRow {
                pending: context.defer(),
                row,
            });
            if self.buffer.len() >= self.buffer_size {
                commit_receipts.extend(self.flush_buffer_for_current_input()?);
            }
        }

        self.row_count = self.row_count.saturating_add(1);

        if let Some(flush_every) = self.flush_every {
            if flush_every > 0 && self.row_count.is_multiple_of(flush_every) {
                commit_receipts.extend(self.flush_buffer_for_current_input()?);
            }
        }

        let report = if self.auto_flush {
            TypedSinkConsumeReport::terminal(self.terminal_outcome())
        } else {
            let middleware_context = json!({
                "csv_sink": {
                    "buffered_rows": self.buffer.len(),
                }
            });
            TypedSinkConsumeReport::buffered(
                self.buffered_outcome()
                    .with_middleware_context(middleware_context),
            )
        };

        Ok(report.with_commit_receipts(commit_receipts))
    }

    fn flush_report(&mut self) -> Result<TypedSinkLifecycleReport, HandlerError> {
        self.write_headers_if_needed()?;
        let commit_receipts = self.flush_buffer()?;

        let audit = if commit_receipts.is_empty() {
            SinkAuditOutcome::success(
                DeliveryMethod::FileWrite {
                    path: self.path.clone(),
                },
                None,
            )
        } else {
            SinkAuditOutcome::success(
                DeliveryMethod::FileWrite {
                    path: self.path.clone(),
                },
                None,
            )
            .with_middleware_context(json!({
                "csv_sink": {
                    "flush": true,
                    "committed_rows": commit_receipts.len(),
                }
            }))
        };

        Ok(TypedSinkLifecycleReport::audit(audit).with_commit_receipts(commit_receipts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, TypedSinkHandlerAdapter};
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

    fn adapted(sink: CsvSink<TestRow>) -> TypedSinkHandlerAdapter<CsvSink<TestRow>> {
        TypedSinkHandlerAdapter::new(sink, StageId::new())
    }

    #[test]
    fn csv_sink_declares_idempotent_delivery() {
        let tmp = NamedTempFile::new().expect("temp file");
        let sink = CsvSink::<TestRow>::new(tmp.path()).unwrap();
        assert_eq!(
            sink.delivery_declaration().safety(),
            Some(SinkDeliverySafety::IdempotentProjection)
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
        let mut sink = adapted(sink);
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
        let mut sink = adapted(sink);
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
        let mut sink = adapted(sink);
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
        let mut sink = adapted(sink);
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
        let sink = CsvSink::<TestRow>::builder()
            .path(tmp.path())
            .buffer_size(2)
            .auto_flush(false)
            .build()
            .unwrap();
        let control = sink.clone();
        let mut sink = adapted(sink);
        let first = event(1, 2);
        let failed = event(3, 4);

        sink.consume_report(first.clone())
            .await
            .expect("first row buffers");
        control
            .inner
            .lock()
            .expect("CSV sink lock")
            .fail_next_buffer_flush = true;

        let error = sink
            .consume_report(failed)
            .await
            .expect_err("threshold flush is forced to fail");
        assert!(matches!(
            error,
            HandlerError::Other(ref detail)
                if detail == "intentional buffered CSV flush failure"
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
        let mut first = adapted(first);
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
        let mut second = adapted(second);
        second.consume(event(3, 4)).await.unwrap();
        second.flush().await.unwrap();

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();

        assert_eq!(out.lines().filter(|l| *l == "a,b").count(), 1);
        assert!(out.contains("1,2"));
        assert!(out.contains("3,4"));
        assert_eq!(out.lines().count(), 3);
    }

    #[test]
    fn csv_sink_append_non_empty_requires_explicit_columns() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "a,b").unwrap();
        writeln!(tmp, "1,2").unwrap();

        let err = CsvSink::<TestRow>::builder()
            .path(tmp.path())
            .append(true)
            .build()
            .unwrap_err();

        assert!(
            err.to_string()
                .contains("append=true requires explicit columns"),
            "unexpected error: {err}"
        );
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
        let mut sink = adapted(sink);
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
        let mut sink = TypedSinkHandlerAdapter::new(sink, StageId::new());
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
            HandlerError::Other(ref detail)
                if detail.contains("intentional serialization failure for 7")
        ));
    }

    #[tokio::test]
    async fn csv_sink_rejects_non_object_typed_payloads_before_deferral() {
        let tmp = NamedTempFile::new().expect("temp file");
        let sink = CsvSink::<ScalarRow>::new(tmp.path()).unwrap();
        let mut sink = TypedSinkHandlerAdapter::new(sink, StageId::new());
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
            HandlerError::Validation(ref detail)
                if detail.contains("CsvSink requires object payloads, got 7")
        ));
    }
}
