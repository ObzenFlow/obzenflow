// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file sink

use async_trait::async_trait;
use csv::{Writer, WriterBuilder};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, EventId};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    CommitReceipt, SinkConsumeReport, SinkHandler, SinkLifecycleReport,
};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone, Debug)]
pub struct CsvSinkBuilder {
    path: Option<PathBuf>,
    columns: Option<Vec<String>>,
    headers: Option<Vec<String>>,
    delimiter: u8,
    buffer_size: usize,
    flush_every: Option<usize>,
    auto_flush: bool,
    append: bool,
}

impl Default for CsvSinkBuilder {
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
        }
    }
}

impl CsvSinkBuilder {
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

    pub fn build(self) -> Result<CsvSink, anyhow::Error> {
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
        };

        Ok(CsvSink {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

#[derive(Clone)]
pub struct CsvSink {
    inner: Arc<Mutex<CsvSinkInner>>,
}

impl std::fmt::Debug for CsvSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink").finish()
    }
}

impl CsvSink {
    pub fn builder() -> CsvSinkBuilder {
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
impl SinkHandler for CsvSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HandlerError::Other("CsvSink mutex poisoned".to_string()))?;
        Ok(inner.consume_report(event)?.primary)
    }

    async fn consume_report(
        &mut self,
        event: ChainEvent,
    ) -> Result<SinkConsumeReport, HandlerError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HandlerError::Other("CsvSink mutex poisoned".to_string()))?;
        inner.consume_report(event)
    }

    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HandlerError::Other("CsvSink mutex poisoned".to_string()))?;
        Ok(inner.flush_report()?.audit_payload)
    }

    async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        let mut inner = self
            .inner
            .lock()
            .map_err(|_| HandlerError::Other("CsvSink mutex poisoned".to_string()))?;
        inner.flush_report()
    }
}

#[derive(Clone, Debug)]
struct BufferedCsvRow {
    parent_event_id: EventId,
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

    fn commit_payload(&self) -> DeliveryPayload {
        DeliveryPayload::success(
            self.path.display().to_string(),
            DeliveryMethod::FileWrite {
                path: self.path.clone(),
            },
            None,
        )
    }

    fn buffered_payload(&self) -> DeliveryPayload {
        DeliveryPayload::buffered(
            self.path.display().to_string(),
            DeliveryMethod::FileWrite {
                path: self.path.clone(),
            },
            None,
        )
    }

    fn flush_buffer(&mut self) -> Result<Vec<CommitReceipt>, HandlerError> {
        if self.buffer.is_empty() {
            return Ok(Vec::new());
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
            .map(|row| CommitReceipt {
                parent_event_id: row.parent_event_id,
                payload: DeliveryPayload::success(
                    path.display().to_string(),
                    DeliveryMethod::FileWrite { path: path.clone() },
                    None,
                ),
            })
            .collect();

        Ok(committed)
    }

    fn consume_report(&mut self, event: ChainEvent) -> Result<SinkConsumeReport, HandlerError> {
        let payload = match &event.content {
            ChainEventContent::Data { payload, .. } => payload,
            _ => return Ok(SinkConsumeReport::new(self.commit_payload())),
        };

        let Value::Object(obj) = payload else {
            return Err(HandlerError::Validation(format!(
                "CsvSink requires object payloads, got {payload}"
            )));
        };

        let row = self.payload_to_row(obj)?;

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
                parent_event_id: event.id,
                row,
            });
            if self.buffer.len() >= self.buffer_size {
                commit_receipts.extend(self.flush_buffer()?);
            }
        }

        self.row_count = self.row_count.saturating_add(1);

        if let Some(flush_every) = self.flush_every {
            if flush_every > 0 && self.row_count.is_multiple_of(flush_every) {
                commit_receipts.extend(self.flush_buffer()?);
            }
        }

        let primary = if self.auto_flush {
            self.commit_payload()
        } else {
            let middleware_context = Some(json!({
                "csv_sink": {
                    "buffered_rows": self.buffer.len(),
                }
            }));

            DeliveryPayload {
                middleware_context,
                ..self.buffered_payload()
            }
        };

        Ok(SinkConsumeReport {
            primary,
            commit_receipts,
        })
    }

    fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
        self.write_headers_if_needed()?;
        let commit_receipts = self.flush_buffer()?;

        let middleware_context = if commit_receipts.is_empty() {
            None
        } else {
            Some(json!({
                "csv_sink": {
                    "flush": true,
                    "committed_rows": commit_receipts.len(),
                }
            }))
        };

        Ok(SinkLifecycleReport {
            audit_payload: Some(DeliveryPayload {
                middleware_context,
                ..self.commit_payload()
            }),
            commit_receipts,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryResult;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::StageId;
    use obzenflow_core::WriterId;
    use std::io::Read;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn csv_sink_auto_detects_headers_and_writes_rows() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let mut sink = CsvSink::builder()
            .path(&path)
            .auto_flush(true)
            .build()
            .unwrap();

        let event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"b": 2, "a": 1}),
        );

        sink.consume(event).await.unwrap();
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

        let mut sink = CsvSink::builder()
            .path(&path)
            .buffer_size(10)
            .auto_flush(false)
            .build()
            .unwrap();

        let first = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"a": 1, "b": 2}),
        );
        let second = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"a": 3, "b": 4}),
        );

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
    async fn csv_sink_buffer_threshold_emits_per_event_commit_receipts() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let mut sink = CsvSink::builder()
            .path(&path)
            .buffer_size(2)
            .auto_flush(false)
            .build()
            .unwrap();

        let first = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"a": 1, "b": 2}),
        );
        let second = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"a": 3, "b": 4}),
        );

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
    async fn csv_sink_append_writes_headers_once() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let mut first = CsvSink::builder()
            .path(&path)
            .append(true)
            .columns(["a", "b"])
            .auto_flush(true)
            .build()
            .unwrap();

        first
            .consume(ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test.event",
                json!({"a": 1, "b": 2}),
            ))
            .await
            .unwrap();
        first.flush().await.unwrap();
        drop(first);

        let mut second = CsvSink::builder()
            .path(&path)
            .append(true)
            .columns(["a", "b"])
            .auto_flush(true)
            .build()
            .unwrap();

        second
            .consume(ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test.event",
                json!({"a": 3, "b": 4}),
            ))
            .await
            .unwrap();
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

        let err = CsvSink::builder()
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

        let mut sink = CsvSink::builder()
            .path(&path)
            .tab_delimited()
            .auto_flush(true)
            .build()
            .unwrap();

        sink.consume(ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.event",
            json!({"b": 2, "a": 1}),
        ))
        .await
        .unwrap();
        sink.flush().await.unwrap();

        let mut out = String::new();
        File::open(&path).unwrap().read_to_string(&mut out).unwrap();
        assert!(out.contains("a\tb"));
        assert!(out.contains("1\t2"));
    }
}
