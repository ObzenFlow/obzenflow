// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file source
//!
//! Design notes (aligned with the FlowIP decisions):
//! - Sync `FiniteSourceHandler` (blocking file IO inside `next()`)
//! - WriterId injected via `bind_writer_id()`
//! - Event types derived from `TypedPayload::versioned_event_type()`
//! - Malformed rows return `SourceError::Deserialization(..)`; middleware converts to error-marked events
//! - Untyped mode preserves strings (no inference)

use anyhow::{anyhow, bail, Context, Result};
use csv::{Reader, ReaderBuilder, StringRecord};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, TypedPayload, WriterId};
use obzenflow_runtime::stages::{FiniteSourceHandler, SourceError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// Untyped CSV row payload (`csv.row.v1`) with string-only values.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct CsvRow(pub BTreeMap<String, String>);

impl TypedPayload for CsvRow {
    const EVENT_TYPE: &'static str = "csv.row";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug)]
pub struct CsvSourceBuilder<T = CsvRow> {
    path: Option<PathBuf>,
    has_headers: bool,
    headers: Option<Vec<String>>,
    delimiter: u8,
    chunk_size: usize,
    skip_rows: usize,
    select_columns: Option<Vec<String>>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> Default for CsvSourceBuilder<T> {
    fn default() -> Self {
        Self {
            path: None,
            has_headers: true,
            headers: None,
            delimiter: b',',
            chunk_size: 1000,
            skip_rows: 0,
            select_columns: None,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> CsvSourceBuilder<T> {
    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    /// Provide headers explicitly (required when `has_headers=false`).
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

    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn skip_rows(mut self, rows: usize) -> Self {
        self.skip_rows = rows;
        self
    }

    pub fn select_columns<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.select_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }
}

impl<T> CsvSourceBuilder<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    pub fn from_file(path: impl Into<PathBuf>) -> Result<CsvSource<T>> {
        Self::default().path(path).build()
    }

    pub fn build(self) -> Result<CsvSource<T>> {
        let path = self.path.ok_or_else(|| anyhow!("path required"))?;

        if self.chunk_size == 0 {
            bail!("chunk_size must be > 0");
        }

        if !self.has_headers && self.headers.as_ref().is_none_or(|h| h.is_empty()) {
            bail!("headers must be provided when has_headers=false");
        }

        let file = File::open(&path)
            .with_context(|| format!("Failed to open CSV file: {}", path.display()))?;
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(self.delimiter)
            .from_reader(file);

        let file_headers = if self.has_headers {
            let mut header_record = StringRecord::new();
            let ok = reader
                .read_record(&mut header_record)
                .with_context(|| format!("Failed to read header row from {}", path.display()))?;
            if !ok {
                bail!("CSV file has no header row: {}", path.display());
            }
            header_record
        } else {
            let mut header_record = StringRecord::new();
            for h in self.headers.as_ref().expect("checked above") {
                header_record.push_field(h);
            }
            header_record
        };

        let (selected_indices, decode_headers) = match self.select_columns.as_ref() {
            None => (None, file_headers.clone()),
            Some(columns) => {
                let mut indices = Vec::with_capacity(columns.len());
                let mut selected_headers = StringRecord::new();
                for col in columns {
                    let idx = file_headers.iter().position(|h| h == col).ok_or_else(|| {
                        anyhow!("select_columns references unknown header '{col}'")
                    })?;
                    indices.push(idx);
                    selected_headers.push_field(col);
                }
                (Some(indices), selected_headers)
            }
        };

        let state = Arc::new(Mutex::new(CsvReaderState {
            path: path.clone(),
            reader,
            file_headers,
            decode_headers,
            selected_indices,
            chunk_size: self.chunk_size,
            skip_rows_remaining: self.skip_rows,
            row_index: 0,
            warned_schema_drift: false,
            pending_error: None,
            done: false,
        }));

        Ok(CsvSource {
            state,
            writer_id: None,
            _phantom: PhantomData,
        })
    }
}

/// CSV file source implementing `FiniteSourceHandler`.
pub struct CsvSource<T = CsvRow>
where
    T: TypedPayload + Send + Sync + 'static,
{
    state: Arc<Mutex<CsvReaderState>>,
    writer_id: Option<WriterId>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T> Clone for CsvSource<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            writer_id: self.writer_id,
            _phantom: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for CsvSource<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSource")
            .field("payload_type", &std::any::type_name::<T>())
            .field("writer_id_bound", &self.writer_id.is_some())
            .finish()
    }
}

impl CsvSource<CsvRow> {
    pub fn builder() -> CsvSourceBuilder<CsvRow> {
        CsvSourceBuilder::default()
    }

    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self> {
        Self::builder().path(path).build()
    }

    pub fn tsv_from_file(path: impl Into<PathBuf>) -> Result<Self> {
        Self::builder().path(path).tab_delimited().build()
    }

    pub fn typed_builder<T>() -> CsvSourceBuilder<T>
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        CsvSourceBuilder::default()
    }

    pub fn typed_from_file<T>(path: impl Into<PathBuf>) -> Result<CsvSource<T>>
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        CsvSourceBuilder::<T>::from_file(path)
    }

    pub fn typed_tsv_from_file<T>(path: impl Into<PathBuf>) -> Result<CsvSource<T>>
    where
        T: TypedPayload + Send + Sync + 'static,
    {
        CsvSourceBuilder::<T>::default()
            .path(path)
            .tab_delimited()
            .build()
    }
}

impl<T> FiniteSourceHandler for CsvSource<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = Some(id);
    }

    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        let writer_id = *self
            .writer_id
            .as_ref()
            .ok_or_else(|| SourceError::Other("WriterId not bound".to_string()))?;

        let items = {
            let mut locked = self
                .state
                .lock()
                .map_err(|_| SourceError::Other("CsvSource mutex poisoned".to_string()))?;
            locked.next_items::<T>()
        }?;

        let Some(items) = items else {
            return Ok(None);
        };

        if items.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let event_type = T::versioned_event_type();
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            let event =
                ChainEventFactory::data_event_from(writer_id, &event_type, &item).map_err(|e| {
                    SourceError::Other(format!(
                        "CsvSource failed to serialize {}: {e}",
                        std::any::type_name::<T>()
                    ))
                })?;
            events.push(event);
        }

        Ok(Some(events))
    }
}

struct CsvReaderState {
    path: PathBuf,
    reader: Reader<File>,
    file_headers: StringRecord,
    decode_headers: StringRecord,
    selected_indices: Option<Vec<usize>>,
    chunk_size: usize,
    skip_rows_remaining: usize,
    row_index: usize,
    warned_schema_drift: bool,
    pending_error: Option<SourceError>,
    done: bool,
}

impl std::fmt::Debug for CsvReaderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvReaderState")
            .field("path", &self.path)
            .field("file_headers_len", &self.file_headers.len())
            .field("decode_headers_len", &self.decode_headers.len())
            .field("selected_indices", &self.selected_indices)
            .field("chunk_size", &self.chunk_size)
            .field("skip_rows_remaining", &self.skip_rows_remaining)
            .field("row_index", &self.row_index)
            .field("warned_schema_drift", &self.warned_schema_drift)
            .field("pending_error", &self.pending_error.as_ref().map(|_| "set"))
            .field("done", &self.done)
            .finish()
    }
}

impl CsvReaderState {
    fn next_items<T: TypedPayload>(&mut self) -> Result<Option<Vec<T>>, SourceError> {
        if let Some(err) = self.pending_error.take() {
            return Err(err);
        }

        if self.done {
            return Ok(None);
        }

        // Apply skip_rows after header consumption (if any).
        while self.skip_rows_remaining > 0 {
            let mut record = StringRecord::new();
            match self.reader.read_record(&mut record) {
                Ok(true) => {
                    self.skip_rows_remaining = self.skip_rows_remaining.saturating_sub(1);
                    self.row_index = self.row_index.saturating_add(1);
                }
                Ok(false) => {
                    self.done = true;
                    return Ok(None);
                }
                Err(e) => {
                    self.done = true;
                    return Err(SourceError::Deserialization(format!(
                        "CSV parse error while skipping rows (file={}): {e}",
                        self.path.display()
                    )));
                }
            }
        }

        let mut batch: Vec<T> = Vec::with_capacity(self.chunk_size);
        while batch.len() < self.chunk_size {
            let mut record = StringRecord::new();
            let read = match self.reader.read_record(&mut record) {
                Ok(read) => read,
                Err(e) => {
                    // Structural CSV errors are not reliably recoverable; emit once and stop.
                    self.done = true;
                    let err = SourceError::Deserialization(format!(
                        "CSV parse error at row {} (file={}): {e}",
                        self.row_index.saturating_add(1),
                        self.path.display()
                    ));
                    if batch.is_empty() {
                        return Err(err);
                    }
                    self.pending_error = Some(err);
                    break;
                }
            };

            if !read {
                self.done = true;
                break;
            }

            self.row_index = self.row_index.saturating_add(1);

            if !self.warned_schema_drift && record.len() != self.file_headers.len() {
                self.warned_schema_drift = true;
                tracing::warn!(
                    file = %self.path.display(),
                    expected_columns = self.file_headers.len(),
                    actual_columns = record.len(),
                    "CSV row column count differs from headers"
                );
            }

            let decode_result: Result<T, csv::Error> = match self.selected_indices.as_ref() {
                None => record.deserialize(Some(&self.decode_headers)),
                Some(indices) => {
                    let mut selected = StringRecord::new();
                    for &idx in indices {
                        selected.push_field(record.get(idx).unwrap_or(""));
                    }
                    selected.deserialize(Some(&self.decode_headers))
                }
            };

            match decode_result {
                Ok(item) => batch.push(item),
                Err(e) => {
                    let err = SourceError::Deserialization(format!(
                        "CSV deserialization error at row {} (file={}): {e}",
                        self.row_index,
                        self.path.display()
                    ));

                    if batch.is_empty() {
                        return Err(err);
                    }

                    // Preserve already-collected items; surface the error next poll.
                    self.pending_error = Some(err);
                    break;
                }
            }
        }

        if batch.is_empty() {
            Ok(None)
        } else {
            Ok(Some(batch))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::stages::FiniteSourceHandler;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn csv_row_source_emits_string_values() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "name,age").unwrap();
        writeln!(tmp, "alice,007").unwrap();

        let mut src = CsvSource::from_file(tmp.path()).expect("source build");
        src.bind_writer_id(WriterId::from(obzenflow_core::StageId::new()));

        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        let payload = batch[0].payload();
        assert_eq!(payload["name"], serde_json::json!("alice"));
        assert_eq!(payload["age"], serde_json::json!("007"));
    }

    #[test]
    fn csv_row_source_supports_explicit_headers_when_file_has_no_headers() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "alice,007").unwrap();

        let mut src = CsvSource::builder()
            .path(tmp.path())
            .has_headers(false)
            .headers(["name", "age"])
            .build()
            .expect("source build");
        src.bind_writer_id(WriterId::from(obzenflow_core::StageId::new()));

        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        let payload = batch[0].payload();
        assert_eq!(payload["name"], serde_json::json!("alice"));
        assert_eq!(payload["age"], serde_json::json!("007"));
    }

    #[test]
    fn csv_row_source_tsv_from_file_uses_tab_delimiter() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "name\tage").unwrap();
        writeln!(tmp, "alice\t007").unwrap();

        let mut src = CsvSource::tsv_from_file(tmp.path()).expect("source build");
        src.bind_writer_id(WriterId::from(obzenflow_core::StageId::new()));

        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        let payload = batch[0].payload();
        assert_eq!(payload["name"], serde_json::json!("alice"));
        assert_eq!(payload["age"], serde_json::json!("007"));
    }
}
