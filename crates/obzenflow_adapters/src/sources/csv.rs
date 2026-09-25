// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file source
//!
//! Design notes (aligned with the FlowIP decisions):
//! - Sync `TypedFiniteSourceHandler` (blocking file IO inside `next()`)
//! - The runtime adapter owns writer identity and event construction
//! - Malformed rows return `SourceError::Deserialization(..)`; middleware converts to error-marked events
//! - Untyped mode preserves strings (no inference)

use anyhow::{anyhow, bail, Result};
use csv::{Reader, ReaderBuilder, StringRecord};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::source::{
    FiniteSourceConnector, SourceError, SourceReaderInitContext, TypedFiniteSourceHandler,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::path::PathBuf;

/// Untyped CSV row payload (`csv.row.v1`) with string-only values.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct CsvRow(pub BTreeMap<String, String>);

impl TypedPayload for CsvRow {
    const EVENT_TYPE: &'static str = "csv.row";
    const SCHEMA_VERSION: u32 = 1;
}

/// A redacted view of one CSV record presented to a [`CsvDecoder`].
///
/// The view exposes named and positional field access without making the
/// connector's reader state part of the application-facing contract.
#[derive(Clone, Copy)]
pub struct CsvRecord<'a> {
    headers: &'a StringRecord,
    fields: &'a StringRecord,
}

impl<'a> CsvRecord<'a> {
    fn new(headers: &'a StringRecord, fields: &'a StringRecord) -> Self {
        Self { headers, fields }
    }

    /// Deserialize this record using its effective headers.
    pub fn deserialize<T>(&self) -> Result<T, CsvDecodeError>
    where
        T: DeserializeOwned,
    {
        self.fields
            .deserialize(Some(self.headers))
            .map_err(CsvDecodeError::from)
    }

    /// Read a field by its effective column name.
    pub fn get(&self, column: &str) -> Option<&'a str> {
        self.headers
            .iter()
            .position(|header| header == column)
            .and_then(|index| self.fields.get(index))
    }

    /// Read a field by its zero-based position.
    pub fn field(&self, index: usize) -> Option<&'a str> {
        self.fields.get(index)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl std::fmt::Debug for CsvRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvRecord")
            .field("header_count", &self.headers.len())
            .field("field_count", &self.fields.len())
            .finish()
    }
}

/// Error returned by an application-owned [`CsvDecoder`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct CsvDecodeError {
    message: String,
}

impl CsvDecodeError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl From<csv::Error> for CsvDecodeError {
    fn from(error: csv::Error) -> Self {
        // Serde messages can echo rejected field values (for example, an unknown
        // enum variant). Retain only the structural field position.
        match error.kind() {
            csv::ErrorKind::Deserialize { err, .. } => match err.field() {
                Some(field) => Self::new(format!("CSV deserialization failed at field {field}")),
                None => Self::new("CSV deserialization failed"),
            },
            _ => Self::new("CSV record could not be deserialized"),
        }
    }
}

/// User-owned mapping from one external CSV record to one domain output.
///
/// Like typed source handlers, the decoder value owns its output contract
/// through an associated type. Applications pass that value to
/// [`CsvSource::builder`] instead of repeating the event type on the source.
pub trait CsvDecoder: Clone + Send + Sync + 'static {
    type Output: TypedPayload + Send + Sync + 'static;

    /// Decode one record into the declared domain output.
    ///
    /// The default uses serde with the record's effective headers. Override it
    /// when the external CSV shape differs from the domain type.
    fn decode(&self, record: CsvRecord<'_>) -> Result<Self::Output, CsvDecodeError> {
        record.deserialize()
    }
}

/// Built-in decoder for string-preserving [`CsvRow`] output.
#[derive(Clone, Copy, Debug, Default)]
pub struct CsvRowDecoder;

impl CsvDecoder for CsvRowDecoder {
    type Output = CsvRow;
}

#[derive(Clone)]
pub struct CsvSourceBuilder<D> {
    decoder: D,
    path: Option<PathBuf>,
    has_headers: bool,
    headers: Option<Vec<String>>,
    delimiter: u8,
    chunk_size: usize,
    skip_rows: usize,
    select_columns: Option<Vec<String>>,
}

impl<D> std::fmt::Debug for CsvSourceBuilder<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSourceBuilder")
            .field("decoder", &std::any::type_name::<D>())
            .field("path", &self.path)
            .field("has_headers", &self.has_headers)
            .field("headers", &self.headers)
            .field("delimiter", &self.delimiter)
            .field("chunk_size", &self.chunk_size)
            .field("skip_rows", &self.skip_rows)
            .field("select_columns", &self.select_columns)
            .finish_non_exhaustive()
    }
}

impl<D> CsvSourceBuilder<D> {
    pub fn new(decoder: D) -> Self {
        Self {
            decoder,
            path: None,
            has_headers: true,
            headers: None,
            delimiter: b',',
            chunk_size: 1000,
            skip_rows: 0,
            select_columns: None,
        }
    }

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

impl<D> CsvSourceBuilder<D>
where
    D: CsvDecoder,
{
    pub fn build(self) -> Result<CsvSource<D>> {
        let path = self.path.ok_or_else(|| anyhow!("path required"))?;

        if self.chunk_size == 0 {
            bail!("chunk_size must be > 0");
        }

        if !self.has_headers && self.headers.as_ref().is_none_or(|h| h.is_empty()) {
            bail!("headers must be provided when has_headers=false");
        }

        Ok(CsvSource {
            path,
            decoder: self.decoder,
            has_headers: self.has_headers,
            headers: self.headers,
            delimiter: self.delimiter,
            chunk_size: self.chunk_size,
            skip_rows: self.skip_rows,
            select_columns: self.select_columns,
        })
    }
}

/// Cold, reusable CSV configuration. File and header I/O happens in `open`.
#[derive(Clone)]
pub struct CsvSource<D> {
    path: PathBuf,
    decoder: D,
    has_headers: bool,
    headers: Option<Vec<String>>,
    delimiter: u8,
    chunk_size: usize,
    skip_rows: usize,
    select_columns: Option<Vec<String>>,
}

/// One independently owned CSV file cursor, returned by [`CsvSource::open`].
pub struct CsvReader<D> {
    state: CsvReaderState,
    decoder: D,
}

impl<D: CsvDecoder> std::fmt::Debug for CsvReader<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvReader")
            .field("decoder", &std::any::type_name::<D>())
            .field("output", &std::any::type_name::<D::Output>())
            .field("row_index", &self.state.row_index)
            .field("done", &self.state.done)
            .finish_non_exhaustive()
    }
}

impl<D: CsvDecoder> FiniteSourceConnector for CsvSource<D> {
    type Output = D::Output;
    type Reader = CsvReader<D>;

    fn open(&self, _context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        self.open_reader()
    }
}

impl<D: CsvDecoder> CsvSource<D> {
    fn open_reader(&self) -> Result<CsvReader<D>, SourceError> {
        let path = &self.path;
        let file = File::open(path).map_err(|error| {
            SourceError::Transport(format!("Failed to open CSV input: {error}"))
        })?;
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(self.delimiter)
            .from_reader(file);

        let file_headers = if self.has_headers {
            let mut header_record = StringRecord::new();
            let ok = reader.read_record(&mut header_record).map_err(|error| {
                SourceError::Deserialization(format!("Failed to read CSV header: {error}"))
            })?;
            if !ok {
                return Err(SourceError::Validation(
                    "CSV input has no header row".into(),
                ));
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
                        SourceError::Validation(format!(
                            "select_columns references unknown header '{col}'"
                        ))
                    })?;
                    indices.push(idx);
                    selected_headers.push_field(col);
                }
                (Some(indices), selected_headers)
            }
        };

        let state = CsvReaderState {
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
        };

        Ok(CsvReader {
            state,
            decoder: self.decoder.clone(),
        })
    }
}

impl<D> std::fmt::Debug for CsvSource<D>
where
    D: CsvDecoder,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSource")
            .field("decoder", &std::any::type_name::<D>())
            .field("output", &std::any::type_name::<D::Output>())
            .finish_non_exhaustive()
    }
}

impl<D> SourceTyping for CsvSource<D>
where
    D: CsvDecoder,
{
    type Output = D::Output;
}

impl<D> CsvSource<D>
where
    D: CsvDecoder,
{
    pub fn builder(decoder: D) -> CsvSourceBuilder<D> {
        CsvSourceBuilder::new(decoder)
    }

    pub fn from_file(decoder: D, path: impl Into<PathBuf>) -> Result<Self> {
        Self::builder(decoder).path(path).build()
    }

    pub fn tsv_from_file(decoder: D, path: impl Into<PathBuf>) -> Result<Self> {
        Self::builder(decoder).path(path).tab_delimited().build()
    }
}

impl<D: CsvDecoder> TypedFiniteSourceHandler for CsvReader<D> {
    type Output = D::Output;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        self.state.next_items(&self.decoder)
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
    fn next_items<D: CsvDecoder>(
        &mut self,
        decoder: &D,
    ) -> Result<Option<Vec<D::Output>>, SourceError> {
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

        let mut batch: Vec<D::Output> = Vec::with_capacity(self.chunk_size);
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

            let decode_result = match self.selected_indices.as_ref() {
                None => decoder.decode(CsvRecord::new(&self.decode_headers, &record)),
                Some(indices) => {
                    let mut selected = StringRecord::new();
                    for &idx in indices {
                        selected.push_field(record.get(idx).unwrap_or(""));
                    }
                    decoder.decode(CsvRecord::new(&self.decode_headers, &selected))
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
    fn context() -> SourceReaderInitContext {
        SourceReaderInitContext {
            stage_id: obzenflow_core::StageId::new(),
            stage_name: "csv".into(),
            flow_name: "test".into(),
        }
    }
    use super::*;
    use obzenflow_runtime::stages::TypedFiniteSourceHandler;
    use obzenflow_runtime::typing::SourceTyping;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct CustomerName {
        display_name: String,
    }

    impl TypedPayload for CustomerName {
        const EVENT_TYPE: &'static str = "csv.customer_name";
    }

    #[derive(Clone)]
    struct CustomerNameCsv {
        prefix: String,
    }

    impl CsvDecoder for CustomerNameCsv {
        type Output = CustomerName;

        fn decode(&self, record: CsvRecord<'_>) -> Result<Self::Output, CsvDecodeError> {
            let name = record
                .get("name")
                .ok_or_else(|| CsvDecodeError::new("name column is required"))?;
            Ok(CustomerName {
                display_name: format!("{}{}", self.prefix, name.to_uppercase()),
            })
        }
    }

    fn assert_customer_name_output<S>(_source: &S)
    where
        S: SourceTyping<Output = CustomerName>,
    {
    }

    #[test]
    fn csv_row_source_emits_string_values() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "name,age").unwrap();
        writeln!(tmp, "alice,007").unwrap();

        let mut src = CsvSource::from_file(CsvRowDecoder, tmp.path())
            .expect("source build")
            .open(context())
            .expect("source open");
        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        assert_eq!(batch[0].0["name"], "alice");
        assert_eq!(batch[0].0["age"], "007");
    }

    #[test]
    fn csv_row_source_supports_explicit_headers_when_file_has_no_headers() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "alice,007").unwrap();

        let mut src = CsvSource::builder(CsvRowDecoder)
            .path(tmp.path())
            .has_headers(false)
            .headers(["name", "age"])
            .build()
            .expect("source build")
            .open(context())
            .expect("source open");
        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        assert_eq!(batch[0].0["name"], "alice");
        assert_eq!(batch[0].0["age"], "007");
    }

    #[test]
    fn csv_row_source_tsv_from_file_uses_tab_delimiter() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "name\tage").unwrap();
        writeln!(tmp, "alice\t007").unwrap();

        let mut src = CsvSource::tsv_from_file(CsvRowDecoder, tmp.path())
            .expect("source build")
            .open(context())
            .expect("source open");
        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        assert_eq!(batch[0].0["name"], "alice");
        assert_eq!(batch[0].0["age"], "007");
    }

    #[test]
    fn decoder_value_owns_output_and_can_project_external_rows() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "name,ignored").unwrap();
        writeln!(tmp, "alice,external-only").unwrap();

        let source = CsvSource::builder(CustomerNameCsv {
            prefix: "customer:".to_string(),
        })
        .path(tmp.path())
        .build()
        .expect("source build");
        assert_customer_name_output(&source);
        let mut source = source.open(context()).expect("source open");

        let batch = source.next().expect("next").expect("one batch");
        assert_eq!(
            batch,
            vec![CustomerName {
                display_name: "customer:ALICE".to_string(),
            }]
        );
    }

    #[test]
    fn decoder_and_record_debug_are_value_redacted() {
        let builder = CsvSource::builder(CustomerNameCsv {
            prefix: "private-prefix".to_string(),
        });
        let builder_debug = format!("{builder:?}");
        assert!(builder_debug.contains("CustomerNameCsv"));
        assert!(!builder_debug.contains("private-prefix"));

        let headers = StringRecord::from(vec!["name"]);
        let fields = StringRecord::from(vec!["private-name"]);
        let record_debug = format!("{:?}", CsvRecord::new(&headers, &fields));
        assert!(!record_debug.contains("private-name"));
    }

    #[test]
    fn default_decoder_error_does_not_echo_an_unknown_enum_value() {
        #[derive(Debug, Serialize, Deserialize)]
        enum Status {
            Ready,
        }

        #[derive(Debug, Serialize, Deserialize)]
        struct StatusRow {
            status: Status,
        }

        impl TypedPayload for StatusRow {
            const EVENT_TYPE: &'static str = "csv.status";
        }

        #[derive(Clone)]
        struct StatusCsv;

        impl CsvDecoder for StatusCsv {
            type Output = StatusRow;
        }

        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "status\nSECRET_SENTINEL").unwrap();
        let mut reader = CsvSource::from_file(StatusCsv, input.path())
            .unwrap()
            .open(context())
            .unwrap();
        let error = reader.next().unwrap_err();
        assert!(matches!(error, SourceError::Deserialization(_)));
        assert!(error.to_string().contains("row 1"));
        assert!(error.to_string().contains("CSV deserialization failed"));
        assert!(!format!("{error} {error:?}").contains("SECRET_SENTINEL"));
        assert!(reader.next().unwrap().is_none());
    }
}
