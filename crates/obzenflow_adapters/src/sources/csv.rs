// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file source
//!
//! Design notes (aligned with the FlowIP decisions):
//! - Sync `PositionedFiniteReader` (blocking file IO inside `next()`); each
//!   record carries the parser position after it (FLOWIP-134i)
//! - The runtime adapter owns writer identity and event construction
//! - Rejected rows return `SourceError::Validation` and reading continues;
//!   only an I/O failure is terminal (FLOWIP-084n B8)
//! - Untyped mode preserves strings (no inference)

use super::file_version::FileVersion;
use anyhow::{anyhow, bail, Result};
use csv::{Reader, ReaderBuilder, StringRecord};
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::source::{
    FiniteSourceConnector, PositionedFiniteReader, Seek, SeekableSource, SourceError,
    SourceInputIdentity, SourcePosition, SourceReaderInitContext, SourceRecord,
};
use obzenflow_runtime::typing::SourceTyping;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::File;
use std::num::NonZeroU32;
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

/// Error returned by an application-owned [`CsvDecoder`]. It carries a typed
/// diagnostic only; the reader adds the record index and line.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct CsvDecodeError(SourceDiagnostic);

impl CsvDecodeError {
    pub fn missing_field(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::MissingField).within_field(field))
    }

    pub fn invalid_value(field: &'static str) -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidValue).within_field(field))
    }

    pub fn invalid_record() -> Self {
        Self(SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord))
    }

    /// Nests the path under a schema-declared parent field.
    pub fn within_field(self, name: &'static str) -> Self {
        Self(self.0.within_field(name))
    }

    pub fn code(self, code: SourceErrorCode) -> Self {
        Self(self.0.code(code))
    }

    fn locate(self, record_index: u64, position: Option<&csv::Position>) -> SourceDiagnostic {
        located(self.0.record(record_index), position)
    }
}

impl From<csv::Error> for CsvDecodeError {
    fn from(error: csv::Error) -> Self {
        // Serde text can echo rejected values, so only the kind and column survive.
        // Missing and unknown fields both arrive as message text: `InvalidRecord`.
        let csv::ErrorKind::Deserialize { err, .. } = error.kind() else {
            return Self::invalid_record();
        };
        let reason = match err.kind() {
            csv::DeserializeErrorKind::ParseBool(_)
            | csv::DeserializeErrorKind::ParseInt(_)
            | csv::DeserializeErrorKind::ParseFloat(_)
            | csv::DeserializeErrorKind::InvalidUtf8(_) => SourceDiagnosticReason::InvalidValue,
            csv::DeserializeErrorKind::UnexpectedEndOfRow => SourceDiagnosticReason::MissingField,
            csv::DeserializeErrorKind::Message(_) | csv::DeserializeErrorKind::Unsupported(_) => {
                SourceDiagnosticReason::InvalidRecord
            }
        };
        let diagnostic = SourceDiagnostic::new(reason);
        match err.field().and_then(|column| u32::try_from(column).ok()) {
            Some(column) => Self(diagnostic.within_index(column)),
            None => Self(diagnostic),
        }
    }
}

fn located(diagnostic: SourceDiagnostic, position: Option<&csv::Position>) -> SourceDiagnostic {
    match position
        .and_then(|position| u32::try_from(position.line()).ok())
        .and_then(NonZeroU32::new)
    {
        Some(line) => diagnostic.position(line, None),
        None => diagnostic,
    }
}

/// csv 1.4 raises `Utf8` and `UnequalLengths` after passing the record, so the
/// reader stays synchronized. An I/O failure makes the crate report end of
/// input, so continuing would certify an unread suffix (B8).
fn read_failure(error: &csv::Error, record_index: u64) -> SourceError {
    let diagnostic = |reason| {
        located(
            SourceDiagnostic::new(reason).record(record_index),
            error.position(),
        )
    };
    match error.kind() {
        csv::ErrorKind::Utf8 { .. } => {
            SourceError::Validation(diagnostic(SourceDiagnosticReason::MalformedInput))
        }
        csv::ErrorKind::UnequalLengths { .. } => {
            SourceError::Validation(diagnostic(SourceDiagnosticReason::UnexpectedShape))
        }
        csv::ErrorKind::Io(_) => SourceError::Terminal {
            kind: SourcePollErrorKind::Transport,
            diagnostic: diagnostic(SourceDiagnosticReason::InputUnavailable),
        },
        _ => SourceError::Terminal {
            kind: SourcePollErrorKind::Other,
            diagnostic: diagnostic(SourceDiagnosticReason::Unclassified),
        },
    }
}

/// The parser position before a CSV reader's next record, with the reader
/// state a seek restores (FLOWIP-134i B1). Seeking reparses no prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvPosition {
    byte: u64,
    line: u64,
    record: u64,
    row_index: u64,
    skip_remaining: u64,
}

impl SourcePosition for CsvPosition {}

impl CsvPosition {
    fn at(parser: &csv::Position, row_index: usize, skip_remaining: usize) -> Self {
        Self {
            byte: parser.byte(),
            line: parser.line(),
            record: parser.record(),
            row_index: u64::try_from(row_index).unwrap_or(u64::MAX),
            skip_remaining: u64::try_from(skip_remaining).unwrap_or(u64::MAX),
        }
    }

    fn parser(&self) -> csv::Position {
        let mut parser = csv::Position::new();
        parser
            .set_byte(self.byte)
            .set_line(self.line)
            .set_record(self.record);
        parser
    }
}

/// What decides a CSV source's record boundaries and order (FLOWIP-134i B9).
#[derive(Serialize)]
struct CsvIdentityParts<'a> {
    file: FileVersion,
    format: CsvFormat<'a>,
    selection: CsvSelection<'a>,
}

#[derive(Serialize)]
struct CsvFormat<'a> {
    has_headers: bool,
    headers: Option<&'a [String]>,
    delimiter: u8,
}

#[derive(Serialize)]
struct CsvSelection<'a> {
    skip_rows: usize,
    columns: Option<&'a [String]>,
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
    fn new(decoder: D) -> Self {
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
    identity: SourceInputIdentity,
    beginning: CsvPosition,
    version: FileVersion,
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
    type Continuation = Seek<CsvPosition>;

    fn open(&self, _context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        self.open_reader()
    }
}

impl<D: CsvDecoder> CsvSource<D> {
    fn open_reader(&self) -> Result<CsvReader<D>, SourceError> {
        let file = File::open(&self.path)
            .map_err(|_| SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into()))?;
        let version = FileVersion::of(&file)?;
        let identity = SourceInputIdentity::new(
            "csv",
            &CsvIdentityParts {
                file: version,
                format: CsvFormat {
                    has_headers: self.has_headers,
                    headers: self.headers.as_deref(),
                    delimiter: self.delimiter,
                },
                selection: CsvSelection {
                    skip_rows: self.skip_rows,
                    columns: self.select_columns.as_deref(),
                },
            },
        )
        .map_err(|_| SourceError::Other(SourceDiagnosticReason::Unclassified.into()))?;
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(self.delimiter)
            .from_reader(file);

        let file_headers = if self.has_headers {
            let mut header_record = StringRecord::new();
            let ok =
                reader
                    .read_record(&mut header_record)
                    .map_err(|error| match error.kind() {
                        csv::ErrorKind::Io(_) => {
                            SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into())
                        }
                        _ => SourceError::Deserialization(located(
                            SourceDiagnosticReason::MalformedInput.into(),
                            error.position(),
                        )),
                    })?;
            if !ok {
                return Err(SourceError::Validation(
                    SourceDiagnosticReason::UnexpectedShape.into(),
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
                    let idx = file_headers.iter().position(|h| h == col).ok_or(
                        SourceError::Validation(SourceDiagnosticReason::SelectionNotFound.into()),
                    )?;
                    indices.push(idx);
                    selected_headers.push_field(col);
                }
                (Some(indices), selected_headers)
            }
        };

        let beginning = CsvPosition::at(reader.position(), 0, self.skip_rows);
        let state = CsvReaderState {
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
            identity,
            beginning,
            version,
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
}

impl<D: CsvDecoder> SeekableSource for CsvReader<D> {
    type Position = CsvPosition;

    fn input_identity(&self) -> &SourceInputIdentity {
        &self.identity
    }

    fn beginning(&self) -> CsvPosition {
        self.beginning.clone()
    }

    fn seek(&mut self, position: CsvPosition) -> Result<(), SourceError> {
        self.state.seek(&position)
    }
}

/// One poll's records, each with the parser position after it.
type CsvPoll<T> = Result<Option<Vec<SourceRecord<T, CsvPosition>>>, SourceError>;

impl<D: CsvDecoder> PositionedFiniteReader for CsvReader<D> {
    type Output = D::Output;

    /// Nothing read from a changed file is published (FLOWIP-134i B10).
    fn next(&mut self) -> CsvPoll<Self::Output> {
        let batch = self.state.next_records(&self.decoder);
        self.version.verify(self.state.reader.get_ref())?;
        batch
    }
}

struct CsvReaderState {
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
    /// Continue strictly after `position`, restoring the row count and any
    /// rows still to skip.
    fn seek(&mut self, position: &CsvPosition) -> Result<(), SourceError> {
        self.reader
            .seek(position.parser())
            .map_err(|_| SourceError::Transport(SourceDiagnosticReason::InputUnavailable.into()))?;
        self.row_index = usize::try_from(position.row_index).unwrap_or(usize::MAX);
        self.skip_rows_remaining = usize::try_from(position.skip_remaining).unwrap_or(usize::MAX);
        self.pending_error = None;
        self.done = false;
        Ok(())
    }

    /// Each valid record's position is captured before the next read, so a
    /// deferred rejection never advances it.
    fn next_records<D: CsvDecoder>(&mut self, decoder: &D) -> CsvPoll<D::Output> {
        if let Some(err) = self.pending_error.take() {
            return Err(err);
        }

        if self.done {
            return Ok(None);
        }

        // Apply skip_rows after header consumption (if any). A skipped row needs
        // no decoding, so only a terminal read failure surfaces here.
        while self.skip_rows_remaining > 0 {
            let mut record = StringRecord::new();
            match self.reader.read_record(&mut record) {
                Ok(true) => {}
                Ok(false) => {
                    self.done = true;
                    return Ok(None);
                }
                Err(error) => {
                    let failure = read_failure(&error, self.record_index());
                    if failure.is_terminal() {
                        self.done = true;
                        return Err(failure);
                    }
                }
            }
            self.skip_rows_remaining = self.skip_rows_remaining.saturating_sub(1);
            self.row_index = self.row_index.saturating_add(1);
        }

        let mut batch = Vec::with_capacity(self.chunk_size);
        while batch.len() < self.chunk_size {
            let mut record = StringRecord::new();
            let read = match self.reader.read_record(&mut record) {
                Ok(read) => read,
                Err(error) => {
                    let failure = read_failure(&error, self.record_index());
                    if failure.is_terminal() {
                        self.done = true;
                    } else {
                        self.row_index = self.row_index.saturating_add(1);
                    }
                    if batch.is_empty() {
                        return Err(failure);
                    }
                    // Preserve already-collected items; surface the error next poll.
                    self.pending_error = Some(failure);
                    break;
                }
            };

            if !read {
                self.done = true;
                break;
            }

            let record_index = self.record_index();
            self.row_index = self.row_index.saturating_add(1);
            let after = CsvPosition::at(self.reader.position(), self.row_index, 0);

            if !self.warned_schema_drift && record.len() != self.file_headers.len() {
                self.warned_schema_drift = true;
                tracing::warn!(
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
                Ok(value) => batch.push(SourceRecord { value, after }),
                Err(error) => {
                    let rejection =
                        SourceError::Validation(error.locate(record_index, record.position()));
                    if batch.is_empty() {
                        return Err(rejection);
                    }

                    // Preserve already-collected items; surface the error next poll.
                    self.pending_error = Some(rejection);
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

    /// Zero-based index of the next record after the header, counting skipped rows.
    fn record_index(&self) -> u64 {
        u64::try_from(self.row_index).unwrap_or(u64::MAX)
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
                .ok_or_else(|| CsvDecodeError::missing_field("name"))?;
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

        let mut src = CsvSource::builder(CsvRowDecoder)
            .path(tmp.path())
            .build()
            .expect("source build")
            .open(context())
            .expect("source open");
        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        assert_eq!(batch[0].value.0["name"], "alice");
        assert_eq!(batch[0].value.0["age"], "007");
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

        assert_eq!(batch[0].value.0["name"], "alice");
        assert_eq!(batch[0].value.0["age"], "007");
    }

    #[test]
    fn csv_row_source_tsv_from_file_uses_tab_delimiter() {
        let mut tmp = NamedTempFile::new().expect("temp file");
        writeln!(tmp, "name\tage").unwrap();
        writeln!(tmp, "alice\t007").unwrap();

        let mut src = CsvSource::builder(CsvRowDecoder)
            .path(tmp.path())
            .tab_delimited()
            .build()
            .expect("source build")
            .open(context())
            .expect("source open");
        let batch = src.next().expect("next").expect("should have one batch");
        assert_eq!(batch.len(), 1);

        assert_eq!(batch[0].value.0["name"], "alice");
        assert_eq!(batch[0].value.0["age"], "007");
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
        let names: Vec<_> = batch.into_iter().map(|record| record.value).collect();
        assert_eq!(
            names,
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
        let mut reader = CsvSource::builder(StatusCsv)
            .path(input.path())
            .build()
            .unwrap()
            .open(context())
            .unwrap();
        let error = reader.next().unwrap_err();
        assert!(matches!(error, SourceError::Validation(_)));
        let location = error.diagnostic().location();
        assert_eq!(location.record_index(), Some(0));
        assert_eq!(location.position().map(|p| p.line.get()), Some(2));
        assert_eq!(
            error.diagnostic().reason(),
            SourceDiagnosticReason::InvalidRecord
        );
        assert!(!format!("{error} {error:?}").contains("SECRET_SENTINEL"));
        assert!(reader.next().unwrap().is_none());
    }

    fn drain_reader<D: CsvDecoder>(
        reader: &mut CsvReader<D>,
    ) -> (Vec<D::Output>, Vec<SourceError>) {
        let mut items = Vec::new();
        let mut errors = Vec::new();
        loop {
            match reader.next() {
                Ok(Some(batch)) => items.extend(batch.into_iter().map(|record| record.value)),
                Ok(None) => return (items, errors),
                Err(error) => {
                    assert!(!error.is_terminal(), "unexpected terminal error: {error}");
                    errors.push(error);
                }
            }
        }
    }

    #[test]
    fn record_local_read_errors_are_rejected_and_later_rows_survive() {
        let mut input = NamedTempFile::new().unwrap();
        input.write_all(b"name,age\n").unwrap();
        input.write_all(b"alice,1\n").unwrap();
        input.write_all(b"short\n").unwrap();
        input.write_all(b"bad\xff,2\n").unwrap();
        input.write_all(b"carol,3\n").unwrap();

        for chunk_size in [1, 25] {
            let mut reader = CsvSource::builder(CsvRowDecoder)
                .path(input.path())
                .chunk_size(chunk_size)
                .build()
                .unwrap()
                .open(context())
                .unwrap();
            let (items, errors) = drain_reader(&mut reader);
            let names: Vec<_> = items.iter().map(|row| row.0["name"].clone()).collect();
            assert_eq!(names, ["alice", "carol"], "chunk size {chunk_size}");

            let rejections: Vec<_> = errors
                .iter()
                .map(|error| {
                    let diagnostic = error.diagnostic();
                    (
                        error.kind(),
                        diagnostic.reason(),
                        diagnostic.location().record_index(),
                        diagnostic.location().position().map(|p| p.line.get()),
                    )
                })
                .collect();
            assert_eq!(
                rejections,
                [
                    (
                        SourcePollErrorKind::Validation,
                        SourceDiagnosticReason::UnexpectedShape,
                        Some(1),
                        Some(3)
                    ),
                    (
                        SourcePollErrorKind::Validation,
                        SourceDiagnosticReason::MalformedInput,
                        Some(2),
                        Some(4)
                    ),
                ],
                "chunk size {chunk_size}"
            );
        }
    }

    #[test]
    fn io_failure_mid_read_is_terminal_input_unavailable() {
        let error = csv::Error::from(std::io::Error::other("disk"));
        let failure = read_failure(&error, 7);
        assert!(failure.is_terminal());
        assert_eq!(failure.kind(), SourcePollErrorKind::Transport);
        assert_eq!(
            failure.diagnostic().reason(),
            SourceDiagnosticReason::InputUnavailable
        );
        assert_eq!(failure.diagnostic().location().record_index(), Some(7));
        assert!(!failure.to_string().contains("disk"));
    }

    #[test]
    fn opening_failures_carry_typed_reasons_without_paths() {
        let missing = CsvSource::builder(CsvRowDecoder)
            .path("/nonexistent/obzenflow/secret-dir/input.csv")
            .build()
            .unwrap()
            .open(context())
            .unwrap_err();
        assert_eq!(missing.kind(), SourcePollErrorKind::Transport);
        assert_eq!(
            missing.diagnostic().reason(),
            SourceDiagnosticReason::InputUnavailable
        );
        assert!(!format!("{missing} {missing:?}").contains("secret-dir"));

        let mut bad_header = NamedTempFile::new().unwrap();
        bad_header.write_all(b"na\xffme,age\n").unwrap();
        let header = CsvSource::builder(CsvRowDecoder)
            .path(bad_header.path())
            .build()
            .unwrap()
            .open(context())
            .unwrap_err();
        assert_eq!(header.kind(), SourcePollErrorKind::Deserialization);
        assert_eq!(
            header.diagnostic().reason(),
            SourceDiagnosticReason::MalformedInput
        );

        let mut good = NamedTempFile::new().unwrap();
        writeln!(good, "name,age").unwrap();
        let unknown_column = CsvSource::builder(CsvRowDecoder)
            .path(good.path())
            .select_columns(["secret_column"])
            .build()
            .unwrap()
            .open(context())
            .unwrap_err();
        assert_eq!(
            unknown_column.diagnostic().reason(),
            SourceDiagnosticReason::SelectionNotFound
        );
        assert!(!format!("{unknown_column} {unknown_column:?}").contains("secret_column"));
    }

    #[test]
    fn typed_parse_failures_name_the_column_and_line() {
        #[derive(Debug, Serialize, Deserialize)]
        struct AgeRow {
            name: String,
            age: u32,
        }

        impl TypedPayload for AgeRow {
            const EVENT_TYPE: &'static str = "csv.age";
        }

        #[derive(Clone)]
        struct AgeCsv;

        impl CsvDecoder for AgeCsv {
            type Output = AgeRow;
        }

        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "name,age\nalice,not-a-number").unwrap();
        let mut reader = CsvSource::builder(AgeCsv)
            .path(input.path())
            .build()
            .unwrap()
            .open(context())
            .unwrap();
        let error = reader.next().unwrap_err();
        let diagnostic = error.diagnostic();
        assert_eq!(diagnostic.reason(), SourceDiagnosticReason::InvalidValue);
        assert_eq!(
            diagnostic.location().field_path(),
            [obzenflow_core::event::FieldSegment::Index(1)]
        );
        assert_eq!(
            diagnostic.location().position().map(|p| p.line.get()),
            Some(2)
        );
        assert!(!format!("{error} {error:?}").contains("not-a-number"));
    }

    fn people() -> NamedTempFile {
        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "name,age\nalice,1\nbob,2\ncarol,3\ndave,4").unwrap();
        input
    }

    fn read_all(reader: &mut CsvReader<CsvRowDecoder>) -> Vec<SourceRecord<CsvRow, CsvPosition>> {
        let mut records = Vec::new();
        while let Some(batch) = reader.next().expect("valid input") {
            records.extend(batch);
        }
        records
    }

    fn names(records: &[SourceRecord<CsvRow, CsvPosition>]) -> Vec<&str> {
        records
            .iter()
            .map(|record| record.value.0["name"].as_str())
            .collect()
    }

    #[test]
    fn seeking_after_any_record_continues_strictly_after_it() {
        let input = people();
        let source = CsvSource::builder(CsvRowDecoder)
            .path(input.path())
            .chunk_size(3)
            .build()
            .unwrap();
        let first = read_all(&mut source.open(context()).unwrap());
        assert_eq!(names(&first), ["alice", "bob", "carol", "dave"]);

        // Positions do not depend on which batch carried the record.
        for (index, record) in first.iter().enumerate() {
            let mut resumed = source.open(context()).unwrap();
            resumed.seek(record.after.clone()).unwrap();
            assert_eq!(names(&read_all(&mut resumed)), names(&first[index + 1..]));
        }
    }

    #[test]
    fn the_beginning_reapplies_skipped_rows_and_record_indices() {
        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "name,age\npreamble,0\nalice,1\nshort\nbob,2").unwrap();
        let source = CsvSource::builder(CsvRowDecoder)
            .path(input.path())
            .skip_rows(1)
            .build()
            .unwrap();
        let mut reader = source.open(context()).unwrap();
        let beginning = reader.beginning();
        let (fresh, fresh_errors) = drain_reader(&mut reader);

        let mut resumed = source.open(context()).unwrap();
        resumed.seek(beginning).unwrap();
        let (again, again_errors) = drain_reader(&mut resumed);
        assert_eq!(again, fresh);
        let indices = |errors: &[SourceError]| -> Vec<Option<u64>> {
            errors
                .iter()
                .map(|error| error.diagnostic().location().record_index())
                .collect()
        };
        assert_eq!(indices(&fresh_errors), [Some(2)]);
        assert_eq!(indices(&again_errors), indices(&fresh_errors));
    }

    #[test]
    fn a_rejection_after_the_last_accepted_record_is_reported_again() {
        let mut input = NamedTempFile::new().unwrap();
        writeln!(input, "name,age\nalice,1\nbob,2\nshort\ndave,4").unwrap();
        let source = CsvSource::builder(CsvRowDecoder)
            .path(input.path())
            .build()
            .unwrap();
        let mut reader = source.open(context()).unwrap();
        let accepted = reader.next().unwrap().unwrap();
        assert_eq!(names(&accepted), ["alice", "bob"]);

        let mut resumed = source.open(context()).unwrap();
        resumed.seek(accepted[1].after.clone()).unwrap();
        let rejection = resumed.next().unwrap_err();
        assert_eq!(rejection.diagnostic().location().record_index(), Some(2));
        assert_eq!(names(&resumed.next().unwrap().unwrap()), ["dave"]);
    }

    #[test]
    fn identity_names_the_part_that_changed() {
        let input = people();
        let open = |builder: CsvSourceBuilder<CsvRowDecoder>| {
            builder
                .path(input.path())
                .build()
                .unwrap()
                .open(context())
                .unwrap()
        };
        let pinned = open(CsvSource::builder(CsvRowDecoder));
        let changed = |reader: CsvReader<CsvRowDecoder>| {
            reader
                .input_identity()
                .changed_part(pinned.input_identity())
        };

        // Batch size never changes which records are admitted.
        assert_eq!(
            changed(open(CsvSource::builder(CsvRowDecoder).chunk_size(1))),
            None
        );
        assert_eq!(
            changed(open(CsvSource::builder(CsvRowDecoder).skip_rows(1))).as_deref(),
            Some("selection")
        );
        assert_eq!(
            changed(open(CsvSource::builder(CsvRowDecoder).delimiter(b';'))).as_deref(),
            Some("format")
        );

        let mut appended = std::fs::OpenOptions::new()
            .append(true)
            .open(input.path())
            .unwrap();
        writeln!(appended, "eve,5").unwrap();
        assert_eq!(
            changed(open(CsvSource::builder(CsvRowDecoder))).as_deref(),
            Some("file")
        );
    }

    #[test]
    fn a_file_changed_mid_read_fails_closed_without_its_batch() {
        let input = people();
        let mut reader = CsvSource::builder(CsvRowDecoder)
            .path(input.path())
            .chunk_size(1)
            .build()
            .unwrap()
            .open(context())
            .unwrap();
        assert_eq!(names(&reader.next().unwrap().unwrap()), ["alice"]);

        let mut appended = std::fs::OpenOptions::new()
            .append(true)
            .open(input.path())
            .unwrap();
        writeln!(appended, "eve,5").unwrap();
        let error = reader.next().unwrap_err();
        assert!(error.is_terminal());
        assert_eq!(
            error.diagnostic().reason(),
            SourceDiagnosticReason::InputChanged
        );
    }
}
