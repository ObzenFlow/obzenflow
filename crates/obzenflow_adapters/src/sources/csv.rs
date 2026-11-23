// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file source
//!
//! A CSV source handler reads records through a `file-system` port. The port
//! records each reply, and the handler decodes each recorded record. A row
//! that the decoder refuses is reported, and the read continues.

use super::file_system::{CsvFile, CsvFormat, CsvLine, CsvRead, FileSystem};
use super::file_version::input_changed;
use anyhow::{anyhow, bail, Result};
use csv::StringRecord;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::Port;
use obzenflow_runtime::stages::common::handlers::source::{SourceRead, TypedFiniteSourceHandler};
use obzenflow_runtime::stages::source::{SourceError, SourcePosition};
use obzenflow_runtime::typing::SourceTyping;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
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

/// The parser position before the next record of a CSV file. Seeking reparses
/// no prefix.
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
    pub(crate) fn at(parser: &csv::Position, row_index: usize, skip_remaining: usize) -> Self {
        Self {
            byte: parser.byte(),
            line: parser.line(),
            record: parser.record(),
            row_index: u64::try_from(row_index).unwrap_or(u64::MAX),
            skip_remaining: u64::try_from(skip_remaining).unwrap_or(u64::MAX),
        }
    }

    pub(crate) fn row_index(&self) -> u64 {
        self.row_index
    }

    pub(crate) fn parser(&self) -> csv::Position {
        let mut parser = csv::Position::new();
        parser
            .set_byte(self.byte)
            .set_line(self.line)
            .set_record(self.record);
        parser
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

        let format = CsvFormat {
            delimiter: self.delimiter,
            headers: if self.has_headers { None } else { self.headers },
        };
        Ok(CsvSource {
            port: FileSystem::csv_port(path, format)?,
            decoder: self.decoder,
            chunk_size: self.chunk_size,
            skip_rows: self.skip_rows,
            select_columns: self.select_columns,
        })
    }
}

/// A CSV source handler. It reads records through a cold `file-system` port
/// and maps each recorded record to its output with its decoder.
#[derive(Clone)]
pub struct CsvSource<D> {
    port: Port<dyn CsvFile>,
    decoder: D,
    chunk_size: usize,
    skip_rows: usize,
    select_columns: Option<Vec<String>>,
}

impl<D: CsvDecoder> TypedFiniteSourceHandler for CsvSource<D> {
    type Output = D::Output;

    fn read(&self) -> Option<SourceRead<D::Output>> {
        let decoder = self.decoder.clone();
        Some(SourceRead::finite_in(
            self.port.clone(),
            CsvRead::first(self.chunk_size, self.skip_rows, self.select_columns.clone()),
            move |read: &CsvRead, line: CsvLine| decode_line(&decoder, read, line),
        ))
    }
}

fn decode_line<D: CsvDecoder>(
    decoder: &D,
    read: &CsvRead,
    line: CsvLine,
) -> Result<D::Output, SourceError> {
    match line {
        CsvLine::Values {
            index,
            line,
            values,
        } => {
            let headers = StringRecord::from(read.headers().to_vec());
            let fields = StringRecord::from(values);
            decoder
                .decode(CsvRecord::new(&headers, &fields))
                .map_err(|error| SourceError::Validation(located_line(error.0.record(index), line)))
        }
        CsvLine::Malformed { index, line } => Err(SourceError::Validation(located_line(
            SourceDiagnostic::from(SourceDiagnosticReason::MalformedInput).record(index),
            line,
        ))),
        CsvLine::Refused { kind, diagnostic } => Err(SourceError::Terminal { kind, diagnostic }),
        CsvLine::Changed => Err(input_changed()),
    }
}

fn located_line(diagnostic: SourceDiagnostic, line: u64) -> SourceDiagnostic {
    match u32::try_from(line).ok().and_then(NonZeroU32::new) {
        Some(line) => diagnostic.position(line, None),
        None => diagnostic,
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
            .field("chunk_size", &self.chunk_size)
            .finish_non_exhaustive()
    }
}

impl<D> SourceTyping for CsvSource<D>
where
    D: CsvDecoder,
{
    type Output = D::Output;
}

impl<D: CsvDecoder> CsvSource<D> {
    pub fn builder(decoder: D) -> CsvSourceBuilder<D> {
        CsvSourceBuilder::new(decoder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct Refuses;

    impl CsvDecoder for Refuses {
        type Output = CsvRow;

        fn decode(&self, _record: CsvRecord<'_>) -> Result<Self::Output, CsvDecodeError> {
            Err(CsvDecodeError::missing_field("customer_id"))
        }
    }

    fn after_header() -> CsvRead {
        serde_json::from_value(serde_json::json!({
            "chunk": 1, "skip_rows": 0, "columns": null,
            "at": {
                "position": {"byte": 0, "line": 1, "record": 0, "row_index": 0, "skip_remaining": 0},
                "version": {"inode": 0, "size": 0, "modified": [0, 0], "changed": [0, 0]},
                "headers": ["name", "city"], "selected": null
            }
        }))
        .unwrap()
    }

    fn values(fields: &[&str]) -> CsvLine {
        CsvLine::Values {
            index: 2,
            line: 3,
            values: fields.iter().map(|field| (*field).to_owned()).collect(),
        }
    }

    #[test]
    fn csv_line_decoded_with_recorded_headers() {
        let row = decode_line(&CsvRowDecoder, &after_header(), values(&["Ada", "London"])).unwrap();
        assert_eq!(row.0.get("city").map(String::as_str), Some("London"));
    }

    #[test]
    fn csv_line_decoder_rejection_returns_validation() {
        let error = decode_line(&Refuses, &after_header(), values(&["Ada", "London"])).unwrap_err();
        assert!(matches!(error, SourceError::Validation(_)), "{error:?}");
        assert_eq!(error.diagnostic().location().record_index(), Some(2));
        let malformed = decode_line(
            &CsvRowDecoder,
            &after_header(),
            CsvLine::Malformed { index: 3, line: 4 },
        )
        .unwrap_err();
        assert!(
            matches!(malformed, SourceError::Validation(_)),
            "{malformed:?}"
        );
        assert_eq!(malformed.diagnostic().location().record_index(), Some(3));
    }

    #[derive(Debug, Serialize, Deserialize)]
    enum Tier {
        Gold,
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct TierRow {
        tier: Tier,
    }

    impl TypedPayload for TierRow {
        const EVENT_TYPE: &'static str = "test.tier_row";
    }

    #[derive(Clone)]
    struct TierCsv;

    impl CsvDecoder for TierCsv {
        type Output = TierRow;
    }

    fn tier_headers() -> CsvRead {
        let mut read = serde_json::to_value(after_header()).unwrap();
        read["at"]["headers"] = serde_json::json!(["tier"]);
        serde_json::from_value(read).unwrap()
    }

    #[test]
    fn csv_line_unknown_variant_returns_invalid_record() {
        let error = decode_line(&TierCsv, &tier_headers(), values(&["Platinum"])).unwrap_err();
        assert!(matches!(error, SourceError::Validation(_)), "{error:?}");
        assert_eq!(
            error.diagnostic().reason(),
            SourceDiagnosticReason::InvalidRecord,
            "{error:?}"
        );
        assert!(!format!("{error} {error:?}").contains("Platinum"));
    }

    #[test]
    fn csv_line_changed_file_returns_input_changed() {
        let error = decode_line(&CsvRowDecoder, &after_header(), CsvLine::Changed).unwrap_err();
        assert!(error.is_terminal(), "{error:?}");
        assert_eq!(
            error.diagnostic().reason(),
            SourceDiagnosticReason::InputChanged,
            "{error:?}"
        );
    }
}
