// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The `csv` module holds the finite CSV source.
//!
//! A [`CsvSource`] handler reads records through a file system port. The port
//! records each reply. The handler decodes each recorded record. The handler
//! reports each record that the decoder rejects. The read then continues.

use super::system::{CsvFile, CsvFileSystem, CsvFormat, CsvLine, CsvPage, CsvRead};
use super::version::input_changed;
use anyhow::{bail, Result};
use csv::StringRecord;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceErrorCode};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::stages::common::handlers::source::{SourceRead, TypedFiniteSourceHandler};
use obzenflow_runtime::stages::source::SourceError;
use obzenflow_runtime::systems::Port;
use obzenflow_runtime::typing::SourceTyping;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::path::PathBuf;

/// `CsvRow` is the untyped `csv.row.v1` payload of one CSV record. Each value is a string.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct CsvRow(pub BTreeMap<String, String>);

impl TypedPayload for CsvRow {
    const EVENT_TYPE: &'static str = "csv.row";
    const SCHEMA_VERSION: u32 = 1;
}

/// `CsvRecord` gives a [`CsvDecoder`] access to the fields of one CSV record,
/// by name or by position.
#[derive(Clone, Copy)]
pub struct CsvRecord<'a> {
    headers: &'a StringRecord,
    fields: &'a StringRecord,
}

impl<'a> CsvRecord<'a> {
    fn new(headers: &'a StringRecord, fields: &'a StringRecord) -> Self {
        Self { headers, fields }
    }

    /// `deserialize` decodes this record with serde and matches each field to its column name.
    pub fn deserialize<T>(&self) -> Result<T, CsvDecodeError>
    where
        T: DeserializeOwned,
    {
        self.fields
            .deserialize(Some(self.headers))
            .map_err(CsvDecodeError::from)
    }

    /// `get` returns the field in the column that has the name `column`.
    pub fn get(&self, column: &str) -> Option<&'a str> {
        self.headers
            .iter()
            .position(|header| header == column)
            .and_then(|index| self.fields.get(index))
    }

    /// `field` returns the field at the zero-based position `index`.
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

/// `CsvDecodeError` is the error that a [`CsvDecoder`] returns. It holds only a
/// typed diagnostic. The source handler adds the record index and the line number.
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

    /// `within_field` puts the schema field `name` at the start of the field path of this error.
    pub fn within_field(self, name: &'static str) -> Self {
        Self(self.0.within_field(name))
    }

    pub fn code(self, code: SourceErrorCode) -> Self {
        Self(self.0.code(code))
    }
}

impl From<csv::Error> for CsvDecodeError {
    fn from(error: csv::Error) -> Self {
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

/// `CsvPosition` is the parser position before the next record of a CSV file. A
/// read from this position does not parse the earlier part of the file again.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvPosition {
    byte: u64,
    line: u64,
    record: u64,
    row_index: u64,
    skip_remaining: u64,
}

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

/// `CsvDecoder` maps one CSV record to one value of the domain type `Output`.
/// An application passes its decoder to [`CsvSource::builder`].
pub trait CsvDecoder: Clone + Send + Sync + 'static {
    type Output: TypedPayload + Send + Sync + 'static;

    /// `decode` maps one record to the domain output. The default implementation calls
    /// [`CsvRecord::deserialize`].
    fn decode(&self, record: CsvRecord<'_>) -> Result<Self::Output, CsvDecodeError> {
        record.deserialize()
    }
}

/// `CsvRowDecoder` decodes each record into a [`CsvRow`] and keeps each value as a string.
#[derive(Clone, Copy, Debug, Default)]
pub struct CsvRowDecoder;

impl CsvDecoder for CsvRowDecoder {
    type Output = CsvRow;
}

#[derive(Clone)]
pub struct CsvSourceBuilder<D> {
    decoder: D,
    path: Option<PathBuf>,
    port: Option<Port<dyn CsvFile>>,
    has_headers: Option<bool>,
    headers: Option<Vec<String>>,
    delimiter: Option<u8>,
    chunk_size: usize,
    skip_rows: usize,
    select_columns: Option<Vec<String>>,
}

impl<D> std::fmt::Debug for CsvSourceBuilder<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSourceBuilder")
            .field("decoder", &std::any::type_name::<D>())
            .field("path", &self.path)
            .field("port", &self.port)
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
            port: None,
            has_headers: None,
            headers: None,
            delimiter: None,
            chunk_size: 1000,
            skip_rows: 0,
            select_columns: None,
        }
    }

    pub fn path(mut self, path: impl Into<PathBuf>) -> Self {
        self.path = Some(path.into());
        self
    }

    /// `through` sets the port that the source reads through, instead of a path. The
    /// system of the port supplies the CSV format. `build` fails when the builder also
    /// has a path, a delimiter or header settings.
    pub fn through(mut self, port: Port<dyn CsvFile>) -> Self {
        self.port = Some(port);
        self
    }

    pub fn has_headers(mut self, has_headers: bool) -> Self {
        self.has_headers = Some(has_headers);
        self
    }

    /// `headers` names the columns of a file without a header row. They are
    /// necessary when `has_headers` is `false`.
    pub fn headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.headers = Some(headers.into_iter().map(Into::into).collect());
        self
    }

    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    pub fn tab_delimited(mut self) -> Self {
        self.delimiter = Some(b'\t');
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
        if self.chunk_size == 0 {
            bail!("chunk_size must be positive");
        }
        let format_set =
            self.has_headers.is_some() || self.headers.is_some() || self.delimiter.is_some();
        let port = match (self.path, self.port) {
            (Some(_), Some(_)) => {
                bail!("a CSV source takes a path or a port, and this builder has both")
            }
            (None, None) => bail!("a CSV source must have a path or a port"),
            (None, Some(_)) if format_set => {
                bail!("a CSV source that reads through a port takes its format from the port")
            }
            (None, Some(port)) => port,
            (Some(path), None) => {
                let has_headers = self.has_headers.unwrap_or(true);
                if !has_headers && self.headers.as_ref().is_none_or(|h| h.is_empty()) {
                    bail!("a CSV source must have headers when has_headers is false");
                }
                let format = CsvFormat {
                    delimiter: self.delimiter.unwrap_or(b','),
                    headers: if has_headers { None } else { self.headers },
                };
                Port::deferred(CsvFileSystem::new(path, format))
            }
        };
        Ok(CsvSource {
            port,
            decoder: self.decoder,
            chunk_size: self.chunk_size,
            skip_rows: self.skip_rows,
            select_columns: self.select_columns,
        })
    }
}

/// `CsvSource` reads the records of a CSV file through a cold port of the file
/// system. Its decoder maps each recorded record to the output.
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

    fn read(&self) -> SourceRead<D::Output> {
        let decoder = self.decoder.clone();
        SourceRead::pages(
            self.port.clone(),
            CsvRead {
                limit: std::num::NonZeroUsize::new(self.chunk_size)
                    .expect("build checks that the chunk size is positive"),
                skip_rows: self.skip_rows,
                columns: self.select_columns.clone(),
                after: None,
            },
            |from, page| {
                Ok(match page {
                    CsvPage::EndOfInput => None,
                    CsvPage::Batch {
                        headers,
                        records,
                        next,
                    } => Some((
                        StringRecord::from(headers),
                        records,
                        CsvRead {
                            limit: from.limit,
                            skip_rows: from.skip_rows,
                            columns: from.columns.clone(),
                            after: Some(next),
                        },
                    )),
                })
            },
            move |headers, line| decode_line(&decoder, headers, line),
        )
    }
}

fn decode_line<D: CsvDecoder>(
    decoder: &D,
    headers: &StringRecord,
    line: CsvLine,
) -> Result<D::Output, SourceError> {
    match line {
        CsvLine::Values {
            index,
            line,
            values,
        } => {
            let fields = StringRecord::from(values);
            decoder
                .decode(CsvRecord::new(headers, &fields))
                .map_err(|error| SourceError::Validation(at_line(error.0.record(index), line)))
        }
        CsvLine::Malformed {
            index,
            line,
            reason,
        } => Err(SourceError::Validation(at_line(
            SourceDiagnostic::from(reason).record(index),
            line,
        ))),
        CsvLine::Refused { kind, diagnostic } => Err(SourceError::Terminal { kind, diagnostic }),
        CsvLine::Changed => Err(input_changed()),
    }
}

fn at_line(diagnostic: SourceDiagnostic, line: u64) -> SourceDiagnostic {
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

    fn after_header() -> StringRecord {
        StringRecord::from(vec!["name", "city"])
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
            CsvLine::Malformed {
                index: 3,
                line: 4,
                reason: SourceDiagnosticReason::UnexpectedShape,
            },
        )
        .unwrap_err();
        assert!(
            matches!(malformed, SourceError::Validation(_)),
            "{malformed:?}"
        );
        assert_eq!(malformed.diagnostic().location().record_index(), Some(3));
        assert_eq!(
            malformed.diagnostic().reason(),
            SourceDiagnosticReason::UnexpectedShape
        );
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

    fn tier_headers() -> StringRecord {
        StringRecord::from(vec!["tier"])
    }

    #[test]
    fn unknown_csv_variant_returns_invalid_record() {
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
    fn changed_csv_line_returns_input_changed() {
        let error = decode_line(&CsvRowDecoder, &after_header(), CsvLine::Changed).unwrap_err();
        assert!(error.is_terminal(), "{error:?}");
        assert_eq!(
            error.diagnostic().reason(),
            SourceDiagnosticReason::InputChanged,
            "{error:?}"
        );
    }

    #[test]
    fn through_port_source_needs_no_path() {
        let store: Port<dyn CsvFile> = Port::deferred(
            super::super::system::CsvFileSystem::at("/data/tickets.csv").delimiter(b';'),
        );
        CsvSource::builder(CsvRowDecoder)
            .chunk_size(25)
            .through(store)
            .build()
            .expect("a port supplies the file");

        let other = || -> Port<dyn CsvFile> {
            Port::deferred(super::super::system::CsvFileSystem::at("/data/other.csv"))
        };
        assert!(CsvSource::builder(CsvRowDecoder)
            .path("/data/tickets.csv")
            .through(other())
            .build()
            .is_err());
        assert!(CsvSource::builder(CsvRowDecoder)
            .through(other())
            .tab_delimited()
            .build()
            .is_err());
    }
}
