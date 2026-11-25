// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::csv::CsvPosition;
use super::version::FileVersion;
use super::yaml::{read_yaml_page, YamlPage, YamlRead};
use async_trait::async_trait;
use csv::{ReaderBuilder, StringRecord};
use obzenflow_adapters::sinks::blocking::blocking_io;
use obzenflow_core::event::payloads::execution_payload::{
    SourcePollContinuation, SourcePollErrorKind,
};
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason, SourceReadFailure};
use obzenflow_runtime::effects::{EffectError, EffectPortResolutionError};
use obzenflow_runtime::queries::{FiniteRead, QueryBookmark, QueryContext, QueryError};
use obzenflow_runtime::systems::{Port, SystemConfig};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::Read;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};

/// `FileSystem` is the framework system that reads a file at a configured path.
pub struct FileSystem {
    path: PathBuf,
    format: FileFormat,
}

#[derive(Clone)]
enum FileFormat {
    Csv(CsvFormat),
    Yaml,
}

#[derive(Clone)]
pub(crate) struct CsvFormat {
    pub(crate) delimiter: u8,
    pub(crate) headers: Option<Vec<String>>,
}

/// `CsvFileSystem` configures a [`FileSystem`] to read a CSV or TSV file at a path.
#[derive(Clone)]
pub struct CsvFileSystem {
    path: PathBuf,
    format: CsvFormat,
}

impl CsvFileSystem {
    /// `CsvFileSystem::at` configures the system for the CSV file at `path`. By default,
    /// the first row of the file names the columns.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            format: CsvFormat {
                delimiter: b',',
                headers: None,
            },
        }
    }

    /// `delimiter` sets the byte between the fields of a record.
    pub fn delimiter(mut self, delimiter: u8) -> Self {
        self.format.delimiter = delimiter;
        self
    }

    /// `headers` names the columns. Each row of the file is then a record.
    pub fn headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.format.headers = Some(headers.into_iter().map(Into::into).collect());
        self
    }

    /// `port` returns a cold port for this file. Its system reads the file only
    /// during a live read.
    pub fn port(self) -> Port<dyn CsvFile> {
        SystemConfig::port(self)
    }

    pub(crate) fn new(path: PathBuf, format: CsvFormat) -> Self {
        Self { path, format }
    }
}

impl SystemConfig for CsvFileSystem {
    type System = FileSystem;

    fn resolve(&self) -> Result<FileSystem, EffectPortResolutionError> {
        Ok(FileSystem {
            path: self.path.clone(),
            format: FileFormat::Csv(self.format.clone()),
        })
    }
}

/// `YamlFileSystem` configures a [`FileSystem`] to read a YAML file at a path.
#[derive(Clone)]
pub struct YamlFileSystem {
    path: PathBuf,
}

impl YamlFileSystem {
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// `port` returns a cold port for this file. Its system reads the file only
    /// during a live read.
    pub fn port(self) -> Port<dyn YamlFile> {
        SystemConfig::port(self)
    }
}

impl SystemConfig for YamlFileSystem {
    type System = FileSystem;

    fn resolve(&self) -> Result<FileSystem, EffectPortResolutionError> {
        Ok(FileSystem {
            path: self.path.clone(),
            format: FileFormat::Yaml,
        })
    }
}

/// `YamlFile` is a port that reads the selected records of a YAML file.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait YamlFile: Send + Sync {
    #[query(type = "obzenflow.yaml.read")]
    async fn read(&self, from: &YamlRead, ctx: &QueryContext) -> Result<YamlPage, QueryError>;
}

#[async_trait]
impl YamlFile for FileSystem {
    async fn read(&self, from: &YamlRead, _ctx: &QueryContext) -> Result<YamlPage, QueryError> {
        let path = self.path.clone();
        let from = from.clone();
        blocking_io(move || read_yaml_page(&path, from))
            .await
            .map_err(file_query_error)?
    }
}

/// `CsvFile` is a port that reads the records of a CSV or TSV file.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait CsvFile: Send + Sync {
    #[query(type = "obzenflow.csv.read")]
    async fn read(&self, from: &CsvRead, ctx: &QueryContext) -> Result<CsvPage, QueryError>;
}

/// `CsvRead` is the request of one CSV read. A replay and a resume compare it
/// without `limit`, so a build can read pages of another size.
#[derive(Clone, Debug, Serialize)]
pub struct CsvRead {
    #[serde(skip_serializing)]
    pub limit: NonZeroUsize,
    pub skip_rows: usize,
    pub columns: Option<Vec<String>>,
    pub after: Option<QueryBookmark>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CsvPage {
    Batch {
        headers: Vec<String>,
        records: Vec<CsvLine>,
        next: QueryBookmark,
    },
    EndOfInput,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FileCsvRead {
    chunk: usize,
    skip_rows: usize,
    columns: Option<Vec<String>>,
    at: Option<CsvAt>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CsvAt {
    position: CsvPosition,
    version: FileVersion,
    headers: Vec<String>,
    selected: Option<Vec<usize>>,
    width: usize,
    delimiter: u8,
}

#[cfg(test)]
impl FileCsvRead {
    pub(crate) fn first(chunk: usize, skip_rows: usize, columns: Option<Vec<String>>) -> Self {
        Self {
            chunk,
            skip_rows,
            columns,
            at: None,
        }
    }

    pub(crate) fn headers(&self) -> &[String] {
        self.at.as_ref().map_or(&[], |at| &at.headers)
    }
}

/// A `CsvLine` is one record as the file held it, a rejected record, a refused
/// file, or a file that changed after a recorded read.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum CsvLine {
    Values {
        index: u64,
        line: u64,
        values: Vec<String>,
    },
    Malformed {
        index: u64,
        line: u64,
        reason: SourceDiagnosticReason,
    },
    Refused {
        kind: SourcePollErrorKind,
        diagnostic: SourceDiagnostic,
    },
    Changed,
}

#[async_trait]
impl CsvFile for FileSystem {
    async fn read(&self, from: &CsvRead, _: &QueryContext) -> Result<CsvPage, QueryError> {
        let FileFormat::Csv(format) = self.format.clone() else {
            return Err(QueryError::terminal(
                SourcePollErrorKind::Validation,
                SourceDiagnosticReason::UnexpectedShape,
            ));
        };
        let seek = FileCsvRead {
            chunk: from.limit.get(),
            skip_rows: from.skip_rows,
            columns: from.columns.clone(),
            at: from.after.as_ref().map(QueryBookmark::decode).transpose()?,
        };
        let path = self.path.clone();
        match blocking_io(move || read_csv(&path, &format, seek))
            .await
            .map_err(file_query_error)?
            .map_err(file_query_error)?
        {
            FiniteRead::EndOfInput => Ok(CsvPage::EndOfInput),
            FiniteRead::Batch { records, next } => {
                match records.as_slice() {
                    [CsvLine::Refused { kind, diagnostic }] => {
                        return Err(QueryError::terminal(*kind, diagnostic.clone()))
                    }
                    [CsvLine::Changed] => {
                        return Err(QueryError::terminal(
                            SourcePollErrorKind::Transport,
                            SourceDiagnosticReason::InputChanged,
                        ))
                    }
                    _ => {}
                }
                let at = next.at.ok_or_else(|| {
                    QueryError::terminal(
                        SourcePollErrorKind::Other,
                        SourceDiagnosticReason::UnexpectedShape,
                    )
                })?;
                Ok(CsvPage::Batch {
                    next: QueryBookmark::encode(&at)?,
                    headers: at.headers,
                    records,
                })
            }
        }
    }
}

fn file_query_error(error: EffectError) -> QueryError {
    match error {
        EffectError::SourceRead { failure, .. } => QueryError {
            failure,
            retry_after: None,
        },
        _ => QueryError::terminal(
            SourcePollErrorKind::Transport,
            SourceDiagnosticReason::InputUnavailable,
        ),
    }
}

fn read_failure(
    path: &Path,
    category: SourcePollErrorKind,
    reason: SourceDiagnosticReason,
) -> EffectError {
    EffectError::SourceRead {
        error: Box::new(EffectError::Transport(format!(
            "the source cannot read the file at '{}'",
            path.display()
        ))),
        failure: Box::new(SourceReadFailure {
            category,
            continuation: SourcePollContinuation::Terminal,
            diagnostic: reason.into(),
        }),
    }
}

fn unreadable(path: &Path) -> EffectError {
    read_failure(
        path,
        SourcePollErrorKind::Transport,
        SourceDiagnosticReason::InputUnavailable,
    )
}

fn record_failure(path: &Path, error: &csv::Error) -> Option<EffectError> {
    match error.kind() {
        csv::ErrorKind::Utf8 { .. } => None,
        csv::ErrorKind::Io(_) => Some(unreadable(path)),
        _ => Some(read_failure(
            path,
            SourcePollErrorKind::Other,
            SourceDiagnosticReason::Unclassified,
        )),
    }
}

fn read_csv(
    path: &Path,
    format: &CsvFormat,
    from: FileCsvRead,
) -> Result<FiniteRead<CsvLine, FileCsvRead>, EffectError> {
    let Ok(file) = File::open(path) else {
        return Ok(FiniteRead::Batch {
            records: vec![CsvLine::Refused {
                kind: SourcePollErrorKind::Transport,
                diagnostic: SourceDiagnosticReason::InputUnavailable.into(),
            }],
            next: from,
        });
    };
    let version = FileVersion::of(&file).map_err(|_| unreadable(path))?;
    read_open_csv(file, version, path, format, from)
}

fn read_open_csv(
    file: File,
    version: FileVersion,
    path: &Path,
    format: &CsvFormat,
    from: FileCsvRead,
) -> Result<FiniteRead<CsvLine, FileCsvRead>, EffectError> {
    let refused = |kind, reason: SourceDiagnosticReason| {
        Ok(FiniteRead::Batch {
            records: vec![CsvLine::Refused {
                kind,
                diagnostic: reason.into(),
            }],
            next: from.clone(),
        })
    };
    let changed = || {
        Ok(FiniteRead::Batch {
            records: vec![CsvLine::Changed],
            next: from.clone(),
        })
    };
    let delimiter = from.at.as_ref().map_or(format.delimiter, |at| at.delimiter);
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(file);
    let mut at = match from.at.clone() {
        Some(at) => {
            if at.version != version {
                return changed();
            }
            reader
                .seek(at.position.parser())
                .map_err(|_| unreadable(path))?;
            at
        }
        None => {
            let headers = match &format.headers {
                Some(headers) => headers.clone(),
                None => {
                    let mut record = StringRecord::new();
                    match reader.read_record(&mut record) {
                        Ok(true) => record.iter().map(str::to_owned).collect(),
                        Ok(false) => {
                            return refused(
                                SourcePollErrorKind::Validation,
                                SourceDiagnosticReason::UnexpectedShape,
                            )
                        }
                        Err(error) => {
                            if let Some(failure) = record_failure(path, &error) {
                                return Err(failure);
                            }
                            return refused(
                                SourcePollErrorKind::Deserialization,
                                SourceDiagnosticReason::MalformedInput,
                            );
                        }
                    }
                }
            };
            let width = headers.len();
            let (headers, selected) = match &from.columns {
                None => (headers, None),
                Some(columns) => {
                    let Some(indices) = columns
                        .iter()
                        .map(|column| headers.iter().position(|header| header == column))
                        .collect::<Option<Vec<usize>>>()
                    else {
                        return refused(
                            SourcePollErrorKind::Validation,
                            SourceDiagnosticReason::SelectionNotFound,
                        );
                    };
                    (columns.clone(), Some(indices))
                }
            };
            let mut skipped = 0;
            let mut record = StringRecord::new();
            while skipped < from.skip_rows {
                match reader.read_record(&mut record) {
                    Ok(true) => skipped += 1,
                    Ok(false) => break,
                    Err(error) => match record_failure(path, &error) {
                        Some(failure) => return Err(failure),
                        None => skipped += 1,
                    },
                }
            }
            CsvAt {
                position: CsvPosition::at(reader.position(), skipped, 0),
                version,
                headers,
                selected,
                width,
                delimiter,
            }
        }
    };
    let rows = read_rows(&mut reader, &at, from.chunk, path);
    if FileVersion::of(reader.get_ref()).ok() != Some(at.version) {
        return changed();
    }
    let Rows {
        records,
        after,
        ended,
        failure,
        index,
    } = rows;
    if let Some(failure) = failure {
        if records.is_empty() {
            return Err(failure);
        }
    } else if records.is_empty() && ended {
        return Ok(FiniteRead::EndOfInput);
    }
    at.position = CsvPosition::at(&after, usize::try_from(index).unwrap_or(usize::MAX), 0);
    Ok(FiniteRead::Batch {
        records,
        next: FileCsvRead {
            at: Some(at),
            ..from
        },
    })
}

struct Rows {
    records: Vec<CsvLine>,
    after: csv::Position,
    ended: bool,
    failure: Option<EffectError>,
    index: u64,
}

fn read_rows<R: Read>(reader: &mut csv::Reader<R>, at: &CsvAt, chunk: usize, path: &Path) -> Rows {
    let mut rows = Rows {
        records: Vec::with_capacity(chunk.min(1024)),
        after: reader.position().clone(),
        ended: false,
        failure: None,
        index: at.position.row_index(),
    };
    let mut record = StringRecord::new();
    while rows.records.len() < chunk {
        match reader.read_record(&mut record) {
            Ok(true) => {
                let line = record.position().map_or(0, csv::Position::line);
                rows.records.push(if record.len() == at.width {
                    CsvLine::Values {
                        index: rows.index,
                        line,
                        values: match &at.selected {
                            None => record.iter().map(str::to_owned).collect(),
                            Some(indices) => indices
                                .iter()
                                .map(|column| record[*column].to_owned())
                                .collect(),
                        },
                    }
                } else {
                    CsvLine::Malformed {
                        index: rows.index,
                        line,
                        reason: SourceDiagnosticReason::UnexpectedShape,
                    }
                });
            }
            Ok(false) => {
                rows.ended = true;
                break;
            }
            Err(error) => match record_failure(path, &error) {
                Some(failure) => {
                    rows.failure = Some(failure);
                    break;
                }
                None => rows.records.push(CsvLine::Malformed {
                    index: rows.index,
                    line: error.position().map_or(0, csv::Position::line),
                    reason: SourceDiagnosticReason::MalformedInput,
                }),
            },
        }
        rows.after = reader.position().clone();
        rows.index = rows.index.saturating_add(1);
    }
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn file(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    fn csv() -> CsvFormat {
        CsvFormat {
            delimiter: b',',
            headers: None,
        }
    }

    fn batch(read: FiniteRead<CsvLine, FileCsvRead>) -> (Vec<Vec<String>>, FileCsvRead) {
        match read {
            FiniteRead::Batch { records, next } => (
                records
                    .into_iter()
                    .map(|line| match line {
                        CsvLine::Values { values, .. } => values,
                        CsvLine::Malformed { line, .. } => panic!("malformed line {line}"),
                        CsvLine::Changed => panic!("changed file"),
                        CsvLine::Refused { diagnostic, .. } => panic!("refused {diagnostic:?}"),
                    })
                    .collect(),
                next,
            ),
            FiniteRead::EndOfInput => panic!("end of input"),
        }
    }

    #[test]
    fn csv_read_chunked_file_reaches_end() {
        let input = file("name\nAda\nGrace\nEdsger\n");
        let path = input.path().to_path_buf();
        let (first, next) = batch(read_csv(&path, &csv(), FileCsvRead::first(2, 0, None)).unwrap());
        assert_eq!(first, [["Ada"], ["Grace"]]);
        assert_eq!(next.headers(), ["name"]);
        let (second, next) = batch(read_csv(&path, &csv(), next).unwrap());
        assert_eq!(second, [["Edsger"]]);
        assert!(matches!(
            read_csv(&path, &csv(), next).unwrap(),
            FiniteRead::EndOfInput
        ));
    }

    #[test]
    fn csv_record_index_continues_across_chunks() {
        let input = file("name\nskipped\nAda\nGrace\nEdsger\n");
        let path = input.path().to_path_buf();
        let indexes = |read: FiniteRead<CsvLine, FileCsvRead>| match read {
            FiniteRead::Batch { records, next } => (
                records
                    .into_iter()
                    .map(|line| match line {
                        CsvLine::Values { index, .. } => index,
                        other => panic!("unexpected {other:?}"),
                    })
                    .collect::<Vec<_>>(),
                next,
            ),
            FiniteRead::EndOfInput => panic!("end of input"),
        };
        let (first, next) =
            indexes(read_csv(&path, &csv(), FileCsvRead::first(2, 1, None)).unwrap());
        let (second, _) = indexes(read_csv(&path, &csv(), next).unwrap());
        assert_eq!((first, second), (vec![1, 2], vec![3]));
    }

    #[test]
    fn csv_read_tab_delimiter_splits_fields() {
        let input = file("name\tcity\nAda\tLondon\n");
        let format = CsvFormat {
            delimiter: b'\t',
            headers: None,
        };
        let (rows, _) =
            batch(read_csv(input.path(), &format, FileCsvRead::first(10, 0, None)).unwrap());
        assert_eq!(rows, [["Ada", "London"]]);
    }

    #[test]
    fn continued_read_keeps_recorded_delimiter() {
        let input = file("name;city\nAda;London\nGrace;Arlington\n");
        let path = input.path().to_path_buf();
        let semicolon = CsvFormat {
            delimiter: b';',
            headers: None,
        };
        let (first, next) =
            batch(read_csv(&path, &semicolon, FileCsvRead::first(1, 0, None)).unwrap());
        assert_eq!(first, [["Ada", "London"]]);
        let (rest, _) = batch(read_csv(&path, &csv(), next).unwrap());
        assert_eq!(rest, [["Grace", "Arlington"]]);
    }

    #[test]
    fn csv_given_headers_keeps_first_row() {
        let input = file("Ada,London\n");
        let format = CsvFormat {
            delimiter: b',',
            headers: Some(vec!["name".into(), "city".into()]),
        };
        let (rows, next) =
            batch(read_csv(input.path(), &format, FileCsvRead::first(10, 0, None)).unwrap());
        assert_eq!(rows, [["Ada", "London"]]);
        assert_eq!(next.headers(), ["name", "city"]);
    }

    #[test]
    fn csv_read_selection_returns_chosen_columns() {
        let input = file("name,city,year\nskipped,x,0\nAda,London,1815\n");
        let read = FileCsvRead::first(10, 1, Some(vec!["year".into(), "name".into()]));
        let (rows, next) = batch(read_csv(input.path(), &csv(), read).unwrap());
        assert_eq!(rows, [["1815", "Ada"]]);
        assert_eq!(next.headers(), ["year", "name"]);
    }

    #[test]
    fn csv_read_changed_file_records_change() {
        let mut input = file("name\nAda\nGrace\n");
        let path = input.path().to_path_buf();
        let (_, next) = batch(read_csv(&path, &csv(), FileCsvRead::first(1, 0, None)).unwrap());
        input.write_all(b"Edsger\n").unwrap();
        input.flush().unwrap();
        match read_csv(&path, &csv(), next).unwrap() {
            FiniteRead::Batch { records, .. } => {
                assert!(
                    matches!(records.as_slice(), [CsvLine::Changed]),
                    "{records:?}"
                );
            }
            FiniteRead::EndOfInput => panic!("end of input"),
        }
    }

    #[test]
    fn csv_read_missing_file_records_refusal() {
        let read = read_csv(
            &PathBuf::from("/nonexistent/obzenflow/tickets.csv"),
            &csv(),
            FileCsvRead::first(1, 0, None),
        )
        .unwrap();
        match read {
            FiniteRead::Batch { records, .. } => assert!(
                matches!(
                    records.as_slice(),
                    [CsvLine::Refused { diagnostic, .. }]
                        if diagnostic.reason() == SourceDiagnosticReason::InputUnavailable
                ),
                "{records:?}"
            ),
            FiniteRead::EndOfInput => panic!("end of input"),
        }
    }

    fn lines(read: FiniteRead<CsvLine, FileCsvRead>) -> Vec<CsvLine> {
        match read {
            FiniteRead::Batch { records, .. } => records,
            FiniteRead::EndOfInput => panic!("end of input"),
        }
    }

    #[test]
    fn short_row_rejected_between_valid_rows() {
        let input = file("a,b\n1,2\n3\n4,5\n");
        let read = read_csv(input.path(), &csv(), FileCsvRead::first(10, 0, None)).unwrap();
        match lines(read).as_slice() {
            [CsvLine::Values { values: first, .. }, CsvLine::Malformed {
                index: 1, reason, ..
            }, CsvLine::Values { values: last, .. }] => {
                assert_eq!(*first, ["1", "2"]);
                assert_eq!(*last, ["4", "5"]);
                assert_eq!(*reason, SourceDiagnosticReason::UnexpectedShape);
            }
            other => panic!("unexpected lines {other:?}"),
        }
    }

    #[test]
    fn missing_header_row_refused() {
        let input = file("");
        let read = read_csv(input.path(), &csv(), FileCsvRead::first(10, 0, None)).unwrap();
        assert!(
            matches!(
                lines(read).as_slice(),
                [CsvLine::Refused { kind: SourcePollErrorKind::Validation, diagnostic }]
                    if diagnostic.reason() == SourceDiagnosticReason::UnexpectedShape
            ),
            "an empty file has no header row"
        );
    }

    #[test]
    fn edit_during_read_records_change() {
        let mut input = file("name\nAda\nGrace\n");
        let path = input.path().to_path_buf();
        let opened = File::open(&path).unwrap();
        let version = FileVersion::of(&opened).unwrap();
        input.write_all(b"Edsger\n").unwrap();
        input.flush().unwrap();
        let read = read_open_csv(
            opened,
            version,
            &path,
            &csv(),
            FileCsvRead::first(10, 0, None),
        )
        .unwrap();
        assert!(
            matches!(lines(read).as_slice(), [CsvLine::Changed]),
            "no row read from an edited file is published"
        );
    }

    struct FailingAfter(std::io::Cursor<Vec<u8>>);

    impl Read for FailingAfter {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            match self.0.read(buf)? {
                0 => Err(std::io::Error::other("disk read failed")),
                read => Ok(read),
            }
        }
    }

    #[test]
    fn read_io_failure_stops_without_eof() {
        let at = CsvAt {
            position: CsvPosition::at(&csv::Position::new(), 0, 0),
            version: FileVersion::of(&File::open(file("x").path()).unwrap()).unwrap(),
            headers: vec!["a".into(), "b".into()],
            selected: None,
            width: 2,
            delimiter: b',',
        };
        let path = PathBuf::from("/data/rows.csv");
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(FailingAfter(std::io::Cursor::new(b"1,2\n".to_vec())));
        let rows = read_rows(&mut reader, &at, 10, &path);
        assert_eq!(rows.records.len(), 1);
        assert!(!rows.ended);
        let failure = rows.failure.expect("the read failure is kept");
        let Some(obzenflow_core::event::EffectFailureDetail::SourceRead(failure)) =
            failure.failure_detail()
        else {
            panic!("the failure carries its source diagnostic");
        };
        assert_eq!(failure.continuation, SourcePollContinuation::Terminal);
        assert_eq!(failure.category, SourcePollErrorKind::Transport);
        assert_eq!(
            failure.diagnostic.reason(),
            SourceDiagnosticReason::InputUnavailable
        );
    }
}
