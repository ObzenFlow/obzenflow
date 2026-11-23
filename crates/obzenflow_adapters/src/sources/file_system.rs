// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::csv::CsvPosition;
use super::file_version::FileVersion;
use super::yaml::{read_yaml, YamlLine, YamlRead};
use crate::sinks::blocking::blocking_io;
use async_trait::async_trait;
use csv::{ReaderBuilder, StringRecord};
use obzenflow_core::effect_binding::SystemLocation;
use obzenflow_core::event::payloads::execution_payload::SourcePollErrorKind;
use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};
use obzenflow_runtime::effects::{EffectContext, EffectError, Port, SystemName};
use obzenflow_runtime::stages::common::handlers::source::FiniteRead;
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::PathBuf;

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

impl SystemName for FileSystem {
    const NAME: &'static str = "file-system";
}

#[derive(Clone)]
pub(crate) struct CsvFormat {
    pub(crate) delimiter: u8,
    pub(crate) headers: Option<Vec<String>>,
}

impl FileSystem {
    pub(crate) fn csv_port(path: PathBuf, format: CsvFormat) -> anyhow::Result<Port<dyn CsvFile>> {
        let location = if format.delimiter == b'\t' {
            SystemLocation::Tsv(path.clone())
        } else {
            SystemLocation::Csv(path.clone())
        };
        Port::deferred(move || {
            Ok(FileSystem {
                path: path.clone(),
                format: FileFormat::Csv(format.clone()),
            })
        })
        .with_system_location(location)
        .map(Port::own_instance)
        .map_err(|error| {
            anyhow::anyhow!("the binding evidence cannot record the file location: {error}")
        })
    }

    pub(crate) fn yaml_port(path: PathBuf) -> anyhow::Result<Port<dyn YamlFile>> {
        let location = SystemLocation::Yaml(path.clone());
        Port::deferred(move || {
            Ok(FileSystem {
                path: path.clone(),
                format: FileFormat::Yaml,
            })
        })
        .with_system_location(location)
        .map(Port::own_instance)
        .map_err(|error| {
            anyhow::anyhow!("the binding evidence cannot record the file location: {error}")
        })
    }
}

/// `YamlFile` is a port that reads the selected records of a YAML file.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait YamlFile: Send + Sync {
    #[effect(type = "obzenflow.file_system.yaml_read", exactly_once, records = reply)]
    async fn read(
        &self,
        from: &YamlRead,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<YamlLine, YamlRead>, EffectError>;
}

#[async_trait]
impl YamlFile for FileSystem {
    async fn read(
        &self,
        from: &YamlRead,
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<YamlLine, YamlRead>, EffectError> {
        let path = self.path.clone();
        let from = from.clone();
        blocking_io(move || read_yaml(&path, from)).await
    }
}

/// `CsvFile` is a port that reads the records of a CSV or TSV file.
#[obzenflow_runtime::effects::port]
#[async_trait]
pub trait CsvFile: Send + Sync {
    #[effect(type = "obzenflow.file_system.csv_read", exactly_once, records = reply)]
    async fn read(
        &self,
        from: &CsvRead,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<CsvLine, CsvRead>, EffectError>;
}

/// `CsvRead` is the argument of one CSV read. It holds the read settings and, after
/// the first read, the position, columns and file version that the previous reply
/// recorded.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CsvRead {
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
}

impl CsvRead {
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

/// A `CsvLine` is one record as the file held it, a malformed record, a refused
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
    },
    Refused {
        kind: SourcePollErrorKind,
        diagnostic: SourceDiagnostic,
    },
    Changed,
}

#[async_trait]
impl CsvFile for FileSystem {
    async fn read(
        &self,
        from: &CsvRead,
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<CsvLine, CsvRead>, EffectError> {
        let FileFormat::Csv(format) = self.format.clone() else {
            return Err(EffectError::Permanent(format!(
                "the file at '{}' is not configured for CSV",
                self.path.display()
            )));
        };
        let path = self.path.clone();
        let from = from.clone();
        blocking_io(move || read_csv(&path, &format, from)).await?
    }
}

fn read_csv(
    path: &PathBuf,
    format: &CsvFormat,
    from: CsvRead,
) -> Result<FiniteRead<CsvLine, CsvRead>, EffectError> {
    let refused = |kind, reason: SourceDiagnosticReason| {
        Ok(FiniteRead::Batch {
            records: vec![CsvLine::Refused {
                kind,
                diagnostic: reason.into(),
            }],
            next: from.clone(),
        })
    };
    let Ok(file) = File::open(path) else {
        return refused(
            SourcePollErrorKind::Transport,
            SourceDiagnosticReason::InputUnavailable,
        );
    };
    let version = FileVersion::of(&file).map_err(|_| {
        EffectError::Transport(format!("the file at '{}' cannot be read", path.display()))
    })?;
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .delimiter(format.delimiter)
        .from_reader(file);
    let unreadable =
        |_| EffectError::Transport(format!("the file at '{}' cannot be read", path.display()));
    let mut at = match from.at {
        Some(at) => {
            if at.version != version {
                return Ok(FiniteRead::Batch {
                    records: vec![CsvLine::Changed],
                    next: CsvRead {
                        at: Some(at),
                        ..from
                    },
                });
            }
            reader.seek(at.position.parser()).map_err(unreadable)?;
            at
        }
        None => {
            let headers = match &format.headers {
                Some(headers) => headers.clone(),
                None => {
                    let mut record = StringRecord::new();
                    if !reader.read_record(&mut record).map_err(unreadable)? {
                        return Ok(FiniteRead::EndOfInput);
                    }
                    record.iter().map(str::to_owned).collect()
                }
            };
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
            while skipped < from.skip_rows && reader.read_record(&mut record).map_err(unreadable)? {
                skipped += 1;
            }
            CsvAt {
                position: CsvPosition::at(reader.position(), skipped, 0),
                version,
                headers,
                selected,
            }
        }
    };
    let mut records = Vec::with_capacity(from.chunk);
    let mut record = StringRecord::new();
    let mut ended = false;
    let mut index = at.position.row_index();
    while records.len() < from.chunk {
        match reader.read_record(&mut record) {
            Ok(true) => {
                let values = match &at.selected {
                    None => record.iter().map(str::to_owned).collect(),
                    Some(indices) => indices
                        .iter()
                        .map(|index| record.get(*index).unwrap_or_default().to_owned())
                        .collect(),
                };
                records.push(CsvLine::Values {
                    index,
                    line: record.position().map_or(0, csv::Position::line),
                    values,
                });
            }
            Ok(false) => {
                ended = true;
                break;
            }
            Err(error) if matches!(error.kind(), csv::ErrorKind::Io(_)) => {
                return Err(unreadable(error));
            }
            Err(error) => records.push(CsvLine::Malformed {
                index,
                line: error.position().map_or(0, csv::Position::line),
            }),
        }
        index = index.saturating_add(1);
    }
    if records.is_empty() && ended {
        return Ok(FiniteRead::EndOfInput);
    }
    at.position = CsvPosition::at(
        reader.position(),
        usize::try_from(index).unwrap_or(usize::MAX),
        0,
    );
    Ok(FiniteRead::Batch {
        records,
        next: CsvRead {
            at: Some(at),
            ..from
        },
    })
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

    fn batch(read: FiniteRead<CsvLine, CsvRead>) -> (Vec<Vec<String>>, CsvRead) {
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
        let (first, next) = batch(read_csv(&path, &csv(), CsvRead::first(2, 0, None)).unwrap());
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
        let indexes = |read: FiniteRead<CsvLine, CsvRead>| match read {
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
        let (first, next) = indexes(read_csv(&path, &csv(), CsvRead::first(2, 1, None)).unwrap());
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
        let (rows, _) = batch(
            read_csv(
                &input.path().to_path_buf(),
                &format,
                CsvRead::first(10, 0, None),
            )
            .unwrap(),
        );
        assert_eq!(rows, [["Ada", "London"]]);
    }

    #[test]
    fn csv_given_headers_keeps_first_row() {
        let input = file("Ada,London\n");
        let format = CsvFormat {
            delimiter: b',',
            headers: Some(vec!["name".into(), "city".into()]),
        };
        let (rows, next) = batch(
            read_csv(
                &input.path().to_path_buf(),
                &format,
                CsvRead::first(10, 0, None),
            )
            .unwrap(),
        );
        assert_eq!(rows, [["Ada", "London"]]);
        assert_eq!(next.headers(), ["name", "city"]);
    }

    #[test]
    fn csv_read_selection_returns_chosen_columns() {
        let input = file("name,city,year\nskipped,x,0\nAda,London,1815\n");
        let read = CsvRead::first(10, 1, Some(vec!["year".into(), "name".into()]));
        let (rows, next) = batch(read_csv(&input.path().to_path_buf(), &csv(), read).unwrap());
        assert_eq!(rows, [["1815", "Ada"]]);
        assert_eq!(next.headers(), ["year", "name"]);
    }

    #[test]
    fn csv_read_changed_file_records_change() {
        let mut input = file("name\nAda\nGrace\n");
        let path = input.path().to_path_buf();
        let (_, next) = batch(read_csv(&path, &csv(), CsvRead::first(1, 0, None)).unwrap());
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
            CsvRead::first(1, 0, None),
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
}
