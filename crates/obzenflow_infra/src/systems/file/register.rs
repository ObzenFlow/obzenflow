// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The `register` module holds the CSV file sink.
//!
//! A [`CsvProjection`] maps each input to one serializable row. The sink appends
//! each row to a CSV file that one run history writes. A new run history empties
//! the file first. Each append records the length and the SHA-256 of the file after
//! the row. Before its first write, a resumed run compares the file with its history.
//! The run moves the bytes that no record holds into the file `<file>.<length>.damage`.

use async_trait::async_trait;
use csv::{ReaderBuilder, WriterBuilder};
use obzenflow_adapters::sinks::blocking::blocking_io;
use obzenflow_adapters::sinks::{hex, queued_batch_defaults};
use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::commands::Command;
use obzenflow_runtime::effects::{
    port, DeclaredEffectSet, EffectContext, EffectError, EffectPortResolutionError, SinkRole,
};
use obzenflow_runtime::effects::{SinkBatchDefaults, SinkGroupSystem};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::systems::Port;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs::{File, OpenOptions, TryLockError};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// `CsvProjection` maps one domain input to one serializable CSV row.
/// An application passes its projection to [`CsvSink::builder`].
pub trait CsvProjection: Clone + Send + Sync + 'static {
    /// `Input` is the domain type that this projection takes.
    type Input: TypedPayload + Send + Sync + 'static;
    /// `Row` must serialize as a struct or a map. The sink fills the cell of each column
    /// from the field of `Row` with the same name.
    type Row: Serialize;

    /// `project` maps one domain input to its CSV row.
    fn project(&self, input: Self::Input) -> Result<Self::Row, HandlerError>;
}

/// `CsvRow` is one row that a CSV sink appends. It holds the fields of the
/// projected row by name.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CsvRow {
    fields: Arc<Map<String, Value>>,
}

/// `Appended` is the reply of one append. It holds the length and the SHA-256 of
/// the file after the row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appended {
    pub length: u64,
    pub sha256: String,
}

/// `CsvRegisterPort` is the port that a CSV sink appends through. Its `append` is
/// `exactly_once`. Before the first write of a resumed run, the file register removes
/// the bytes that no record holds.
#[port]
#[async_trait]
pub trait CsvRegisterPort {
    #[effect(type = "obzenflow.csv.row", exactly_once, records = reply)]
    async fn append(&self, row: &CsvRow, ctx: &EffectContext) -> Result<Appended, EffectError>;
}

#[derive(Clone, Debug)]
struct CsvLayout {
    path: PathBuf,
    columns: Option<Vec<String>>,
    headers: Option<Vec<String>>,
    delimiter: u8,
}

enum RegisterState {
    Closed,
    Open(Box<OpenRegister>),
}

struct OpenRegister {
    file: File,
    length: u64,
    digest: Context,
    columns: Option<Vec<String>>,
    header_written: Written,
    poisoned: Option<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Written {
    Nothing,
    Header,
}

struct CsvRegister {
    layout: Arc<CsvLayout>,
    state: Arc<Mutex<RegisterState>>,
}

#[async_trait]
impl CsvRegisterPort for CsvRegister {
    async fn append(&self, row: &CsvRow, ctx: &EffectContext) -> Result<Appended, EffectError> {
        self.write_group(Arc::from([row.clone()]), ctx)
            .await
            .map(|mut replies| replies.remove(0))
    }
}

#[async_trait]
impl SinkGroupSystem<CsvRow> for CsvRegister {
    async fn write_group(
        &self,
        rows: Arc<[CsvRow]>,
        ctx: &EffectContext,
    ) -> Result<Vec<Appended>, EffectError> {
        let closed = matches!(
            *self.state.lock().map_err(|_| poisoned_lock())?,
            RegisterState::Closed
        );
        let recorded = if closed {
            ctx.last_answer::<Appended>()?
        } else {
            None
        };
        let continues_history = ctx.continues_history();
        let layout = self.layout.clone();
        let state = Arc::clone(&self.state);
        blocking_io(move || {
            let mut state = state.lock().map_err(|_| poisoned_lock())?;
            if matches!(*state, RegisterState::Closed) {
                *state = RegisterState::Open(Box::new(OpenRegister::open(
                    &layout,
                    recorded.as_ref(),
                    continues_history,
                )?));
            }
            let register = match &mut *state {
                RegisterState::Open(register) => register,
                RegisterState::Closed => unreachable!("the code above opens a closed register"),
            };
            if let Some(reason) = &register.poisoned {
                return Err(EffectError::DestinationChanged {
                    message: reason.clone(),
                });
            }
            match register.append_group(&layout, &rows) {
                Ok(appended) => Ok(appended),
                Err(error) => {
                    let reason = format!(
                        "{} is in an unknown state after a failed write: {error}",
                        layout.path.display()
                    );
                    register.poisoned = Some(reason.clone());
                    Err(EffectError::DestinationChanged { message: reason })
                }
            }
        })
        .await?
    }
}

impl OpenRegister {
    fn open(
        layout: &CsvLayout,
        recorded: Option<&Appended>,
        continues_history: bool,
    ) -> Result<Self, EffectError> {
        let Some(recorded) = recorded else {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&layout.path)
                .map_err(|error| io_failure(&layout.path, error))?;
            hold(&file, &layout.path)?;
            if !continues_history {
                file.set_len(0)
                    .map_err(|error| io_failure(&layout.path, error))?;
            }
            if continues_history
                && file
                    .metadata()
                    .map_err(|error| io_failure(&layout.path, error))?
                    .len()
                    > 0
            {
                set_aside_tail(&mut file, &layout.path, 0)?;
                file.rewind()
                    .map_err(|error| io_failure(&layout.path, error))?;
            }
            return Ok(Self {
                file,
                length: 0,
                digest: Context::new(&SHA256),
                columns: layout.columns.clone(),
                header_written: Written::Nothing,
                poisoned: None,
            });
        };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&layout.path)
            .map_err(|_| no_longer_matches(&layout.path, recorded))?;
        hold(&file, &layout.path)?;
        let actual = file
            .metadata()
            .map_err(|error| io_failure(&layout.path, error))?
            .len();
        if actual < recorded.length {
            return Err(no_longer_matches(&layout.path, recorded));
        }
        let mut digest = Context::new(&SHA256);
        let mut prefix = BufReader::new(Read::by_ref(&mut file).take(recorded.length));
        loop {
            let chunk = prefix
                .fill_buf()
                .map_err(|error| io_failure(&layout.path, error))?;
            if chunk.is_empty() {
                break;
            }
            digest.update(chunk);
            let consumed = chunk.len();
            prefix.consume(consumed);
        }
        if hex(digest.clone().finish().as_ref()) != recorded.sha256 {
            return Err(no_longer_matches(&layout.path, recorded));
        }
        if actual > recorded.length {
            set_aside_tail(&mut file, &layout.path, recorded.length)?;
        }
        let columns = match &layout.columns {
            Some(columns) => Some(columns.clone()),
            None => {
                file.rewind()
                    .map_err(|error| io_failure(&layout.path, error))?;
                header_columns(
                    Read::by_ref(&mut file).take(recorded.length),
                    layout.delimiter,
                )
                .map_err(|error| io_failure(&layout.path, error.into()))?
            }
        };
        file.seek(SeekFrom::Start(recorded.length))
            .map_err(|error| io_failure(&layout.path, error))?;
        Ok(Self {
            file,
            length: recorded.length,
            digest,
            columns,
            header_written: if recorded.length > 0 {
                Written::Header
            } else {
                Written::Nothing
            },
            poisoned: None,
        })
    }

    fn append_group(&mut self, layout: &CsvLayout, rows: &[CsvRow]) -> io::Result<Vec<Appended>> {
        let mut writer = WriterBuilder::new()
            .delimiter(layout.delimiter)
            .from_writer(Vec::new());
        let mut replies = Vec::with_capacity(rows.len());
        for row in rows {
            let columns = self.columns.get_or_insert_with(|| {
                let mut names: Vec<_> = row.fields.keys().cloned().collect();
                names.sort();
                names
            });
            let start = writer.get_ref().len();
            if self.header_written == Written::Nothing {
                writer.write_record(layout.headers.as_ref().unwrap_or(columns))?;
            }
            writer.write_record(columns.iter().map(|name| cell(row.fields.get(name))))?;
            writer.flush()?;
            let bytes = &writer.get_ref()[start..];
            self.header_written = Written::Header;
            self.length += bytes.len() as u64;
            self.digest.update(bytes);
            replies.push(Appended {
                length: self.length,
                sha256: hex(self.digest.clone().finish().as_ref()),
            });
        }
        self.file.write_all(writer.get_ref())?;
        self.file.sync_data()?;
        Ok(replies)
    }
}

fn hold(file: &File, path: &Path) -> Result<(), EffectError> {
    let message = match file.try_lock() {
        Ok(()) => return Ok(()),
        Err(TryLockError::WouldBlock) => format!(
            "Another CSV sink or another run writes {}. Give each CSV sink its own file, and let one run write the file at a time.",
            path.display()
        ),
        Err(TryLockError::Error(error)) => format!(
            "The CSV sink cannot lock {} ({error}). Write CSV files to a local file system.",
            path.display()
        ),
    };
    Err(EffectError::DestinationChanged { message })
}

fn set_aside_tail(file: &mut File, path: &Path, length: u64) -> Result<(), EffectError> {
    file.seek(SeekFrom::Start(length))
        .map_err(|error| io_failure(path, error))?;
    let damage = damage_path(path, length);
    let mut out = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&damage)
        .map_err(|error| io_failure(&damage, error))?;
    io::copy(file, &mut out)
        .and_then(|_| out.sync_all())
        .map_err(|error| io_failure(&damage, error))?;
    file.set_len(length)
        .and_then(|()| file.sync_all())
        .map_err(|error| io_failure(path, error))?;
    tracing::warn!(
        "the CSV register moved the bytes after offset {length} of {} into {}. The history of this run has no record of these bytes.",
        path.display(),
        damage.display()
    );
    Ok(())
}

fn damage_path(path: &Path, length: u64) -> PathBuf {
    let mut candidate = PathBuf::from(format!("{}.{length}.damage", path.display()));
    let mut attempt = 1u32;
    while candidate.exists() {
        candidate = PathBuf::from(format!("{}.{length}.{attempt}.damage", path.display()));
        attempt += 1;
    }
    candidate
}

fn header_columns(prefix: impl Read, delimiter: u8) -> csv::Result<Option<Vec<String>>> {
    ReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .from_reader(prefix)
        .records()
        .next()
        .map(|result| result.map(|record| record.iter().map(str::to_string).collect()))
        .transpose()
}

fn cell(value: Option<&Value>) -> std::borrow::Cow<'_, [u8]> {
    match value {
        None | Some(Value::Null) => b"".as_slice().into(),
        Some(Value::String(text)) => text.as_bytes().into(),
        Some(Value::Number(number)) => number.to_string().into_bytes().into(),
        Some(Value::Bool(flag)) => if *flag {
            b"true".as_slice()
        } else {
            b"false".as_slice()
        }
        .into(),
        Some(other @ (Value::Array(_) | Value::Object(_))) => other.to_string().into_bytes().into(),
    }
}

fn register_port(layout: CsvLayout) -> Port<dyn CsvRegisterPort> {
    let layout = Arc::new(layout);
    Port::deferred_grouped::<CsvRow, _, _>(move || {
        Ok::<_, EffectPortResolutionError>(CsvRegister {
            layout: Arc::clone(&layout),
            state: Arc::new(Mutex::new(RegisterState::Closed)),
        })
    })
}

fn poisoned_lock() -> EffectError {
    EffectError::DestinationChanged {
        message: "the lock of the CSV register is poisoned".to_string(),
    }
}

fn io_failure(path: &Path, error: io::Error) -> EffectError {
    EffectError::Execution(format!("CSV register {} failed: {error}", path.display()))
}

fn no_longer_matches(path: &Path, recorded: &Appended) -> EffectError {
    let short = recorded.sha256.get(..4).unwrap_or(&recorded.sha256);
    EffectError::DestinationChanged {
        message: format!(
            "{} no longer holds the bytes that the history of this run recorded (length {}, \
             SHA-256 {short}…). Restore the file to these bytes, then resume the run again.",
            path.display(),
            recorded.length
        ),
    }
}

/// `CsvSinkBuilder` builds a [`CsvSink`] for the projection `P`.
#[derive(Clone)]
pub struct CsvSinkBuilder<P> {
    projection: P,
    batch: SinkBatchDefaults,
    path: Option<PathBuf>,
    columns: Option<Vec<String>>,
    headers: Option<Vec<String>>,
    delimiter: u8,
}

impl<P> std::fmt::Debug for CsvSinkBuilder<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSinkBuilder")
            .field("projection", &std::any::type_name::<P>())
            .field("path", &self.path)
            .field("columns", &self.columns)
            .field("headers", &self.headers)
            .field("delimiter", &self.delimiter)
            .finish()
    }
}

impl<P> CsvSinkBuilder<P> {
    fn new(projection: P) -> Self {
        Self {
            projection,
            batch: SinkBatchDefaults::default(),
            path: None,
            columns: None,
            headers: None,
            delimiter: b',',
        }
    }

    /// `batch_size` sets the default number of rows that the register writes and syncs
    /// together. It returns an error for zero.
    pub fn batch_size(mut self, size: usize) -> Result<Self, anyhow::Error> {
        self.batch.size = Some(
            std::num::NonZeroUsize::new(size)
                .ok_or_else(|| anyhow::anyhow!("CSV batch size must be positive"))?,
        );
        Ok(self)
    }

    /// `batch_max_wait` sets the longest time that an open group waits for more rows.
    /// If the wait is zero, a group takes only the rows in the queue.
    pub fn batch_max_wait(mut self, wait: std::time::Duration) -> Self {
        self.batch.max_wait = Some(wait);
        self
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

    pub fn build(self) -> Result<CsvSink<P>, anyhow::Error>
    where
        P: CsvProjection,
    {
        let path = self
            .path
            .ok_or_else(|| anyhow::anyhow!("a CSV sink must have a path"))?;
        if let Some(columns) = self.columns.as_ref() {
            if columns.is_empty() {
                anyhow::bail!("columns must not be empty");
            }
        }
        if let Some(headers) = self.headers.as_ref() {
            let Some(columns) = self.columns.as_ref() else {
                anyhow::bail!("headers must have columns, with one header for each column");
            };
            if headers.len() != columns.len() {
                anyhow::bail!("the number of headers must be equal to the number of columns");
            }
        }
        let layout = CsvLayout {
            path,
            columns: self.columns,
            headers: self.headers,
            delimiter: self.delimiter,
        };
        Ok(CsvSink {
            projection: self.projection,
            batch: self.batch,
            destination: CsvDestination::File(layout),
        })
    }
}

/// `CsvSink` appends each input as one CSV row, which the projection `P` makes. By
/// default, the file register writes and syncs up to 64 queued rows together and does
/// not wait for more rows.
#[derive(Clone)]
pub struct CsvSink<P> {
    projection: P,
    batch: SinkBatchDefaults,
    destination: CsvDestination,
}

#[derive(Clone)]
enum CsvDestination {
    File(CsvLayout),
    Port(Port<dyn CsvRegisterPort>),
}

impl<P> std::fmt::Debug for CsvSink<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink")
            .field("projection", &std::any::type_name::<P>())
            .field(
                "destination",
                &match &self.destination {
                    CsvDestination::File(_) => "file",
                    CsvDestination::Port(_) => "port",
                },
            )
            .finish()
    }
}

impl<P> CsvSink<P>
where
    P: CsvProjection,
{
    pub fn builder(projection: P) -> CsvSinkBuilder<P> {
        CsvSinkBuilder::new(projection)
    }
}

impl<P> CsvSink<P> {
    /// `through` appends through another system that answers [`CsvRegisterPort`].
    /// If the builder sets no batch size, that system receives one row in each request.
    pub fn through(mut self, port: Port<dyn CsvRegisterPort>) -> Self {
        self.destination = CsvDestination::Port(port);
        self
    }
}

impl<P> Command for CsvSink<P> {
    type Ports = (Port<dyn CsvRegisterPort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<CsvRow, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        let port = match &self.destination {
            CsvDestination::File(layout) => register_port(layout.clone()),
            CsvDestination::Port(port) => port.clone(),
        };
        (port, ())
    }
}

impl<P: CsvProjection> SinkRole<P::Input> for CsvSink<P> {
    type Request = CsvRow;

    fn batch_defaults(&self) -> SinkBatchDefaults {
        let mut batch = self.batch;
        if batch.size.is_none() && matches!(self.destination, CsvDestination::File(_)) {
            batch.size = queued_batch_defaults().size;
        }
        batch
    }

    fn prepare(&self, input: P::Input) -> Result<CsvRow, HandlerError> {
        let row = self.projection.project(input)?;
        match serde_json::to_value(row) {
            Ok(Value::Object(fields)) => Ok(CsvRow {
                fields: Arc::new(fields),
            }),
            Ok(_) => Err(HandlerError::Validation(
                "CsvSink must have a projected row that serializes as a struct or a map"
                    .to_string(),
            )),
            Err(error) => Err(HandlerError::Validation(format!(
                "CsvSink cannot serialize the projected row: {error}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::FlowId;

    fn layout(path: &Path) -> CsvLayout {
        CsvLayout {
            path: path.to_path_buf(),
            columns: None,
            headers: None,
            delimiter: b',',
        }
    }

    fn fields(id: u64) -> Map<String, Value> {
        let mut fields = Map::new();
        fields.insert("id".into(), Value::from(id));
        fields.insert("name".into(), Value::from(format!("row-{id}")));
        fields
    }

    fn row(id: u64) -> CsvRow {
        CsvRow {
            fields: fields(id).into(),
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Entry {
        id: u64,
    }

    impl TypedPayload for Entry {
        const EVENT_TYPE: &'static str = "csv_test.entry";
    }

    #[derive(Clone)]
    struct EntryRow;

    impl CsvProjection for EntryRow {
        type Input = Entry;
        type Row = Entry;

        fn project(&self, entry: Entry) -> Result<Entry, HandlerError> {
            Ok(entry)
        }
    }

    #[test]
    fn register_defaults_follow_destination() {
        let builder = CsvSink::builder(EntryRow).path("register.csv");
        let make_port = || register_port(layout(Path::new("replacement.csv")));
        let sink = builder.clone().build().unwrap();
        assert_eq!(sink.batch_defaults().size.unwrap().get(), 64);
        assert!(sink.through(make_port()).batch_defaults().size.is_none());
        let configured = builder
            .batch_size(8)
            .unwrap()
            .build()
            .unwrap()
            .through(make_port());
        assert_eq!(configured.batch_defaults().size.unwrap().get(), 8);
    }

    #[test]
    fn grouped_replies_match_csv_prefixes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let layout = CsvLayout {
            columns: Some(vec!["id".into(), "name".into()]),
            headers: Some(vec!["ID".into(), "Name".into()]),
            delimiter: b';',
            ..layout(&path)
        };
        let mut first = fields(1);
        first.insert("name".into(), Value::String("a;\"b\"\nc".into()));
        let first = CsvRow {
            fields: first.into(),
        };
        let mut register = OpenRegister::open(&layout, None, false).unwrap();
        let replies = register.append_group(&layout, &[first, row(2)]).unwrap();
        let expected = b"ID;Name\n1;\"a;\"\"b\"\"\nc\"\n2;row-2\n";
        assert_eq!(std::fs::read(path).unwrap(), expected);
        for (reply, length) in replies.iter().zip([expected.len() - 8, expected.len()]) {
            assert_eq!(reply.length, length as u64);
            assert_eq!(
                reply.sha256,
                hex(ring::digest::digest(&SHA256, &expected[..length]).as_ref())
            );
        }
    }

    #[test]
    fn new_history_starts_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        std::fs::write(&path, "stale\n").unwrap();
        let mut register = OpenRegister::open(&layout(&path), None, false).unwrap();
        let appended = register
            .append_group(&layout(&path), &[row(1)])
            .unwrap()
            .remove(0);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n"
        );
        assert_eq!(appended.length, 16);
    }

    #[test]
    fn uncommitted_tail_moved_aside() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let mut register = OpenRegister::open(&layout(&path), None, false).unwrap();
        let recorded = register
            .append_group(&layout(&path), &[row(1)])
            .unwrap()
            .remove(0);
        register.append_group(&layout(&path), &[row(2)]).unwrap();
        drop(register);

        let mut resumed = OpenRegister::open(&layout(&path), Some(&recorded), true).unwrap();
        resumed.append_group(&layout(&path), &[row(2)]).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n2,row-2\n"
        );
        let damage = dir.path().join("register.csv.16.damage");
        assert_eq!(std::fs::read_to_string(damage).unwrap(), "2,row-2\n");
    }

    #[test]
    fn file_changed_since_history_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let mut register = OpenRegister::open(&layout(&path), None, false).unwrap();
        let recorded = register
            .append_group(&layout(&path), &[row(1)])
            .unwrap()
            .remove(0);
        drop(register);
        std::fs::write(&path, "id,name\n9,row-9\n").unwrap();
        let error = OpenRegister::open(&layout(&path), Some(&recorded), true)
            .err()
            .expect("a changed file is refused");
        assert!(error.to_string().contains("no longer holds"), "{error}");
    }

    #[test]
    fn shared_register_file_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let link = dir.path().join("register-link.csv");
        let mut first = OpenRegister::open(&layout(&path), None, false).unwrap();
        first.append_group(&layout(&path), &[row(1)]).unwrap();
        std::fs::hard_link(&path, &link).unwrap();
        let error = OpenRegister::open(&layout(&link), None, false)
            .err()
            .expect("a held file is refused");
        assert!(
            matches!(error, EffectError::DestinationChanged { .. }),
            "{error}"
        );
        drop(first);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n"
        );
    }

    #[test]
    fn held_register_file_resume_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let mut first = OpenRegister::open(&layout(&path), None, false).unwrap();
        let recorded = first
            .append_group(&layout(&path), &[row(1)])
            .unwrap()
            .remove(0);
        first.append_group(&layout(&path), &[row(2)]).unwrap();
        let error = OpenRegister::open(&layout(&path), Some(&recorded), true)
            .err()
            .expect("a held file is refused");
        assert!(
            matches!(error, EffectError::DestinationChanged { .. }),
            "{error}"
        );
        drop(first);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n2,row-2\n"
        );
    }

    #[test]
    fn dropped_register_file_reopens() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        drop(OpenRegister::open(&layout(&path), None, false).unwrap());
        assert!(OpenRegister::open(&layout(&path), None, false).is_ok());
    }

    #[tokio::test]
    async fn poisoned_register_second_writer_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let layout = layout(&path);
        let mut register = OpenRegister::open(&layout, None, false).unwrap();
        register.append_group(&layout, &[row(1)]).unwrap();
        register.file = File::open(&path).unwrap();
        register.file.try_lock().unwrap();
        let first = CsvRegister {
            layout: layout.clone().into(),
            state: Arc::new(Mutex::new(RegisterState::Open(Box::new(register)))),
        };
        let ctx = EffectContext::for_test(FlowId::new(), "register", 1);
        first.append(&row(2), &ctx).await.unwrap_err();
        let second = CsvRegister {
            layout: layout.into(),
            state: Arc::new(Mutex::new(RegisterState::Closed)),
        };
        let error = second.append(&row(9), &ctx).await.unwrap_err();
        assert!(
            matches!(error, EffectError::DestinationChanged { .. }),
            "{error}"
        );
        drop(first);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n"
        );
    }

    #[test]
    fn large_prefix_resume_preserves_digest() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let layout = layout(&path);
        let mut first = fields(1);
        first.insert("name".into(), Value::from("x".repeat(128 * 1024)));
        let recorded = OpenRegister::open(&layout, None, false)
            .unwrap()
            .append_group(
                &layout,
                &[CsvRow {
                    fields: first.into(),
                }],
            )
            .unwrap()
            .remove(0);
        let mut next = fields(2);
        next.insert("extra".into(), Value::from("ignored"));
        let resumed = OpenRegister::open(&layout, Some(&recorded), true)
            .unwrap()
            .append_group(
                &layout,
                &[CsvRow {
                    fields: next.into(),
                }],
            )
            .unwrap()
            .remove(0);
        let file = std::fs::read(path).unwrap();
        assert!(file.ends_with(b"2,row-2\n"));
        assert_eq!(
            resumed.sha256,
            hex(ring::digest::digest(&SHA256, &file).as_ref())
        );
    }

    #[tokio::test]
    async fn poisoned_register_refuses_append() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let layout = layout(&path);
        let mut register = OpenRegister::open(&layout, None, false).unwrap();
        register.file = File::open(&path).unwrap();
        let register = CsvRegister {
            layout: layout.into(),
            state: Arc::new(Mutex::new(RegisterState::Open(Box::new(register)))),
        };
        let ctx = EffectContext::for_test(FlowId::new(), "register", 1);
        for id in 1..=2 {
            let error = register.append(&row(id), &ctx).await.unwrap_err();
            assert!(matches!(error, EffectError::DestinationChanged { .. }));
        }
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
    }

    #[tokio::test]
    async fn live_register_appends_without_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let register = CsvRegister {
            layout: layout(&path).into(),
            state: Arc::new(Mutex::new(RegisterState::Closed)),
        };
        let ctx = EffectContext::for_test(FlowId::new(), "register", 0);
        let first = register.append(&row(1), &ctx).await.unwrap();
        let second = register.append(&row(2), &ctx).await.unwrap();
        assert!(second.length > first.length);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n2,row-2\n"
        );
    }
}
