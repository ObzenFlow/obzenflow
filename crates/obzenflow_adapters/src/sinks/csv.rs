// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! CSV file sink.
//!
//! A [`CsvProjection`] maps each input to one serializable row, and the sink
//! appends it to a CSV file that one run history writes. A new history starts
//! the file empty. Each append records the file's length and SHA-256 after the
//! row, so a resumed run checks the file against its history before its first
//! write, and moves a tail that no record holds into an inert file beside it,
//! named `<file>.<length>.damage`.

use super::blocking::blocking_io;
use async_trait::async_trait;
use csv::{ReaderBuilder, WriterBuilder};
use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    port, system, Command, DeclaredEffectSet, EffectContext, EffectError,
    EffectPortResolutionError, Port, SinkRole,
};
use obzenflow_runtime::effects::{SinkBatchDefaults, SinkGroupSystem};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use ring::digest::{Context, SHA256};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// User-owned mapping from one domain input to one serializable CSV row.
///
/// The projection value owns its input contract through an associated type,
/// so applications pass it to [`CsvSink::builder`] without repeating the event
/// type.
pub trait CsvProjection: Clone + Send + Sync + 'static {
    /// Domain input accepted by this projection.
    type Input: TypedPayload + Send + Sync + 'static;
    /// Serializable object used for column selection and row encoding.
    type Row: Serialize;

    /// Project one domain input into its CSV-facing row.
    fn project(&self, input: &Self::Input) -> Result<Self::Row, HandlerError>;
}

/// One row a CSV sink appends: the projected row's fields, by name.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CsvRow {
    fields: Map<String, Value>,
}

/// What one append records: the file's length and SHA-256 after the row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Appended {
    pub length: u64,
    pub sha256: String,
}

/// The port a CSV sink appends through. A resume truncates what no record
/// holds before its first write, so each row lands once and the method is
/// `exactly_once`.
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
    Poisoned(String),
}

struct OpenRegister {
    file: File,
    length: u64,
    digest: Context,
    columns: Option<Vec<String>>,
    header_written: Written,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Written {
    Nothing,
    Header,
}

struct CsvRegister {
    layout: CsvLayout,
    state: Arc<Mutex<RegisterState>>,
}

#[system("csv-register")]
#[async_trait]
impl CsvRegisterPort for CsvRegister {
    async fn append(&self, row: &CsvRow, ctx: &EffectContext) -> Result<Appended, EffectError> {
        self.write_group(std::slice::from_ref(row), ctx)
            .await
            .map(|mut replies| replies.remove(0))
    }
}

#[async_trait]
impl SinkGroupSystem<CsvRow> for CsvRegister {
    async fn write_group(
        &self,
        rows: &[CsvRow],
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
        let rows = rows.to_vec();
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
                RegisterState::Poisoned(reason) => {
                    return Err(EffectError::Execution(reason.clone()));
                }
                RegisterState::Closed => unreachable!("a closed register is opened above"),
            };
            match register.append_group(&layout, &rows) {
                Ok(appended) => Ok(appended),
                Err(error) => {
                    let reason = format!(
                        "{} is in an unknown state after a failed write: {error}",
                        layout.path.display()
                    );
                    *state = RegisterState::Poisoned(reason.clone());
                    Err(EffectError::Execution(reason))
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
                .truncate(!continues_history)
                .open(&layout.path)
                .map_err(|error| io_failure(&layout.path, error))?;
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
            });
        };
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&layout.path)
            .map_err(|_| no_longer_matches(&layout.path, recorded))?;
        let actual = file
            .metadata()
            .map_err(|error| io_failure(&layout.path, error))?
            .len();
        if actual < recorded.length {
            return Err(no_longer_matches(&layout.path, recorded));
        }
        let mut prefix = vec![0u8; usize::try_from(recorded.length).unwrap_or(usize::MAX)];
        file.read_exact(&mut prefix)
            .map_err(|error| io_failure(&layout.path, error))?;
        let mut digest = Context::new(&SHA256);
        digest.update(&prefix);
        if hex(digest.clone().finish().as_ref()) != recorded.sha256 {
            return Err(no_longer_matches(&layout.path, recorded));
        }
        if actual > recorded.length {
            set_aside_tail(&mut file, &layout.path, recorded.length)?;
        }
        file.seek(SeekFrom::Start(recorded.length))
            .map_err(|error| io_failure(&layout.path, error))?;
        let columns = layout
            .columns
            .clone()
            .or_else(|| header_columns(&prefix, layout.delimiter));
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
        })
    }

    fn append_group(&mut self, layout: &CsvLayout, rows: &[CsvRow]) -> io::Result<Vec<Appended>> {
        let mut bytes = Vec::new();
        let mut replies = Vec::with_capacity(rows.len());
        for row in rows {
            replies.push(self.encode(layout, &row.fields, &mut bytes)?);
        }
        self.file.write_all(&bytes)?;
        self.file.sync_data()?;
        Ok(replies)
    }

    fn encode(
        &mut self,
        layout: &CsvLayout,
        fields: &Map<String, Value>,
        bytes: &mut Vec<u8>,
    ) -> io::Result<Appended> {
        let columns = self
            .columns
            .get_or_insert_with(|| {
                let mut columns: Vec<String> = fields.keys().cloned().collect();
                columns.sort();
                columns
            })
            .clone();
        let start = bytes.len();
        {
            let mut writer = WriterBuilder::new()
                .delimiter(layout.delimiter)
                .from_writer(&mut *bytes);
            if self.header_written == Written::Nothing {
                writer.write_record(layout.headers.as_ref().unwrap_or(&columns))?;
            }
            writer.write_record(columns.iter().map(|column| cell(fields.get(column))))?;
            writer.flush()?;
        }
        let bytes = &bytes[start..];
        self.header_written = Written::Header;
        self.length += bytes.len() as u64;
        self.digest.update(bytes);
        Ok(Appended {
            length: self.length,
            sha256: hex(self.digest.clone().finish().as_ref()),
        })
    }
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
        "moved the bytes after offset {length} of {}, which this run's history never recorded, into {}",
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

fn header_columns(prefix: &[u8], delimiter: u8) -> Option<Vec<String>> {
    ReaderBuilder::new()
        .has_headers(false)
        .delimiter(delimiter)
        .from_reader(prefix)
        .records()
        .next()
        .and_then(Result::ok)
        .map(|record| record.iter().map(str::to_string).collect())
}

fn cell(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(Value::Number(number)) => number.to_string(),
        Some(Value::Bool(flag)) => flag.to_string(),
        Some(other @ (Value::Array(_) | Value::Object(_))) => other.to_string(),
    }
}

fn register_port(layout: CsvLayout) -> Port<dyn CsvRegisterPort> {
    let path = &layout.path;
    let digest = ring::digest::digest(&SHA256, path.as_os_str().as_encoded_bytes());
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let instance = format!("{}.{name}", hex(&digest.as_ref()[..16]));
    Port::deferred_grouped::<CsvRow, _, _>(move || {
        Ok::<_, EffectPortResolutionError>(CsvRegister {
            layout: layout.clone(),
            state: Arc::new(Mutex::new(RegisterState::Closed)),
        })
    })
    .instance(&instance)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn poisoned_lock() -> EffectError {
    EffectError::Execution("CSV register lock poisoned".to_string())
}

fn io_failure(path: &Path, error: io::Error) -> EffectError {
    EffectError::Execution(format!("CSV register {} failed: {error}", path.display()))
}

fn no_longer_matches(path: &Path, recorded: &Appended) -> EffectError {
    let short = recorded.sha256.get(..4).unwrap_or(&recorded.sha256);
    EffectError::Execution(format!(
        "{} no longer holds what this run's history recorded (length {}, SHA-256 {short}…); \
         restore it as the history recorded it, then resume again",
        path.display(),
        recorded.length
    ))
}

/// Builder for a CSV projection whose input contract is owned by `P`.
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

    /// Set how many rows the register writes and syncs together by default.
    pub fn batch_size(mut self, size: usize) -> Result<Self, anyhow::Error> {
        self.batch.size = Some(
            std::num::NonZeroUsize::new(size)
                .ok_or_else(|| anyhow::anyhow!("CSV batch size must be positive"))?,
        );
        Ok(self)
    }

    /// Set how long an open group may wait for more rows. Zero takes only queued rows.
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
        let path = self.path.ok_or_else(|| anyhow::anyhow!("path required"))?;
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
        let layout = CsvLayout {
            path,
            columns: self.columns,
            headers: self.headers,
            delimiter: self.delimiter,
        };
        Ok(CsvSink {
            projection: self.projection,
            batch: self.batch,
            port: register_port(layout),
        })
    }
}

/// A sink that appends each input, projected by `P`, as one CSV row.
pub struct CsvSink<P> {
    projection: P,
    batch: SinkBatchDefaults,
    port: Port<dyn CsvRegisterPort>,
}

impl<P: Clone> Clone for CsvSink<P> {
    fn clone(&self) -> Self {
        Self {
            projection: self.projection.clone(),
            batch: self.batch,
            port: self.port.clone(),
        }
    }
}

impl<P> std::fmt::Debug for CsvSink<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvSink")
            .field("projection", &std::any::type_name::<P>())
            .field("port", &self.port)
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
    /// Append through another system that answers `CsvRegisterPort`, such as
    /// a test port.
    pub fn through(mut self, port: Port<dyn CsvRegisterPort>) -> Self {
        self.port = port;
        self
    }
}

impl<P> Command for CsvSink<P> {
    type Ports = (Port<dyn CsvRegisterPort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<CsvRow, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<P: CsvProjection> SinkRole<P::Input> for CsvSink<P> {
    type Request = CsvRow;

    fn batch_defaults(&self) -> SinkBatchDefaults {
        self.batch
    }

    fn prepare(&self, input: &P::Input) -> Result<CsvRow, HandlerError> {
        let row = self.projection.project(input)?;
        match serde_json::to_value(row) {
            Ok(Value::Object(fields)) => Ok(CsvRow { fields }),
            Ok(other) => Err(HandlerError::Validation(format!(
                "CsvSink requires object rows, got {other}"
            ))),
            Err(error) => Err(HandlerError::Validation(format!(
                "CsvSink failed to serialize projected row: {error}"
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

    #[test]
    fn register_directories_distinguish_bindings() {
        let east = register_port(layout(Path::new("east/register.csv")));
        let west = register_port(layout(Path::new("west/register.csv")));
        assert_ne!(east.system_name(), west.system_name());
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

        fn project(&self, entry: &Entry) -> Result<Entry, HandlerError> {
            Ok(entry.clone())
        }
    }

    #[test]
    fn csv_sinks_sharing_path_refused() {
        use obzenflow_runtime::effects::{
            effectful_transform_effects, EffectPortRegistry, EffectRegistrationCollectionError,
            SinkHandler,
        };
        let declarations = || {
            let sink = CsvSink::builder(EntryRow)
                .path("shared/register.csv")
                .build()
                .unwrap();
            effectful_transform_effects(&SinkHandler::<_, Entry>::new(sink)).unwrap()
        };
        let (first, second) = (declarations(), declarations());
        let refused = EffectPortRegistry::collect_from_declarations(first.iter().chain(&second));
        assert!(matches!(
            refused,
            Err(EffectRegistrationCollectionError::DistinctBindings { .. })
        ));
    }

    #[test]
    fn new_history_starts_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        std::fs::write(&path, "stale\n").unwrap();
        let mut register = OpenRegister::open(&layout(&path), None, false).unwrap();
        let appended = register
            .append_group(&layout(&path), &[CsvRow { fields: fields(1) }])
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
            .append_group(&layout(&path), &[CsvRow { fields: fields(1) }])
            .unwrap()
            .remove(0);
        register
            .append_group(&layout(&path), &[CsvRow { fields: fields(2) }])
            .unwrap();
        drop(register);

        let mut resumed = OpenRegister::open(&layout(&path), Some(&recorded), true).unwrap();
        resumed
            .append_group(&layout(&path), &[CsvRow { fields: fields(2) }])
            .unwrap();
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
            .append_group(&layout(&path), &[CsvRow { fields: fields(1) }])
            .unwrap()
            .remove(0);
        drop(register);
        std::fs::write(&path, "id,name\n9,row-9\n").unwrap();
        let error = OpenRegister::open(&layout(&path), Some(&recorded), true)
            .err()
            .expect("a changed file is refused");
        assert!(error.to_string().contains("no longer holds"), "{error}");
    }

    #[tokio::test]
    async fn live_register_appends_without_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("register.csv");
        let register = CsvRegister {
            layout: layout(&path),
            state: Arc::new(Mutex::new(RegisterState::Closed)),
        };
        let ctx = EffectContext::for_test(FlowId::new(), "register", 0);
        let first = register
            .append(&CsvRow { fields: fields(1) }, &ctx)
            .await
            .unwrap();
        let second = register
            .append(&CsvRow { fields: fields(2) }, &ctx)
            .await
            .unwrap();
        assert!(second.length > first.length);
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "id,name\n1,row-1\n2,row-2\n"
        );
    }
}
