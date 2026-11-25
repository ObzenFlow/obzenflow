// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Console sink
//!
//! A sink that writes each input, formatted, to stdout or stderr.

use super::blocking::blocking_io;
use async_trait::async_trait;
use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    port, system, Command, DeclaredEffectSet, EffectContext, EffectError, Port, SinkBatchDefaults,
    SinkGroupSystem, SinkRole,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::Serialize;
use std::collections::HashMap;
use std::io::{self, Write};
use std::marker::PhantomData;
use std::sync::{Mutex, OnceLock};

/// Output destination for `ConsoleSink`.
#[derive(Clone, Copy, Debug, Default)]
pub enum OutputDestination {
    #[default]
    Stdout,
    Stderr,
}

impl OutputDestination {
    fn write_frame(self, frame: &str) -> io::Result<u64> {
        match self {
            Self::Stdout => write_frame_to(&mut io::stdout().lock(), frame),
            Self::Stderr => write_frame_to(&mut io::stderr().lock(), frame),
        }
    }
}

/// A formatted frame and its presentation contract.
///
/// `Text` and `Verbatim` append one line feed and flush the destination, even
/// for an empty body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", content = "body", rename_all = "snake_case")]
pub enum ConsoleOutput {
    /// Human-readable text.
    Text(String),
    /// A body whose bytes belong to the formatter, such as JSON.
    Verbatim(String),
    /// Intentional zero-item no-op: no write, line feed, or flush.
    Empty,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleFormatError(String);
impl ConsoleFormatError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}
impl std::fmt::Display for ConsoleFormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for ConsoleFormatError {}
impl From<serde_json::Error> for ConsoleFormatError {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

/// Format one item into the output its console sink writes.
pub trait Formatter<T>: Send + Sync + Clone {
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError>;
}

fn write_frame_to(output: &mut impl Write, frame: &str) -> io::Result<u64> {
    output.write_all(frame.as_bytes())?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(frame.len() as u64 + 1)
}

impl<T, F> Formatter<T> for F
where
    F: Fn(&T) -> String + Send + Sync + Clone,
{
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError> {
        Ok(ConsoleOutput::Text((self)(item)))
    }
}

/// JSON formatter (compact, single-line).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonFormatter;

impl<T> Formatter<T> for JsonFormatter
where
    T: Serialize,
{
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError> {
        Ok(ConsoleOutput::Verbatim(serde_json::to_string(item)?))
    }
}

/// JSON formatter (pretty, multi-line).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonPrettyFormatter;

impl<T> Formatter<T> for JsonPrettyFormatter
where
    T: Serialize,
{
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError> {
        Ok(ConsoleOutput::Verbatim(serde_json::to_string_pretty(item)?))
    }
}

/// Debug formatter (uses `std::fmt::Debug`).
#[derive(Clone, Copy, Debug, Default)]
pub struct DebugFormatter;

impl<T> Formatter<T> for DebugFormatter
where
    T: std::fmt::Debug,
{
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError> {
        Ok(ConsoleOutput::Text(format!("{item:?}")))
    }
}

/// Extracts an ordered row of cells from each item for the configured columns.
pub struct TableFormatter<T, E> {
    columns: Vec<String>,
    extractor: E,
    max_col_width: usize,
    _phantom: PhantomData<fn() -> T>,
}

fn render_table<R: AsRef<[String]>>(
    columns: &[String],
    rows: &[R],
    max_col_width: usize,
) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let col_count = columns.len();
    let widths: Vec<usize> = (0..col_count)
        .map(|col_idx| {
            let header_width = columns
                .get(col_idx)
                .map(|s| display_width(s.as_str()))
                .unwrap_or(0);

            let max_value_width = rows
                .iter()
                .filter_map(|row| row.as_ref().get(col_idx).map(|s| display_width(s.as_str())))
                .max()
                .unwrap_or(0);

            header_width.max(max_value_width).min(max_col_width).max(1)
        })
        .collect();

    let mut out = String::new();

    // Top border
    out.push('┌');
    for (i, width) in widths.iter().enumerate() {
        out.push_str(&"─".repeat(width + 2));
        out.push(if i < widths.len() - 1 { '┬' } else { '┐' });
    }
    out.push('\n');

    // Header row
    out.push('│');
    for (i, col) in columns.iter().enumerate() {
        let col = truncate_with_ellipsis(col, widths[i]);
        out.push(' ');
        out.push_str(&pad_center(&col, widths[i]));
        out.push(' ');
        out.push('│');
    }
    out.push('\n');

    // Header separator
    out.push('├');
    for (i, width) in widths.iter().enumerate() {
        out.push_str(&"─".repeat(width + 2));
        out.push(if i < widths.len() - 1 { '┼' } else { '┤' });
    }
    out.push('\n');

    // Data rows
    for row in rows {
        out.push('│');
        for (col_idx, width) in widths.iter().enumerate().take(col_count) {
            let cell = row.as_ref().get(col_idx).map(String::as_str).unwrap_or("-");
            let truncated = truncate_with_ellipsis(cell, *width);
            out.push(' ');
            out.push_str(&pad_right(&truncated, *width));
            out.push(' ');
            out.push('│');
        }
        out.push('\n');
    }

    // Bottom border
    out.push('└');
    for (i, width) in widths.iter().enumerate() {
        out.push_str(&"─".repeat(width + 2));
        out.push(if i < widths.len() - 1 { '┴' } else { '┘' });
    }

    out
}

impl<T, E> Clone for TableFormatter<T, E>
where
    E: Clone,
{
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            extractor: self.extractor.clone(),
            max_col_width: self.max_col_width,
            _phantom: PhantomData,
        }
    }
}

impl<T, E> std::fmt::Debug for TableFormatter<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableFormatter")
            .field("column_count", &self.columns.len())
            .field("max_col_width", &self.max_col_width)
            .finish()
    }
}

impl<T, E> TableFormatter<T, E>
where
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    pub fn new(columns: &[&str], extractor: E) -> Self {
        Self {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            extractor,
            max_col_width: 30,
            _phantom: PhantomData,
        }
    }

    pub fn max_width(mut self, width: usize) -> Self {
        self.max_col_width = width.max(1);
        self
    }

    fn prepare_row(&self, item: &T) -> Result<Vec<String>, ConsoleFormatError> {
        let row = (self.extractor)(item);
        if self.columns.is_empty() || row.len() != self.columns.len() {
            return Err(ConsoleFormatError::new(
                "table row does not match its columns",
            ));
        }
        Ok(row)
    }
}

impl<T, E> Formatter<T> for TableFormatter<T, E>
where
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError> {
        Ok(ConsoleOutput::Text(render_table(
            &self.columns,
            &[self.prepare_row(item)?],
            self.max_col_width,
        )))
    }
}

fn empty_lines<T>(_item: &T) -> Vec<String> {
    Vec::new()
}

/// Snapshot table formatter - renders a full table on every `format()` call.
///
/// This is useful when each item is already a snapshot (e.g. a "materialized view"
/// event containing multiple rows).
pub struct SnapshotTableFormatter<T, E, H = fn(&T) -> Vec<String>, F = fn(&T) -> Vec<String>> {
    columns: Vec<String>,
    header: H,
    extractor: E,
    footer: F,
    max_col_width: usize,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, E, H, F> Clone for SnapshotTableFormatter<T, E, H, F>
where
    E: Clone,
    H: Clone,
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            columns: self.columns.clone(),
            header: self.header.clone(),
            extractor: self.extractor.clone(),
            footer: self.footer.clone(),
            max_col_width: self.max_col_width,
            _phantom: PhantomData,
        }
    }
}

impl<T, E, H, F> std::fmt::Debug for SnapshotTableFormatter<T, E, H, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotTableFormatter")
            .field("column_count", &self.columns.len())
            .field("max_col_width", &self.max_col_width)
            .finish()
    }
}

impl<T, E> SnapshotTableFormatter<T, E>
where
    E: Fn(&T) -> Vec<Vec<String>> + Send + Sync + Clone,
{
    pub fn new(columns: &[&str], extractor: E) -> Self {
        Self {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            header: empty_lines::<T>,
            extractor,
            footer: empty_lines::<T>,
            max_col_width: 30,
            _phantom: PhantomData,
        }
    }
}

impl<T, E, H, F> SnapshotTableFormatter<T, E, H, F> {
    pub fn max_width(mut self, width: usize) -> Self {
        self.max_col_width = width.max(1);
        self
    }

    pub fn with_header<H2>(self, header: H2) -> SnapshotTableFormatter<T, E, H2, F> {
        SnapshotTableFormatter {
            columns: self.columns,
            header,
            extractor: self.extractor,
            footer: self.footer,
            max_col_width: self.max_col_width,
            _phantom: PhantomData,
        }
    }

    pub fn with_footer<F2>(self, footer: F2) -> SnapshotTableFormatter<T, E, H, F2> {
        SnapshotTableFormatter {
            columns: self.columns,
            header: self.header,
            extractor: self.extractor,
            footer,
            max_col_width: self.max_col_width,
            _phantom: PhantomData,
        }
    }
}

impl<T, E, H, F> Formatter<T> for SnapshotTableFormatter<T, E, H, F>
where
    E: Fn(&T) -> Vec<Vec<String>> + Send + Sync + Clone,
    H: Fn(&T) -> Vec<String> + Send + Sync + Clone,
    F: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    fn format(&self, item: &T) -> Result<ConsoleOutput, ConsoleFormatError> {
        let header = (self.header)(item);
        let rows = (self.extractor)(item);
        let footer = (self.footer)(item);
        let table = render_table(&self.columns, &rows, self.max_col_width);

        let mut parts = Vec::new();
        if !header.is_empty() {
            parts.push(header.join("\n"));
        }
        if !table.is_empty() {
            parts.push(table);
        }
        if !footer.is_empty() {
            parts.push(footer.join("\n"));
        }

        if parts.is_empty() {
            Ok(ConsoleOutput::Empty)
        } else {
            Ok(ConsoleOutput::Text(parts.join("\n")))
        }
    }
}

fn truncate_with_ellipsis(s: &str, max_chars: usize) -> String {
    let current_width = display_width(s);
    if current_width <= max_chars {
        return s.to_string();
    }

    if max_chars <= 1 {
        return "…".to_string();
    }

    let mut truncated = String::new();
    let mut width = 0usize;
    let available = max_chars - 1;
    for ch in s.chars() {
        let ch_width = char_display_width(ch);
        if width + ch_width > available {
            break;
        }
        width += ch_width;
        truncated.push(ch);
    }
    truncated.push('…');
    truncated
}

fn pad_right(s: &str, width: usize) -> String {
    let s_width = display_width(s);
    if s_width >= width {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len() + (width - s_width));
    out.push_str(s);
    out.push_str(&" ".repeat(width - s_width));
    out
}

fn pad_center(s: &str, width: usize) -> String {
    let s_width = display_width(s);
    if s_width >= width {
        return s.to_string();
    }
    let total_pad = width - s_width;
    let left = total_pad / 2;
    let right = total_pad - left;
    let mut out = String::with_capacity(s.len() + total_pad);
    out.push_str(&" ".repeat(left));
    out.push_str(s);
    out.push_str(&" ".repeat(right));
    out
}

fn display_width(s: &str) -> usize {
    s.chars().map(char_display_width).sum()
}

fn char_display_width(ch: char) -> usize {
    let code = ch as u32;

    // Zero-width joiner
    if code == 0x200D {
        return 0;
    }

    // Variation selectors (VS1..VS16 and supplement)
    if (0xFE00..=0xFE0F).contains(&code) || (0xE0100..=0xE01EF).contains(&code) {
        return 0;
    }

    // Common combining marks
    if (0x0300..=0x036F).contains(&code)
        || (0x1AB0..=0x1AFF).contains(&code)
        || (0x1DC0..=0x1DFF).contains(&code)
        || (0x20D0..=0x20FF).contains(&code)
        || (0xFE20..=0xFE2F).contains(&code)
    {
        return 0;
    }

    // Heuristic: treat emoji + CJK wide characters as width 2.
    if is_wide(code) {
        return 2;
    }

    1
}

fn is_wide(code: u32) -> bool {
    // Emoji-ish ranges (good enough for terminals; fixes 🟡/🟢/🔴 alignment)
    if (0x1F1E6..=0x1F1FF).contains(&code) // regional indicators
        || (0x1F300..=0x1FAFF).contains(&code) // misc pictographs + extended
        || (0x2600..=0x26FF).contains(&code) // misc symbols
        || (0x2700..=0x27BF).contains(&code)
        || (0x2300..=0x23FF).contains(&code)
    {
        return true;
    }

    // CJK wide blocks
    (0x1100..=0x115F).contains(&code) // Hangul Jamo init
        || (0x2329..=0x232A).contains(&code)
        || (0x2E80..=0xA4CF).contains(&code) // CJK/CJK Symbols/Kana/etc (broad)
        || (0xAC00..=0xD7A3).contains(&code) // Hangul syllables
        || (0xF900..=0xFAFF).contains(&code) // CJK compatibility ideographs
        || (0xFE10..=0xFE19).contains(&code)
        || (0xFE30..=0xFE6F).contains(&code)
        || (0xFF01..=0xFF60).contains(&code) // fullwidth forms
        || (0xFFE0..=0xFFE6).contains(&code)
}

/// The port a console sink writes through. A console line that a crash left
/// in doubt may print again after a resume, so the method is `at_least_once`.
#[port]
#[async_trait]
pub trait ConsolePort {
    #[effect(type = "obzenflow.console.output", at_least_once, records = reply)]
    async fn write(&self, output: &ConsoleOutput) -> Result<(), EffectError>;
}

#[system("console")]
struct Stdout;

#[async_trait]
impl ConsolePort for Stdout {
    async fn write(&self, output: &ConsoleOutput) -> Result<(), EffectError> {
        write_output(OutputDestination::Stdout, output).await
    }
}

#[system("console-stderr")]
struct Stderr;

#[async_trait]
impl ConsolePort for Stderr {
    async fn write(&self, output: &ConsoleOutput) -> Result<(), EffectError> {
        write_output(OutputDestination::Stderr, output).await
    }
}

async fn write_output(
    destination: OutputDestination,
    output: &ConsoleOutput,
) -> Result<(), EffectError> {
    let frame = match output {
        ConsoleOutput::Text(frame) | ConsoleOutput::Verbatim(frame) => frame.clone(),
        ConsoleOutput::Empty => return Ok(()),
    };
    write_frame(destination, frame).await
}

async fn write_frame(destination: OutputDestination, frame: String) -> Result<(), EffectError> {
    blocking_io(move || destination.write_frame(&frame))
        .await?
        .map(|_| ())
        .map_err(|error| EffectError::Execution(format!("console write failed: {error}")))
}

fn console_port(destination: OutputDestination) -> Port<dyn ConsolePort> {
    static STDOUT: OnceLock<Port<dyn ConsolePort>> = OnceLock::new();
    static STDERR: OnceLock<Port<dyn ConsolePort>> = OnceLock::new();
    match destination {
        OutputDestination::Stdout => STDOUT.get_or_init(|| Port::to(Stdout)).clone(),
        OutputDestination::Stderr => STDERR.get_or_init(|| Port::to(Stderr)).clone(),
    }
}

/// A sink that writes each input to the console, formatted by `F`.
/// Stdout and stderr write once per input. The runtime journals up to 64
/// queued results together by default, with zero maximum wait.
pub struct ConsoleSink<T, F = JsonFormatter> {
    formatter: F,
    destination: Option<OutputDestination>,
    port: Port<dyn ConsolePort>,
    _phantom: PhantomData<fn() -> T>,
}

impl<T, F> Clone for ConsoleSink<T, F>
where
    F: Clone,
{
    fn clone(&self) -> Self {
        Self {
            formatter: self.formatter.clone(),
            destination: self.destination,
            port: self.port.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for ConsoleSink<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsoleSink")
            .field("type", &std::any::type_name::<T>())
            .field("port", &self.port)
            .finish()
    }
}

impl<T, F> ConsoleSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Formatter<T>,
{
    /// Write each input to stdout, formatted by `formatter`.
    pub fn new(formatter: F) -> Self {
        Self {
            formatter,
            destination: Some(OutputDestination::Stdout),
            port: console_port(OutputDestination::Stdout),
            _phantom: PhantomData,
        }
    }
}

impl<T, F> ConsoleSink<T, F> {
    pub fn to_stderr(mut self) -> Self {
        self.destination = Some(OutputDestination::Stderr);
        self.port = console_port(OutputDestination::Stderr);
        self
    }

    /// Write through another system that answers `ConsolePort`. A replacement
    /// uses the framework's one-request default.
    pub fn through(mut self, port: Port<dyn ConsolePort>) -> Self {
        self.destination = None;
        self.port = port;
        self
    }
}

impl<T, F> Command for ConsoleSink<T, F> {
    type Ports = (Port<dyn ConsolePort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<ConsoleOutput, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<T, F> SinkRole<T> for ConsoleSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Formatter<T> + 'static,
{
    type Request = ConsoleOutput;

    fn batch_defaults(&self) -> SinkBatchDefaults {
        if self.destination.is_some() {
            super::queued_batch_defaults()
        } else {
            SinkBatchDefaults::default()
        }
    }

    fn prepare(&self, input: T) -> Result<ConsoleOutput, HandlerError> {
        self.formatter
            .format(&input)
            .map_err(|error| HandlerError::Validation(error.to_string()))
    }
}

/// Ordered cells contributed by one input to a console table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TableRow {
    pub cells: Vec<String>,
}

#[port]
#[async_trait]
pub trait ConsoleTablePort {
    /// Write the cells from one input. The built-in table system renders a whole group together.
    #[effect(type = "obzenflow.console.table_row", at_least_once, records = reply)]
    async fn write_row(&self, row: &TableRow, ctx: &EffectContext) -> Result<(), EffectError>;
}

#[system("console-table")]
#[derive(Clone)]
struct ConsoleTable {
    columns: Vec<String>,
    max_width: usize,
    destination: OutputDestination,
}

impl ConsoleTable {
    fn port(self) -> Result<Port<dyn ConsoleTablePort>, ConsoleFormatError> {
        static PORTS: OnceLock<Mutex<HashMap<String, Port<dyn ConsoleTablePort>>>> =
            OnceLock::new();
        let shape = serde_json::to_vec(&(
            &self.columns,
            self.max_width,
            matches!(self.destination, OutputDestination::Stderr),
        ))
        .map_err(|error| ConsoleFormatError::new(error.to_string()))?;
        let digest = ring::digest::digest(&ring::digest::SHA256, &shape);
        let instance = super::hex(&digest.as_ref()[..16]);
        let mut ports = PORTS
            .get_or_init(Mutex::default)
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        Ok(ports
            .entry(instance.clone())
            .or_insert_with(|| {
                Port::deferred_grouped::<TableRow, _, _>(move || Ok(self.clone()))
                    .instance(&instance)
            })
            .clone())
    }
}

#[async_trait]
impl ConsoleTablePort for ConsoleTable {
    async fn write_row(&self, row: &TableRow, _: &EffectContext) -> Result<(), EffectError> {
        self.render_rows(std::slice::from_ref(row)).await
    }
}

#[async_trait]
impl SinkGroupSystem<TableRow> for ConsoleTable {
    async fn write_group(
        &self,
        rows: std::sync::Arc<[TableRow]>,
        _: &EffectContext,
    ) -> Result<Vec<()>, EffectError> {
        self.render_rows(&rows).await?;
        Ok(vec![(); rows.len()])
    }
}

impl ConsoleTable {
    async fn render_rows(&self, rows: &[TableRow]) -> Result<(), EffectError> {
        let cells: Vec<_> = rows.iter().map(|row| &row.cells).collect();
        let frame = render_table(&self.columns, &cells, self.max_width);
        write_frame(self.destination, frame).await
    }
}

/// A sink that contributes one row per input to the current console table.
pub struct TableConsoleSink<T, E> {
    formatter: TableFormatter<T, E>,
    port: Port<dyn ConsoleTablePort>,
    batch: SinkBatchDefaults,
}

impl<T, E: Clone> Clone for TableConsoleSink<T, E> {
    fn clone(&self) -> Self {
        Self {
            formatter: self.formatter.clone(),
            port: self.port.clone(),
            batch: self.batch,
        }
    }
}

impl<T, E> std::fmt::Debug for TableConsoleSink<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableConsoleSink")
            .field("port", &self.port)
            .field("batch", &self.batch)
            .finish()
    }
}

impl<T, E> ConsoleSink<T, TableFormatter<T, E>> {
    /// Use the table port with this default number of rows per table.
    pub fn batch_size(self, size: usize) -> Result<TableConsoleSink<T, E>, ConsoleFormatError> {
        let size = std::num::NonZeroUsize::new(size)
            .ok_or_else(|| ConsoleFormatError::new("console table batch size must be positive"))?;
        let destination = self.destination.ok_or_else(|| {
            ConsoleFormatError::new("select the table system with through() after batch_size()")
        })?;
        let port = ConsoleTable {
            columns: self.formatter.columns.clone(),
            max_width: self.formatter.max_col_width,
            destination,
        }
        .port()?;
        Ok(TableConsoleSink {
            formatter: self.formatter,
            port,
            batch: SinkBatchDefaults {
                size: Some(size),
                max_wait: None,
            },
        })
    }
}

impl<T, E> TableConsoleSink<T, E> {
    /// Set how long an open group may wait for more rows. Zero takes only queued rows.
    pub fn batch_max_wait(mut self, wait: std::time::Duration) -> Self {
        self.batch.max_wait = Some(wait);
        self
    }

    pub fn through(mut self, port: Port<dyn ConsoleTablePort>) -> Self {
        self.port = port;
        self
    }
}

impl<T, E> Command for TableConsoleSink<T, E> {
    type Ports = (Port<dyn ConsoleTablePort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<TableRow, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<T, E> SinkRole<T> for TableConsoleSink<T, E>
where
    T: TypedPayload + Send + Sync + 'static,
    E: Fn(&T) -> Vec<String> + Clone + Send + Sync + 'static,
{
    type Request = TableRow;

    fn prepare(&self, input: T) -> Result<TableRow, HandlerError> {
        self.formatter
            .prepare_row(&input)
            .map(|cells| TableRow { cells })
            .map_err(|error| HandlerError::Validation(error.to_string()))
    }

    fn batch_defaults(&self) -> SinkBatchDefaults {
        self.batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        value: String,
    }
    impl TypedPayload for TestEvent {
        const EVENT_TYPE: &'static str = "test.event";
    }

    fn text(output: ConsoleOutput) -> String {
        match output {
            ConsoleOutput::Text(value) => value,
            ConsoleOutput::Verbatim(_) | ConsoleOutput::Empty => {
                panic!("expected human-readable text")
            }
        }
    }

    #[system("captured-console")]
    struct Captured(Arc<Mutex<Vec<ConsoleOutput>>>);

    #[async_trait]
    impl ConsolePort for Captured {
        async fn write(&self, output: &ConsoleOutput) -> Result<(), EffectError> {
            self.0.lock().unwrap().push(output.clone());
            Ok(())
        }
    }

    #[test]
    fn console_defaults_follow_destination() {
        let stdout = ConsoleSink::<TestEvent>::new(JsonFormatter);
        assert_eq!(stdout.batch_defaults().size.unwrap().get(), 64);
        let stderr = stdout.to_stderr();
        assert_eq!(stderr.batch_defaults().size.unwrap().get(), 64);
        let external = stderr.through(Port::to(Captured(Default::default())));
        assert!(external.batch_defaults().size.is_none());
    }

    #[test]
    fn formatters_render_one_item() {
        let item = TestEvent {
            value: "hello".into(),
        };
        assert_eq!(
            JsonFormatter.format(&item).unwrap(),
            ConsoleOutput::Verbatim(r#"{"value":"hello"}"#.into())
        );
        assert_eq!(
            text(DebugFormatter.format(&item).unwrap()),
            r#"TestEvent { value: "hello" }"#
        );
        let formatter =
            TableFormatter::new(&["value"], |e: &TestEvent| vec![e.value.clone()]).max_width(3);
        assert!(text(formatter.format(&item).unwrap()).contains("he…"));
        let snapshot =
            SnapshotTableFormatter::new(&["value"], |e: &TestEvent| vec![vec![e.value.clone()]])
                .with_header(|_: &TestEvent| vec!["header".into()])
                .with_footer(|_: &TestEvent| vec!["footer".into()]);
        let frame = text(snapshot.format(&item).unwrap());
        assert!(frame.contains("header") && frame.contains("hello") && frame.contains("footer"));
        let unicode = render_table(&["status".into()], &[vec!["🟡".into()]], 30);
        let widths: Vec<_> = unicode.lines().map(display_width).collect();
        assert!(widths.iter().all(|width| *width == widths[0]));
    }

    #[test]
    fn mismatched_table_row_refused() {
        let sink =
            ConsoleSink::<TestEvent, _>::new(TableFormatter::new(&["a", "b"], |e: &TestEvent| {
                vec![e.value.clone()]
            }));
        let error = sink
            .prepare(TestEvent { value: "x".into() })
            .expect_err("one cell for two columns");
        assert!(error.to_string().contains("does not match its columns"));
    }

    #[test]
    fn distinct_console_tables_collect() {
        use obzenflow_runtime::commands::effectful_transform_effects;
        use obzenflow_runtime::effects::{EffectPortRegistry, SinkHandler};
        let table = |column: &str, width, stderr, size| {
            let formatter =
                TableFormatter::new(&[column], |row: &TestEvent| vec![row.value.clone()])
                    .max_width(width);
            let sink = ConsoleSink::new(formatter);
            let sink = if stderr { sink.to_stderr() } else { sink };
            sink.batch_size(size).unwrap()
        };
        let tables = [
            table("customer", 30, false, 8),
            table("invoice", 30, false, 8),
            table("customer", 40, false, 8),
            table("customer", 30, true, 8),
            table("customer", 30, false, 16),
        ];
        let names: std::collections::HashSet<_> = tables
            .iter()
            .map(|table| table.port.system_name())
            .collect();
        assert_eq!(names.len(), 4);
        let declarations: Vec<_> = tables
            .into_iter()
            .flat_map(|table| {
                effectful_transform_effects(&SinkHandler::<_, TestEvent>::new(table)).unwrap()
            })
            .collect();
        EffectPortRegistry::collect_from_declarations(&declarations).unwrap();
    }

    #[tokio::test]
    async fn console_helper_test_port_answers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let captured = Captured(seen.clone());
        let sink = ConsoleSink::<TestEvent, _>::new(|e: &TestEvent| format!("got {}", e.value))
            .through(Port::to(Captured(seen.clone())));
        assert_eq!(sink.ports().0.system_name(), "captured-console");
        let request = sink
            .prepare(TestEvent {
                value: "one".into(),
            })
            .unwrap();
        captured.write(&request).await.expect("captured");
        assert_eq!(
            *seen.lock().unwrap(),
            [ConsoleOutput::Text("got one".into())]
        );
    }

    #[test]
    fn frame_ends_with_one_line_feed() {
        let mut bytes = Vec::new();
        assert_eq!(write_frame_to(&mut bytes, "abcde").unwrap(), 6);
        assert_eq!(bytes, b"abcde\n");
    }
}
