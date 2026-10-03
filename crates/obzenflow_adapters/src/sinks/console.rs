// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Console sink
//!
//! A typed sink for printing events to stdout/stderr with reusable formatters.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, PendingSinkInput, SinkAuditOutcome, SinkBufferedOutcome, SinkCommitReceipt,
    SinkConnector, SinkDescription, SinkOperationError, SinkOperationResult, SinkTerminalOutcome,
    SinkWriteContext, SinkWriteFailure, SinkWritePhase, SinkWriteReport, SinkWriteResult,
    SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{self, Write};
use std::marker::PhantomData;

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

    fn delivery_method(self) -> DeliveryMethod {
        match self {
            Self::Stdout => DeliveryMethod::ConsoleStdout,
            Self::Stderr => DeliveryMethod::ConsoleStderr,
        }
    }

    fn description(self) -> SinkDescription {
        SinkDescription::method(self.delivery_method())
            .with_redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
    }
}

/// Immediate formatting result. Empty is an intentional no-op with no I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsoleOutput {
    Text(String),
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

/// Format one item without retaining rows or settlement authority.
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
        Ok(ConsoleOutput::Text(serde_json::to_string(item)?))
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
        Ok(ConsoleOutput::Text(serde_json::to_string_pretty(item)?))
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

/// Stateless table layout and row extraction. Buffered rows belong to TableConsoleWriter.
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

/// Typed console sink with a pluggable `Formatter`.
pub struct ConsoleSink<T, F = JsonFormatter> {
    formatter: F,
    destination: OutputDestination,
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
            _phantom: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for ConsoleSink<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsoleSink")
            .field("type", &std::any::type_name::<T>())
            .field("destination", &self.destination)
            .finish()
    }
}

impl<T, F> ConsoleSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Formatter<T>,
{
    /// Configure immediate console output with a pure formatter.
    pub fn new(formatter: F) -> Self {
        Self {
            formatter,
            destination: OutputDestination::Stdout,
            _phantom: PhantomData,
        }
    }
}

impl<T, E> ConsoleSink<T, TableFormatter<T, E>>
where
    T: TypedPayload + Send + Sync + 'static,
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    /// Buffer table rows in each stage-local writer until a size limit or drain.
    /// Defaults to 256 rows and 64 KiB of prepared cell bytes; there is no timer.
    pub fn buffered(self) -> TableConsoleSink<T, E> {
        TableConsoleSink {
            formatter: self.formatter,
            destination: self.destination,
            max_rows: 256,
            max_bytes: 64 * 1024,
        }
    }
}

impl<T, F> ConsoleSink<T, F> {
    pub fn to_stderr(mut self) -> Self {
        self.destination = OutputDestination::Stderr;
        self
    }
}

#[async_trait]
impl<T, F> InlineSink for ConsoleSink<T, F>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: Formatter<T> + 'static,
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        self.destination.description()
    }

    async fn write(&mut self, input: T, _context: SinkWriteContext) -> SinkWriteResult {
        let output = self.formatter.format(&input).map_err(|error| {
            SinkWriteFailure::current_only(
                SinkWritePhase::Encode,
                SinkOperationError::validation(error.to_string()),
            )
        })?;
        let outcome = match output {
            ConsoleOutput::Empty => {
                SinkTerminalOutcome::success_via(DeliveryMethod::Noop, Some(0)).with_items(0)
            }
            ConsoleOutput::Text(frame) => {
                let bytes = self.destination.write_frame(&frame).map_err(|error| {
                    SinkWriteFailure::poisoned(
                        SinkWritePhase::Execute,
                        SinkOperationError::other(error.to_string()),
                    )
                })?;
                SinkTerminalOutcome::success(Some(bytes)).with_items(1)
            }
        };
        Ok(SinkWriteReport::terminal(outcome))
    }
}

/// Reusable table configuration. Opening it never copies another writer's rows.
pub struct TableConsoleSink<T, E> {
    formatter: TableFormatter<T, E>,
    destination: OutputDestination,
    max_rows: usize,
    max_bytes: usize,
}
impl<T, E> TableConsoleSink<T, E> {
    pub fn to_stderr(mut self) -> Self {
        self.destination = OutputDestination::Stderr;
        self
    }
    pub fn batch_limits(
        mut self,
        rows: std::num::NonZeroUsize,
        bytes: std::num::NonZeroUsize,
    ) -> Self {
        self.max_rows = rows.get();
        self.max_bytes = bytes.get();
        self
    }
}
impl<T, E> std::fmt::Debug for TableConsoleSink<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableConsoleSink")
            .field("formatter", &self.formatter)
            .field("max_rows", &self.max_rows)
            .field("max_bytes", &self.max_bytes)
            .finish()
    }
}

pub struct TableConsoleWriter<T, E> {
    formatter: TableFormatter<T, E>,
    destination: OutputDestination,
    rows: Vec<(Vec<String>, PendingSinkInput)>,
    prepared_bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    poisoned: bool,
    #[cfg(test)]
    output: Option<std::sync::Arc<std::sync::Mutex<Box<dyn Write + Send>>>>,
}

#[async_trait]
impl<T, E> SinkConnector for TableConsoleSink<T, E>
where
    T: TypedPayload + Send + Sync + 'static,
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone + 'static,
{
    type Input = T;
    type Writer = TableConsoleWriter<T, E>;
    fn describe(&self) -> SinkDescription {
        self.destination.description()
    }
    async fn open(&self, _context: SinkWriterInitContext) -> SinkOperationResult<Self::Writer> {
        Ok(TableConsoleWriter {
            formatter: self.formatter.clone(),
            destination: self.destination,
            rows: Vec::new(),
            prepared_bytes: 0,
            max_rows: self.max_rows,
            max_bytes: self.max_bytes,
            poisoned: false,
            #[cfg(test)]
            output: None,
        })
    }
}

impl<T, E> TableConsoleWriter<T, E> {
    fn flush_pending(&mut self) -> io::Result<(Vec<SinkCommitReceipt>, u64)> {
        if self.poisoned {
            return Err(io::Error::other("console writer is poisoned"));
        }
        if self.rows.is_empty() {
            return Ok((Vec::new(), 0));
        }
        let borrowed = self
            .rows
            .iter()
            .map(|(row, _)| row.as_slice())
            .collect::<Vec<_>>();
        let frame = render_table(
            &self.formatter.columns,
            &borrowed,
            self.formatter.max_col_width,
        );
        #[cfg(test)]
        let result = match &self.output {
            Some(output) => write_frame_to(&mut *output.lock().unwrap(), &frame),
            None => self.destination.write_frame(&frame),
        };
        #[cfg(not(test))]
        let result = self.destination.write_frame(&frame);
        let bytes = match result {
            Ok(bytes) => bytes,
            Err(error) => {
                self.poisoned = true;
                return Err(error);
            }
        };
        self.prepared_bytes = 0;
        let receipts = self
            .rows
            .drain(..)
            .map(|(_, pending)| {
                SinkCommitReceipt::new(pending, SinkTerminalOutcome::success(None).with_items(1))
            })
            .collect();
        Ok((receipts, bytes))
    }
}

#[async_trait]
impl<T, E> SinkWriter for TableConsoleWriter<T, E>
where
    T: TypedPayload + Send + Sync + 'static,
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone + 'static,
{
    type Input = T;
    async fn write(&mut self, input: T, context: SinkWriteContext) -> SinkWriteResult {
        if self.poisoned {
            return Err(SinkWriteFailure::poisoned(
                SinkWritePhase::Execute,
                SinkOperationError::other("console writer is poisoned"),
            ));
        }
        let row = self.formatter.prepare_row(&input).map_err(|error| {
            SinkWriteFailure::current_only(
                SinkWritePhase::Encode,
                SinkOperationError::validation(error.to_string()),
            )
        })?;
        let bytes = row
            .iter()
            .try_fold(0usize, |sum, cell| sum.checked_add(cell.len()))
            .ok_or_else(|| {
                SinkWriteFailure::current_only(
                    SinkWritePhase::Encode,
                    SinkOperationError::validation("table row size overflow"),
                )
            })?;
        if bytes > self.max_bytes {
            return Err(SinkWriteFailure::current_only(
                SinkWritePhase::Encode,
                SinkOperationError::validation("table row exceeds batch byte limit"),
            ));
        }
        let mut receipts = Vec::new();
        if self.rows.len() >= self.max_rows
            || bytes > self.max_bytes.saturating_sub(self.prepared_bytes)
        {
            receipts = self
                .flush_pending()
                .map_err(|error| {
                    SinkWriteFailure::poisoned(
                        SinkWritePhase::Execute,
                        SinkOperationError::other(error.to_string()),
                    )
                })?
                .0;
        }
        self.rows.push((row, context.defer()));
        self.prepared_bytes += bytes;
        if receipts.is_empty()
            && (self.rows.len() >= self.max_rows || self.prepared_bytes >= self.max_bytes)
        {
            receipts = self
                .flush_pending()
                .map_err(|error| {
                    SinkWriteFailure::poisoned(
                        SinkWritePhase::Execute,
                        SinkOperationError::other(error.to_string()),
                    )
                })?
                .0;
        }
        Ok(
            SinkWriteReport::buffered(SinkBufferedOutcome::accepted(Some(bytes as u64)))
                .with_commit_receipts(receipts),
        )
    }
    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        let (receipts, bytes) = self
            .flush_pending()
            .map_err(|error| SinkOperationError::other(error.to_string()))?;
        if receipts.is_empty() {
            return Ok(SinkWriterLifecycleReport::default());
        }
        let items = receipts.len() as u64;
        Ok(SinkWriterLifecycleReport::audit(
            SinkAuditOutcome::success(Some(bytes)).with_items(items),
        )
        .with_commit_receipts(receipts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{ChainPayload, JournalRecord};
    use obzenflow_core::{JournalWriterId, StageId};
    use obzenflow_runtime::messaging::DeliveredRecord;
    use obzenflow_runtime::stages::common::handlers::{SinkHandler, SinkWriterAdapter};
    use serde::{Deserialize, Serialize};
    use std::sync::{Arc, Mutex};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        value: String,
    }
    impl TypedPayload for TestEvent {
        const EVENT_TYPE: &'static str = "test.event";
    }
    fn input(value: &str) -> DeliveredRecord<ChainPayload> {
        JournalRecord::new(
            JournalWriterId::new(),
            TestEvent {
                value: value.into(),
            }
            .to_event(StageId::new().into()),
        )
        .into()
    }
    fn text(output: ConsoleOutput) -> String {
        match output {
            ConsoleOutput::Text(value) => value,
            ConsoleOutput::Empty => panic!("expected text"),
        }
    }
    #[derive(Default)]
    struct OutputState {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
        fail_after: Option<usize>,
        fail_flush: bool,
    }
    struct Output(Arc<Mutex<OutputState>>);
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let mut state = self.0.lock().unwrap();
            state.writes += 1;
            if state
                .fail_after
                .is_some_and(|limit| state.bytes.len() >= limit)
            {
                return Err(io::Error::other("injected write failure"));
            }
            let count = bytes.len().min(2);
            state.bytes.extend_from_slice(&bytes[..count]);
            Ok(count)
        }
        fn flush(&mut self) -> io::Result<()> {
            let mut state = self.0.lock().unwrap();
            state.flushes += 1;
            if state.fail_flush {
                Err(io::Error::other("injected flush failure"))
            } else {
                Ok(())
            }
        }
    }
    async fn table<E>(
        connector: &TableConsoleSink<TestEvent, E>,
        output: Arc<Mutex<OutputState>>,
    ) -> SinkWriterAdapter<TableConsoleWriter<TestEvent, E>>
    where
        E: Fn(&TestEvent) -> Vec<String> + Send + Sync + Clone + 'static,
    {
        let stage = StageId::new();
        let mut writer = connector
            .open(SinkWriterInitContext::new(
                stage,
                "table".into(),
                "test".into(),
            ))
            .await
            .unwrap();
        writer.output = Some(Arc::new(Mutex::new(Box::new(Output(output)))));
        SinkWriterAdapter::new(writer, stage, connector.describe().default_method().clone())
    }
    #[test]
    fn immediate_formatters_and_snapshot_are_stateless() {
        let item = TestEvent {
            value: "hello".into(),
        };
        assert_eq!(
            text(JsonFormatter.format(&item).unwrap()),
            r#"{"value":"hello"}"#
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
    fn serialization_errors_and_checked_frame_io_propagate() {
        struct Invalid;
        impl Serialize for Invalid {
            fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom("cannot encode"))
            }
        }
        assert!(JsonFormatter
            .format(&Invalid)
            .unwrap_err()
            .to_string()
            .contains("cannot encode"));
        assert!(JsonPrettyFormatter.format(&Invalid).is_err());
        let state = Arc::new(Mutex::new(OutputState::default()));
        assert_eq!(
            write_frame_to(&mut Output(state.clone()), "abcde").unwrap(),
            6
        );
        assert_eq!(state.lock().unwrap().bytes, b"abcde\n");
        assert_eq!(state.lock().unwrap().flushes, 1);
        state.lock().unwrap().fail_flush = true;
        assert!(write_frame_to(&mut Output(state), "x").is_err());
    }
    #[tokio::test]
    async fn explicit_empty_is_a_zero_item_noop() {
        #[derive(Clone)]
        struct Empty;
        impl Formatter<TestEvent> for Empty {
            fn format(&self, _: &TestEvent) -> Result<ConsoleOutput, ConsoleFormatError> {
                Ok(ConsoleOutput::Empty)
            }
        }
        let connector = ConsoleSink::<TestEvent, _>::new(Empty);
        let stage = StageId::new();
        let writer = SinkConnector::open(
            &connector,
            SinkWriterInitContext::new(stage, "empty".into(), "test".into()),
        )
        .await
        .unwrap();
        let mut adapter = SinkWriterAdapter::new(writer, stage, DeliveryMethod::ConsoleStdout);
        let report = adapter
            .consume_committed_report(input("ignored"), Default::default())
            .await
            .unwrap();
        assert_eq!(report.primary.delivery_method, DeliveryMethod::Noop);
        assert_eq!(report.primary.items_delivered, Some(0));
        assert_eq!(report.primary.bytes_processed, Some(0));
        assert!(report.commit_receipts.is_empty());
    }
    #[tokio::test]
    async fn table_receipts_follow_output_and_keep_exact_reconvergent_subjects() {
        let connector =
            ConsoleSink::<TestEvent, _>::new(TableFormatter::new(&["value"], |e: &TestEvent| {
                vec![e.value.clone()]
            }))
            .buffered()
            .batch_limits(
                std::num::NonZeroUsize::new(2).unwrap(),
                std::num::NonZeroUsize::new(1024).unwrap(),
            );
        let output = Arc::new(Mutex::new(OutputState::default()));
        let mut writer = table(&connector, output.clone()).await;
        let first = input("first");
        let sibling = JournalRecord::new(JournalWriterId::new(), first.authored());
        let first_ref = first.commitment();
        let sibling_ref = sibling.commitment();
        assert_ne!(first_ref, sibling_ref);
        let report = writer
            .consume_committed_report(first, Default::default())
            .await
            .unwrap();
        assert!(matches!(
            report.primary.result,
            obzenflow_core::event::payloads::delivery_payload::DeliveryResult::Buffered { .. }
        ));
        assert!(report.commit_receipts.is_empty());
        assert!(output.lock().unwrap().bytes.is_empty());
        let report = writer
            .consume_committed_report(sibling.into(), Default::default())
            .await
            .unwrap();
        assert_eq!(report.commit_receipts.len(), 2);
        assert_eq!(report.commit_receipts[0].subject.input, first_ref);
        assert_eq!(report.commit_receipts[1].subject.input, sibling_ref);
        assert_eq!(output.lock().unwrap().flushes, 1);
        assert!(writer
            .flush_report()
            .await
            .unwrap()
            .commit_receipts
            .is_empty());
        assert!(writer
            .drain_report()
            .await
            .unwrap()
            .commit_receipts
            .is_empty());
    }
    #[tokio::test]
    async fn encoding_failure_preserves_earlier_pending_rows_and_writers_are_isolated() {
        let connector =
            ConsoleSink::<TestEvent, _>::new(TableFormatter::new(&["value"], |e: &TestEvent| {
                vec![e.value.clone()]
            }))
            .buffered()
            .batch_limits(
                std::num::NonZeroUsize::new(10).unwrap(),
                std::num::NonZeroUsize::new(3).unwrap(),
            );
        let one = Arc::new(Mutex::new(OutputState::default()));
        let two = Arc::new(Mutex::new(OutputState::default()));
        let mut first = table(&connector, one.clone()).await;
        let mut second = table(&connector, two.clone()).await;
        first
            .consume_committed_report(input("a"), Default::default())
            .await
            .unwrap();
        second
            .consume_committed_report(input("b"), Default::default())
            .await
            .unwrap();
        let error = first
            .consume_committed_report(input("oversize"), Default::default())
            .await
            .unwrap_err();
        let obzenflow_runtime::stages::common::HandlerError::SinkWrite(error) = error else {
            panic!("expected encoding failure")
        };
        assert_eq!(error.phase(), SinkWritePhase::Encode);
        assert_eq!(
            error.disposition(),
            obzenflow_runtime::stages::common::handlers::SinkWriteFailureDisposition::CurrentOnly
        );
        assert!(one.lock().unwrap().bytes.is_empty());
        assert_eq!(first.flush_report().await.unwrap().commit_receipts.len(), 1);
        assert!(two.lock().unwrap().bytes.is_empty());
        drop(second);
        assert!(
            two.lock().unwrap().bytes.is_empty(),
            "drop cannot flush deferred work"
        );
    }
    #[tokio::test]
    async fn byte_limit_flushes_and_uncertain_output_never_releases_partial_receipts() {
        let connector =
            ConsoleSink::<TestEvent, _>::new(TableFormatter::new(&["value"], |e: &TestEvent| {
                vec![e.value.clone()]
            }))
            .buffered()
            .batch_limits(
                std::num::NonZeroUsize::new(10).unwrap(),
                std::num::NonZeroUsize::new(4).unwrap(),
            );
        let output = Arc::new(Mutex::new(OutputState::default()));
        let mut writer = table(&connector, output.clone()).await;
        writer
            .consume_committed_report(input("aa"), Default::default())
            .await
            .unwrap();
        let report = writer
            .consume_committed_report(input("bb"), Default::default())
            .await
            .unwrap();
        assert_eq!(report.commit_receipts.len(), 2);
        assert_eq!(output.lock().unwrap().flushes, 1);
        for fail_flush in [false, true] {
            let output = Arc::new(Mutex::new(OutputState {
                fail_after: (!fail_flush).then_some(4),
                fail_flush,
                ..Default::default()
            }));
            let mut writer = table(&connector, output.clone()).await;
            writer
                .consume_committed_report(input("x"), Default::default())
                .await
                .unwrap();
            assert!(writer.flush_report().await.is_err());
            let writes = output.lock().unwrap().writes;
            assert!(writer.drain_report().await.is_err());
            drop(writer);
            assert_eq!(
                output.lock().unwrap().writes,
                writes,
                "poisoned lifecycle and drop cannot retry"
            );
        }
    }
    #[tokio::test]
    async fn descriptor_mismatch_reaches_no_writer_output() {
        let connector =
            ConsoleSink::<TestEvent, _>::new(TableFormatter::new(&["value"], |e: &TestEvent| {
                vec![e.value.clone()]
            }))
            .to_stderr()
            .buffered();
        assert_eq!(
            connector.describe().default_method(),
            &DeliveryMethod::ConsoleStderr
        );
        let output = Arc::new(Mutex::new(OutputState::default()));
        let mut writer = table(&connector, output.clone()).await;
        let mut event = input("x").authored();
        event.payload_schema_version = std::num::NonZeroU32::new(2).unwrap();
        let input = JournalRecord::new(JournalWriterId::new(), event);
        assert!(writer
            .consume_committed_report(input.into(), Default::default())
            .await
            .is_err());
        assert!(output.lock().unwrap().bytes.is_empty());
    }
}
