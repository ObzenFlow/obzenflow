// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Console sink (FLOWIP-083d).
//!
//! A typed sink for printing events to stdout/stderr with reusable formatters.

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::SinkHandler;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;

/// Output destination for `ConsoleSink`.
#[derive(Clone, Copy, Debug, Default)]
pub enum OutputDestination {
    #[default]
    Stdout,
    Stderr,
}

impl OutputDestination {
    fn write_line(self, line: &str) {
        match self {
            Self::Stdout => println!("{line}"),
            Self::Stderr => eprintln!("{line}"),
        }
    }

    fn delivery_method(self) -> DeliveryMethod {
        match self {
            Self::Stdout => DeliveryMethod::Custom("console:stdout".to_string()),
            Self::Stderr => DeliveryMethod::Custom("console:stderr".to_string()),
        }
    }
}

/// Transform typed data into an output string.
///
/// Returning `None` allows formatters to buffer (e.g., table output) and emit on
/// `flush()`.
pub trait Formatter<T>: Send + Sync + Clone {
    fn format(&mut self, item: &T) -> Option<String>;

    fn flush(&mut self) -> Option<String> {
        None
    }
}

impl<T, F> Formatter<T> for F
where
    F: Fn(&T) -> String + Send + Sync + Clone,
{
    fn format(&mut self, item: &T) -> Option<String> {
        Some((self)(item))
    }
}

/// JSON formatter (compact, single-line).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonFormatter;

impl<T> Formatter<T> for JsonFormatter
where
    T: Serialize,
{
    fn format(&mut self, item: &T) -> Option<String> {
        serde_json::to_string(item).ok()
    }
}

/// JSON formatter (pretty, multi-line).
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonPrettyFormatter;

impl<T> Formatter<T> for JsonPrettyFormatter
where
    T: Serialize,
{
    fn format(&mut self, item: &T) -> Option<String> {
        serde_json::to_string_pretty(item).ok()
    }
}

/// Debug formatter (uses `std::fmt::Debug`).
#[derive(Clone, Copy, Debug, Default)]
pub struct DebugFormatter;

impl<T> Formatter<T> for DebugFormatter
where
    T: std::fmt::Debug,
{
    fn format(&mut self, item: &T) -> Option<String> {
        Some(format!("{item:?}"))
    }
}

/// Table formatter - buffers rows and renders on `flush()`.
pub struct TableFormatter<T, E> {
    columns: Vec<String>,
    extractor: E,
    rows: Vec<Vec<String>>,
    max_col_width: usize,
    _phantom: PhantomData<fn() -> T>,
}

fn render_table(columns: &[String], rows: &[Vec<String>], max_col_width: usize) -> String {
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
                .filter_map(|row| row.get(col_idx).map(|s| display_width(s.as_str())))
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
            let cell = row.get(col_idx).map(String::as_str).unwrap_or("-");
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
            rows: Vec::new(),
            max_col_width: self.max_col_width,
            _phantom: PhantomData,
        }
    }
}

impl<T, E> std::fmt::Debug for TableFormatter<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableFormatter")
            .field("column_count", &self.columns.len())
            .field("buffered_rows", &self.rows.len())
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
            rows: Vec::new(),
            max_col_width: 30,
            _phantom: PhantomData,
        }
    }

    pub fn max_width(mut self, width: usize) -> Self {
        self.max_col_width = width.max(1);
        self
    }

    fn render(&self) -> String {
        render_table(&self.columns, &self.rows, self.max_col_width)
    }
}

impl<T, E> Formatter<T> for TableFormatter<T, E>
where
    E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
{
    fn format(&mut self, item: &T) -> Option<String> {
        self.rows.push((self.extractor)(item));
        None
    }

    fn flush(&mut self) -> Option<String> {
        if self.rows.is_empty() {
            return None;
        }

        let rendered = self.render();
        self.rows.clear();
        Some(rendered)
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
    fn format(&mut self, item: &T) -> Option<String> {
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
            None
        } else {
            Some(parts.join("\n"))
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
    include_non_data: bool,
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
            include_non_data: self.include_non_data,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for ConsoleSink<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsoleSink")
            .field("type", &std::any::type_name::<T>())
            .field("destination", &self.destination)
            .field("include_non_data", &self.include_non_data)
            .finish()
    }
}

impl<T, F0> ConsoleSink<T, F0>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
{
    pub fn json() -> ConsoleSink<T, JsonFormatter>
    where
        T: Serialize,
    {
        ConsoleSink {
            formatter: JsonFormatter,
            destination: OutputDestination::Stdout,
            include_non_data: false,
            _phantom: PhantomData,
        }
    }

    pub fn json_pretty() -> ConsoleSink<T, JsonPrettyFormatter>
    where
        T: Serialize,
    {
        ConsoleSink {
            formatter: JsonPrettyFormatter,
            destination: OutputDestination::Stdout,
            include_non_data: false,
            _phantom: PhantomData,
        }
    }

    pub fn debug() -> ConsoleSink<T, DebugFormatter>
    where
        T: std::fmt::Debug,
    {
        ConsoleSink {
            formatter: DebugFormatter,
            destination: OutputDestination::Stdout,
            include_non_data: false,
            _phantom: PhantomData,
        }
    }

    pub fn table<E>(columns: &[&str], extractor: E) -> ConsoleSink<T, TableFormatter<T, E>>
    where
        E: Fn(&T) -> Vec<String> + Send + Sync + Clone,
    {
        ConsoleSink {
            formatter: TableFormatter::new(columns, extractor),
            destination: OutputDestination::Stdout,
            include_non_data: false,
            _phantom: PhantomData,
        }
    }

    pub fn snapshot_table<E>(
        columns: &[&str],
        extractor: E,
    ) -> ConsoleSink<T, SnapshotTableFormatter<T, E>>
    where
        E: Fn(&T) -> Vec<Vec<String>> + Send + Sync + Clone,
    {
        ConsoleSink {
            formatter: SnapshotTableFormatter::new(columns, extractor),
            destination: OutputDestination::Stdout,
            include_non_data: false,
            _phantom: PhantomData,
        }
    }

    pub fn new<F>(formatter: F) -> ConsoleSink<T, F>
    where
        F: Formatter<T>,
    {
        ConsoleSink {
            formatter,
            destination: OutputDestination::Stdout,
            include_non_data: false,
            _phantom: PhantomData,
        }
    }
}

impl<T, F> ConsoleSink<T, F> {
    pub fn to_stderr(mut self) -> Self {
        self.destination = OutputDestination::Stderr;
        self
    }

    pub fn include_all(mut self) -> Self {
        self.include_non_data = true;
        self
    }
}

#[async_trait]
impl<T, F> SinkHandler for ConsoleSink<T, F>
where
    T: TypedPayload + DeserializeOwned + Send + Sync + 'static,
    F: Formatter<T>,
{
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        match &event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => {
                if !T::event_type_matches(event_type) {
                    return Ok(DeliveryPayload::success(
                        "console",
                        self.destination.delivery_method(),
                        None,
                    ));
                }

                let typed: T = serde_json::from_value(payload.clone()).map_err(|e| {
                    HandlerError::Deserialization(format!(
                        "ConsoleSink failed to deserialize {}: {e}",
                        std::any::type_name::<T>()
                    ))
                })?;

                if let Some(output) = self.formatter.format(&typed) {
                    self.destination.write_line(&output);
                }
            }
            _ => {
                if self.include_non_data {
                    let output = format!("Event: {} | {}", event.event_type(), event.payload());
                    self.destination.write_line(&output);
                }
            }
        }

        Ok(DeliveryPayload::success(
            "console",
            self.destination.delivery_method(),
            None,
        ))
    }

    async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
        let Some(output) = self.formatter.flush() else {
            return Ok(None);
        };

        self.destination.write_line(&output);

        Ok(Some(DeliveryPayload::success(
            "console",
            self.destination.delivery_method(),
            None,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::WriterId;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestEvent {
        value: String,
    }

    impl TypedPayload for TestEvent {
        const EVENT_TYPE: &'static str = "test.event";
        const SCHEMA_VERSION: u32 = 1;
    }

    #[test]
    fn json_formatter_prints_compact_json() {
        let mut formatter = JsonFormatter;
        let out = formatter
            .format(&TestEvent {
                value: "hello".to_string(),
            })
            .expect("json formatter should emit output");
        assert_eq!(out, r#"{"value":"hello"}"#);
    }

    #[test]
    fn debug_formatter_prints_debug() {
        let mut formatter = DebugFormatter;
        let out = formatter
            .format(&TestEvent {
                value: "hello".to_string(),
            })
            .expect("debug formatter should emit output");
        assert_eq!(out, r#"TestEvent { value: "hello" }"#);
    }

    #[test]
    fn table_formatter_buffers_and_flushes() {
        let mut formatter = TableFormatter::new(&["value"], |e: &TestEvent| vec![e.value.clone()]);

        assert_eq!(
            formatter.format(&TestEvent {
                value: "a".to_string()
            }),
            None
        );
        assert_eq!(
            formatter.format(&TestEvent {
                value: "b".to_string()
            }),
            None
        );

        let out = formatter.flush().expect("table formatter should flush");
        assert!(out.contains("┌"));
        assert!(out.contains("value"));
        assert!(out.contains("a"));
        assert!(out.contains("b"));
        assert!(formatter.flush().is_none(), "flush clears buffered rows");
    }

    #[test]
    fn table_formatter_truncates_with_ellipsis() {
        let mut formatter =
            TableFormatter::new(&["value"], |e: &TestEvent| vec![e.value.clone()]).max_width(3);

        let _ = formatter.format(&TestEvent {
            value: "abcdef".to_string(),
        });

        let out = formatter.flush().expect("should flush");
        assert!(out.contains("ab…"));
    }

    #[test]
    fn table_formatter_aligns_unicode_width() {
        let mut formatter = TableFormatter::new(&["status", "value"], |e: &TestEvent| {
            vec![e.value.clone(), "ok".to_string()]
        });

        let _ = formatter.format(&TestEvent {
            value: "🟡".to_string(),
        });

        let out = formatter.flush().expect("should flush");
        let lines: Vec<&str> = out.lines().collect();
        let widths: Vec<usize> = lines.iter().map(|l| display_width(l)).collect();

        let first = widths.first().copied().unwrap_or(0);
        assert!(
            widths.iter().all(|w| *w == first),
            "all table lines should have equal display width"
        );
    }

    #[test]
    fn table_formatter_clone_resets_buffer() {
        let mut formatter = TableFormatter::new(&["value"], |e: &TestEvent| vec![e.value.clone()]);
        let _ = formatter.format(&TestEvent {
            value: "buffered".to_string(),
        });

        let cloned = formatter.clone();
        assert!(
            cloned.rows.is_empty(),
            "clone should not copy buffered rows"
        );
    }

    #[test]
    fn snapshot_table_formatter_renders_per_item_with_header_footer() {
        #[derive(Clone, Debug, Serialize, Deserialize)]
        struct Snapshot {
            header: String,
            rows: Vec<String>,
        }

        let mut formatter = SnapshotTableFormatter::new(&["value"], |s: &Snapshot| {
            s.rows
                .iter()
                .map(|row| vec![row.clone()])
                .collect::<Vec<_>>()
        })
        .with_header(|s: &Snapshot| vec![format!("header={}", s.header)])
        .with_footer(|s: &Snapshot| vec![format!("rows={}", s.rows.len())]);

        let out = formatter
            .format(&Snapshot {
                header: "demo".to_string(),
                rows: vec!["a".to_string(), "b".to_string()],
            })
            .expect("snapshot table formatter should render");

        assert!(out.contains("header=demo"));
        assert!(out.contains("┌"));
        assert!(out.contains("value"));
        assert!(out.contains("a"));
        assert!(out.contains("b"));
        assert!(out.contains("rows=2"));
    }

    #[tokio::test]
    async fn console_sink_skips_non_matching_event_types() {
        let mut sink = ConsoleSink::<TestEvent>::table(&["value"], |e| vec![e.value.clone()]);

        let event = ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            "other.event",
            json!({"value": "ignored"}),
        );

        let payload = sink.consume(event).await.expect("should not error");
        assert!(matches!(payload.delivery_method, DeliveryMethod::Custom(_)));
    }

    #[tokio::test]
    async fn console_sink_to_stderr_sets_delivery_method() {
        let mut sink =
            ConsoleSink::<TestEvent>::table(&["value"], |e| vec![e.value.clone()]).to_stderr();

        let event = ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            "other.event",
            json!({"value": "ignored"}),
        );

        let payload = sink.consume(event).await.expect("should not error");
        assert!(matches!(
            payload.delivery_method,
            DeliveryMethod::Custom(ref s) if s == "console:stderr"
        ));
    }

    #[tokio::test]
    async fn console_sink_accepts_closure_formatter() {
        let called = Arc::new(AtomicUsize::new(0));
        let called_for_formatter = called.clone();

        let mut sink = ConsoleSink::<TestEvent>::new(move |e: &TestEvent| {
            called_for_formatter.fetch_add(1, Ordering::SeqCst);
            format!("value={}", e.value)
        });

        let event = ChainEventFactory::data_event(
            WriterId::from(obzenflow_core::StageId::new()),
            TestEvent::EVENT_TYPE,
            json!({"value": "hello"}),
        );

        let _ = sink.consume(event).await.expect("should not error");
        assert_eq!(called.load(Ordering::SeqCst), 1);
    }
}
