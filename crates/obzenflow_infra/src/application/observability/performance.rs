// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded diagnostic aggregation, not an execution or journal authority.
//!
//! Span lifetimes are inclusive and can overlap across async work. Entered time
//! measures wall time in polls/blocks, never CPU time. Only the runner's separate
//! phase ledger provides a conserved exclusive elapsed-time denominator.

use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;
use std::time::Instant;
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::{Event, Metadata, Subscriber};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

const MAX_PATHS: usize = 4096;
const MAX_SUMMARIES: usize = 4096;

pub(super) fn accepts(metadata: &Metadata<'_>) -> bool {
    (metadata.is_span() && metadata.target() == "obzenflow::performance")
        || (metadata.is_event()
            && metadata.target() == "obzenflow::supervisor_timing"
            && metadata.fields().field("report").is_none())
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
struct Frame {
    name: &'static str,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    identity: BTreeMap<String, String>,
}

type Path = Vec<Frame>;
type PathId = usize;

#[derive(Clone, Default, Serialize)]
struct Totals {
    calls: u64,
    elapsed_ns: u64,
    entered_ns: u64,
    enters: u64,
    max_elapsed_ns: u64,
    open_calls: u64,
}

impl Totals {
    fn add(&mut self, elapsed_ns: u64, entered_ns: u64, enters: u64, open: bool) {
        self.calls += 1;
        self.elapsed_ns += elapsed_ns;
        self.entered_ns += entered_ns;
        self.enters += enters;
        self.max_elapsed_ns = self.max_elapsed_ns.max(elapsed_ns);
        self.open_calls += u64::from(open);
    }
}

struct SpanTiming {
    // Immutable ancestry needs neither a deep path clone nor the timing lock.
    path: PathId,
    started: Instant,
    entered: Mutex<EnteredTiming>,
}

#[derive(Default)]
struct EnteredTiming {
    entered_ns: u64,
    enters: u64,
    // The same span may be entered on different threads or recursively. Count
    // each thread's outer entered interval once; never subtract child lifetimes.
    // Ordinary single-thread polls need no per-span collection allocation.
    first: Option<ActiveThread>,
    others: Vec<ActiveThread>,
}

struct ActiveThread {
    thread: ThreadId,
    started: Instant,
    depth: u64,
}

impl EnteredTiming {
    fn enter(&mut self, thread: ThreadId) {
        self.enters += 1;
        if let Some(active) = self
            .first
            .iter_mut()
            .chain(&mut self.others)
            .find(|active| active.thread == thread)
        {
            active.depth += 1;
            return;
        }
        let active = ActiveThread {
            thread,
            started: Instant::now(),
            depth: 1,
        };
        if self.first.is_none() {
            self.first = Some(active);
        } else {
            self.others.push(active);
        }
    }

    fn exit(&mut self, thread: ThreadId) {
        if let Some(active) = self.first.as_mut().filter(|active| active.thread == thread) {
            active.depth -= 1;
            if active.depth == 0 {
                self.entered_ns += nanos(active.started.elapsed());
                self.first = self.others.pop();
            }
        } else if let Some(index) = self
            .others
            .iter()
            .position(|active| active.thread == thread)
        {
            let active = &mut self.others[index];
            active.depth -= 1;
            if active.depth == 0 {
                self.entered_ns += nanos(active.started.elapsed());
                self.others.swap_remove(index);
            }
        }
    }

    fn entered_at(&self, now: Instant) -> u64 {
        self.entered_ns
            + self
                .first
                .iter()
                .chain(&self.others)
                .map(|active| nanos(now.duration_since(active.started)))
                .sum::<u64>()
    }
}

#[derive(Default)]
struct State {
    // Intern only a single frame and its parent ID on the hot path. Full paths
    // are materialised once per distinct report row, never once per operation.
    paths: HashMap<(Option<PathId>, Frame), PathId>,
    rows: Vec<Row>,
    open: HashMap<Id, Arc<SpanTiming>>,
    summaries: Vec<Value>,
    cycle_summaries: Vec<Value>,
    dropped_spans: u64,
    dropped_summaries: u64,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    callback_ns: AtomicU64,
    callback_calls: AtomicU64,
}

struct CallbackTimer<'a> {
    shared: &'a Shared,
    started: Instant,
}

impl Shared {
    fn callback(&self) -> CallbackTimer<'_> {
        CallbackTimer {
            shared: self,
            started: Instant::now(),
        }
    }
}

impl Drop for CallbackTimer<'_> {
    fn drop(&mut self) {
        // Declared before callback locks, hence measured after they are released.
        // Atomic updates do not enter tracing and cannot recursively observe us.
        self.shared
            .callback_ns
            .fetch_add(nanos(self.started.elapsed()), Ordering::Relaxed);
        self.shared.callback_calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[derive(Clone, Default)]
pub(super) struct PerformanceCapture(Arc<Shared>);

#[derive(Clone, Serialize)]
struct Row {
    path: Path,
    #[serde(flatten)]
    totals: Totals,
}

#[derive(Serialize)]
struct Report {
    version: u32,
    capture_mode: &'static str,
    clock: &'static str,
    elapsed_semantics: &'static str,
    entered_semantics: &'static str,
    collector_callback_ns: u64,
    collector_callback_calls: u64,
    collector_callback_semantics: &'static str,
    interrupted: bool,
    open_spans: usize,
    dropped_spans: u64,
    dropped_summaries: u64,
    spans: Vec<Row>,
    supervisor_summaries: Vec<Value>,
    cycle_summaries: Vec<Value>,
}

impl PerformanceCapture {
    pub(super) fn layer(&self) -> PerformanceLayer {
        PerformanceLayer(self.clone())
    }

    fn report(&self, interrupted: bool) -> Report {
        let deep = tracing::span_enabled!(target: "obzenflow::performance", tracing::Level::DEBUG);
        let state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows = state.rows.clone();
        // A scoped dispatcher can have exited before a retained capture is
        // inspected. Actual recorded spans still establish a deep capture.
        let deep = deep || !rows.is_empty();
        let mut report = Report {
            version: 2,
            capture_mode: if deep { "deep" } else { "coarse" },
            clock: "std::time::Instant",
            elapsed_semantics: "inclusive span lifetime; concurrent or nested rows must not be summed as flow wall time",
            entered_semantics: "wall time entered on each thread; includes descheduling and nested work, not CPU time",
            collector_callback_ns: self.0.callback_ns.load(Ordering::Relaxed),
            collector_callback_calls: self.0.callback_calls.load(Ordering::Relaxed),
            collector_callback_semantics: "summed collector callback wall time including lock wait; concurrent callbacks overlap, not CPU time or additive flow wall time; excludes report serialisation",
            interrupted,
            open_spans: state.open.len(),
            dropped_spans: state.dropped_spans,
            dropped_summaries: state.dropped_summaries,
            spans: Vec::new(),
            supervisor_summaries: state.summaries.clone(),
            cycle_summaries: state.cycle_summaries.clone(),
        };
        // Keep close/new-span callbacks behind State while taking each open
        // row under its timing lock. Each interrupted row has its own cutoff;
        // no entered counter can be newer than that row's elapsed timestamp.
        for timing in state.open.values() {
            let entered = timing.entered.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            rows[timing.path].totals.add(
                nanos(now.saturating_duration_since(timing.started)),
                entered.entered_at(now),
                entered.enters,
                true,
            );
        }
        drop(state);
        // Preserve the original lexicographic report order independently of
        // concurrent first-use order. Sorting is paid once when reporting.
        rows.sort_unstable_by(|a, b| a.path.cmp(&b.path));
        report.spans = rows;
        report
    }

    pub(super) fn emit(self, interrupted: bool) {
        let report = self.report(interrupted);
        if report.spans.is_empty()
            && report.supervisor_summaries.is_empty()
            && report.cycle_summaries.is_empty()
        {
            return;
        }
        // One bounded capture, not an event per physical journal operation.
        // This event is deliberately excluded from this layer's own filter.
        match serde_json::to_string(&report) {
            Ok(report) => {
                if tracing::event_enabled!(target: "obzenflow::supervisor_timing", tracing::Level::DEBUG)
                {
                    tracing::debug!(target: "obzenflow::supervisor_timing", %report, "performance_capture");
                } else {
                    tracing::debug!(target: "obzenflow::performance", %report, "performance_capture");
                }
            }
            Err(error) => tracing::warn!(%error, "Could not serialise performance capture"),
        }
    }
}

pub(super) struct PerformanceLayer(PerformanceCapture);

#[derive(Default)]
struct Fields(Map<String, Value>);

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().into(), format!("{value:?}").into());
    }
}

impl<S> Layer<S> for PerformanceLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let _callback = self.0 .0.callback();
        if attrs.metadata().target() != "obzenflow::performance" {
            return;
        }
        let started = Instant::now();
        let mut fields = Fields::default();
        attrs.record(&mut fields);
        // Aggregate by stable scope only. Event IDs, payloads, record sequences
        // and file paths must not create one row per operation.
        let identity = fields
            .0
            .into_iter()
            .filter(|(name, _)| {
                matches!(
                    name.as_str(),
                    "supervisor"
                        | "writer_id"
                        | "supervision_mode"
                        | "supervisor_kind"
                        | "state"
                        | "journal_kind"
                )
            })
            .map(|(name, value)| {
                let value = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                (name, value)
            })
            .collect();
        let parent = ctx.span(id).and_then(|span| {
            span.scope().skip(1).find_map(|ancestor| {
                ancestor
                    .extensions()
                    .get::<Arc<SpanTiming>>()
                    .map(|timing| timing.path)
            })
        });
        let frame = Frame {
            name: attrs.metadata().name(),
            identity,
        };

        let mut state = self.0 .0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.open.len() >= MAX_PATHS {
            state.dropped_spans += 1;
            return;
        }
        let key = (parent, frame);
        let path = match state.paths.get(&key) {
            Some(path) => *path,
            None => {
                if state.rows.len() >= MAX_PATHS {
                    state.dropped_spans += 1;
                    return;
                }
                let mut path = parent
                    .map(|parent| state.rows[parent].path.clone())
                    .unwrap_or_default();
                path.push(key.1.clone());
                let path_id = state.rows.len();
                state.rows.push(Row {
                    path,
                    totals: Totals::default(),
                });
                state.paths.insert(key, path_id);
                path_id
            }
        };
        let timing = Arc::new(SpanTiming {
            path,
            started,
            entered: Mutex::new(EnteredTiming::default()),
        });
        state.open.insert(id.clone(), timing.clone());
        drop(state);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(timing);
        }
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let _callback = self.0 .0.callback();
        with_timing(id, &ctx, |timing| {
            timing.enter(std::thread::current().id());
        });
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        let _callback = self.0 .0.callback();
        with_timing(id, &ctx, |timing| {
            timing.exit(std::thread::current().id());
        });
    }

    fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
        let _callback = self.0 .0.callback();
        let mut state = self.0 .0.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(timing) = state.open.remove(&id) {
            let entered = timing.entered.lock().unwrap_or_else(|e| e.into_inner());
            let now = Instant::now();
            state.rows[timing.path].totals.add(
                nanos(now.duration_since(timing.started)),
                entered.entered_at(now),
                entered.enters,
                false,
            );
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let _callback = self.0 .0.callback();
        if event.metadata().target() != "obzenflow::supervisor_timing" {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let cycle = match fields.0.get("message").and_then(Value::as_str) {
            Some("supervisor_loop_summary") => false,
            Some("supervisor_cycle_summary") => true,
            _ => return,
        };
        let mut state = self.0 .0.state.lock().unwrap_or_else(|e| e.into_inner());
        if state.summaries.len() + state.cycle_summaries.len() == MAX_SUMMARIES {
            state.dropped_summaries += 1;
        } else if cycle {
            state.cycle_summaries.push(Value::Object(fields.0));
        } else {
            state.summaries.push(Value::Object(fields.0));
        }
    }
}

fn with_timing<S>(id: &Id, ctx: &Context<'_, S>, f: impl FnOnce(&mut EnteredTiming))
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let timing = ctx
        .span(id)
        .and_then(|span| span.extensions().get::<Arc<SpanTiming>>().cloned());
    if let Some(timing) = timing {
        f(&mut timing.entered.lock().unwrap_or_else(|e| e.into_inner()));
    }
}

fn nanos(duration: std::time::Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod bookkeeping_tests {
    use super::*;
    use tracing_subscriber::prelude::*;

    #[test]
    fn interned_paths_preserve_report_shape_and_callback_accounting() {
        let capture = PerformanceCapture::default();
        let subscriber = tracing_subscriber::registry().with(
            capture
                .layer()
                .with_filter(tracing_subscriber::filter::FilterFn::new(accepts)),
        );
        tracing::subscriber::with_default(subscriber, || {
            for _ in 0..3 {
                tracing::debug_span!(target: "obzenflow::performance", "supervisor", supervisor = "reader")
                    .in_scope(|| {
                        tracing::debug_span!(target: "obzenflow::performance", "read")
                            .in_scope(|| ());
                    });
            }
        });

        let state = capture.0.state.lock().unwrap();
        assert_eq!(state.paths.len(), 2);
        assert_eq!(state.rows.len(), 2);
        drop(state);
        let report = capture.report(false);
        assert_eq!(report.capture_mode, "deep");
        assert_eq!(report.collector_callback_calls, 24);
        assert!(report.collector_callback_ns > 0);
        assert_eq!(report.open_spans, 0);
        assert_eq!(report.dropped_spans, 0);
        assert_eq!(report.spans.len(), 2);
        assert_eq!(report.spans[0].path.len(), 1);
        assert_eq!(report.spans[1].path.len(), 2);
        assert_eq!(report.spans[1].path[0].identity["supervisor"], "reader");
        assert_eq!(report.spans[1].path[1].name, "read");
        for row in &report.spans {
            assert_eq!(row.totals.calls, 3);
            assert_eq!(row.totals.enters, 3);
            assert_eq!(row.totals.open_calls, 0);
        }
        let json = serde_json::to_value(report).unwrap();
        assert_eq!(json["version"], 2);
        assert_eq!(json["spans"][1]["calls"], 3);
        assert!(json["spans"][1].get("totals").is_none());
    }

    #[test]
    fn entered_timing_preserves_recursive_and_simultaneous_entries() {
        let first = std::thread::current().id();
        let other = std::thread::spawn(|| std::thread::current().id())
            .join()
            .unwrap();
        let mut timing = EnteredTiming::default();
        timing.enter(first);
        timing.enter(first);
        assert_eq!(timing.others.capacity(), 0);
        timing.enter(other);
        assert_eq!(timing.first.as_ref().unwrap().depth, 2);
        assert_eq!(timing.others.len(), 1);
        timing.exit(first);
        assert_eq!(timing.first.as_ref().unwrap().depth, 1);
        timing.exit(first);
        assert_eq!(timing.first.as_ref().unwrap().thread, other);
        assert!(timing.others.is_empty());
        timing.enter(other);
        timing.exit(other);
        assert_eq!(timing.first.as_ref().unwrap().depth, 1);
        timing.exit(other);
        assert!(timing.first.is_none());
        assert!(timing.others.is_empty());
        assert_eq!(timing.enters, 4);
        assert_eq!(timing.entered_at(Instant::now()), timing.entered_ns);
    }
}
