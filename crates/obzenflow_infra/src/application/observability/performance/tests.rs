// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use tracing::instrument::WithSubscriber;
use tracing::Instrument;
use tracing_subscriber::prelude::*;

fn dispatcher(capture: &PerformanceCapture) -> tracing::Dispatch {
    tracing::Dispatch::new(
        tracing_subscriber::registry().with(
            capture
                .layer()
                .with_filter(tracing_subscriber::filter::FilterFn::new(accepts)),
        ),
    )
}

#[tokio::test]
async fn nested_capture_keeps_parentage_across_pending_and_blocking_work() {
    let capture = PerformanceCapture::default();
    let dispatch = dispatcher(&capture);
    async {
        let root = tracing::debug_span!(target: "obzenflow::performance", "supervisor", supervisor = "reader", writer_id = "writer", supervision_mode = "self_supervised");
        async {
            let state = tracing::debug_span!(target: "obzenflow::performance", "supervisor_state", state = "Running");
            async {
                let read = tracing::debug_span!(target: "obzenflow::performance", "read");
                async {
                    tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                    let parent = tracing::Span::current();
                    let dispatch = tracing::dispatcher::get_default(Clone::clone);
                    tokio::task::spawn_blocking(move || {
                        tracing::dispatcher::with_default(&dispatch, || {
                            tracing::debug_span!(target: "obzenflow::performance", parent: &parent, "worker")
                                .in_scope(|| std::hint::black_box(1 + 1))
                        })
                    }).await.unwrap();
                }.instrument(read).await;
            }.instrument(state).await;
        }.instrument(root).await;
        tracing::debug!(target: "obzenflow::supervisor_timing", supervisor = "reader", state = "Running", elapsed_ns = 10_u64, direct_dispatch_ns = 8_u64, residual_ns = 2_u64, interrupted = false, "supervisor_loop_summary");
    }.with_subscriber(dispatch).await;

    let report = capture.report(false);
    assert_eq!(report.open_spans, 0);
    assert_eq!(report.dropped_spans, 0);
    assert_eq!(report.spans.len(), 4);
    let worker = report
        .spans
        .iter()
        .find(|row| row.path.last().unwrap().name == "worker")
        .unwrap();
    assert_eq!(
        worker
            .path
            .iter()
            .map(|frame| frame.name)
            .collect::<Vec<_>>(),
        ["supervisor", "supervisor_state", "read", "worker"]
    );
    assert_eq!(worker.path[0].identity["supervisor"], "reader");
    assert_eq!(worker.path[1].identity["state"], "Running");
    let read = report
        .spans
        .iter()
        .find(|row| row.path.last().unwrap().name == "read")
        .unwrap();
    assert!(
        read.totals.enters >= 2,
        "async span must exit and re-enter across pending"
    );
    assert!(
        read.totals.elapsed_ns > read.totals.entered_ns,
        "pending interval must not count as entered work"
    );
    assert_eq!(report.supervisor_summaries[0]["elapsed_ns"], 10);
    assert_eq!(report.supervisor_summaries[0]["interrupted"], false);
}

#[tokio::test]
async fn cancellation_closes_spans_and_open_snapshot_is_explicit() {
    let capture = PerformanceCapture::default();
    let dispatch = dispatcher(&capture);
    let (started, waiting) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        async move {
            async {
                started.send(()).unwrap();
                std::future::pending::<()>().await;
            }
            .instrument(
                tracing::debug_span!(target: "obzenflow::performance", "cancelled_operation"),
            )
            .await;
        }
        .with_subscriber(dispatch),
    );
    waiting.await.unwrap();
    let partial = capture.report(true);
    assert!(partial.interrupted);
    assert_eq!(partial.open_spans, 1);
    assert_eq!(partial.spans[0].totals.open_calls, 1);
    assert!(partial.spans[0].totals.entered_ns <= partial.spans[0].totals.elapsed_ns);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let closed = capture.report(true);
    assert_eq!(closed.open_spans, 0);
    assert_eq!(closed.spans[0].totals.calls, 1);
    assert_eq!(closed.spans[0].totals.open_calls, 0);
}

#[test]
fn repeated_paths_aggregate_and_capacity_loss_is_visible() {
    let capture = PerformanceCapture::default();
    tracing::dispatcher::with_default(&dispatcher(&capture), || {
        for _ in 0..3 {
            tracing::debug_span!(target: "obzenflow::performance", "same").in_scope(|| ());
        }
        for state in 0..MAX_PATHS {
            tracing::debug_span!(target: "obzenflow::performance", "bounded", state)
                .in_scope(|| ());
        }
        tracing::info_span!("unrelated").in_scope(|| ());
    });
    let report = capture.report(false);
    assert_eq!(report.spans.len(), MAX_PATHS);
    assert_eq!(report.dropped_spans, 1);
    assert_eq!(report.open_spans, 0);
    assert_eq!(
        report
            .spans
            .iter()
            .find(|row| row.path.last().unwrap().name == "same")
            .unwrap()
            .totals
            .calls,
        3
    );
}

#[test]
fn disabled_filter_collects_no_spans_or_summaries() {
    let capture = PerformanceCapture::default();
    let subscriber = tracing_subscriber::registry().with(
        capture
            .layer()
            .with_filter(tracing_subscriber::filter::FilterFn::new(accepts))
            .with_filter(tracing_subscriber::EnvFilter::new("info")),
    );
    tracing::subscriber::with_default(subscriber, || {
        tracing::debug_span!(target: "obzenflow::performance", "disabled").in_scope(|| ());
        tracing::debug!(target: "obzenflow::supervisor_timing", "supervisor_loop_summary");
    });
    let report = capture.report(false);
    assert!(report.spans.is_empty());
    assert!(report.supervisor_summaries.is_empty());
    assert!(report.cycle_summaries.is_empty());
}

#[test]
fn coarse_capture_emits_complete_summaries_without_allocating_performance_spans() {
    #[derive(Clone, Default)]
    struct Output(Arc<Mutex<Vec<u8>>>);
    impl std::io::Write for Output {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let capture = PerformanceCapture::default();
    let output = Output::default();
    let writer = output.clone();
    let filter = tracing_subscriber::EnvFilter::new(
        "info,obzenflow::supervisor_timing=debug,obzenflow::performance=off",
    );
    let subscriber = tracing_subscriber::registry()
        .with(
            capture
                .layer()
                .with_filter(tracing_subscriber::filter::FilterFn::new(accepts))
                .with_filter(filter.clone()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .without_time()
                .with_writer(move || writer.clone())
                .with_filter(filter),
        );
    tracing::subscriber::with_default(subscriber, || {
        assert!(
            tracing::debug_span!(target: "obzenflow::performance", "disk_journal_codec_encode")
                .id()
                .is_none()
        );
        tracing::debug!(target: "obzenflow::supervisor_timing", supervisor = "source", state = "Running", elapsed_ns = 100_u64, "supervisor_loop_summary");
        tracing::debug!(target: "obzenflow::supervisor_timing", supervisor = "source", state = "Running", outcome = "completed", dispatch_count = 2_u64, business_input_count = 1_u64, elapsed_ns = 100_u64, residual_ns = 5_u64, interrupted = false, "supervisor_cycle_summary");
        capture.clone().emit(false);
        let report = capture.report(false);
        assert_eq!(report.capture_mode, "coarse");
        assert!(report.spans.is_empty());
        assert_eq!(report.supervisor_summaries.len(), 1);
        assert_eq!(report.cycle_summaries.len(), 1);
        assert_eq!(
            report.collector_callback_calls, 2,
            "capture must not observe its report event"
        );
    });
    let log = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    let captures: Vec<_> = log
        .lines()
        .filter(|line| line.contains("performance_capture"))
        .collect();
    assert_eq!(captures.len(), 1);
    let (_, encoded) = captures[0].split_once("report=").unwrap();
    let report: Value = serde_json::from_str(encoded).unwrap();
    assert_eq!(report["version"], 2);
    assert_eq!(report["capture_mode"], "coarse");
    assert_eq!(report["open_spans"], 0);
    assert_eq!(report["dropped_spans"], 0);
    assert_eq!(report["dropped_summaries"], 0);
    assert_eq!(report["cycle_summaries"][0]["dispatch_count"], 2);
    assert_eq!(report["cycle_summaries"][0]["business_input_count"], 1);
}

#[test]
fn runner_and_cycle_summaries_share_the_bounded_capture_budget() {
    let capture = PerformanceCapture::default();
    tracing::dispatcher::with_default(&dispatcher(&capture), || {
        tracing::debug!(target: "obzenflow::supervisor_timing", "supervisor_loop_summary");
        for _ in 0..MAX_SUMMARIES {
            tracing::debug!(target: "obzenflow::supervisor_timing", "supervisor_cycle_summary");
        }
    });
    let report = capture.report(false);
    assert_eq!(report.supervisor_summaries.len(), 1);
    assert_eq!(report.cycle_summaries.len(), MAX_SUMMARIES - 1);
    assert_eq!(report.dropped_summaries, 1);
}
