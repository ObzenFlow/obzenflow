// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded, per-run latest-value observations with owned read views.

use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotSink};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Clone, Default)]
pub struct MetricsReadView {
    pub app: Option<Arc<AppMetricsSnapshot>>,
    pub infra: Option<Arc<InfraMetricsSnapshot>>,
}

pub struct MetricsReadModel {
    latest: watch::Sender<MetricsReadView>,
}

impl Default for MetricsReadModel {
    fn default() -> Self {
        Self {
            latest: watch::Sender::new(MetricsReadView::default()),
        }
    }
}

/// Coalesced change notification without exposing a publication lock.
pub struct MetricsSubscription(watch::Receiver<MetricsReadView>);

impl MetricsSubscription {
    pub async fn changed(&mut self) -> Result<MetricsReadView, watch::error::RecvError> {
        self.0.changed().await?;
        Ok(self.0.borrow_and_update().clone())
    }
}

impl MetricsReadModel {
    pub fn subscribe(&self) -> MetricsSubscription {
        MetricsSubscription(self.latest.subscribe())
    }

    pub fn snapshot(&self) -> MetricsReadView {
        self.latest.borrow().clone()
    }
}

impl MetricsSnapshotSink for MetricsReadModel {
    fn publish_app_snapshot(&self, snapshot: AppMetricsSnapshot) {
        let next = Arc::new(snapshot);
        let mut retired = None;
        self.latest
            .send_modify(|view| retired = view.app.replace(next));
        drop(retired);
    }

    fn publish_infra_snapshot(&self, snapshot: InfraMetricsSnapshot) {
        let next = Arc::new(snapshot);
        let mut retired = None;
        self.latest
            .send_modify(|view| retired = view.infra.replace(next));
        drop(retired);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::id::StageId;
    use std::sync::{mpsc, Barrier};
    use std::time::Duration;

    fn app(stage: StageId, count: u64) -> AppMetricsSnapshot {
        let mut value = AppMetricsSnapshot::default();
        value.event_counts.insert(stage, count);
        value
    }

    fn infra(count: u64) -> InfraMetricsSnapshot {
        let mut value = InfraMetricsSnapshot::default();
        value.journal_metrics.writes_total = count;
        value
    }

    #[test]
    fn publication_survives_zero_readers_and_late_subscription() {
        let model = MetricsReadModel::default();
        let stage = StageId::new();
        model.publish_app_snapshot(app(stage, 7));
        model.publish_infra_snapshot(infra(11));
        let rx = model.latest.subscribe();
        assert!(
            !rx.has_changed().unwrap(),
            "new subscribers must explicitly read the initial view"
        );
        assert_eq!(rx.borrow().app.as_ref().unwrap().event_counts[&stage], 7);
        assert_eq!(
            rx.borrow()
                .infra
                .as_ref()
                .unwrap()
                .journal_metrics
                .writes_total,
            11
        );
        drop(rx);
        model.publish_app_snapshot(app(stage, 8));
        assert_eq!(model.snapshot().app.unwrap().event_counts[&stage], 8);
    }

    #[test]
    fn concurrent_app_and_infra_updates_preserve_both_streams() {
        let model = MetricsReadModel::default();
        let barrier = Barrier::new(2);
        let stage = StageId::new();
        std::thread::scope(|scope| {
            scope.spawn(|| {
                barrier.wait();
                for n in 1..=10_000 {
                    model.publish_app_snapshot(app(stage, n));
                }
            });
            scope.spawn(|| {
                barrier.wait();
                for n in 1..=10_000 {
                    model.publish_infra_snapshot(infra(n));
                }
            });
        });
        let final_view = model.snapshot();
        assert_eq!(final_view.app.unwrap().event_counts[&stage], 10_000);
        assert_eq!(
            final_view.infra.unwrap().journal_metrics.writes_total,
            10_000
        );
    }

    #[test]
    fn stalled_owned_reader_does_not_block_publication_or_retain_update_history() {
        let model = Arc::new(MetricsReadModel::default());
        let stage = StageId::new();
        model.publish_app_snapshot(app(stage, 0));
        let held = model.snapshot();
        let old = Arc::downgrade(held.app.as_ref().unwrap());
        let publisher = Arc::clone(&model);
        let (done_tx, done_rx) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let mut historical = Vec::new();
            for n in 1..=10_000 {
                publisher.publish_app_snapshot(app(stage, n));
                historical.push(Arc::downgrade(publisher.snapshot().app.as_ref().unwrap()));
            }
            done_tx.send(historical).unwrap();
        });
        let published = done_rx.recv_timeout(Duration::from_secs(2));
        // Release before assertion so a failing probe can still reclaim its worker.
        assert_eq!(held.app.as_ref().unwrap().event_counts[&stage], 0);
        drop(held);
        worker.join().unwrap();
        let historical = published.expect("publisher must finish while old view is held");
        assert!(old.upgrade().is_none());
        assert_eq!(
            historical
                .iter()
                .filter(|value| value.upgrade().is_some())
                .count(),
            1
        );
        assert_eq!(model.snapshot().app.unwrap().event_counts[&stage], 10_000);
        drop(model);
        assert!(historical.iter().all(|value| value.upgrade().is_none()));
    }

    #[test]
    fn closing_view_is_immutable_under_late_publication() {
        let model = MetricsReadModel::default();
        let stage = StageId::new();
        model.publish_app_snapshot(app(stage, 1));
        model.publish_infra_snapshot(infra(2));
        let closing = model.snapshot();
        let app_time = closing.app.as_ref().unwrap().timestamp;
        let infra_time = closing.infra.as_ref().unwrap().timestamp;
        model.publish_app_snapshot(app(stage, 3));
        model.publish_infra_snapshot(infra(4));
        assert_eq!(closing.app.as_ref().unwrap().event_counts[&stage], 1);
        assert_eq!(
            closing.infra.as_ref().unwrap().journal_metrics.writes_total,
            2
        );
        assert_eq!(closing.app.as_ref().unwrap().timestamp, app_time);
        assert_eq!(closing.infra.as_ref().unwrap().timestamp, infra_time);
        assert_eq!(model.snapshot().app.unwrap().event_counts[&stage], 3);
    }

    #[test]
    fn independent_runs_do_not_inherit_observations() {
        let first = MetricsReadModel::default();
        first.publish_infra_snapshot(infra(12));
        let second = MetricsReadModel::default();
        assert!(second.snapshot().app.is_none());
        assert!(second.snapshot().infra.is_none());
        assert_eq!(
            first.snapshot().infra.unwrap().journal_metrics.writes_total,
            12
        );
    }

    #[test]
    fn ordinary_watch_send_loses_updates_without_receivers() {
        let sender = watch::Sender::new(0);
        assert!(sender.send(5).is_err());
        assert_eq!(*sender.subscribe().borrow(), 0);
    }

    #[test]
    fn replacing_a_stale_whole_view_loses_the_other_stream() {
        let model = MetricsReadModel::default();
        let mut pending_infra = model.snapshot();
        pending_infra.infra = Some(Arc::new(infra(9)));
        let stage = StageId::new();
        model.publish_app_snapshot(app(stage, 8));
        model.latest.send_replace(pending_infra);
        assert!(
            model.snapshot().app.is_none(),
            "stale whole-view replacement erases app publication"
        );
    }
}
