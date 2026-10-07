// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use futures::poll;
use obzenflow_core::StageId;
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::field::{Field, Visit};
use tracing::instrument::WithSubscriber;
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

#[test]
fn nested_overrides_and_out_of_order_drop_conserve_clock_remainders() {
    let start = Instant::now();
    let at = |ns| start + Duration::from_nanos(ns);
    let mut cycle = Cycle::new(1, "Running", start, Vec::new());
    let read = cycle.enter(Phase::Read, at(2));
    let handler = cycle.enter(Phase::Handler, at(11));
    cycle.leave(handler, at(18));
    cycle.leave(read, at(25));
    let publish = cycle.enter(Phase::Publish, at(29));
    let credit = cycle.enter(Phase::CreditWait, at(35));
    cycle.leave(publish, at(42));
    cycle.leave(credit, at(47));
    cycle.charge(at(53));
    assert_eq!(cycle.nanoseconds, [16, 7, 0, 6, 12, 0, 0, 0, 12]);
    assert_eq!(cycle.nanoseconds.iter().sum::<u64>(), 53);
    let mut totals = Totals::default();
    totals.add(&cycle, at(53));
    assert_eq!(totals.elapsed_ns, 53);
    assert_eq!(totals.conservation_failures, 0);
    assert_eq!(totals.max_conservation_error_ns, 0);
}

#[test]
fn worst_residual_is_a_paired_fraction_and_five_percent_is_strict() {
    let start = Instant::now();
    let mut totals = Totals::default();
    for (elapsed, residual) in [(100, 90), (1000, 100), (20, 1)] {
        let mut cycle = Cycle::new(1, "Running", start, Vec::new());
        cycle.enter(Phase::Read, start + Duration::from_nanos(residual));
        cycle.charge(start + Duration::from_nanos(elapsed));
        totals.add(&cycle, start + Duration::from_nanos(elapsed));
    }
    assert_eq!(totals.dispatch_count, 3);
    assert_eq!(totals.elapsed_ns, 1120);
    assert_eq!(totals.nanoseconds[Phase::Residual as usize], 191);
    assert_eq!(totals.max_residual_ns, 100);
    assert_eq!(
        (totals.worst_residual_ns, totals.worst_residual_elapsed_ns),
        (90, 100)
    );
    assert_eq!(totals.residual_over_five_percent_count, 2);
}

#[derive(Default)]
struct Fields(BTreeMap<String, Value>);

impl Visit for Fields {
    fn record_u64(&mut self, field: &Field, value: u64) {
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

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<BTreeMap<String, Value>>>>);

impl Subscriber for Capture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.is_event() && metadata.target() == "obzenflow::supervisor_timing"
    }
    fn new_span(&self, _: &Attributes<'_>) -> Id {
        panic!("cycle accounting must not create spans")
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        assert_eq!(fields.0["message"], "supervisor_cycle_summary");
        self.0.lock().unwrap().push(fields.0);
    }
}

impl Capture {
    fn row(&self, outcome: &str) -> BTreeMap<String, Value> {
        let rows = self.0.lock().unwrap();
        let rows: Vec<_> = rows
            .iter()
            .filter(|row| row["outcome"] == outcome)
            .collect();
        assert_eq!(rows.len(), 1, "expected one {outcome} row");
        rows[0].clone()
    }

    fn assert_conserved(&self) {
        for row in self.0.lock().unwrap().iter() {
            let sum = [
                "read_ns",
                "handler_ns",
                "prepare_ns",
                "publish_ns",
                "credit_wait_ns",
                "acknowledge_ns",
                "control_ns",
                "idle_ns",
                "residual_ns",
            ]
            .iter()
            .map(|name| row[*name].as_u64().unwrap())
            .sum::<u64>();
            assert_eq!(sum, row["elapsed_ns"].as_u64().unwrap());
            assert_eq!(row["conservation_failures"], 0);
            assert_eq!(row["max_conservation_error_ns"], 0);
        }
    }
}

async fn test_scope<F: Future>(future: F) -> F::Output {
    scope(
        "cycle-fixture",
        SupervisorKind::Transform,
        StageId::new().into(),
        SupervisionMode::HandlerSupervised,
        future,
    )
    .await
}

#[tokio::test]
async fn cancelled_dispatch_restores_live_scope_and_outcomes_remain_separate() {
    let capture = Capture::default();
    test_scope(async {
        let mut pending = Box::pin(measure("Running", async {
            let _read = phase(Phase::Read);
            input_delivered();
            std::future::pending::<Result<(), ()>>().await
        }));
        assert!(poll!(&mut pending).is_pending());
        drop(pending);
        AGGREGATE.with(|aggregate| assert!(aggregate.borrow().current.is_none()));
        measure("Running", async {
            let _ack = phase(Phase::Acknowledge);
            acknowledgements(3);
            Ok::<_, ()>(())
        })
        .await
        .unwrap();
        assert!(measure("Running", async {
            let _prepare = phase(Phase::Prepare);
            Err::<(), _>("failure")
        })
        .await
        .is_err());
    })
    .with_subscriber(tracing::Dispatch::new(capture.clone()))
    .await;
    let cancelled = capture.row("cancelled");
    assert_eq!(cancelled["business_input_count"], 1);
    assert_eq!(cancelled["dispatch_count"], 1);
    assert_eq!(cancelled["interrupted"], false);
    let completed = capture.row("completed");
    assert_eq!(completed["business_input_count"], 0);
    assert_eq!(completed["acknowledgement_count"], 3);
    assert_eq!(capture.row("error")["dispatch_count"], 1);
    capture.assert_conserved();
}

#[tokio::test]
async fn abort_finalizes_cycle_before_scope_report_while_runtime_stays_alive() {
    let capture = Capture::default();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(
        test_scope(measure("Running", async move {
            let _handler = phase(Phase::Handler);
            input_delivered();
            started_tx.send(()).unwrap();
            std::future::pending::<Result<(), ()>>().await
        }))
        .with_subscriber(tracing::Dispatch::new(capture.clone())),
    );
    started_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let cancelled = capture.row("cancelled");
    assert_eq!(cancelled["dispatch_count"], 1);
    assert_eq!(cancelled["business_input_count"], 1);
    assert_eq!(cancelled["interrupted"], true);
    assert!(cancelled["handler_ns"].as_u64().unwrap() > 0);
    assert!(AGGREGATE.try_with(|_| ()).is_err());
    tokio::task::yield_now().await;
    capture.assert_conserved();
}

#[tokio::test]
async fn spawned_work_does_not_inherit_parent_phase_inputs_or_cycle() {
    let capture = Capture::default();
    test_scope(measure("Running", async {
        let _read = phase(Phase::Read);
        input_delivered();
        tokio::spawn(async {
            let _handler = phase(Phase::Handler);
            inputs_delivered(100);
            acknowledgements(100);
            measure("Child", async { Ok::<_, ()>(()) }).await.unwrap();
            assert!(AGGREGATE.try_with(|_| ()).is_err());
        })
        .await
        .unwrap();
        acknowledgement();
        Ok::<_, ()>(())
    }))
    .with_subscriber(tracing::Dispatch::new(capture.clone()))
    .await
    .unwrap();
    let row = capture.row("completed");
    assert_eq!(row["dispatch_count"], 1);
    assert_eq!(row["business_input_count"], 1);
    assert_eq!(row["acknowledgement_count"], 1);
    assert_eq!(row["handler_ns"], 0);
    capture.assert_conserved();
}

#[tokio::test]
async fn retained_cycle_control_override_restores_pending_operation_phase() {
    let capture = Capture::default();
    test_scope(async {
        let (release, wait) = tokio::sync::oneshot::channel();
        let mut pending = Box::pin(measure("Running", async {
            let _publish = phase(Phase::Publish);
            wait.await.unwrap();
            Ok::<_, ()>(())
        }));
        assert!(poll!(&mut pending).is_pending());
        let mut losing_control = Box::pin(control_poll(std::future::pending::<()>()));
        assert!(poll!(&mut losing_control).is_pending());
        drop(losing_control);
        AGGREGATE.with(|aggregate| {
            assert_eq!(
                aggregate
                    .borrow()
                    .current
                    .as_ref()
                    .unwrap()
                    .overrides
                    .last()
                    .unwrap()
                    .phase,
                Phase::Publish
            );
        });
        {
            let _control = phase(Phase::Control);
            tokio::task::yield_now().await;
            {
                let _idle = phase(Phase::Idle);
                tokio::task::yield_now().await;
            }
        }
        AGGREGATE.with(|aggregate| {
            assert_eq!(
                aggregate
                    .borrow()
                    .current
                    .as_ref()
                    .unwrap()
                    .overrides
                    .last()
                    .unwrap()
                    .phase,
                Phase::Publish
            );
        });
        release.send(()).unwrap();
        pending.await.unwrap();
    })
    .with_subscriber(tracing::Dispatch::new(capture.clone()))
    .await;
    let row = capture.row("completed");
    assert_eq!(row["dispatch_count"], 1);
    assert!(row["publish_ns"].as_u64().unwrap() > 0);
    assert!(row["control_ns"].as_u64().unwrap() > 0);
    assert!(row["idle_ns"].as_u64().unwrap() > 0);
    capture.assert_conserved();
}

#[tokio::test]
async fn disabled_target_has_no_task_local_aggregate_or_phase_clock() {
    test_scope(measure("Running", async {
        assert!(AGGREGATE.try_with(|_| ()).is_err());
        let guard = phase(Phase::Read);
        assert_eq!(guard.owner, 0);
        input_delivered();
        Ok::<_, ()>(())
    }))
    .with_subscriber(tracing::Dispatch::none())
    .await
    .unwrap();
}
