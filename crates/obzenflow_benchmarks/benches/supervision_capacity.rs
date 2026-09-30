// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Initial isolated lifecycle service curves for FLOWIP-145i B7.
//! These saturated finite bursts are measurements, not supported load limits.
//! Observer interaction, publication congestion and full memory attribution
//! require separate experiments before any capacity acceptance is claimed.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use obzenflow_benchmarks::support::{self, journal, measure, timed, Census, Sample};
use obzenflow_core::event::{CausalFrontier, SystemEvent};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{FlowId, Journal, JournalOwner, StageId, SystemId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_runtime::pipeline::benchmark::{AvailableResult, ParentLifecycle, PressureControl};
use serde_json::json;
use std::cell::LazyCell;
use std::sync::Arc;
use std::time::Duration;

#[global_allocator]
static ALLOCATOR: support::allocations::Allocator = support::allocations::Allocator;

struct Inputs {
    run: FlowId,
    children: Vec<(StageId, CausalFrontier)>,
    retained: CausalFrontier,
    expected: CausalFrontier,
}

impl Inputs {
    async fn new(children: usize, incoming_width: usize, retained_width: usize) -> Self {
        assert!(incoming_width > 0);
        let run = FlowId::new();
        let mut roots = Vec::new();
        // Independent retained and incoming coordinates expose the wide-parent,
        // narrow-child case without editing admitted clocks by hand.
        for _ in 0..retained_width + incoming_width - 1 {
            let id = StageId::new();
            let journal = MemoryJournal::with_owner_in_run(JournalOwner::stage(id), run);
            let row = journal
                .append(journal::business(id, 0), Default::default())
                .await
                .unwrap();
            roots.push(CausalFrontier::from_record(&row).unwrap());
        }
        let mut retained = CausalFrontier::default();
        for root in &roots[..retained_width] {
            retained.merge(root).unwrap();
        }
        let mut incoming = CausalFrontier::default();
        for root in &roots[retained_width..] {
            incoming.merge(root).unwrap();
        }
        let mut expected = retained.clone();
        let mut inputs = Vec::new();
        for _ in 0..children {
            let id = StageId::new();
            let journal = MemoryJournal::with_owner_in_run(JournalOwner::stage(id), run);
            let row = journal
                .append(journal::running(id), AppendOptions::new(incoming.clone()))
                .await
                .unwrap();
            let frontier = CausalFrontier::from_record(&row).unwrap();
            assert_eq!(frontier.clock().clocks.len(), incoming_width);
            expected.merge(&frontier).unwrap();
            inputs.push((id, frontier));
        }
        Self {
            run,
            children: inputs,
            retained,
            expected,
        }
    }
}

fn operation(
    runtime: &tokio::runtime::Runtime,
    inputs: &Inputs,
    selected: AvailableResult,
    phase: &str,
) -> Sample {
    // A fresh parent and fresh retained results make every child outcome unique
    // within the measured lifecycle. Only admitted input records are reused.
    let directory = tempfile::tempdir().unwrap();
    let system = SystemId::new();
    let path = directory.path().join("parent.log");
    let journal: Arc<dyn Journal<SystemEvent>> = if phase == "selected_to_committed" {
        Arc::new(
            DiskJournal::with_owner_in_run(path.clone(), JournalOwner::system(system), inputs.run)
                .unwrap(),
        )
    } else {
        Arc::new(MemoryJournal::with_owner_in_run(
            JournalOwner::system(system),
            inputs.run,
        ))
    };
    let mut parent = ParentLifecycle::new(
        journal.clone(),
        inputs.children.clone(),
        inputs.retained.clone(),
        selected,
    );
    let (_, mut sample) = match phase {
        "child_result_retention" => {
            let measured = timed(|| runtime.block_on(parent.make_available()));
            runtime.block_on(parent.apply_available());
            measured
        }
        "available_to_applied" => {
            runtime.block_on(parent.make_available());
            timed(|| runtime.block_on(parent.apply_available()))
        }
        "selected_to_committed" => {
            runtime.block_on(parent.make_available());
            runtime.block_on(parent.apply_available());
            parent.verify_applied();
            timed(|| {
                runtime.block_on(async {
                    tokio::time::timeout(support::DEADLINE, parent.publish_terminal())
                        .await
                        .expect("parent terminal publication stalled")
                })
            })
        }
        _ => unreachable!(),
    };
    let mut committed = 0;
    if phase == "selected_to_committed" {
        let rows = runtime.block_on(journal.read_all_unordered()).unwrap();
        assert_eq!(rows.len(), 1, "exactly one parent terminal publication");
        let expected_kind = if matches!(selected, AvailableResult::Failed { .. }) {
            "system.pipeline.failed"
        } else {
            "system.pipeline.completed"
        };
        assert_eq!(rows[0].event_type_name(), expected_kind);
        let clock = &rows[0].envelope.provenance.journal.vector_clock;
        for (coordinate, sequence) in &inputs.expected.clock().clocks {
            assert_eq!(
                clock.clocks.get(coordinate),
                Some(sequence),
                "parent retains every applied child cause"
            );
        }
        assert_eq!(clock.clocks.len(), inputs.expected.clock().clocks.len() + 1);
        committed = 1;
    } else {
        parent.verify_applied();
        assert!(
            runtime
                .block_on(journal.read_all_unordered())
                .unwrap()
                .is_empty(),
            "application boundary must not perform physical publication"
        );
    }
    sample.observations = json!({
        "applied_child_results": inputs.children.len(),
        "terminal_publications": committed,
        "parent_clock_width_before_publication": inputs.expected.clock().clocks.len(),
        "parent_journal_bytes": if committed == 1 { std::fs::metadata(path).unwrap().len() } else { 0 },
        "physical_settlement_verified": committed == 1,
        "complete_memory_attribution": false,
        "supported_capacity_claim": false,
    });
    sample
}

fn pressure_operation(
    runtime: &tokio::runtime::Runtime,
    inputs: &Inputs,
    result: AvailableResult,
    hosts: usize,
    hold: Duration,
    control: PressureControl,
) -> Sample {
    let directory = tempfile::tempdir().unwrap();
    let system = SystemId::new();
    let journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::with_owner_in_run(
            directory.path().join("parent.log"),
            JournalOwner::system(system),
            inputs.run,
        )
        .unwrap(),
    );
    let mut parent = ParentLifecycle::new(
        journal.clone(),
        inputs.children.clone(),
        inputs.retained.clone(),
        result,
    );
    let congestion = runtime.block_on(async {
        parent.make_available().await;
        tokio::time::timeout(support::DEADLINE, parent.congest_hosts(hosts))
            .await
            .expect("host command did not enter storage")
    });
    let (observation, mut sample) = timed(|| {
        runtime.block_on(async {
            tokio::time::timeout(
                support::DEADLINE,
                congestion.settle(&mut parent, hold, control),
            )
            .await
            .expect("parent publication pressure failed to settle")
        })
    });
    let rows = runtime.block_on(journal.read_all_unordered()).unwrap();
    assert_eq!(
        rows.len(),
        hosts + usize::from(control != PressureControl::None) + 1
    );
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(row.local_sequence(), index as u64 + 1);
    }
    for (index, row) in rows.iter().take(hosts).enumerate() {
        let obzenflow_core::event::SystemPayload::IngressRefusal { attempt_seq, .. } = &row.payload
        else {
            panic!("accepted host order changed");
        };
        assert_eq!(attempt_seq.0, index as u64);
    }
    if control != PressureControl::None {
        assert_eq!(
            rows[hosts].event_type_name(),
            "system.pipeline.stop_admitted"
        );
    }
    let terminal = rows.last().unwrap();
    assert_eq!(
        terminal.event_type_name(),
        if matches!(result, AvailableResult::Failed { .. }) {
            "system.pipeline.failed"
        } else if control == PressureControl::Cancel {
            "system.pipeline.cancelled"
        } else {
            "system.pipeline.completed"
        }
    );
    let clock = &terminal.envelope.provenance.journal.vector_clock;
    for (coordinate, sequence) in &inputs.expected.clock().clocks {
        assert_eq!(
            clock.clocks.get(coordinate),
            Some(sequence),
            "congestion cannot erase applied child causes"
        );
    }
    sample.observations = json!({
        "pressure":observation,"physical_commits":rows.len(),"accepted_host_order_verified":true,
        "child_frontier_verified":true,"receipt_waiter_cancellation_preserves_accepted_work":true,
        "physical_journal_bytes":std::fs::metadata(directory.path().join("parent.log")).unwrap().len(),
        "complete_memory_attribution":false,"supported_capacity_claim":false,
    });
    sample
}

fn bench(c: &mut Criterion) {
    let runtime = support::runtime();
    let mut censuses: Vec<Census> = Vec::new();
    support::capacity::save_census(&censuses, false);
    for (family, phase) in [
        ("parent_lifecycle_application", "available_to_applied"),
        ("supervision_phase_costs", "child_result_retention"),
        ("supervision_phase_costs", "selected_to_committed"),
    ] {
        let mut group = c.benchmark_group(family);
        for (children, incoming, retained, regime) in [
            (2, 1, 0, "baseline"),
            (3, 1, 0, "baseline"),
            (8, 1, 0, "baseline"),
            (32, 1, 0, "scaling"),
            (8, 1, 32, "wide_parent"),
            (8, 33, 0, "wide_input"),
            (50, 1, 0, "stress"),
            (75, 1, 0, "stress"),
            (100, 1, 0, "stress"),
        ] {
            let inputs =
                LazyCell::new(|| runtime.block_on(Inputs::new(children, incoming, retained)));
            for (name, result) in [
                ("initialized", AvailableResult::Initialized),
                ("completed", AvailableResult::Completed),
                ("one_failure", AvailableResult::Failed { child: 0 }),
            ] {
                if phase == "selected_to_committed" && name == "initialized" {
                    continue;
                }
                let case = format!("{phase}/{name}/{regime}_children_{children}_incoming_{incoming}_retained_{retained}");
                let input = json!({
                    "children": children, "source_children": 1, "consumer_children": children - 1,
                    "incoming_clock_width": incoming, "retained_clock_width": retained,
                    "result": name, "regime": regime, "arrival": "saturated_finite_burst",
                    "metrics_readers": 0, "studio_connections": 0, "timed_journal_readers": 0,
                    "journal_storage": if phase == "selected_to_committed" { "disk" } else { "memory_unwritten" }, "runtime_workers": 2, "blocking_workers": 2,
                    "timing_boundary": phase, "unit": "nanoseconds_per_completed_burst",
                    "publication_congestion": false,
                });
                let mut taken = false;
                group.throughput(Throughput::Elements(children as u64));
                group.bench_function(&case, |b| {
                    measure(
                        b,
                        &mut censuses,
                        &mut taken,
                        &format!("{family}/{case}"),
                        &input,
                        || operation(&runtime, &inputs, result, phase),
                    )
                });
                support::capacity::save_census(&censuses, false);
            }
        }
        group.finish();
    }
    let mut group = c.benchmark_group("parent_publication_pressure");
    for (name, children, hosts, hold_ms, control, result) in [
        (
            "one_host_completed",
            8,
            1,
            0,
            PressureControl::None,
            AvailableResult::Completed,
        ),
        (
            "eight_hosts_completed",
            8,
            8,
            0,
            PressureControl::None,
            AvailableResult::Completed,
        ),
        (
            "eight_hosts_hold_25ms",
            8,
            8,
            25,
            PressureControl::None,
            AvailableResult::Completed,
        ),
        (
            "eight_hosts_hold_100ms",
            8,
            8,
            100,
            PressureControl::None,
            AvailableResult::Completed,
        ),
        (
            "reserved_stop_while_blocked",
            8,
            8,
            25,
            PressureControl::GracefulStop,
            AvailableResult::Completed,
        ),
        (
            "failure_while_blocked",
            8,
            8,
            25,
            PressureControl::None,
            AvailableResult::Failed { child: 0 },
        ),
        (
            "scaling_children_32",
            32,
            8,
            25,
            PressureControl::GracefulStop,
            AvailableResult::Completed,
        ),
        (
            "cancel_while_blocked",
            8,
            8,
            25,
            PressureControl::Cancel,
            AvailableResult::Completed,
        ),
    ] {
        let inputs = LazyCell::new(|| runtime.block_on(Inputs::new(children, 1, 0)));
        let mut taken = false;
        let input = json!({"children":children,"accepted_host_commands":hosts,
            "additional_waiting_host_command":usize::from(hosts==8),"storage_hold_ms":hold_ms,
            "control":control,"child_failure":matches!(result,AvailableResult::Failed{..}),
            "runtime_workers":2,"blocking_workers":2,"storage":"disk","metrics_readers":0,"studio_connections":0,
            "boundary":"available child results and held host append to required committed decisions and settlement",
            "stage_command_execution_included":false,"unit":"nanoseconds_per_completed_burst"});
        group.bench_function(name, |b| {
            measure(
                b,
                &mut censuses,
                &mut taken,
                &format!("parent_publication_pressure/{name}"),
                &input,
                || {
                    pressure_operation(
                        &runtime,
                        &inputs,
                        result,
                        hosts,
                        Duration::from_millis(hold_ms),
                        control,
                    )
                },
            )
        });
        support::capacity::save_census(&censuses, false);
    }
    group.finish();
    support::capacity::save_census(&censuses, true);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(40).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(3));
    targets = bench
}
criterion_main!(benches);
