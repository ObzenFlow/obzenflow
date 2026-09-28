// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{fixtures, measure, Census, Meter, Sample};
use criterion::{Criterion, Throughput};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{SupervisorRecord, SystemEvent};
use obzenflow_core::{
    ChainEvent, EventId, FlowId, Journal, JournalId, JournalOwner, StageId, SystemId,
};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::supervised_base::report_reader::{ReportRead, ReportReaders};
use obzenflow_runtime::testing::pipeline::{ParentAdmission, ReportParent};
use std::cell::LazyCell;
use std::collections::BTreeMap;
use std::future::poll_fn;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;

struct Child {
    journal: Arc<dyn Journal<ChainEvent>>,
    events: Vec<ChainEvent>,
    expected: Vec<(u64, EventId)>,
}
struct History {
    topology: Arc<obzenflow_topology::Topology>,
    stages: Vec<StageId>,
    children: Vec<Child>,
    run: FlowId,
    _directory: tempfile::TempDir,
}
impl History {
    async fn build(journals: usize, reports: usize, business_between: usize, live: bool) -> Self {
        Self::build_distribution(journals, reports * journals, business_between, 0, 256, live).await
    }

    async fn build_distribution(
        journals: usize,
        total_reports: usize,
        business_between: usize,
        business_after_first: usize,
        payload_bytes: usize,
        live: bool,
    ) -> Self {
        assert!(total_reports >= journals);
        let (topology, stages) = ParentAdmission::topology(journals);
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let mut children = Vec::new();
        for (index, stage) in stages.iter().enumerate() {
            let reports = total_reports / journals + usize::from(index < total_reports % journals);
            let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
                DiskJournal::with_owner_in_run(
                    directory.path().join(format!("child-{index}.log")),
                    JournalOwner::stage(*stage),
                    run,
                )
                .unwrap(),
            );
            let mut events = Vec::new();
            let mut expected = Vec::new();
            for report in 0..reports {
                events.extend(
                    (0..business_between).map(|_| fixtures::business(*stage, payload_bytes)),
                );
                let event = if report + 1 == reports {
                    fixtures::running(*stage)
                } else {
                    fixtures::execution(
                        *stage,
                        ExecutionPayload::ContractStatus {
                            upstream: *stage,
                            reader: stages[(index + 1) % journals],
                            selected_event_type: None,
                            feed_role: None,
                            pass: true,
                            reader_seq: None,
                            advertised_writer_seq: None,
                            reason: None,
                        },
                    )
                };
                expected.push((events.len() as u64 + 1, event.id));
                events.push(event);
                if report == 0 {
                    events.extend(
                        (0..business_after_first)
                            .map(|_| fixtures::business(*stage, payload_bytes)),
                    );
                }
            }
            if !live {
                fixtures::append_events(&journal, events.clone(), 1).await;
            }
            children.push(Child {
                journal,
                events,
                expected,
            });
        }
        Self {
            topology,
            stages,
            children,
            run,
            _directory: directory,
        }
    }
}

/// New measurement contract, separate from the unchanged hot-path inventory.
/// Both metrics finish and validate the entire operation before returning a
/// sample. First delivery measures only the first child report reaching the
/// parent, before that report's FSM application or resulting publication.
#[allow(dead_code)] // Used by the separate supervision_delivery executable.
pub fn bench_delivery(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    struct Input {
        name: String,
        journals: usize,
        reports: usize,
        between: usize,
        after_first: usize,
        payload: usize,
        live: bool,
    }
    let mut inputs = Vec::new();
    for (after_first, payload) in [(0, 256), (63, 256), (63, 8192), (1024, 256), (1024, 8192)] {
        inputs.push(Input {
            name: format!("prepared_suffix/business_after_first_{after_first}/payload_{payload}"),
            journals: 1,
            reports: 2,
            between: 0,
            after_first,
            payload,
            live: false,
        });
    }
    for journals in [8, 32, 50, 75, 100] {
        for between in [0, 7] {
            inputs.push(Input {
                name: format!(
                    "live_fixed_reports_600/journals_{journals}/business_between_{between}"
                ),
                journals,
                reports: 600,
                between,
                after_first: 0,
                payload: 256,
                live: true,
            });
        }
    }
    let mut group = c.benchmark_group("supervision_delivery");
    // Short first-delivery values can accompany much longer complete operations.
    // Flat sampling avoids steep iteration ramps while every operation still
    // completes and tears down its readers before the next one starts.
    group.sampling_mode(criterion::SamplingMode::Flat);
    for input in inputs {
        let history = LazyCell::new(|| {
            runtime.block_on(History::build_distribution(
                input.journals,
                input.reports,
                input.between,
                input.after_first,
                input.payload,
                false,
            ))
        });
        for metric in ["first_report_delivery", "completion"] {
            let case = format!("{metric}/{}", input.name);
            let dimensions = serde_json::json!({
                "journals": input.journals, "reports": input.reports,
                "business_between_reports": input.between,
                "business_after_first_report_per_journal": input.after_first,
                "business_records": input.reports * input.between + input.journals * input.after_first,
                "payload_string_bytes": input.payload,
                "concurrent_appends": input.live,
                "async_workers": 2, "blocking_workers": 2,
                "metric": metric,
            });
            let mut taken = false;
            group.bench_function(&case, |b| {
                measure(
                    b,
                    censuses,
                    &mut taken,
                    &format!("supervision_delivery/{case}"),
                    &dimensions,
                    || {
                        let mut sample = if input.live {
                            let fresh = runtime.block_on(History::build_distribution(
                                input.journals,
                                input.reports,
                                input.between,
                                input.after_first,
                                input.payload,
                                true,
                            ));
                            consume(runtime, &fresh, true)
                        } else {
                            consume(runtime, &history, false)
                        };
                        sample.observations["completed_elapsed_ns"] =
                            (sample.elapsed.as_nanos() as u64).into();
                        if metric == "first_report_delivery" {
                            sample.elapsed = Duration::from_nanos(
                                sample.observations["first_report_ns"].as_u64().unwrap(),
                            );
                        }
                        sample
                    },
                );
            });
        }
    }
    group.finish();
}

fn consume(runtime: &Runtime, history: &History, live: bool) -> Sample {
    let directory = tempfile::tempdir().unwrap();
    let parent_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::with_owner_in_run(
            directory.path().join("parent.log"),
            JournalOwner::system(SystemId::new()),
            history.run,
        )
        .unwrap(),
    );
    let mut parent = ReportParent::new(
        parent_journal.clone(),
        history.topology.clone(),
        &history.stages,
    );
    let mut next: BTreeMap<JournalId, usize> = history
        .children
        .iter()
        .map(|c| (*c.journal.id(), 0))
        .collect();
    let lookup: BTreeMap<_, _> = history
        .children
        .iter()
        .map(|c| (*c.journal.id(), c))
        .collect();
    let mut last_service: BTreeMap<JournalId, Duration> =
        next.keys().map(|id| (*id, Duration::ZERO)).collect();
    let mut gaps = Vec::with_capacity(history.children.iter().map(|c| c.expected.len()).sum());
    let mut last_reports: BTreeMap<JournalId, SupervisorRecord> = BTreeMap::new();
    let mut publication = None;
    let writes: Vec<_> = if live {
        history
            .children
            .iter()
            .map(|c| (c.journal.clone(), c.events.clone()))
            .collect()
    } else {
        vec![]
    };
    let mut first = None;
    let meter = Meter::start();
    let start = Instant::now();
    let (elapsed, retained) = runtime.block_on(async {
        let mut readers = ReportReaders::default();
        let elapsed = tokio::time::timeout(fixtures::DEADLINE, async {
            for child in &history.children {
                readers.stage(child.journal.clone());
            }
            readers.system(parent_journal.clone());
            let mut writers = tokio::task::JoinSet::new();
            for (journal, events) in writes {
                writers.spawn(async move { fixtures::append_events(&journal, events, 1).await });
            }
            loop {
                let read = poll_fn(|cx| readers.poll_next(cx)).await.unwrap();
                match &read {
                    ReportRead::Record(row) if row.journal_id() == *parent_journal.id() => {
                        assert!(publication.is_none(), "unexpected extra parent publication");
                        publication = Some(row.as_ref().clone());
                    }
                    ReportRead::Record(row) => {
                        first.get_or_insert_with(|| start.elapsed());
                        let id = row.journal_id();
                        let index = next.get_mut(&id).unwrap();
                        assert_eq!(
                            (row.position(), *row.id()),
                            lookup[&id].expected[*index],
                            "report identity/order"
                        );
                        *index += 1;
                        let now = start.elapsed();
                        gaps.push(now - last_service.insert(id, now).unwrap());
                        if *index == lookup[&id].expected.len() {
                            last_reports.insert(id, row.as_ref().clone());
                        }
                    }
                    ReportRead::Coverage { journal, through }
                        if *journal != *parent_journal.id() =>
                    {
                        assert!(*through >= parent.covered(journal));
                        assert!(*through <= lookup[journal].events.len() as u64);
                        assert!(
                            lookup[journal]
                                .expected
                                .get(next[journal])
                                .is_none_or(|(position, _)| position > through),
                            "coverage overtook an unapplied report"
                        );
                    }
                    _ => {}
                }
                parent.apply(read).await;
                if parent.ready()
                    && history.children.iter().all(|child| {
                        parent.covered(child.journal.id()) == child.events.len() as u64
                            && next[child.journal.id()] == child.expected.len()
                    })
                {
                    break;
                }
            }
            while let Some(writer) = writers.join_next().await {
                writer.unwrap();
            }
            parent.settle().await;
            start.elapsed()
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "fan-in incomplete: ready={}, actions={}, delivered={next:?}, readers={:?}",
                parent.ready(),
                parent.actions_executed,
                readers.diagnostics()
            )
        });
        let retained = readers
            .diagnostics()
            .iter()
            .map(|d| d.high_water_record_bytes)
            .sum::<usize>();
        tokio::time::timeout(fixtures::DEADLINE, readers.finish_reads_for_benchmark())
            .await
            .expect("bounded reader teardown did not finish");
        (elapsed, retained)
    });
    let mut sample = meter.finish(elapsed);
    assert!(parent.ready());
    assert_eq!(parent.actions_executed, 1);
    let publication = publication.unwrap();
    assert_eq!(last_reports.len(), history.children.len());
    for row in last_reports.values() {
        for (coordinate, sequence) in &row.journal().vector_clock.clocks {
            assert!(
                publication.journal().vector_clock.get(coordinate) >= *sequence,
                "parent publication lost child causality"
            );
        }
    }
    let own_rows = runtime
        .block_on(parent_journal.read_all_unordered())
        .unwrap();
    assert_eq!(own_rows.len(), 1);
    assert_eq!(*own_rows[0].id(), *publication.id());
    sample.expect_work("business_payload_decodes", 0);
    if !live {
        sample.expect_work("business_records_constructed", 0);
        sample.expect_work("business_record_accounting_serializations", 0);
    }
    gaps.sort_unstable();
    sample.observations = serde_json::json!({
        "first_report_ns":first.unwrap().as_nanos() as u64,
        "service_gap_p50_ns":gaps[gaps.len()/2].as_nanos() as u64,
        "service_gap_p95_ns":gaps[(gaps.len()-1)*95/100].as_nanos() as u64,
        "service_gap_max_ns":gaps.last().unwrap().as_nanos() as u64,
        "retained_report_bytes_high_water_sum":retained,
        "applied_child_reports":next.values().sum::<usize>(),"required_parent_publications":1,
    });
    sample
}

pub fn bench(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    // Keep pool-starvation reproduction available without disguising failed
    // work as a timing sample. The capacity is in the case name and census.
    let live_workers: usize = std::env::var("OBZENFLOW_LIVE_BLOCKING_THREADS")
        .map(|value| value.parse().expect("positive blocking worker count"))
        .unwrap_or(512);
    let live_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .max_blocking_threads(live_workers)
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("supervisor_fan_in");
    for journals in [1, 8, 32, 100] {
        for between in [0, 7] {
            for mode in ["fixed_total_800", "per_journal_8", "live_per_journal_8"] {
                let reports = if mode == "fixed_total_800" {
                    800 / journals
                } else {
                    8
                };
                let live = mode == "live_per_journal_8";
                let history = LazyCell::new(|| {
                    runtime.block_on(History::build(journals, reports, between, false))
                });
                let pool = if live {
                    format!("/blocking_capacity_{live_workers}")
                } else {
                    String::new()
                };
                let case = format!("{mode}{pool}/journals_{journals}/business_between_{between}");
                let input = serde_json::json!({"journals":journals,"reports":reports*journals,"business_between_reports":between,"concurrent_appends":live,"async_workers":2,"blocking_workers":if live {live_workers} else {2}});
                let mut taken = false;
                group.throughput(Throughput::Elements((reports * journals) as u64));
                group.bench_function(&case, |b| {
                    measure(
                        b,
                        censuses,
                        &mut taken,
                        &format!("supervisor_fan_in/{case}"),
                        &input,
                        || {
                            if live {
                                let fresh = live_runtime
                                    .block_on(History::build(journals, reports, between, true));
                                consume(&live_runtime, &fresh, true)
                            } else {
                                consume(runtime, &history, false)
                            }
                        },
                    )
                });
            }
        }
    }
    group.finish();
}
