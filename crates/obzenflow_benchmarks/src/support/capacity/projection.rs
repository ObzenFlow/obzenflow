// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::support::{journal, Meter, Sample, DEADLINE};
use futures::StreamExt;
use obzenflow_adapters::studio::{ContractBoundaryAliases, StudioProjection};
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::provenance::{ExecutionAccounting, RuntimeProvenance};
use obzenflow_core::event::{CausalFrontier, PipelineLifecycleEvent, SystemEvent, SystemPayload};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{
    ChainEvent, EventId, FlowId, Journal, JournalId, JournalOwner, StageId, SystemId,
};
use obzenflow_infra::benchmark::studio::{AppliedRecord, StudioCapacityProbe};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::metrics::{benchmark::MetricsProjection, MetricsInputs};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::watch;

#[derive(Clone, Copy)]
pub struct Workload {
    pub journals: usize,
    pub records: usize,
    /// Aggregate business/execution arrivals across all data journals. Zero is
    /// an already committed finite burst, not an unbounded maximum-rate claim.
    pub offered_per_second: u64,
    pub payload: usize,
    pub group: usize,
    pub execution_every: usize,
    pub studio: usize,
    pub metrics: bool,
    pub slow_resume: bool,
    pub uneven: bool,
}

impl Workload {
    pub fn input(self, detailed: bool) -> Value {
        json!({
            "data_journals":self.journals,"error_journals":self.journals,"system_journals":1,
            "total_journals":2*self.journals+1,"original_work_records":self.records,
            "payload_string_bytes":self.payload,"physical_group_size":self.group,
            "execution_fact_every":self.execution_every,"studio_connections":self.studio,
            "metrics_enabled":self.metrics,"ordinary_readers_per_data_journal":1,
            "studio_readers_per_journal":self.studio,
            "metrics_tail_readers_per_journal":usize::from(self.metrics),
            "ordinary_error_and_system_readers":0,"diagnostic_journal_readers_during_timing":0,
            "offered_records_per_second":self.offered_per_second,
            "arrival":if self.offered_per_second==0 {"precommitted_finite_burst"} else {"fixed_absolute_schedule_no_dropped_arrivals"},
            "ordinary_idle_poll_ms":1,"metrics_refresh_ms":25,"metrics_apply_ms":25,
            "slow_connection":if self.slow_resume {Some(self.studio-1)} else {None},
            "slow_connection_initial_pause_ms":if self.slow_resume {50} else {0},
            "slow_connection_frame_delay_ms":if self.slow_resume {2} else {0},
            "traffic":if self.uneven {"half_to_first_journal"} else {"round_robin_groups"},
            "runtime_workers":2,"blocking_workers":2,"storage":"disk",
            "definition_cache":"live_writer_shared","filesystem_pages":"uncontrolled",
            "detailed_projection_probe":detailed,"unit":"nanoseconds_per_complete_window_and_drain",
            "metrics_export_and_network_transport_included":false,
            "supported_capacity_claim":false,
        })
    }

    fn destination(self, group_index: usize) -> usize {
        if self.uneven && group_index.is_multiple_of(2) {
            0
        } else {
            group_index % self.journals
        }
    }
}

struct Stage {
    id: StageId,
    data: Arc<dyn Journal<ChainEvent>>,
    error: Arc<dyn Journal<ChainEvent>>,
    expected: Vec<EventId>,
    inputs: usize,
}

#[derive(serde::Serialize)]
struct Commit {
    journal: JournalId,
    sequence: u64,
    acknowledged_ns: u64,
}

struct Fixture {
    directory: tempfile::TempDir,
    stages: Vec<Stage>,
    system: Arc<dyn Journal<SystemEvent>>,
    system_id: SystemId,
    start: Instant,
    commits: Vec<Commit>,
    completed: Vec<EventId>,
    schedule_lateness_ns: Vec<u64>,
    production_window_ns: u64,
}

impl Fixture {
    async fn new(w: Workload) -> Self {
        assert!(w.records > 0 && w.journals > 0 && w.group > 0);
        assert!(!w.slow_resume || w.studio > 0);
        let directory = tempfile::tempdir().unwrap();
        let flow = FlowId::new();
        let system_id = SystemId::new();
        let system: Arc<dyn Journal<SystemEvent>> = Arc::new(
            DiskJournal::with_owner_in_run(
                directory.path().join("system.log"),
                JournalOwner::system(system_id),
                flow,
            )
            .unwrap(),
        );
        let mut fixture = Self {
            directory,
            stages: Vec::new(),
            system,
            system_id,
            start: Instant::now(),
            commits: Vec::new(),
            completed: Vec::new(),
            schedule_lateness_ns: Vec::new(),
            production_window_ns: 0,
        };
        let initial = fixture
            .system
            .append(
                SystemEvent::new(
                    system_id.into(),
                    SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
                        stage_count: Some(w.journals),
                    }),
                ),
                Default::default(),
            )
            .await
            .unwrap();
        fixture.commits.push(Commit {
            journal: *fixture.system.id(),
            sequence: initial.local_sequence(),
            acknowledged_ns: ns(fixture.start.elapsed()),
        });
        for index in 0..w.journals {
            let id = StageId::new();
            let data: Arc<dyn Journal<ChainEvent>> = Arc::new(
                DiskJournal::with_owner_in_run(
                    fixture.directory.path().join(format!("data_{index}.log")),
                    JournalOwner::stage(id),
                    flow,
                )
                .unwrap(),
            );
            let error: Arc<dyn Journal<ChainEvent>> = Arc::new(
                DiskJournal::with_owner_in_run(
                    fixture.directory.path().join(format!("error_{index}.log")),
                    JournalOwner::stage(id),
                    flow,
                )
                .unwrap(),
            );
            let running = data
                .append(journal::running(id), Default::default())
                .await
                .unwrap();
            fixture.commits.push(Commit {
                journal: *data.id(),
                sequence: 1,
                acknowledged_ns: ns(fixture.start.elapsed()),
            });
            let mut error_event =
                journal::business(id, 0).with_runtime_provenance(RuntimeProvenance {
                    accounting: ExecutionAccounting {
                        errors_total: 4,
                        ..Default::default()
                    },
                });
            error_event.flow_context.stage_id = id;
            error.append(error_event, Default::default()).await.unwrap();
            fixture.commits.push(Commit {
                journal: *error.id(),
                sequence: 1,
                acknowledged_ns: ns(fixture.start.elapsed()),
            });
            fixture.stages.push(Stage {
                id,
                data,
                error,
                expected: vec![*running.id()],
                inputs: 0,
            });
        }
        fixture
    }

    async fn produce(&mut self, w: Workload) {
        let arrival_start = tokio::time::Instant::now();
        for first in (0..w.records).step_by(w.group) {
            if w.offered_per_second > 0 {
                let scheduled = arrival_start
                    + Duration::from_secs_f64(first as f64 / w.offered_per_second as f64);
                tokio::time::sleep_until(scheduled).await;
                self.schedule_lateness_ns.push(ns(
                    tokio::time::Instant::now().saturating_duration_since(scheduled)
                ));
            }
            let stage = &mut self.stages[w.destination(first / w.group)];
            let events: Vec<_> = (first..(first + w.group).min(w.records))
                .map(|n| {
                    stage.inputs += 1;
                    let mut event = if w.execution_every > 0 && n.is_multiple_of(w.execution_every)
                    {
                        journal::execution_fact(stage.id, w.payload)
                    } else {
                        journal::business(stage.id, w.payload)
                    };
                    event.flow_context.stage_id = stage.id;
                    event.with_runtime_provenance(RuntimeProvenance {
                        accounting: ExecutionAccounting {
                            events_processed_total: stage.inputs as u64,
                            events_emitted_total: stage.inputs as u64,
                            ..Default::default()
                        },
                    })
                })
                .collect();
            let rows = if w.group == 1 {
                vec![stage
                    .data
                    .append(events.into_iter().next().unwrap(), Default::default())
                    .await
                    .unwrap()]
            } else {
                stage
                    .data
                    .append_group(&format!("capacity-{first}"), events, Default::default())
                    .await
                    .unwrap()
            };
            let acknowledged_ns = ns(self.start.elapsed());
            for row in rows {
                stage.expected.push(*row.id());
                self.commits.push(Commit {
                    journal: *stage.data.id(),
                    sequence: row.local_sequence(),
                    acknowledged_ns,
                });
            }
        }
        self.production_window_ns = ns(arrival_start.elapsed());
        let mut frontier = CausalFrontier::default();
        for stage in &mut self.stages {
            let accounting = ExecutionAccounting {
                events_processed_total: stage.inputs as u64,
                events_emitted_total: stage.inputs as u64,
                ..Default::default()
            };
            let row = stage
                .data
                .append(
                    journal::execution(
                        stage.id,
                        ExecutionPayload::StageLifecycle(StageLifecycleFact::Completed {
                            stage_id: stage.id,
                            accounting: Some(accounting.clone()),
                        }),
                    )
                    .with_runtime_provenance(RuntimeProvenance { accounting }),
                    Default::default(),
                )
                .await
                .unwrap();
            frontier
                .merge(&CausalFrontier::from_record(&row).unwrap())
                .unwrap();
            self.completed.push(*row.id());
            stage.expected.push(*row.id());
            self.commits.push(Commit {
                journal: *stage.data.id(),
                sequence: row.local_sequence(),
                acknowledged_ns: ns(self.start.elapsed()),
            });
        }
        let row = self
            .system
            .append(
                SystemEvent::new(
                    self.system_id.into(),
                    SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
                ),
                AppendOptions::new(frontier),
            )
            .await
            .unwrap();
        self.commits.push(Commit {
            journal: *self.system.id(),
            sequence: row.local_sequence(),
            acknowledged_ns: ns(self.start.elapsed()),
        });
    }
}

fn ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap()
}

/// Each sample starts and joins all actual reader tasks. The closing watch is
/// sent only after the producer's final physical commit acknowledges.
pub fn operation(runtime: &tokio::runtime::Runtime, w: Workload, detailed: bool) -> Sample {
    let mut fixture = runtime.block_on(Fixture::new(w));
    if w.offered_per_second == 0 {
        runtime.block_on(fixture.produce(w));
    }
    let usage_start = super::process_usage();
    let meter = Meter::start();
    let (mut ordinary, studio, metrics, producer_finished_ns) = runtime.block_on(async {
        tokio::time::timeout(DEADLINE, async {
            let (done, finished) = watch::channel(None::<tokio::time::Instant>);
            let (closing, closed) = watch::channel(false);
            let mut ordinary = Vec::new();
            for stage in &fixture.stages {
                let journal = stage.data.clone();
                let mut finished = finished.clone();
                let origin = fixture.start;
                ordinary.push(tokio::spawn(async move {
                    let mut reader = journal.reader().await.unwrap();
                    let mut output = Vec::new();
                    let mut final_refresh = false;
                    loop {
                        if let Some(row) = reader.next().await.unwrap() {
                            output.push((*row.id(), row.local_sequence(), ns(origin.elapsed())));
                        } else if finished.borrow().is_some() {
                            // The empty read may have started before the final
                            // append. Require a new tail read after observing
                            // the producer's physical-completion barrier.
                            if final_refresh {
                                break;
                            }
                            final_refresh = true;
                        } else {
                            tokio::select! {
                                _ = finished.changed() => {},
                                _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                            }
                        }
                    }
                    (*journal.id(), output)
                }));
            }
            let mut studio = Vec::new();
            for index in 0..w.studio {
                let stages = fixture
                    .stages
                    .iter()
                    .flat_map(|s| [(s.id, s.data.clone()), (s.id, s.error.clone())])
                    .collect();
                let projection =
                    StudioProjection::new(Vec::new(), ContractBoundaryAliases::default()).unwrap();
                let probe = detailed.then(|| {
                    Arc::new(StudioCapacityProbe::new(
                        fixture.start,
                        w.records + 3 * w.journals + 2,
                    ))
                });
                let mut body = if let Some(probe) = &probe {
                    obzenflow_infra::testing::studio_capacity::connect(
                        stages,
                        vec![fixture.system.clone()],
                        projection,
                        closed.clone(),
                        Some("jr1:{}"),
                        probe.clone(),
                    )
                    .await
                } else {
                    obzenflow_infra::testing::studio::connect(
                        stages,
                        vec![fixture.system.clone()],
                        projection,
                        closed.clone(),
                        Some("jr1:{}"),
                        Duration::from_millis(250),
                    )
                    .await
                };
                studio.push(tokio::spawn(async move {
                    if w.slow_resume && index + 1 == w.studio {
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                    let mut frames = Vec::new();
                    while let Some(frame) = body.next().await {
                        frames.push(frame);
                        if w.slow_resume && index + 1 == w.studio {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                    (frames, probe.map(|probe| probe.snapshot()))
                }));
            }
            let metrics = if w.metrics {
                let inputs = MetricsInputs::new(
                    fixture
                        .stages
                        .iter()
                        .map(|s| (s.id, s.data.clone()))
                        .collect(),
                    fixture
                        .stages
                        .iter()
                        .map(|s| (s.id, s.error.clone()))
                        .collect(),
                );
                let system = fixture.system.clone();
                let finished = finished.clone();
                Some(tokio::spawn(async move {
                    let mut metrics = MetricsProjection::start(inputs, system).await;
                    assert_eq!(metrics.reader_count(), 2 * w.journals + 1);
                    let mut interval = tokio::time::interval(Duration::from_millis(25));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    let mut rounds = 0;
                    loop {
                        interval.tick().await;
                        let complete = finished
                            .borrow()
                            .is_some_and(|at| metrics.refreshed_since(at));
                        metrics.apply(rounds < 3).await;
                        rounds += 1;
                        if complete {
                            break;
                        }
                    }
                    metrics.stop().await;
                    (
                        metrics.accounting(),
                        metrics.retained_arrays_and_records(),
                        rounds,
                    )
                }))
            } else {
                None
            };
            if w.offered_per_second > 0 {
                fixture.produce(w).await;
            }
            let producer_finished_ns = ns(fixture.start.elapsed());
            done.send(Some(tokio::time::Instant::now())).unwrap();
            if w.studio > 0 {
                closing.send(true).unwrap();
            }
            let ordinary = futures::future::join_all(ordinary)
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            let studio = futures::future::join_all(studio)
                .await
                .into_iter()
                .map(Result::unwrap)
                .collect::<Vec<_>>();
            let metrics = match metrics {
                Some(task) => Some(task.await.unwrap()),
                None => None,
            };
            (ordinary, studio, metrics, producer_finished_ns)
        })
        .await
        .expect("projection capacity workload or recovery failed to settle")
    });
    let elapsed = meter.elapsed();
    let finished_ns = ns(fixture.start.elapsed());
    let mut sample = meter.finish(elapsed);
    match std::env::var("OBZENFLOW_CAPACITY_CONTROL").ok().as_deref() {
        None => {}
        Some("missing-reader-output") => {
            ordinary[0].1.pop();
        }
        Some(value) => panic!("unknown capacity rejection control: {value}"),
    }
    let usage_end = super::process_usage();
    for (stage, (_, observed)) in fixture.stages.iter().zip(&ordinary) {
        assert_eq!(
            observed.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
            stage.expected,
            "ordinary reader preserves every original record in order"
        );
        for (index, (_, sequence, _)) in observed.iter().enumerate() {
            assert_eq!(*sequence, index as u64 + 1);
        }
    }
    let expected_positions: BTreeMap<_, _> = fixture
        .commits
        .iter()
        .map(|c| (c.journal, c.sequence))
        .fold(BTreeMap::new(), |mut map, (id, pos)| {
            map.entry(id)
                .and_modify(|old: &mut u64| *old = (*old).max(pos))
                .or_insert(pos);
            map
        });
    let mut observer_results = Vec::new();
    for (frames, probe) in &studio {
        assert_eq!(
            frames.last().unwrap().event.as_deref(),
            Some("server_shutdown")
        );
        assert!(!frames
            .iter()
            .any(|f| f.event.as_deref() == Some("stream_error")));
        let mut checkpoints = BTreeMap::<JournalId, u64>::new();
        for frame in frames {
            if let Some(cursor) = &frame.id {
                for (id, pos) in serde_json::from_str::<BTreeMap<JournalId, u64>>(
                    cursor.strip_prefix("jr1:").unwrap(),
                )
                .unwrap()
                {
                    checkpoints
                        .entry(id)
                        .and_modify(|old| *old = (*old).max(pos))
                        .or_insert(pos);
                }
            }
        }
        assert_eq!(
            checkpoints, expected_positions,
            "Studio's final delivered checkpoint covers every journal"
        );
        for completed in &fixture.completed {
            assert!(
                frames
                    .iter()
                    .any(|frame| serde_json::from_str::<Value>(&frame.data)
                        .ok()
                        .is_some_and(|v| v["commitment"]["event_id"] == completed.to_string())),
                "missing stage completion frame"
            );
        }
        if let Some(probe) = probe {
            assert!(!probe.overflowed);
            assert_eq!(probe.applied.len(), fixture.commits.len());
            let mut actual = BTreeMap::<JournalId, u64>::new();
            for record in &probe.applied {
                let previous = actual.entry(record.journal).or_default();
                assert_eq!(record.sequence, *previous + 1);
                *previous = record.sequence;
            }
            assert_eq!(actual, expected_positions);
        }
        observer_results.push(json!({"frames":frames.len(),"checkpoint":checkpoints,"probe":probe,
            "lag":probe.as_ref().map(|p|lags(&fixture.commits,&p.applied)),
            "client_fixture_retained_frame_string_capacity":frames.iter().map(|f|f.data.capacity()+f.id.as_ref().map_or(0,String::capacity)+f.event.as_ref().map_or(0,String::capacity)).sum::<usize>()}));
    }
    if let Some((accounting, _, _)) = &metrics {
        for stage in &fixture.stages {
            assert_eq!(accounting[&stage.id], (stage.inputs as u64, 4));
        }
    }
    let ordinary_applied = ordinary
        .iter()
        .flat_map(|(journal, rows)| {
            rows.iter().map(|(_, sequence, elapsed)| AppliedRecord {
                journal: *journal,
                sequence: *sequence,
                elapsed: Duration::from_nanos(*elapsed),
            })
        })
        .collect::<Vec<_>>();
    let data_commits = fixture
        .commits
        .iter()
        .filter(|commit| {
            fixture
                .stages
                .iter()
                .any(|s| *s.data.id() == commit.journal)
        })
        .collect::<Vec<_>>();
    let physical_bytes = directory_bytes(fixture.directory.path());
    sample.observations = json!({
        "completed_original_records":fixture.commits.len(),"ordinary_records":ordinary_applied.len(),
        "completed_work_records":w.records,"offered_work_records":w.records,
        "production_window_ns":fixture.production_window_ns,
        "actual_work_commit_rate_per_second":w.records as f64/(fixture.production_window_ns as f64/1e9),
        "completed_work_rate_per_second_including_drain":w.records as f64/elapsed.as_secs_f64(),
        "elapsed_ns":ns(elapsed),"drain_after_last_commit_ack_ns":finished_ns.saturating_sub(producer_finished_ns),
        "producer_finished_ns_from_fixture_start":producer_finished_ns,
        "group_schedule_lateness_ns":fixture.schedule_lateness_ns,
        "groups_missing_their_next_arrival_slot":if w.offered_per_second>0 {fixture.schedule_lateness_ns.iter().filter(|late|**late>1_000_000_000*w.group as u64/w.offered_per_second).count()}else{0},
        "physical_journal_and_auxiliary_file_bytes":physical_bytes,"not_device_io_bytes":true,
        "commits":if detailed {serde_json::to_value(&fixture.commits).unwrap()}else{Value::Null},
        "ordinary_lag":lags_refs(&data_commits,&ordinary_applied),"studio":observer_results,
        "metrics_accounting":metrics.as_ref().map(|m|&m.0),"metrics_shared_arrays_and_records":metrics.as_ref().map(|m|m.1),
        "metrics_apply_rounds":metrics.as_ref().map(|m|m.2),"metrics_held_snapshot_limit":3,
        "process_start":usage_start,"process_end":usage_end,
        "memory_limitations":"requested heap includes all roles; role attribution, allocator overhead and per-case RSS peak are not qualified",
        "complete_memory_attribution":false,"supported_capacity_claim":false,
    });
    sample
}

fn lags(commits: &[Commit], applied: &[AppliedRecord]) -> Value {
    lags_refs(&commits.iter().collect::<Vec<_>>(), applied)
}
fn lags_refs(commits: &[&Commit], applied: &[AppliedRecord]) -> Value {
    let by_position: BTreeMap<_, _> = commits
        .iter()
        .map(|c| ((c.journal, c.sequence), c.acknowledged_ns))
        .collect();
    let mut lags = applied
        .iter()
        .map(|r| i128::from(ns(r.elapsed)) - i128::from(by_position[&(r.journal, r.sequence)]))
        .collect::<Vec<_>>();
    lags.sort_unstable();
    let at = |percent: usize| lags[(lags.len() - 1) * percent / 100];
    let mut timeline = commits
        .iter()
        .map(|c| (c.acknowledged_ns, false, (c.journal, c.sequence)))
        .chain(
            applied
                .iter()
                .map(|r| (ns(r.elapsed), true, (r.journal, r.sequence))),
        )
        .collect::<Vec<_>>();
    timeline.sort_unstable();
    let mut pending = BTreeMap::new();
    let mut seen = std::collections::BTreeSet::new();
    let mut peak_backlog = 0;
    let mut oldest_outstanding_ns = 0;
    for (at, application, position) in timeline {
        if application {
            pending.remove(&position);
            seen.insert(position);
        } else if !seen.contains(&position) {
            pending.insert(position, at);
        }
        peak_backlog = peak_backlog.max(pending.len());
        if let Some(oldest) = pending.values().min() {
            oldest_outstanding_ns = oldest_outstanding_ns.max(at - oldest);
        }
    }
    assert!(
        pending.is_empty(),
        "every acknowledged position must settle"
    );
    // A reader can apply a record after its physical write but before the
    // append future returns. Preserve that signed observation instead of
    // fabricating a zero or a physical-commit timestamp.
    json!({"boundary":"append acknowledgement to consumer application","samples":lags.len(),
        "min_ns":lags[0],"p50_ns":at(50),"p95_ns":at(95),"p99_ns":at(99),"max_ns":lags[lags.len()-1],
        "applied_before_append_acknowledgement":lags.iter().filter(|v|**v<0).count(),
        "peak_acknowledged_position_backlog":peak_backlog,
        "oldest_outstanding_ns_at_observed_milestones":oldest_outstanding_ns,
        "final_acknowledged_position_backlog":0})
}

fn directory_bytes(root: &std::path::Path) -> u64 {
    std::fs::read_dir(root)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let metadata = entry.metadata().unwrap();
            if metadata.is_dir() {
                directory_bytes(&entry.path())
            } else {
                metadata.len()
            }
        })
        .sum()
}
