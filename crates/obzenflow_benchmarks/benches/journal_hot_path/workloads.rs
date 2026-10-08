// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Codec-independent workloads derived from the 080n native CPU profile.
use super::{fixtures, measure, timed, Census};
use criterion::{Criterion, Throughput};
use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::observability::*;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::CausalFrontier;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{ChainEvent, Journal, JournalOwner, ReaderGeneration, StageId};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_runtime::execution::{RuntimeExecution, RuntimeMode};
use obzenflow_runtime::metrics::instrumentation::StageInstrumentation;
use obzenflow_runtime::metrics::observations::LatestObservationMap;
use std::{cell::LazyCell, sync::Arc};
use tokio::runtime::Runtime;

const RECORDS: usize = 64;
const READ_RECORDS: usize = 1024;

struct Fixture {
    seed: fixtures::RecordFixture,
    stage: StageId,
    events: Vec<ChainEvent>,
}
impl Fixture {
    async fn new(width: usize, observations: bool, distinct: bool, payload: usize) -> Self {
        Self::with_records(width, observations, distinct, payload, RECORDS).await
    }

    async fn with_records(
        width: usize,
        observations: bool,
        distinct: bool,
        payload: usize,
        count: usize,
    ) -> Self {
        let seed = fixtures::RecordFixture::build(fixtures::Dimensions {
            clock: width,
            advanced_inputs: width.saturating_sub(1),
            payload: 256,
        })
        .await;
        let stage = StageId::new();
        let events = (0..count)
            .map(|i| {
                let name = if distinct {
                    format!("stage_{i}")
                } else {
                    "stage".into()
                };
                let mut event = fixtures::business(stage, payload)
                    .with_flow_context(FlowContext::new(name, stage));
                if observations {
                    event =
                        event.with_observability_context(Self::packet(&seed, stage, i as u64 + 1));
                }
                event
            })
            .collect();
        Self {
            seed,
            stage,
            events,
        }
    }
    fn packet(seed: &fixtures::RecordFixture, stage: StageId, seq: u64) -> ObservabilityContext {
        let row = &seed.record;
        let capture = CaptureStamp {
            capture_scope: CaptureScope {
                flow_id: row.envelope.provenance.journal.run_id,
                resume_generation: ReaderGeneration::default(),
            },
            observer: stage.into(),
            capture_seq: CaptureSeq(seq),
            capture_reason: CaptureReason::Record,
            observed_at_ms: 1_728_000_000_123 + seq,
        };
        let mut packet = ObservabilityContext::new(capture);
        packet.runtime_snapshot = Some(RuntimeSnapshot {
            capture,
            fsm_state: "Running".into(),
            progress: ExecutionProgress {
                reader_seq: seq,
                writer_seq: seq,
                receipted_seq: 0,
                last_consumed_event_id: Some(*row.id()),
                last_consumed_vector_clock: Some(
                    row.envelope.provenance.journal.vector_clock.clone(),
                ),
                ..Default::default()
            },
        });
        packet.metrics = Some(MetricsSnapshot {
            events_processed: seq,
            events_in_flight: 0,
            queue_depth: 0,
            processing_rate: 128.5,
            error_rate: 0.0,
            latency_p50_ms: 0.125,
            latency_p99_ms: 2.75,
        });
        packet.sli = Some(SliSnapshot {
            availability: 1.0,
            error_budget_remaining: 0.99,
            latency_budget_used: 0.0,
        });
        packet.records.push(ObservationRecord::ResourceUsage {
            cpu_percent: 12.5,
            memory_bytes: 1_048_576,
            thread_count: None,
        });
        assert!(
            packet.clone().validated().is_some(),
            "fixture must be accepted"
        );
        packet
    }
    fn journal(&self, name: &str) -> Arc<dyn Journal<ChainEvent>> {
        self.journal_at(self.seed._directory.path().join(name))
    }
    fn journal_at(&self, path: std::path::PathBuf) -> Arc<dyn Journal<ChainEvent>> {
        Arc::new(
            DiskJournal::with_owner_in_run(
                path,
                JournalOwner::stage(self.stage),
                self.seed.record.envelope.provenance.journal.run_id,
            )
            .unwrap(),
        )
    }
    async fn append(&self, journal: &Arc<dyn Journal<ChainEvent>>) -> Vec<ChainJournalRecord> {
        let mut rows = Vec::with_capacity(self.events.len());
        for event in &self.events {
            // Authored cloning and causal preparation belong to the operation.
            rows.push(
                journal
                    .append(
                        event.clone(),
                        AppendOptions::new(CausalFrontier::from_record(&self.seed.record).unwrap()),
                    )
                    .await
                    .unwrap(),
            );
        }
        rows
    }
    fn check(&self, rows: &[ChainJournalRecord]) {
        assert_eq!(rows.len(), self.events.len());
        for (index, (row, event)) in rows.iter().zip(&self.events).enumerate() {
            assert_eq!(row.id(), &event.id);
            assert_eq!(
                serde_json::to_value(&row.payload).unwrap(),
                serde_json::to_value(&event.payload).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&row.envelope.provenance.event).unwrap(),
                serde_json::to_value(&event.envelope.provenance.event).unwrap()
            );
            assert_eq!(
                serde_json::to_value(&row.envelope.observability).unwrap(),
                serde_json::to_value(&event.envelope.observability).unwrap()
            );
            assert_eq!(row.local_sequence(), index as u64 + 1);
            assert_eq!(
                row.envelope.provenance.journal.run_id,
                self.seed.record.envelope.provenance.journal.run_id
            );
            assert_eq!(
                row.envelope.provenance.journal.vector_clock.clocks.len(),
                self.seed
                    .record
                    .envelope
                    .provenance
                    .journal
                    .vector_clock
                    .clocks
                    .len()
                    + 1
            );
            for (coordinate, sequence) in &self
                .seed
                .record
                .envelope
                .provenance
                .journal
                .vector_clock
                .clocks
            {
                assert_eq!(
                    row.envelope
                        .provenance
                        .journal
                        .vector_clock
                        .clocks
                        .get(coordinate),
                    Some(sequence)
                );
            }
        }
    }
}

async fn scan(journal: &Arc<dyn Journal<ChainEvent>>) -> Vec<ChainJournalRecord> {
    let mut reader = journal.reader().await.unwrap();
    let mut rows = Vec::new();
    while let Some(row) = reader.next().await.unwrap() {
        rows.push(row);
    }
    assert_eq!(reader.position(), rows.len() as u64);
    rows
}

pub fn bench(c: &mut Criterion, rt: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("hotspots");
    group.throughput(Throughput::Elements(RECORDS as u64));
    for (label, width, observations, distinct, payload) in [
        ("narrow_observed", 1, true, false, 256),
        ("wide_observed", 33, true, false, 256),
        ("wide_distinct", 33, true, true, 256),
        ("wide_absent_control", 33, false, false, 256),
        ("large_observed", 33, true, false, 8192),
    ] {
        let fixture =
            LazyCell::new(|| rt.block_on(Fixture::new(width, observations, distinct, payload)));
        let read_fixture = LazyCell::new(|| {
            rt.block_on(Fixture::with_records(
                width,
                observations,
                distinct,
                payload,
                READ_RECORDS,
            ))
        });
        let archive = LazyCell::new(|| {
            let journal = read_fixture.journal("read.log");
            let rows = rt.block_on(read_fixture.append(&journal));
            read_fixture.check(&rows);
            (journal, rows)
        });
        let reopened = LazyCell::new(|| {
            let journal = read_fixture.journal("reopened.log");
            let rows = rt.block_on(read_fixture.append(&journal));
            read_fixture.check(&rows);
            drop(journal);
            rows
        });
        for operation in [
            "append",
            "sequential_read",
            "reopened_scan",
            "reader_creation",
        ] {
            let count = if operation == "append" {
                RECORDS
            } else {
                READ_RECORDS
            };
            group.throughput(Throughput::Elements(if operation == "reader_creation" {
                1
            } else {
                count as u64
            }));
            let name = format!("{operation}/{label}");
            let mut taken = false;
            let input = serde_json::json!({"records":count,"inherited_clock_width":width,"observations":observations,"distinct_provenance":distinct,"payload_bytes":payload,"workers":2,"sync_on_write":false,"reopened_scan_includes_open":true});
            group.bench_function(&name, |b| measure(b, censuses, &mut taken, &format!("hotspots/{name}"), &input, || {
                let (rows, mut sample) = if operation == "append" {
                    let directory = tempfile::tempdir().unwrap();
                    let path = directory.path().join("write.log");
                    let journal = fixture.journal_at(path.clone());
                    let (rows, mut sample) = timed(|| rt.block_on(async { tokio::time::timeout(fixtures::DEADLINE, fixture.append(&journal)).await }).expect("append deadline"));
                    assert_eq!(serde_json::to_value(rt.block_on(scan(&journal))).unwrap(), serde_json::to_value(&rows).unwrap());
                    sample.observations = serde_json::json!({"archive_bytes":std::fs::metadata(path).unwrap().len()});
                    (rows, sample)
                } else if operation == "reader_creation" {
                    let journal = &archive.0;
                    let (reader, sample) = timed(|| rt.block_on(journal.reader()).unwrap());
                    assert_eq!(reader.position(), 0);
                    let mut sample = sample;
                    sample.completed("created_readers", 1);
                    return sample;
                } else {
                    // Both are warmed-process cases. Reopening is not a cold-cache claim.
                    if operation == "reopened_scan" {
                        let _ = &*reopened;
                        timed(|| rt.block_on(async {
                            tokio::time::timeout(fixtures::DEADLINE, async {
                                let journal = read_fixture.journal("reopened.log");
                                scan(&journal).await
                            }).await.expect("open and read deadline")
                        }))
                    } else {
                        let mut reader = rt.block_on(archive.0.reader()).unwrap();
                        timed(|| rt.block_on(async {
                        tokio::time::timeout(fixtures::DEADLINE, async {
                            let mut rows = Vec::new();
                            while let Some(row) = reader.next().await.unwrap() { rows.push(row); }
                            assert_eq!(reader.position(), count as u64);
                            rows
                        }).await.expect("read deadline")
                        }))
                    }
                };
                if operation == "append" {
                    fixture.check(&rows);
                } else {
                    read_fixture.check(&rows);
                    let expected = if operation == "reopened_scan" { &*reopened } else { &archive.1 };
                    assert_eq!(serde_json::to_value(&rows).unwrap(), serde_json::to_value(expected).unwrap(), "full committed records including precise timestamps must survive reads");
                }
                sample.completed("complete_records", rows.len() as u64);
                sample
            }));
        }
    }
    let fixture = LazyCell::new(|| rt.block_on(Fixture::new(1025, false, false, 256)));
    let mut taken = false;
    group.throughput(Throughput::Elements(RECORDS as u64));
    group.bench_function("append/clock_1025_absent_control", |b| measure(b, censuses, &mut taken,
        "hotspots/append/clock_1025_absent_control", &serde_json::json!({"records":RECORDS,"inherited_clock_width":1025,"advanced_inputs":1024,"observations":false,"payload_bytes":256}), || {
            let fixture = &*fixture;
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("write.log");
            let journal = fixture.journal_at(path.clone());
            let (rows, mut sample) = timed(|| rt.block_on(async {
                tokio::time::timeout(fixtures::DEADLINE, fixture.append(&journal)).await.expect("wide-clock append deadline")
            }));
            fixture.check(&rows);
            assert_eq!(serde_json::to_value(rt.block_on(scan(&journal))).unwrap(), serde_json::to_value(&rows).unwrap());
            sample.observations = serde_json::json!({"archive_bytes":std::fs::metadata(path).unwrap().len()});
            sample.completed("complete_records", RECORDS as u64);
            sample
        }));
    group.finish();
    cross_journal_reads(c, rt, censuses);
    observations(c, rt, censuses);
    tails(c, rt, censuses);
    cache_workloads(c, rt, censuses);
}

fn cross_journal_reads(c: &mut Criterion, rt: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("cross_journal_reads");
    group.throughput(Throughput::Elements(READ_RECORDS as u64));
    let fixture =
        LazyCell::new(|| rt.block_on(Fixture::with_records(33, true, true, 256, READ_RECORDS / 8)));
    let journals = LazyCell::new(|| {
        let fixture = &*fixture;
        rt.block_on(async {
            let mut journals = Vec::new();
            for index in 0..8 {
                // One archive directory shares ordinary committed definitions.
                // Each physical journal has its own writer and absolute sequence.
                let journal = fixture.journal(&format!("stage_{index}.log"));
                let rows = fixture.append(&journal).await;
                fixture.check(&rows);
                journals.push((journal, rows));
            }
            journals
        })
    });
    let mut taken = false;
    group.bench_function("eight_journals_1024_records", |b| measure(b, censuses, &mut taken,
        "cross_journal_reads/eight_journals_1024_records", &serde_json::json!({"journals":8,"records_per_journal":READ_RECORDS/8,"total_records":READ_RECORDS,"clock_width":33,"observations":true,"workers":2,"includes_reader_creation":true}), || {
            let journals = &*journals;
            let (results, mut sample) = timed(|| rt.block_on(async {
                tokio::time::timeout(fixtures::DEADLINE, async {
                    let mut rows = Vec::new();
                    for (journal, _) in journals { rows.push(scan(journal).await); }
                    rows
                }).await.expect("cross-journal read deadline")
            }));
            for (rows, (_, expected)) in results.iter().zip(journals) {
                fixture.check(rows);
                assert_eq!(serde_json::to_value(rows).unwrap(), serde_json::to_value(expected).unwrap());
            }
            assert_eq!(results.iter().map(Vec::len).sum::<usize>(), READ_RECORDS);
            sample.completed("complete_records", READ_RECORDS as u64);
            sample
        }));
    group.finish();
}

fn observations(c: &mut Criterion, rt: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("observation_handling");
    group.throughput(Throughput::Elements(RECORDS as u64));
    let mut capture_taken = false;
    group.bench_function("capture_for_record", |b| {
            measure(b, censuses, &mut capture_taken, "observation_handling/capture_for_record",
                &serde_json::json!({"captures":RECORDS,"includes_live_offer":true,"processing_time_ns":125000}), || {
                    let execution = RuntimeExecution::new(RuntimeMode::Live, None);
                    let instrumentation = Arc::new(StageInstrumentation::new());
                    let flow = obzenflow_core::FlowId::new();
                    let stage = StageId::new();
                    instrumentation.bind_observations(flow, stage.into(), &execution);
                    instrumentation.record_processing_time(std::time::Duration::from_micros(125));
                    let (packets, mut sample) = timed(|| (0..RECORDS)
                        .map(|_| instrumentation.capture_for_record().expect("bound live capture"))
                        .collect::<Vec<_>>());
                    let first = packets[0].capture.capture_seq.0;
                    for (index, packet) in packets.iter().enumerate() {
                        assert_eq!(packet.capture.capture_scope.flow_id, flow);
                        assert_eq!(packet.capture.observer, stage.into());
                        assert_eq!(packet.capture.capture_seq, CaptureSeq(first + index as u64));
                        assert_eq!(packet.capture.capture_reason, CaptureReason::Record);
                        assert!(packet.clone().validated().is_some());
                        let runtime = packet.runtime.as_ref().expect("runtime measurements");
                        let timing = runtime.timing.as_ref().expect("recorded processing time");
                        assert_eq!(timing.processing_time_count, 1);
                        assert_eq!(timing.processing_time_sum_nanos, 125000);
                        assert_eq!(runtime.in_flight, Some(0));
                    }
                    let last = packets.last().unwrap();
                    let retained = execution.observations().snapshot();
                    let expected = serde_json::to_value(&last.runtime).unwrap();
                    for (family, value) in expected.as_object().unwrap() {
                        if value.is_null() || value.as_array().is_some_and(Vec::is_empty) { continue; }
                        assert!(retained.iter().any(|packet| packet.capture == last.capture
                            && serde_json::to_value(&packet.runtime).unwrap()[family] == *value), "capture lost runtime family {family}");
                    }
                    assert!(retained.iter().any(|packet| packet.capture == last.capture
                        && serde_json::to_value(packet.processing_time).unwrap() == serde_json::to_value(last.processing_time).unwrap()));
                    sample.completed("constructed_and_offered_captures", RECORDS as u64);
                    sample
                });
        });
    for width in [1, 33, 128] {
        let fixture = LazyCell::new(|| rt.block_on(Fixture::new(width, true, false, 256)));
        for live in [false, true] {
            let name = format!(
                "{}/clock_{width}",
                if live {
                    "live_submission"
                } else {
                    "validation"
                }
            );
            let mut taken = false;
            group.bench_function(&name, |b| {
                measure(
                    b,
                    censuses,
                    &mut taken,
                    &format!("observation_handling/{name}"),
                    &serde_json::json!({"packets":RECORDS,"clock_width":width}),
                    || {
                        let packets: Vec<_> = fixture
                            .events
                            .iter()
                            .map(|e| e.envelope.observability.clone().unwrap())
                            .collect();
                        let latest = LatestObservationMap::default();
                        latest.activate_scope(packets[0].capture.capture_scope);
                        let (outputs, mut sample) = timed(|| {
                            packets
                                .iter()
                                .map(|packet| {
                                    if live {
                                        assert_eq!(
                                            latest.offer(packet.clone()),
                                            ObservationOffer::Accepted
                                        );
                                        None
                                    } else {
                                        Some(
                                            packet
                                                .clone()
                                                .validated()
                                                .expect("valid packet rejected"),
                                        )
                                    }
                                })
                                .collect::<Vec<_>>()
                        });
                        if live {
                            let actual = latest.snapshot();
                            assert!(!actual.is_empty());
                            for packet in &actual {
                                assert_eq!(packet.capture.capture_seq, CaptureSeq(RECORDS as u64));
                            }
                            let expected = packets.last().unwrap();
                            for packet in &actual {
                                if let Some(metrics) = &packet.metrics {
                                    assert_eq!(metrics.events_processed, RECORDS as u64);
                                }
                                if let Some(snapshot) = &packet.runtime_snapshot {
                                    assert_eq!(
                                        serde_json::to_value(snapshot).unwrap(),
                                        serde_json::to_value(&expected.runtime_snapshot).unwrap()
                                    );
                                }
                            }
                            assert!(actual.iter().any(|p| p.metrics.is_some()));
                            assert!(actual.iter().any(|p| p.runtime_snapshot.is_some()));
                            let expected = serde_json::to_value(expected).unwrap();
                            for family in ["metrics", "sli", "runtime_snapshot", "records"] {
                                assert!(
                                    actual.iter().any(|packet| {
                                        serde_json::to_value(packet).unwrap()[family]
                                            == expected[family]
                                    }),
                                    "missing or altered observation family {family}"
                                );
                            }
                        } else {
                            for (actual, expected) in outputs.iter().zip(&packets) {
                                assert_eq!(
                                    serde_json::to_value(actual).unwrap(),
                                    serde_json::to_value(expected).unwrap()
                                );
                            }
                        }
                        sample.completed("accepted_advancing_packets", RECORDS as u64);
                        sample
                    },
                )
            });
        }
    }
    group.finish();
}

fn tails(c: &mut Criterion, rt: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("journal_refresh");
    for stages in [1, 4] {
        for advancing in [false, true] {
            let mut journals = None;
            let mut seq = RECORDS as u64 + 2;
            let name = format!(
                "metrics/{stages}_journals/{}",
                if advancing { "advancing" } else { "unchanged" }
            );
            let mut taken = false;
            group.throughput(Throughput::Elements(stages));
            group.bench_function(&name, |b| {
                measure(
                    b,
                    censuses,
                    &mut taken,
                    &format!("journal_refresh/{name}"),
                    &serde_json::json!({"journals":stages,"advancing":advancing}),
                    || {
                        let journals = journals.get_or_insert_with(|| {
                            (0..stages)
                                .map(|_| {
                                    let fixture = rt.block_on(Fixture::new(33, true, false, 256));
                                    let journal = fixture.journal("tail.log");
                                    let rows = rt.block_on(fixture.append(&journal));
                                    let mut expected = vec![rows.last().unwrap().clone()];
                                    expected.extend(
                                        rt.block_on(append_partial_metrics(
                                            &fixture, &journal, seq,
                                        )),
                                    );
                                    (fixture, journal, expected)
                                })
                                .collect::<Vec<_>>()
                        });
                        if advancing {
                            seq += 2;
                            for (fixture, journal, expected) in journals.iter_mut() {
                                expected.truncate(1);
                                expected.extend(
                                    rt.block_on(append_partial_metrics(fixture, journal, seq)),
                                );
                            }
                        }
                        let (snapshots, mut sample) = timed(|| {
                            rt.block_on(async {
                                tokio::time::timeout(fixtures::DEADLINE, async {
                                    let mut snapshots = Vec::new();
                                    for (_, journal, _) in journals.iter() {
                                        snapshots.push(journal.read_metrics_tail().await.unwrap());
                                    }
                                    snapshots
                                })
                                .await
                                .expect("metrics refresh deadline")
                            })
                        });
                        for (rows, (_, _, expected)) in snapshots.iter().zip(journals.iter()) {
                            assert_eq!(
                                rows.len(),
                                expected.len(),
                                "each independently stamped family needs its committed carrier"
                            );
                            for expected in expected {
                                let actual = rows
                                    .iter()
                                    .find(|row| row.id() == expected.id())
                                    .expect("missing metrics carrier");
                                assert_eq!(
                                    serde_json::to_value(actual).unwrap(),
                                    serde_json::to_value(expected).unwrap()
                                );
                            }
                        }
                        sample.completed("refreshed_journals", stages);
                        sample
                    },
                )
            });
        }
    }
    let fixture = LazyCell::new(|| rt.block_on(Fixture::new(33, true, false, 256)));
    let mut taken = false;
    group.throughput(Throughput::Elements(RECORDS as u64));
    group.bench_function("append_after_eof", |b| {
        measure(
            b,
            censuses,
            &mut taken,
            "journal_refresh/append_after_eof",
            &serde_json::json!({"records":RECORDS}),
            || {
                let directory = tempfile::tempdir().unwrap();
                let journal = fixture.journal_at(directory.path().join("live.log"));
                let mut reader = rt.block_on(journal.reader()).unwrap();
                assert!(rt.block_on(reader.next()).unwrap().is_none());
                let (rows, mut sample) = timed(|| {
                    rt.block_on(async {
                        tokio::time::timeout(fixtures::DEADLINE, async {
                            let mut rows = Vec::new();
                            for event in &fixture.events {
                                journal
                                    .append(
                                        event.clone(),
                                        AppendOptions::new(
                                            CausalFrontier::from_record(&fixture.seed.record)
                                                .unwrap(),
                                        ),
                                    )
                                    .await
                                    .unwrap();
                                rows.push(
                                    reader.next().await.unwrap().expect("new commit after EOF"),
                                );
                                assert!(reader.next().await.unwrap().is_none());
                            }
                            rows
                        })
                        .await
                        .expect("live-tail completion deadline")
                    })
                });
                fixture.check(&rows);
                assert_eq!(reader.position(), RECORDS as u64);
                sample.completed("completed_tail_records", RECORDS as u64);
                sample
            },
        )
    });
    group.finish();
}

async fn append_partial_metrics(
    fixture: &Fixture,
    journal: &Arc<dyn Journal<ChainEvent>>,
    seq: u64,
) -> Vec<ChainJournalRecord> {
    let mut metrics = Fixture::packet(&fixture.seed, fixture.stage, seq - 1);
    metrics.runtime_snapshot = None;
    metrics.sli = None;
    metrics.records.clear();
    let mut runtime = Fixture::packet(&fixture.seed, fixture.stage, seq);
    runtime.metrics = None;
    runtime.sli = None;
    runtime.records.clear();
    // A forwarded packet's surrounding capture must not replace the runtime
    // snapshot's independent stamp when selecting the latest family.
    runtime.capture.capture_seq = CaptureSeq(1);
    let mut rows = Vec::new();
    for packet in [metrics, runtime] {
        rows.push(
            journal
                .append(
                    fixtures::business(fixture.stage, 256).with_observability_context(packet),
                    Default::default(),
                )
                .await
                .unwrap(),
        );
    }
    rows
}

fn cache_workloads(c: &mut Criterion, rt: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("mixed_journal");
    group.sample_size(10);
    let mut taken = false;
    group.throughput(Throughput::Elements(192));
    group.bench_function("distinct_12mib_then_reuse", |b| measure(b, censuses, &mut taken,
        "mixed_journal/distinct_12mib_then_reuse", &serde_json::json!({"records":192,"distinct_provenance_bytes":12*1024*1024,"rounds":2}), || {
            let mut fixture = rt.block_on(Fixture::new(33, true, false, 256));
            // Capacity stress is a labelled diagnostic dimension, not the ordinary workload.
            fixture.events = (0..192).map(|i| fixtures::business(fixture.stage, 256)
                .with_flow_context(FlowContext::new(format!("{}_{:03}", "p".repeat(128*1024), i%96), fixture.stage))
                .with_observability_context(Fixture::packet(&fixture.seed, fixture.stage, i+1))).collect();
            let journal = fixture.journal("mixed.log");
            let mut reader = rt.block_on(journal.reader()).unwrap();
            let heap_before = obzenflow_benchmarks::support::allocations::live_requested_bytes();
            let (rows, mut sample) = timed(|| rt.block_on(async {
                tokio::time::timeout(fixtures::DEADLINE, async {
                    let mut rows = Vec::new();
                    for event in &fixture.events {
                        journal.append(event.clone(), AppendOptions::new(CausalFrontier::from_record(&fixture.seed.record).unwrap())).await.unwrap();
                        rows.push(reader.next().await.unwrap().expect("acknowledged mixed append"));
                    }
                    rows
                }).await.expect("mixed append/read deadline")
            }));
            fixture.check(&rows);
            assert_eq!(reader.position(), 192);
            sample.completed("appended_and_read_records", 192);
            drop(rows);
            drop(reader);
            sample.observations = serde_json::json!({"archive_bytes":std::fs::metadata(fixture.seed._directory.path().join("mixed.log")).unwrap().len(),"retained_heap_delta_after_dropping_read_results":obzenflow_benchmarks::support::allocations::live_requested_bytes() as i128 - heap_before as i128});
            sample
        }));
    let fixture = LazyCell::new(|| rt.block_on(Fixture::new(33, true, false, 256)));
    let journal = LazyCell::new(|| {
        let journal = fixture.journal("concurrent.log");
        fixture.check(&rt.block_on(fixture.append(&journal)));
        journal
    });
    let mut taken = false;
    group.throughput(Throughput::Elements((2 * RECORDS) as u64));
    group.bench_function("two_concurrent_scans", |b| {
        measure(
            b,
            censuses,
            &mut taken,
            "mixed_journal/two_concurrent_scans",
            &serde_json::json!({"records_per_reader":RECORDS,"readers":2,"workers":2}),
            || {
                let journal = &*journal;
                let ((a, b), mut sample) = timed(|| {
                    rt.block_on(async {
                        tokio::time::timeout(fixtures::DEADLINE, async {
                            tokio::join!(scan(journal), scan(journal))
                        })
                        .await
                        .expect("concurrent reader deadline")
                    })
                });
                fixture.check(&a);
                fixture.check(&b);
                sample.completed("complete_records", (2 * RECORDS) as u64);
                sample
            },
        )
    });
    group.finish();
}
