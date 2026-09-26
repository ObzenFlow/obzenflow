// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Matched current-record control in the provider suite, with identical framing
//! and write/flush calls. Timing output is evidence, never a flaky CI assertion.
//! Run this test in release mode and alone when collecting performance evidence.

use super::*;
use obzenflow_core::event::{ChainEvent, ChainPayload};
use obzenflow_core::journal::JournalConfig;
use obzenflow_core::{Journal, JournalOwner, StageId};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::time::Instant;

#[tokio::test(start_paused = true)]
async fn periodic_capture_reduces_representative_storage_without_changing_protected_records() {
    use crate::journal::DiskJournal;
    use obzenflow_core::journal::ObservabilityPolicy;
    use std::collections::HashMap;
    use std::time::Duration;
    let directory = tempfile::tempdir().unwrap();
    let samples = super::test_data::records();
    let mut dense = HashMap::new();
    let mut sparse = HashMap::new();
    for record in &samples {
        let stage = *record
            .envelope
            .provenance
            .event
            .writer_id
            .as_stage()
            .unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) = dense.entry(stage) {
            entry.insert(
                DiskJournal::<ChainEvent>::with_owner(
                    directory.path().join(format!("dense-{stage}.log")),
                    JournalOwner::stage(stage),
                )
                .unwrap(),
            );
            let journal = DiskJournal::<ChainEvent>::with_owner(
                directory.path().join(format!("sparse-{stage}.log")),
                JournalOwner::stage(stage),
            )
            .unwrap();
            journal
                .configure(JournalConfig {
                    observability: ObservabilityPolicy::Periodic {
                        interval: Duration::from_millis(250),
                    },
                })
                .unwrap();
            sparse.insert(stage, journal);
        }
    }
    let mut dense_packets = 0;
    let mut sparse_packets = 0;
    for (index, sample) in samples.iter().cycle().take(1000).enumerate() {
        let mut event = sample.authored();
        event.id = obzenflow_core::EventId::new();
        if let Some(packet) = event.envelope.observability.as_mut() {
            packet.capture.capture_seq =
                obzenflow_core::event::observability::CaptureSeq(index as u64 + 1);
            if let Some(snapshot) = packet.runtime_snapshot.as_mut() {
                snapshot.capture.capture_seq = packet.capture.capture_seq;
            }
        }
        let stage = *event.writer_id.as_stage().unwrap();
        let control = dense[&stage]
            .append(event.clone(), Default::default())
            .await
            .unwrap();
        let treatment = sparse[&stage]
            .append(event, Default::default())
            .await
            .unwrap();
        dense_packets += usize::from(control.envelope.observability.is_some());
        sparse_packets += usize::from(treatment.envelope.observability.is_some());
        assert_eq!(
            serde_json::to_value(&control.payload).unwrap(),
            serde_json::to_value(&treatment.payload).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&control.envelope.provenance.event).unwrap(),
            serde_json::to_value(&treatment.envelope.provenance.event).unwrap()
        );
        tokio::time::advance(Duration::from_millis(1)).await;
    }
    let bytes = |prefix: &str| {
        dense
            .keys()
            .map(|stage| {
                std::fs::metadata(directory.path().join(format!("{prefix}-{stage}.log")))
                    .unwrap()
                    .len()
            })
            .sum::<u64>()
    };
    let dense_bytes = bytes("dense");
    let sparse_bytes = bytes("sparse");
    assert_eq!(dense_packets, 1000);
    assert!(sparse_packets <= 12, "four packets per second per journal");
    assert!(sparse_bytes < dense_bytes);
    println!("FLOWIP-145b representative storage: records=1000, every_record_packets={dense_packets}, periodic_packets={sparse_packets}, every_record_bytes={dense_bytes}, periodic_bytes={sparse_bytes}");
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Control<P> {
    record: P,
}

#[derive(Debug, serde::Serialize)]
struct Timings {
    compact: bool,
    records: usize,
    bytes: u64,
    encode_append_ns_per_record: f64,
    append_p95_ns: u128,
    append_p99_ns: u128,
    cold_scan_ns_per_record: f64,
    warm_scan_ns_per_record: f64,
    cold_addressed_ns: u128,
    cold_addressed_bytes: u64,
    cold_addressed_record_bytes: usize,
    cold_addressed_carrier_frames: u64,
    cold_addressed_carrier_bytes: u64,
    retained_cache_peak_bytes: usize,
    reopen_ns: u128,
    reverse_tail_ns: u128,
}

#[tokio::test]
async fn matched_representative_stream_append_read_and_reopen_costs() {
    use crate::journal::disk::{scanner::read_frame_sync, DiskJournal};
    let samples = super::test_data::records();
    let identity = super::super::identity::JournalIdentity {
        run_id: obzenflow_core::FlowId::new(),
        journal_id: obzenflow_core::JournalId::new(),
    };
    let mut previous = None;
    let committed: Vec<_> = samples
        .iter()
        .cycle()
        .take(768)
        .map(|sample| {
            super::super::identity::fixture_record(identity, sample.authored(), &mut previous)
        })
        .collect();
    let records: Vec<_> = committed.iter().collect();
    let dir = tempfile::tempdir().unwrap();
    for repetition in 0..3 {
        // Alternate execution order to avoid consistently favouring one codec.
        for compact in if repetition % 2 == 0 {
            [false, true]
        } else {
            [true, false]
        } {
            let path = dir.path().join(format!("trial-{repetition}-{compact}.log"));
            super::super::identity::write_fixture_identity(&path, identity);
            let mut file = std::fs::File::create(&path).unwrap();
            let store = DefinitionStore::default();
            let mut latency = Vec::new();
            let mut offsets = Vec::new();
            let started = Instant::now();
            for record in &records {
                let start = Instant::now();
                let mut prepared = compact.then(|| {
                    prepare(std::slice::from_ref(*record), None, &path, store.clone()).unwrap()
                });
                let encoded = match &mut prepared {
                    Some(prepared) => std::mem::take(&mut prepared.bytes),
                    None => frame::encode(&serde_json::to_vec(&Control { record }).unwrap()),
                };
                let offset = file.seek(SeekFrom::End(0)).unwrap();
                file.write_all(&encoded).unwrap();
                file.flush().unwrap();
                if let Some(prepared) = prepared {
                    prepared.commit(offset);
                }
                latency.push(start.elapsed().as_nanos());
                offsets.push(offset);
            }
            let elapsed = started.elapsed().as_nanos() as f64;
            let bytes = file.metadata().unwrap().len();
            drop(file);
            latency.sort_unstable();
            let mut decoder = Decoder::cold(&path);
            let mut scans = [0.0; 2];
            for scan in &mut scans {
                let mut reader = BufReader::new(std::fs::File::open(&path).unwrap());
                let mut encoded = Vec::new();
                let start = Instant::now();
                for &offset in &offsets {
                    read_frame_sync(&mut reader, &mut encoded).unwrap().unwrap();
                    let body = frame::validate(&encoded).unwrap();
                    let record = if compact {
                        decoder
                            .decode::<ChainEvent>(body, offset)
                            .unwrap()
                            .into_records()
                            .remove(0)
                    } else {
                        serde_json::from_slice::<Control<JournalRecord<ChainPayload>>>(body)
                            .unwrap()
                            .record
                    };
                    std::hint::black_box(record);
                }
                *scan = start.elapsed().as_nanos() as f64 / records.len() as f64;
            }
            let last = *offsets.last().unwrap();
            let mut file = std::fs::File::open(&path).unwrap();
            let start = Instant::now();
            file.seek(SeekFrom::Start(last)).unwrap();
            let mut encoded = Vec::new();
            file.read_to_end(&mut encoded).unwrap();
            let body = frame::validate(&encoded).unwrap();
            let mut addressed = Decoder::cold(&path);
            let restored = if compact {
                addressed
                    .decode::<ChainEvent>(body, last)
                    .unwrap()
                    .into_records()
                    .remove(0)
            } else {
                serde_json::from_slice::<Control<JournalRecord<ChainPayload>>>(body)
                    .unwrap()
                    .record
            };
            let cold_addressed_ns = start.elapsed().as_nanos();
            assert_eq!(
                serde_json::to_vec(&serde_json::to_value(restored).unwrap()).unwrap(),
                serde_json::to_vec(&serde_json::to_value(records.last().unwrap()).unwrap())
                    .unwrap()
            );
            let (reopen_ns, reverse_tail_ns) = if compact {
                let start = Instant::now();
                let journal = DiskJournal::<ChainEvent>::with_owner(
                    path.clone(),
                    JournalOwner::stage(StageId::new()),
                )
                .unwrap();
                let reopen = start.elapsed().as_nanos();
                let start = Instant::now();
                assert_eq!(journal.read_last_n(16).await.unwrap().len(), 16);
                (reopen, start.elapsed().as_nanos())
            } else {
                (0, 0)
            };
            let timing = Timings {
                compact,
                records: records.len(),
                bytes,
                encode_append_ns_per_record: elapsed / records.len() as f64,
                append_p95_ns: latency[(latency.len() - 1) * 95 / 100],
                append_p99_ns: latency[(latency.len() - 1) * 99 / 100],
                cold_scan_ns_per_record: scans[0],
                warm_scan_ns_per_record: scans[1],
                cold_addressed_ns,
                cold_addressed_bytes: encoded.len() as u64 + addressed.cache_stats().carrier_bytes,
                cold_addressed_record_bytes: encoded.len(),
                cold_addressed_carrier_frames: addressed.cache_stats().carrier_frames,
                cold_addressed_carrier_bytes: addressed.cache_stats().carrier_bytes,
                retained_cache_peak_bytes: decoder.cache_stats().peak_retained_bytes,
                reopen_ns,
                reverse_tail_ns,
            };
            println!(
                "FLOWIP-145c timing trial {repetition}: {}",
                serde_json::to_string(&timing).unwrap()
            );
        }
    }
}
