// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private reconstruction and sparse-observation correctness controls.

use super::*;
use obzenflow_core::event::ChainEvent;
use obzenflow_core::journal::JournalConfig;
use obzenflow_core::{Journal, JournalOwner, StageId};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};

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

#[tokio::test]
async fn representative_stream_preserves_every_record_on_cold_warm_addressed_and_reopened_reads() {
    use crate::journal::disk::{scanner::read_frame_sync, DiskJournal};
    let samples = super::test_data::records();
    let identity = super::super::identity::JournalIdentity {
        run_id: obzenflow_core::FlowId::new(),
        journal_id: obzenflow_core::JournalId::new(),
    };
    let mut previous = None;
    let records: Vec<_> = samples
        .iter()
        .cycle()
        .take(768)
        .map(|sample| {
            super::super::identity::fixture_record(identity, sample.authored(), &mut previous)
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("representative.log");
    super::super::identity::write_fixture_identity(&path, identity);
    let mut file = std::fs::File::create(&path).unwrap();
    let store = DefinitionStore::default();
    let mut offsets = Vec::new();
    for record in &records {
        let prepared = prepare(std::slice::from_ref(record), None, &path, store.clone()).unwrap();
        let offset = file.seek(SeekFrom::End(0)).unwrap();
        file.write_all(&prepared.bytes).unwrap();
        file.flush().unwrap();
        prepared.commit(offset);
        offsets.push(offset);
    }
    drop(file);
    let mut decoder = Decoder::cold(&path);
    // The same decoder first resolves cold definitions, then reuses its cache.
    for _ in 0..2 {
        let mut reader = BufReader::new(std::fs::File::open(&path).unwrap());
        let mut encoded = Vec::new();
        for (&offset, expected) in offsets.iter().zip(&records) {
            read_frame_sync(&mut reader, &mut encoded).unwrap().unwrap();
            let body = frame::validate(&encoded).unwrap();
            let restored = decoder
                .decode::<ChainEvent>(body, offset)
                .unwrap()
                .into_records();
            assert_eq!(restored.len(), 1);
            assert_eq!(
                serde_json::to_value(&restored[0]).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
        assert!(read_frame_sync(&mut reader, &mut encoded)
            .unwrap()
            .is_none());
    }
    let last = *offsets.last().unwrap();
    let mut file = std::fs::File::open(&path).unwrap();
    file.seek(SeekFrom::Start(last)).unwrap();
    let mut encoded = Vec::new();
    file.read_to_end(&mut encoded).unwrap();
    let restored = Decoder::cold(&path)
        .decode::<ChainEvent>(frame::validate(&encoded).unwrap(), last)
        .unwrap()
        .into_records();
    assert_eq!(
        serde_json::to_value(&restored[0]).unwrap(),
        serde_json::to_value(records.last().unwrap()).unwrap()
    );
    let journal =
        DiskJournal::<ChainEvent>::with_owner(path, JournalOwner::stage(StageId::new())).unwrap();
    let tail = journal.read_last_n(16).await.unwrap();
    assert_eq!(tail.len(), 16);
    // The public tail contract returns newest first.
    for (actual, expected) in tail.iter().zip(records.iter().rev()) {
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }
}
