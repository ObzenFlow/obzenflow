// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{fixtures, measure, timed, Census, Sample};
use criterion::{Criterion, Throughput};
use obzenflow_core::{ChainEvent, Journal, JournalOwner};
use obzenflow_infra::journal::DiskJournal;
use obzenflow_infra::testing::journal_bench::{write_preencoded, EncodingCursor, FrameCorpus};
use std::cell::LazyCell;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

fn run(runtime: &Runtime, history: &fixtures::History, operation: &str) -> Sample {
    let encoded = history.corpus.encoded_frames();
    let (bytes, mut sample) = match operation {
        "prepare_encoding" => {
            let mut cursor = EncodingCursor::new(history.path.clone());
            let (frames, sample) = timed(|| {
                history
                    .rows
                    .chunks(history.group)
                    .enumerate()
                    .map(|(index, rows)| {
                        let group = (history.group != 1).then(|| format!("group-{index}"));
                        cursor.encode(rows, group.as_deref())
                    })
                    .collect::<Vec<_>>()
            });
            for (actual, expected) in frames.iter().zip(&encoded) {
                assert_eq!(actual, expected.as_ref());
            }
            (frames.iter().map(Vec::len).sum::<usize>(), sample)
        }
        "write_preencoded" => {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("encoded.log");
            let file = Arc::new(Mutex::new(std::fs::File::create(&path).unwrap()));
            let (position, sample) = timed(|| {
                runtime.block_on(async {
                    let mut position = 0;
                    for bytes in &encoded {
                        position =
                            write_preencoded(file.clone(), path.clone(), bytes.clone()).await;
                    }
                    position
                })
            });
            let actual = std::fs::read(&path).unwrap();
            assert_eq!(
                actual,
                encoded
                    .iter()
                    .flat_map(|b| b.iter().copied())
                    .collect::<Vec<_>>()
            );
            sample.expect_work("append_blocking_jobs", encoded.len() as u64);
            (position as usize, sample)
        }
        "complete_append" => {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("append.log");
            let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(
                DiskJournal::with_owner_in_run(
                    path.clone(),
                    JournalOwner::stage(history.stage),
                    history.run,
                )
                .unwrap(),
            );
            let events = history.events.clone();
            let (rows, sample) = timed(|| {
                runtime.block_on(fixtures::append_events(&journal, events, history.group))
            });
            assert_eq!(
                rows.iter().map(|r| *r.id()).collect::<Vec<_>>(),
                history.rows.iter().map(|r| *r.id()).collect::<Vec<_>>()
            );
            let corpus =
                FrameCorpus::load(&path, &rows.iter().map(|r| *r.id()).collect::<Vec<_>>())
                    .unwrap();
            let mut encoder = EncodingCursor::new(path.clone());
            let expected: Vec<_> = rows
                .chunks(history.group)
                .enumerate()
                .flat_map(|(index, rows)| {
                    let group = (history.group != 1).then(|| format!("group-{index}"));
                    encoder.encode(rows, group.as_deref())
                })
                .collect();
            assert_eq!(std::fs::read(&path).unwrap(), expected);
            sample.expect_work("append_blocking_jobs", encoded.len() as u64);
            (corpus.encoded_bytes(), sample)
        }
        _ => unreachable!(),
    };
    // Complete appends have fresh commitment identities/timestamps. Verify their
    // exact bytes against their own receipts above, not another journal's size.
    if operation != "complete_append" {
        assert_eq!(bytes, history.corpus.encoded_bytes());
    }
    sample.observations =
        serde_json::json!({"encoded_bytes":bytes,"physical_frames":encoded.len()});
    sample
}

pub fn bench(c: &mut Criterion, runtime: &Runtime, censuses: &mut Vec<Census>) {
    let mut group = c.benchmark_group("journal_append_cost");
    group.throughput(Throughput::Elements(64));
    for (name, payload, group_size, every) in [
        ("ordinary_business_64", 256, 1, 0),
        ("large_business_64", 8192, 1, 0),
        ("mixed_group_64", 256, 64, 8),
        ("report_group_64", 256, 64, 1),
    ] {
        let history = LazyCell::new(|| {
            runtime.block_on(fixtures::History::build(64, payload, group_size, every))
        });
        for op in ["prepare_encoding", "write_preencoded", "complete_append"] {
            let case = format!("{op}/{name}");
            let mut taken = false;
            let input = serde_json::json!({"records":64,"business_payload_bytes":payload,"group_size":group_size,"report_every":every});
            group.bench_function(&case, |b| {
                measure(
                    b,
                    censuses,
                    &mut taken,
                    &format!("journal_append_cost/{case}"),
                    &input,
                    || run(runtime, &history, op),
                )
            });
        }
    }
    group.finish();
}
