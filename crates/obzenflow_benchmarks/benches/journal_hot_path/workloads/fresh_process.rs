// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-use archive reconstruction through ordinary journal APIs. Every operation
//! executes this same benchmark binary anew; process launch is outside timing.
use super::{fixtures, Fixture, READ_RECORDS};
use crate::{control, measure, timed, Census, Sample};
use criterion::{measurement::WallTime, BenchmarkGroup, Throughput};
use obzenflow_core::{ChainEvent, FlowId, Journal, JournalOwner, StageId};
use obzenflow_infra::journal::DiskJournal;
use serde::{Deserialize, Serialize};
use std::{cell::LazyCell, path::PathBuf, process::Stdio};
use tokio::{process::Command, runtime::Runtime};

const CHILD_ARG: &str = "--first-process-open-and-scan-child";
const CASE: &str = "first_process_open_and_scan/wide_observed";

#[derive(Serialize, Deserialize)]
struct Request {
    journal: PathBuf,
    stage: StageId,
    run: FlowId,
    expected: PathBuf,
}

#[derive(Serialize, Deserialize)]
struct Response {
    pid: u32,
    sample: Sample,
}

pub(crate) fn run_child() -> bool {
    let mut args = std::env::args_os().skip(1);
    if args.next().as_deref() != Some(std::ffi::OsStr::new(CHILD_ARG)) {
        return false;
    }
    let request_path = args.next().expect("child request path");
    let response_path = args.next().expect("child response path");
    assert!(args.next().is_none(), "unexpected child argument");
    let request: Request = serde_json::from_slice(&std::fs::read(request_path).unwrap()).unwrap();
    let request = std::hint::black_box(request);
    let runtime = fixtures::runtime();
    // No journal API, archive opening or expected-record loading precedes timing.
    let ((journal, reader, mut rows), mut sample) = timed(|| {
        runtime.block_on(async {
            let journal = DiskJournal::<ChainEvent>::with_owner_in_run(
                request.journal,
                JournalOwner::stage(request.stage),
                request.run,
            )
            .unwrap();
            let mut reader = journal.reader().await.unwrap();
            let mut rows = Vec::new();
            while let Some(row) = reader.next().await.unwrap() {
                rows.push(row);
            }
            (journal, reader, rows)
        })
    });
    // The parent owns the hard deadline, including synchronous opening and this
    // oracle. Retain reader/journal until after timing so the interval ends at EOF.
    if control::selected() == control::Control::MissingReaderOutput {
        rows.pop();
    }
    assert_eq!(
        rows.len(),
        READ_RECORDS,
        "first-process complete record count"
    );
    assert_eq!(reader.position(), READ_RECORDS as u64, "final cursor");
    let expected: Vec<serde_json::Value> =
        serde_json::from_slice(&std::fs::read(&request.expected).unwrap()).unwrap();
    assert_eq!(expected.len(), READ_RECORDS, "independent append receipts");
    for (index, (row, expected)) in rows.iter().zip(expected).enumerate() {
        assert_eq!(
            serde_json::to_value(row).unwrap(),
            expected,
            "first-process full record at index {index}"
        );
    }
    sample.completed("complete_records", rows.len() as u64);
    sample.observations = serde_json::json!({"final_cursor": reader.position()});
    drop((reader, journal, rows));
    std::fs::write(
        response_path,
        serde_json::to_vec(&Response {
            pid: std::process::id(),
            sample,
        })
        .unwrap(),
    )
    .unwrap();
    true
}

pub(super) fn bench(
    group: &mut BenchmarkGroup<'_, WallTime>,
    rt: &Runtime,
    censuses: &mut Vec<Census>,
) {
    let prepared = LazyCell::new(|| {
        let fixture = rt.block_on(Fixture::with_records(33, true, false, 256, READ_RECORDS));
        let journal = fixture.journal("first-process.log");
        let receipts = rt.block_on(fixture.append(&journal));
        fixture.check(&receipts);
        drop(journal);
        let directory = fixture.seed._directory.path();
        let expected = directory.join("expected.json");
        std::fs::write(&expected, serde_json::to_vec(&receipts).unwrap()).unwrap();
        let request_path = directory.join("request.json");
        std::fs::write(
            &request_path,
            serde_json::to_vec(&Request {
                journal: directory.join("first-process.log"),
                stage: fixture.stage,
                run: fixture.seed.record.envelope.provenance.journal.run_id,
                expected,
            })
            .unwrap(),
        )
        .unwrap();
        // Keep the complete archive, including referenced seed journals, alive.
        (fixture, request_path, std::env::current_exe().unwrap())
    });
    let input = serde_json::json!({
        "records": READ_RECORDS, "inherited_clock_width": 33,
        "observations": true, "distinct_provenance": false, "payload_bytes": 256,
        "workers": 2, "blocking_workers": 2, "sync_on_write": false,
        "process_state": "fresh_per_operation", "filesystem_cache": "uncontrolled",
        "includes_journal_open": true, "includes_process_launch": false,
        "includes_runtime_construction": false, "includes_output_check": false
    });
    let mut taken = false;
    group.throughput(Throughput::Elements(READ_RECORDS as u64));
    group.bench_function(CASE, |b| {
        measure(
            b,
            censuses,
            &mut taken,
            &format!("hotspots/{CASE}"),
            &input,
            || {
                let (fixture, request, executable) = &*prepared;
                let response =
                    tempfile::NamedTempFile::new_in(fixture.seed._directory.path()).unwrap();
                let pid = rt.block_on(async {
                    let mut child = Command::new(executable)
                        .arg(CHILD_ARG)
                        .arg(request)
                        .arg(response.path())
                        .env_remove("OBZENFLOW_WORK_CENSUS")
                        .stdin(Stdio::null())
                        .kill_on_drop(true)
                        .spawn()
                        .expect("launch first-process benchmark child");
                    let pid = child.id().unwrap();
                    assert_ne!(pid, std::process::id());
                    let status = match tokio::time::timeout(fixtures::DEADLINE, child.wait()).await
                    {
                        Ok(status) => status.expect("wait for first-process benchmark child"),
                        Err(_) => {
                            child.kill().await.expect("kill and reap timed-out child");
                            panic!("first-process benchmark exceeded {:?}", fixtures::DEADLINE);
                        }
                    };
                    assert!(
                        status.success(),
                        "first-process benchmark child failed: {status}"
                    );
                    pid
                });
                let response: Response =
                    serde_json::from_slice(&std::fs::read(response.path()).unwrap()).unwrap();
                assert_eq!(response.pid, pid, "result must come from the owned child");
                assert!(!response.sample.elapsed.is_zero());
                response.sample
            },
        );
    });
}
