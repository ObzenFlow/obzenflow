// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-140h: tail reads must preserve committed records across I/O boundaries.

use super::*;
use obzenflow_core::event::{ChainEvent, ChainEventFactory};
use obzenflow_core::StageId;

struct Fixture {
    _dir: tempfile::TempDir,
    journal: DiskJournal<ChainEvent>,
    path: PathBuf,
    writer: WriterId,
    stage: StageId,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/test-logs");
        std::fs::create_dir_all(&root).unwrap();
        let dir = tempfile::Builder::new()
            .prefix("journal-tail-")
            .tempdir_in(root)
            .unwrap();
        let path = dir.path().join("events.log");
        let stage = StageId::new();
        let journal = DiskJournal::with_owner(path.clone(), JournalOwner::stage(stage)).unwrap();
        Self {
            _dir: dir,
            journal,
            path,
            writer: WriterId::from(stage),
            stage,
        }
    }

    fn event(&self, index: usize, padding: &str) -> ChainEvent {
        ChainEventFactory::data_event(
            self.writer,
            "tail.test",
            serde_json::json!({ "index": index, "padding": padding }),
        )
        .with_flow_context(obzenflow_core::event::context::FlowContext::new(
            "tail.test",
            self.stage,
        ))
    }
}

async fn assert_tail(
    journal: &DiskJournal<ChainEvent>,
    expected: &[EventEnvelope<ChainEvent>],
    count: usize,
) {
    let actual = journal.read_last_n(count).await.unwrap();
    let expected: Vec<_> = expected.iter().rev().take(count).collect();
    assert_eq!(actual.len(), expected.len(), "requested {count} records");
    for (actual, expected) in actual.iter().zip(expected) {
        assert_eq!(
            actual.event.id, expected.event.id,
            "newest-first event identity"
        );
        assert_eq!(
            serde_json::to_value(&actual.event).unwrap(),
            serde_json::to_value(&expected.event).unwrap(),
        );
        assert_eq!(actual.journal_group_id, expected.journal_group_id);
        assert_eq!(
            actual
                .journal_group_member
                .as_ref()
                .map(|m| (m.index, m.size)),
            expected
                .journal_group_member
                .as_ref()
                .map(|m| (m.index, m.size)),
        );
    }
}

#[tokio::test]
async fn metrics_tail_read_keeps_the_latest_snapshot_across_chunks() {
    use obzenflow_runtime::metrics::instrumentation::StageInstrumentation;
    use obzenflow_runtime::metrics::tail_read::read_latest_runtime_context_for_stage;

    let f = Fixture::new();
    let mut snapshot = StageInstrumentation::new().snapshot();
    snapshot.events_processed_total = 1;
    f.journal
        .append(
            f.event(0, "old").with_runtime_context(snapshot.clone()),
            None,
        )
        .await
        .unwrap();
    snapshot.events_processed_total = 100_000;
    snapshot.errors_total = 1_000;
    f.journal
        .append(
            f.event(1, &"é🙂".repeat(30_000))
                .with_runtime_context(snapshot),
            None,
        )
        .await
        .unwrap();
    // Force the graduated metrics search beyond its first, non-metric row.
    f.journal
        .append(f.event(2, "no snapshot"), None)
        .await
        .unwrap();
    let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(f.journal);
    let latest = read_latest_runtime_context_for_stage(&journal, f.stage)
        .await
        .unwrap();
    assert_eq!(latest.events_processed_total, 100_000);
    assert_eq!(latest.errors_total, 1_000);
}

#[tokio::test]
async fn read_last_n_preserves_records_across_chunk_boundaries() {
    let f = Fixture::new();
    assert_tail(&f.journal, &[], 10).await;
    let mut written = Vec::new();
    for index in 0..80 {
        written.push(
            f.journal
                .append(f.event(index, &"é🙂:0:1372:".repeat(180)), None)
                .await
                .unwrap(),
        );
    }
    assert!(std::fs::metadata(&f.path).unwrap().len() > 3 * 65_536);
    for count in [0, 1, 23, 70, 100] {
        assert_tail(&f.journal, &written, count).await;
    }
}

#[tokio::test]
async fn read_last_n_preserves_records_larger_than_multiple_chunks() {
    let f = Fixture::new();
    let mut written = Vec::new();
    for index in 0..3 {
        written.push(
            f.journal
                .append(f.event(index, &"界🙂".repeat(30_000)), None)
                .await
                .unwrap(),
        );
    }
    for count in [1, 2, 10] {
        assert_tail(&f.journal, &written, count).await;
    }
}

#[tokio::test]
async fn read_last_n_preserves_large_atomic_groups_and_member_positions() {
    let f = Fixture::new();
    let mut written = vec![f.journal.append(f.event(0, "older"), None).await.unwrap()];
    written.extend(
        f.journal
            .append_group(
                "tail-regression",
                (1..=80).map(|i| f.event(i, &"界🙂".repeat(500))).collect(),
                None,
            )
            .await
            .unwrap(),
    );
    written.push(f.journal.append(f.event(81, "newest"), None).await.unwrap());
    for count in [1, 2, 40, 81, 100] {
        assert_tail(&f.journal, &written, count).await;
    }
}

#[tokio::test]
async fn read_last_n_excludes_unterminated_single_and_group_tails() {
    for grouped in [false, true] {
        let f = Fixture::new();
        let committed = f
            .journal
            .append(f.event(0, "committed"), None)
            .await
            .unwrap();
        let committed_len = std::fs::metadata(&f.path).unwrap().len() as usize;
        if grouped {
            f.journal
                .append_group(
                    "uncommitted",
                    vec![f.event(1, "tail"), f.event(2, "tail")],
                    None,
                )
                .await
                .unwrap();
        } else {
            f.journal.append(f.event(1, "tail"), None).await.unwrap();
        }
        let bytes = std::fs::read(&f.path).unwrap();
        // Emulate a crash at the header, body, or final commit-marker boundary
        // without reopening the journal (open would recover the torn tail).
        for cut in [committed_len + 1, committed_len + 30, bytes.len() - 1] {
            std::fs::write(&f.path, &bytes[..cut]).unwrap();
            assert_tail(&f.journal, std::slice::from_ref(&committed), 10).await;
        }
    }
}

#[tokio::test]
async fn read_last_n_retains_best_effort_behavior_for_real_corruption() {
    let f = Fixture::new();
    let first = f.journal.append(f.event(0, "older"), None).await.unwrap();
    let middle_offset = std::fs::metadata(&f.path).unwrap().len() as usize;
    f.journal.append(f.event(1, "corrupt"), None).await.unwrap();
    let last = f.journal.append(f.event(2, "newer"), None).await.unwrap();
    let mut bytes = std::fs::read(&f.path).unwrap();
    let crc_start = middle_offset
        + bytes[middle_offset..]
            .iter()
            .position(|b| *b == b':')
            .unwrap()
        + 1;
    bytes[crc_start] = if bytes[crc_start] == b'1' { b'2' } else { b'1' };
    std::fs::write(&f.path, bytes).unwrap();
    assert_tail(&f.journal, &[first, last], 10).await;
}
