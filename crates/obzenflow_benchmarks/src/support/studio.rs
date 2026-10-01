// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public Studio projection over committed fixture records. No listener,
//! transport, private stream access or copied projection implementation.

use super::validation::ObserverJournals;
use obzenflow_adapters::studio::{ContractBoundaryAliases, StudioProjection};
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::read::RunRecordData;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub struct ProjectionFixture {
    records: Vec<RunRecordData>,
    expected_events: Vec<String>,
    completion: String,
    inputs: usize,
}

impl ProjectionFixture {
    pub async fn read(fixture: &ObserverJournals) -> Self {
        let mut system = fixture.system.read_all_unordered().await.unwrap();
        system.sort_by_key(|row| row.local_sequence());
        assert_eq!(system.len(), 2);
        let mut records = vec![RunRecordData::from(system.remove(0))];
        for journal in [&fixture.data, &fixture.error] {
            let mut reader = journal.reader().await.unwrap();
            while let Some(row) = reader.next().await.unwrap() {
                records.push(row.into());
            }
        }
        records.push(system.remove(0).into());
        assert_eq!(records.len(), fixture.inputs + 5);
        let expected_events = records
            .iter()
            .filter_map(|row| match row {
                RunRecordData::System(row) => Some(row.id().to_string()),
                RunRecordData::Chain(row)
                    if matches!(
                        &row.payload,
                        ChainPayload::Execution(ExecutionPayload::StageLifecycle(_))
                    ) =>
                {
                    Some(row.id().to_string())
                }
                _ => None,
            })
            .collect();
        let completion = records
            .iter()
            .find_map(|row| match row {
                RunRecordData::Chain(row)
                    if matches!(
                        &row.payload,
                        ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                            StageLifecycleFact::Completed { .. }
                        ))
                    ) =>
                {
                    Some(row.id().to_string())
                }
                _ => None,
            })
            .expect("fixture has a committed stage completion");
        Self {
            records,
            expected_events,
            completion,
            inputs: fixture.inputs,
        }
    }

    pub fn measure(&self) -> (Duration, Value) {
        let mut projection =
            StudioProjection::new(Vec::new(), ContractBoundaryAliases::default()).unwrap();
        let started = Instant::now();
        let mut frames = Vec::new();
        for row in &self.records {
            frames.extend(projection.project_deferred(std::hint::black_box(row)));
        }
        frames.extend(projection.current_measurements());
        let snapshots = projection.snapshots();
        let elapsed = started.elapsed();

        if std::env::var("OBZENFLOW_BENCH_CONTROL").as_deref() == Ok("missing-studio-output") {
            frames.retain(|frame| frame.id.as_deref() != Some(self.completion.as_str()));
        }
        let events: Vec<_> = frames.iter().filter_map(|frame| frame.id.clone()).collect();
        assert_eq!(
            events, self.expected_events,
            "Studio projection output completeness and order"
        );
        assert!(
            projection.terminal_observed(),
            "committed terminal state projected"
        );
        assert!(!projection.active_observed());
        let completed: Vec<Value> = snapshots
            .iter()
            .filter(|frame| frame.event.as_deref() == Some("stage_lifecycle"))
            .map(|frame| serde_json::from_str(&frame.data).unwrap())
            .collect();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0]["event_type"], "stage_completed");
        assert_eq!(completed[0]["commitment"]["event_id"], self.completion);
        assert_eq!(
            completed[0]["accounting"]["events_processed_total"],
            self.inputs as u64
        );
        (
            elapsed,
            json!({"projected_records":self.records.len(),"lifecycle_events":events.len(),"completed_stage_snapshots":completed.len()}),
        )
    }
}
