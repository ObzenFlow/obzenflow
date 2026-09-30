// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Development-only access to real tail refresh tasks and projection actions.
//! No metrics supervisor, export publication or alternative reader is created.

use super::buffer::{MetricsBufferSnapshot, TailReaders};
use super::builder::MetricsJournals;
use super::{MetricsAggregatorAction, MetricsAggregatorContext, MetricsInputs};
use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotExporter};
use obzenflow_core::{event::SystemEvent, Journal, JournalOwner, StageId};
use obzenflow_fsm::FsmAction;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

struct NoExport;
impl MetricsSnapshotExporter for NoExport {
    fn publish_app_snapshot(&self, _: AppMetricsSnapshot) {}
    fn publish_infra_snapshot(&self, _: InfraMetricsSnapshot) {}
}

pub struct MetricsProjection {
    context: MetricsAggregatorContext,
    readers: TailReaders,
    // Explicit slow snapshot consumers retain old Arc arrays. The production
    // overwrite slot is still the only producer of those arrays.
    held: Vec<MetricsBufferSnapshot>,
}

impl MetricsProjection {
    pub async fn start(inputs: MetricsInputs, system: Arc<dyn Journal<SystemEvent>>) -> Self {
        let Some(JournalOwner::System { system_id }) = system.owner() else {
            panic!("system journal owner is required");
        };
        let context = MetricsAggregatorContext::new(
            inputs,
            system.clone(),
            MetricsJournals {
                system_id: *system_id,
                coordination: system.clone(),
                export: system,
            },
            Arc::new(NoExport),
            Duration::from_millis(250),
            Default::default(),
            Vec::new(),
        )
        .await
        .unwrap();
        let readers = TailReaders::start(&context);
        Self {
            context,
            readers,
            held: Vec::new(),
        }
    }

    pub fn reader_count(&self) -> usize {
        self.readers.len()
    }

    pub fn refreshed_since(&self, since: tokio::time::Instant) -> bool {
        self.context
            .metrics_store
            .buffer
            .refreshed_since(since, self.readers.len())
    }

    /// Apply the same original buffered carriers and action as ExportMetrics.
    /// Export serialization, publication and HTTP scraping are outside this boundary.
    pub async fn apply(&mut self, retain_snapshot: bool) {
        let snapshot = self.context.metrics_store.buffer.snapshot();
        for ((stage, kind), records) in &snapshot.stage_records {
            MetricsAggregatorAction::UpdateMetrics {
                events: records.clone(),
                journal_kind: *kind,
                journal_stage: *stage,
            }
            .execute(&mut self.context)
            .await
            .unwrap();
        }
        for record in snapshot.system_records.iter().rev() {
            self.context.fold_system_record(record).unwrap();
        }
        if retain_snapshot {
            self.held.push(snapshot);
        }
    }

    pub fn accounting(&self) -> BTreeMap<StageId, (u64, u64)> {
        self.context
            .metrics_store
            .stage_metrics
            .iter()
            .map(|(stage, metrics)| {
                (
                    *stage,
                    (
                        metrics.latest_events_processed_total.unwrap_or(0),
                        metrics.latest_errors_total.unwrap_or(0),
                    ),
                )
            })
            .collect()
    }

    /// Count Arc-backed record arrays once across the current slot and every
    /// consumer-held snapshot. This is an object census, not deep heap bytes.
    pub fn retained_arrays_and_records(&self) -> (usize, usize) {
        let current = self.context.metrics_store.buffer.snapshot();
        let mut identities = HashSet::new();
        let mut records = 0;
        for snapshot in std::iter::once(&current).chain(&self.held) {
            for rows in snapshot.stage_records.values() {
                if !rows.is_empty() && identities.insert(rows.as_ptr() as usize) {
                    records += rows.len();
                }
            }
            let rows = &snapshot.system_records;
            if !rows.is_empty() && identities.insert(rows.as_ptr() as usize) {
                records += rows.len();
            }
        }
        (identities.len(), records)
    }

    pub async fn stop(&mut self) {
        self.readers.stop().await;
    }
}
