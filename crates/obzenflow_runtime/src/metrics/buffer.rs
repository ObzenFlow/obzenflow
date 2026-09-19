// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal tail -> overwrite slots -> independently scheduled publisher.

use super::fsm::{MetricsAggregatorContext, MetricsJournalKind};
use obzenflow_core::event::{ChainPayload, JournalEvent, PipelineLifecycleEvent, SystemPayload};
use obzenflow_core::{Journal, JournalId, JournalRecord, StageId, WriterId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};

const REFRESH_INTERVAL: Duration = Duration::from_millis(25);

#[derive(Default, Clone)]
pub(super) struct MetricsBufferSnapshot {
    pub stage_records: HashMap<(StageId, MetricsJournalKind), Arc<[JournalRecord<ChainPayload>]>>,
    pub system_records: Arc<[JournalRecord<SystemPayload>]>,
    refreshed_at_by_journal: HashMap<JournalId, Instant>,
}

#[derive(Default)]
pub(crate) struct MetricsBuffer {
    state: Mutex<MetricsBufferSnapshot>,
    pub updated: tokio::sync::Notify,
}

impl MetricsBuffer {
    pub(super) fn snapshot(&self) -> MetricsBufferSnapshot {
        self.state.lock().unwrap().clone()
    }

    pub fn terminal(&self, writer: Option<WriterId>) -> bool {
        self.state
            .lock()
            .unwrap()
            .system_records
            .iter()
            .any(|record| {
                writer.is_none_or(|writer| writer == record.envelope.provenance.event.writer_id)
                    && matches!(
                        record.payload,
                        SystemPayload::PipelineLifecycle(
                            PipelineLifecycleEvent::Completed { .. }
                                | PipelineLifecycleEvent::Failed { .. }
                                | PipelineLifecycleEvent::Cancelled { .. }
                                | PipelineLifecycleEvent::NotStarted
                        )
                    )
            })
    }

    pub fn refreshed_since(&self, since: Instant, readers: usize) -> bool {
        let buffer_state = self.state.lock().unwrap();
        buffer_state.refreshed_at_by_journal.len() == readers
            && buffer_state
                .refreshed_at_by_journal
                .values()
                .all(|refreshed_at| *refreshed_at >= since)
    }
}

#[derive(Default)]
pub(crate) struct TailReaders(JoinSet<()>);

impl TailReaders {
    pub fn start(ctx: &MetricsAggregatorContext) -> Self {
        let mut readers = Self::default();
        for (kind, journals) in [
            (MetricsJournalKind::Data, &ctx.stage_data_journals),
            (MetricsJournalKind::Error, &ctx.stage_error_journals),
        ] {
            if kind == MetricsJournalKind::Error && !ctx.include_error_journals {
                continue;
            }
            for (stage, journal) in journals {
                let stage = *stage;
                readers.spawn(
                    journal.clone(),
                    ctx.metrics_store.buffer.clone(),
                    move |buffer_state, records| {
                        buffer_state.stage_records.insert((stage, kind), records);
                    },
                );
            }
        }
        readers.spawn(
            ctx.system_journal.clone(),
            ctx.metrics_store.buffer.clone(),
            |buffer_state, records| buffer_state.system_records = records,
        );
        readers
    }

    fn spawn<T: JournalEvent>(
        &mut self,
        journal: Arc<dyn Journal<T>>,
        buffer: Arc<MetricsBuffer>,
        replace_records: impl Fn(&mut MetricsBufferSnapshot, Arc<[JournalRecord<T::Payload>]>)
            + Send
            + 'static,
    ) {
        self.0.spawn(async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let started = Instant::now();
                let result = journal.read_metrics_tail().await;
                {
                    let mut buffer_state = buffer.state.lock().unwrap();
                    match result {
                        Ok(records) if !records.is_empty() => {
                            replace_records(&mut buffer_state, records.into());
                        }
                        Ok(_) => {}
                        Err(error) => tracing::debug!(journal_id = %journal.id(), %error, "Metrics tail unavailable; retaining buffered records"),
                    }
                    buffer_state.refreshed_at_by_journal.insert(*journal.id(), started);
                }
                buffer.updated.notify_one();
            }
        });
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub async fn stop(&mut self) {
        self.0.shutdown().await;
    }
}
