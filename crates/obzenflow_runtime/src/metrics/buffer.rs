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
pub(super) struct Values {
    pub stages: HashMap<(StageId, MetricsJournalKind), Arc<[JournalRecord<ChainPayload>]>>,
    pub system: Arc<[JournalRecord<SystemPayload>]>,
    refreshed: HashMap<JournalId, Instant>,
}

#[derive(Default)]
pub(crate) struct MetricsBuffer {
    values: Mutex<Values>,
    pub updated: tokio::sync::Notify,
}

impl MetricsBuffer {
    pub(super) fn snapshot(&self) -> Values {
        self.values.lock().unwrap().clone()
    }

    pub fn terminal(&self, writer: Option<WriterId>) -> bool {
        self.values.lock().unwrap().system.iter().any(|record| {
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
        let values = self.values.lock().unwrap();
        values.refreshed.len() == readers && values.refreshed.values().all(|at| *at >= since)
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
                    move |values, rows| {
                        values.stages.insert((stage, kind), rows);
                    },
                );
            }
        }
        readers.spawn(
            ctx.system_journal.clone(),
            ctx.metrics_store.buffer.clone(),
            |values, rows| values.system = rows,
        );
        readers
    }

    fn spawn<T: JournalEvent>(
        &mut self,
        journal: Arc<dyn Journal<T>>,
        buffer: Arc<MetricsBuffer>,
        replace: impl Fn(&mut Values, Arc<[JournalRecord<T::Payload>]>) + Send + 'static,
    ) {
        self.0.spawn(async move {
            let mut interval = tokio::time::interval(REFRESH_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let started = Instant::now();
                let result = journal.read_metrics_tail().await;
                {
                    let mut values = buffer.values.lock().unwrap();
                    match result {
                        Ok(rows) if !rows.is_empty() => replace(&mut values, rows.into()),
                        Ok(_) => {}
                        Err(error) => tracing::debug!(journal_id = %journal.id(), %error, "Metrics tail unavailable; retaining buffered values"),
                    }
                    values.refreshed.insert(*journal.id(), started);
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
