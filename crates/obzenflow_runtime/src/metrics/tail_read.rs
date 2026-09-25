// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Explicit, best-effort inspection of protected accounting in journal tails.
//!
//! These helpers use the same live current-key lookup as background refresh.
//! They never scan or reconstruct journal history.

use obzenflow_core::event::context::StageType;
use obzenflow_core::event::provenance::RuntimeProvenance;
use obzenflow_core::event::ChainEvent;
use obzenflow_core::id::StageId;
use obzenflow_core::metrics::{FlowLifecycleMetricsSnapshot, StageMetadata, StageMetricsSnapshot};
use obzenflow_core::{Journal, WriterId};
use std::collections::HashMap;
use std::sync::Arc;

type StageJournalEntry = (
    StageId,
    Arc<dyn Journal<ChainEvent>>,
    Option<Arc<dyn Journal<ChainEvent>>>,
);

/// Read the most recent `RuntimeProvenance` from a journal's tail.
///
/// Reads selected current carriers, newest first. Missing live locators yield
/// no value; archive history is not rebuilt by a metrics helper.
pub async fn read_latest_runtime_context(
    journal: &Arc<dyn Journal<ChainEvent>>,
) -> Option<RuntimeProvenance> {
    journal
        .read_metrics_tail()
        .await
        .ok()?
        .into_iter()
        .find_map(|record| record.envelope.provenance.event.runtime)
}

/// Read the most recent `RuntimeProvenance` from a journal's tail for a specific
/// stage, filtering by `flow_context.stage_id`.
///
/// This stricter variant is used by metrics code to ensure that only runtime
/// snapshots authored by the stage associated with a journal are considered
/// when deriving per-stage metrics. Forwarded control events that still carry
/// an upstream `flow_context` are ignored.
pub async fn read_latest_runtime_context_for_stage(
    journal: &Arc<dyn Journal<ChainEvent>>,
    stage_id: StageId,
) -> Option<RuntimeProvenance> {
    journal
        .read_metrics_tail()
        .await
        .ok()?
        .into_iter()
        .find_map(|record| {
            let event = record.envelope.provenance.event;
            (event.flow_context.stage_id == stage_id && event.writer_id == WriterId::from(stage_id))
                .then_some(event.runtime)
                .flatten()
        })
}

/// Read stage metrics from journal tails.
///
/// Checks both data and error journals (if provided):
/// Protected counters retain their physical populations and max semantics.
/// Measurement families are selected independently by capture scope and sequence
/// across both journals; a later error-rail read has no special precedence.
pub async fn read_stage_metrics_from_tail(
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: Option<&Arc<dyn Journal<ChainEvent>>>,
    stage_id: StageId,
) -> Option<StageMetricsSnapshot> {
    use obzenflow_core::event::observability::ObservationSource;
    let mut metrics = super::fsm::StageMetrics::default();
    let observations = super::observations::LatestObservationMap::default();
    for journal in std::iter::once(data_journal).chain(error_journal) {
        if let Ok(records) = journal.read_metrics_tail().await {
            for record in records {
                if let Some(packet) = &record.envelope.observability {
                    observations.offer_recorded(packet);
                }
                let event = record.envelope.provenance.event;
                if event.flow_context.stage_id == stage_id
                    && event.writer_id == WriterId::from(stage_id)
                {
                    if let Some(provenance) = event.runtime {
                        metrics.merge_runtime_context(&provenance);
                    }
                }
            }
        }
    }
    for packet in observations.snapshot() {
        if let Some(runtime) = packet.runtime {
            metrics.merge_runtime_measurements(&runtime);
        }
    }
    metrics
        .latest_events_processed_total
        .map(|events| StageMetricsSnapshot {
            events_processed_total: events,
            events_accumulated_total: metrics.latest_events_accumulated_total.unwrap_or(0),
            events_emitted_total: metrics.latest_events_emitted_total.unwrap_or(0),
            errors_total: metrics.latest_errors_total.unwrap_or(0),
            errors_by_kind: metrics.errors_by_kind,
            in_flight: metrics.last_in_flight,
            recent_p50_ms: metrics.snapshot_p50_ms,
            recent_p90_ms: metrics.snapshot_p90_ms,
            recent_p95_ms: metrics.snapshot_p95_ms,
            recent_p99_ms: metrics.snapshot_p99_ms,
            recent_p999_ms: metrics.snapshot_p999_ms,
            processing_time_count: metrics.processing_time_count,
            processing_time_sum_nanos: metrics.processing_time_sum_nanos,
            timing_window: metrics.timing_window,
            event_loops_total: metrics.event_loops_total,
            event_loops_with_work_total: metrics.event_loops_with_work_total,
        })
}

/// Read flow-level metrics by aggregating all stage journal tails.
///
/// Returns `FlowLifecycleMetricsSnapshot` (the lifecycle event payload
/// type), not `FlowMetricsSnapshot` (the Prometheus/AppMetricsSnapshot
/// type). These are intentionally different:
/// - FlowLifecycleMetricsSnapshot: minimal view for lifecycle events.
/// - FlowMetricsSnapshot: full view for Prometheus export.
pub async fn read_flow_metrics_from_tails(
    stage_journals: &[StageJournalEntry],
    stage_metadata: &HashMap<StageId, StageMetadata>,
) -> FlowLifecycleMetricsSnapshot {
    let mut events_in_total: u64 = 0;
    let mut events_out_total: u64 = 0;
    let mut errors_total: u64 = 0;

    for (stage_id, data_journal, error_journal) in stage_journals {
        if let Some(snapshot) =
            read_stage_metrics_from_tail(data_journal, error_journal.as_ref(), *stage_id).await
        {
            if let Some(metadata) = stage_metadata.get(stage_id) {
                match metadata.stage_type {
                    StageType::FiniteSource | StageType::InfiniteSource => {
                        events_in_total += snapshot.events_processed_total;
                    }
                    StageType::Sink => {
                        events_out_total += snapshot.events_processed_total;
                    }
                    _ => {}
                }
            }
            errors_total += snapshot.errors_total;
        }
    }

    FlowLifecycleMetricsSnapshot {
        events_in_total,
        events_out_total,
        errors_total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::journal_record::JournalRecord;
    use obzenflow_core::event::provenance::{ExecutionAccounting, RuntimeProvenance};
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::reader::JournalReader;
    use obzenflow_core::{ChainEvent, WriterId};
    use std::sync::{Arc, Mutex};

    /// Minimal in-memory journal for ChainEvent used in tail-read tests.
    ///
    /// Stores envelopes in a Vec and implements `read_last_n` with the contract
    /// expected by `read_latest_runtime_context` (most recent first).
    struct InMemoryChainJournal {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<JournalRecord<ChainPayload>>>>,
    }

    impl InMemoryChainJournal {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn append_raw(&self, event: ChainEvent) {
            let mut guard = self.events.lock().unwrap();
            let envelope =
                crate::testing::causal_fixture::commit(self.id, event, &Default::default(), &guard)
                    .unwrap();
            guard.push(envelope);
        }
    }

    struct InMemoryReader {
        events: Vec<JournalRecord<ChainPayload>>,
        pos: usize,
    }

    #[async_trait]
    impl Journal<ChainEvent> for InMemoryChainJournal {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: ChainEvent,
            mut options: obzenflow_core::journal::AppendOptions<ChainEvent>,
        ) -> Result<JournalRecord<ChainPayload>, JournalError> {
            let event = options.capture.prepare(0, event);
            let mut guard = self.events.lock().unwrap();
            let envelope =
                crate::testing::causal_fixture::commit(self.id, event, &options, &guard)?;
            guard.push(envelope.clone());
            Ok(envelope)
        }

        async fn read_metrics_tail(
            &self,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(self.events.lock().unwrap().iter().rev().cloned().collect())
        }

        async fn read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(guard.clone())
        }

        async fn read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            Ok(None)
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(InMemoryReader {
                events: guard.clone(),
                pos: position as usize,
            }))
        }

        async fn read_last_n(
            &self,
            count: usize,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            let guard = self.events.lock().unwrap();
            let len = guard.len();
            let start = len.saturating_sub(count);
            // Return most recent first, matching Journal::read_last_n contract.
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl JournalReader<ChainEvent> for InMemoryReader {
        async fn next(&mut self) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            if self.pos >= self.events.len() {
                Ok(None)
            } else {
                let envelope = self.events.get(self.pos).cloned();
                self.pos += 1;
                Ok(envelope)
            }
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn is_at_end(&self) -> bool {
            self.pos >= self.events.len()
        }
    }

    fn runtime_context_with_errors(
        events_processed_total: u64,
        errors_total: u64,
        by_kind: &[(ErrorKind, u64)],
    ) -> RuntimeProvenance {
        RuntimeProvenance {
            accounting: ExecutionAccounting {
                events_processed_total,
                events_accumulated_total: 0,
                events_emitted_total: 0,
                data_outputs_by_event_type: Vec::new(),
                data_inputs_by_upstream_event_type: Vec::new(),
                errors_total,
                failures_total: 0,
                errors_by_kind: by_kind.iter().cloned().collect(),
            },
        }
    }

    #[tokio::test]
    async fn read_stage_metrics_from_tail_uses_max_semantics_for_errors_by_kind() {
        use obzenflow_core::event::ChainEventFactory;
        use obzenflow_core::StageId;

        let stage_id = StageId::new();
        let owner = JournalOwner::stage(stage_id);

        let data_journal_raw = Arc::new(InMemoryChainJournal::new(owner.clone()));
        let error_journal_raw = Arc::new(InMemoryChainJournal::new(owner));
        let data_journal: Arc<dyn Journal<ChainEvent>> = data_journal_raw.clone();
        let error_journal: Arc<dyn Journal<ChainEvent>> = error_journal_raw.clone();

        // Data journal snapshot: 10 events, 2 domain errors.
        let data_ctx = runtime_context_with_errors(10, 2, &[(ErrorKind::Domain, 2)]);
        let mut data_event = ChainEventFactory::data_event(
            WriterId::from(stage_id),
            "test.data",
            serde_json::json!({"k": "v"}),
        );
        data_event.flow_context.stage_id = stage_id;
        data_event = data_event.with_runtime_provenance(data_ctx);
        data_journal_raw.append_raw(data_event);

        // Error journal snapshot: later snapshot with 5 total errors, including the same
        // domain errors plus additional remote errors.
        let error_ctx =
            runtime_context_with_errors(10, 5, &[(ErrorKind::Domain, 2), (ErrorKind::Remote, 3)]);
        let mut error_event = ChainEventFactory::data_event(
            WriterId::from(stage_id),
            "test.error",
            serde_json::json!({"k": "v2"}),
        );
        error_event.flow_context.stage_id = stage_id;
        error_event = error_event.with_runtime_provenance(error_ctx);
        error_journal_raw.append_raw(error_event);

        let snapshot = read_stage_metrics_from_tail(&data_journal, Some(&error_journal), stage_id)
            .await
            .expect("snapshot should be present");

        // Counters use monotonic max semantics across journals.
        assert_eq!(snapshot.events_processed_total, 10);
        assert_eq!(snapshot.errors_total, 5);

        // errors_by_kind uses monotonic max semantics (avoid double-counting across journals).
        assert_eq!(snapshot.errors_by_kind.get(&ErrorKind::Domain), Some(&2));
        assert_eq!(snapshot.errors_by_kind.get(&ErrorKind::Remote), Some(&3));
    }

    #[tokio::test]
    async fn read_latest_runtime_context_for_stage_ignores_mismatched_stage_ids() {
        use obzenflow_core::event::ChainEventFactory;
        use obzenflow_core::StageId;

        let local_stage_id = StageId::new();
        let upstream_stage_id = StageId::new();
        let owner = JournalOwner::stage(local_stage_id);

        let journal_raw = Arc::new(InMemoryChainJournal::new(owner));
        let journal: Arc<dyn Journal<ChainEvent>> = journal_raw.clone();

        // Upstream event with non-zero errors_total but wrong stage_id.
        let upstream_ctx = runtime_context_with_errors(100, 10, &[]);
        let mut upstream_event = ChainEventFactory::data_event(
            WriterId::from(upstream_stage_id),
            "upstream.data",
            serde_json::json!({"k": "v_upstream"}),
        );
        upstream_event.flow_context.stage_id = upstream_stage_id;
        upstream_event = upstream_event.with_runtime_provenance(upstream_ctx);
        journal_raw.append_raw(upstream_event);

        // Local event with zero errors_total and matching stage_id.
        let local_ctx = runtime_context_with_errors(50, 0, &[]);
        let mut local_event = ChainEventFactory::data_event(
            WriterId::from(local_stage_id),
            "local.data",
            serde_json::json!({"k": "v_local"}),
        );
        local_event.flow_context.stage_id = local_stage_id;
        local_event = local_event.with_runtime_provenance(local_ctx);
        journal_raw.append_raw(local_event);

        // Generic helper may see either; the stage-aware helper must only see the local snapshot.
        let generic_ctx = read_latest_runtime_context(&journal).await.expect("ctx");
        assert_eq!(generic_ctx.accounting.events_processed_total, 50);

        let filtered_ctx = read_latest_runtime_context_for_stage(&journal, local_stage_id)
            .await
            .expect("ctx");
        assert_eq!(filtered_ctx.accounting.events_processed_total, 50);
        assert_eq!(filtered_ctx.accounting.errors_total, 0);
    }

    #[tokio::test]
    async fn read_latest_runtime_context_for_stage_searches_past_forwarded_events() {
        use obzenflow_core::event::ChainEventFactory;
        use obzenflow_core::StageId;
        use serde_json::json;

        let stage_id = StageId::new();
        let owner = JournalOwner::stage(stage_id);

        let journal_raw = Arc::new(InMemoryChainJournal::new(owner));
        let journal: Arc<dyn Journal<ChainEvent>> = journal_raw.clone();

        // Seed a stage-authored event with runtime_context.
        let seeded_ctx = runtime_context_with_errors(42, 0, &[]);
        let mut seeded_event = ChainEventFactory::data_event(
            WriterId::from(stage_id),
            "seeded.data",
            json!({"k": "v"}),
        );
        seeded_event.flow_context.stage_id = stage_id;
        seeded_event = seeded_event.with_runtime_provenance(seeded_ctx);
        journal_raw.append_raw(seeded_event);

        // Append many forwarded/control-like events without runtime_context.
        // This simulates stateful/join stage journals where tail events may not carry a snapshot.
        for i in 0..50 {
            let mut forwarded = ChainEventFactory::data_event(
                WriterId::from(stage_id),
                "forwarded.control",
                json!({"i": i}),
            );
            forwarded.flow_context.stage_id = stage_id;
            journal_raw.append_raw(forwarded);
        }

        let ctx = read_latest_runtime_context_for_stage(&journal, stage_id)
            .await
            .expect("ctx");
        assert_eq!(ctx.accounting.events_processed_total, 42);
    }
}
