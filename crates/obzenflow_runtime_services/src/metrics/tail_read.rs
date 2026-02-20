// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared tail-read utilities for metrics.
//!
//! These helpers provide a single, journal-based source of truth for
//! stage- and flow-level metrics. Both the metrics aggregator and
//! supervisors should use these functions so that `/metrics` and SSE
//! lifecycle events report consistent data.

use obzenflow_core::event::context::{RuntimeContext, StageType};
use obzenflow_core::event::ChainEvent;
use obzenflow_core::id::StageId;
use obzenflow_core::metrics::{FlowLifecycleMetricsSnapshot, StageMetadata, StageMetricsSnapshot};
use obzenflow_core::Journal;
use std::collections::HashMap;
use std::sync::Arc;

type StageJournalEntry = (
    StageId,
    Arc<dyn Journal<ChainEvent>>,
    Option<Arc<dyn Journal<ChainEvent>>>,
);

/// Read the most recent `RuntimeContext` from a journal's tail.
///
/// Uses a graduated search to handle cases where the very last events may not
/// carry `runtime_context` (e.g., control/forwarded events, partial writes).
/// In typical flows, a small tail window is enough. For stateful stages that
/// only emit on EOF, runtime snapshots may be sparse relative to control and
/// middleware events, so we expand the window to keep `/metrics` accurate.
///
/// This relies on `Journal::read_last_n` returning events in
/// most-recent-first order.
pub async fn read_latest_runtime_context(
    journal: &Arc<dyn Journal<ChainEvent>>,
) -> Option<RuntimeContext> {
    // In most flows, the last few events contain a runtime_context snapshot.
    // However, some stages can end with a large number of control or forwarded
    // events that omit runtime_context, so we expand the search window.
    for n in [1, 5, 20, 100, 500, 2_000, 10_000, 50_000] {
        match journal.read_last_n(n).await {
            Ok(events) => {
                // IMPORTANT: read_last_n returns most recent first (API contract).
                for env in events.into_iter() {
                    if let Some(ctx) = env.event.runtime_context {
                        return Some(ctx);
                    }
                }
            }
            Err(e) => {
                tracing::debug!("Failed to read last {} from journal: {}", n, e);
                continue;
            }
        }
    }
    None
}

/// Read the most recent `RuntimeContext` from a journal's tail for a specific
/// stage, filtering by `flow_context.stage_id`.
///
/// This stricter variant is used by metrics code to ensure that only runtime
/// snapshots authored by the stage associated with a journal are considered
/// when deriving per-stage metrics. Forwarded control events that still carry
/// an upstream `flow_context` are ignored.
pub async fn read_latest_runtime_context_for_stage(
    journal: &Arc<dyn Journal<ChainEvent>>,
    stage_id: StageId,
) -> Option<RuntimeContext> {
    // See read_latest_runtime_context. The stage-filtered variant can be more
    // sensitive to "tail noise" because forwarded events often re-stamp
    // flow_context but omit runtime_context.
    for n in [1, 5, 20, 100, 500, 2_000, 10_000, 50_000] {
        match journal.read_last_n(n).await {
            Ok(events) => {
                for env in events.into_iter() {
                    if let Some(ctx) = env.event.runtime_context.clone() {
                        if env.event.flow_context.stage_id == stage_id {
                            return Some(ctx);
                        }
                    }
                }
            }
            Err(e) => {
                tracing::debug!(
                    "Failed to read last {} from journal for stage {:?}: {}",
                    n,
                    stage_id,
                    e
                );
                continue;
            }
        }
    }
    None
}

/// Read stage metrics from journal tails.
///
/// Checks both data and error journals (if provided):
/// - Counters (events_processed_total, errors_total, event_loops_*) use
///   monotonic max semantics.
/// - Gauges/percentiles (in_flight, p50-p999) treat the error journal as
///   potentially more recent: data journal is consulted first, then error
///   journal overrides gauge fields when present.
pub async fn read_stage_metrics_from_tail(
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    error_journal: Option<&Arc<dyn Journal<ChainEvent>>>,
    stage_id: StageId,
) -> Option<StageMetricsSnapshot> {
    let mut events_processed_total: Option<u64> = None;
    let mut events_accumulated_total: Option<u64> = None;
    let mut events_emitted_total: Option<u64> = None;
    let mut errors_total: Option<u64> = None;
    let mut in_flight: Option<u32> = None;
    let mut p50_ms: Option<u64> = None;
    let mut p90_ms: Option<u64> = None;
    let mut p95_ms: Option<u64> = None;
    let mut p99_ms: Option<u64> = None;
    let mut p999_ms: Option<u64> = None;
    let mut processing_time_sum_nanos: Option<u64> = None;
    let mut event_loops_total: Option<u64> = None;
    let mut event_loops_with_work_total: Option<u64> = None;
    let mut errors_by_kind: std::collections::HashMap<
        obzenflow_core::event::status::processing_status::ErrorKind,
        u64,
    > = std::collections::HashMap::new();

    fn update_max<T: Ord + Copy>(current: &mut Option<T>, new: T) {
        *current = Some(current.map_or(new, |c| c.max(new)));
    }

    fn update_kind_max(
        map: &mut std::collections::HashMap<
            obzenflow_core::event::status::processing_status::ErrorKind,
            u64,
        >,
        kind: &obzenflow_core::event::status::processing_status::ErrorKind,
        new: u64,
    ) {
        map.entry(kind.clone())
            .and_modify(|current| *current = (*current).max(new))
            .or_insert(new);
    }

    // Read from data journal first.
    if let Some(ctx) = read_latest_runtime_context_for_stage(data_journal, stage_id).await {
        update_max(&mut events_processed_total, ctx.events_processed_total);
        update_max(&mut events_accumulated_total, ctx.events_accumulated_total);
        update_max(&mut events_emitted_total, ctx.events_emitted_total);
        update_max(&mut errors_total, ctx.errors_total);
        in_flight = Some(ctx.in_flight);
        p50_ms = Some(ctx.recent_p50_ms);
        p90_ms = Some(ctx.recent_p90_ms);
        p95_ms = Some(ctx.recent_p95_ms);
        p99_ms = Some(ctx.recent_p99_ms);
        p999_ms = Some(ctx.recent_p999_ms);
        update_max(
            &mut processing_time_sum_nanos,
            ctx.processing_time_sum_nanos,
        );
        update_max(&mut event_loops_total, ctx.event_loops_total);
        update_max(
            &mut event_loops_with_work_total,
            ctx.event_loops_with_work_total,
        );

        for (kind, count) in ctx.errors_by_kind.iter() {
            update_kind_max(&mut errors_by_kind, kind, *count);
        }
    }

    // Then consult error journal if provided: counters via max, gauges override.
    if let Some(error_journal) = error_journal {
        if let Some(ctx) = read_latest_runtime_context_for_stage(error_journal, stage_id).await {
            update_max(&mut events_processed_total, ctx.events_processed_total);
            update_max(&mut events_accumulated_total, ctx.events_accumulated_total);
            update_max(&mut events_emitted_total, ctx.events_emitted_total);
            update_max(&mut errors_total, ctx.errors_total);
            update_max(
                &mut processing_time_sum_nanos,
                ctx.processing_time_sum_nanos,
            );
            update_max(&mut event_loops_total, ctx.event_loops_total);
            update_max(
                &mut event_loops_with_work_total,
                ctx.event_loops_with_work_total,
            );

            in_flight = Some(ctx.in_flight);
            p50_ms = Some(ctx.recent_p50_ms);
            p90_ms = Some(ctx.recent_p90_ms);
            p95_ms = Some(ctx.recent_p95_ms);
            p99_ms = Some(ctx.recent_p99_ms);
            p999_ms = Some(ctx.recent_p999_ms);

            for (kind, count) in ctx.errors_by_kind.iter() {
                update_kind_max(&mut errors_by_kind, kind, *count);
            }
        }
    }

    // Only return Some if we observed at least one runtime context.
    events_processed_total.map(|events| StageMetricsSnapshot {
        events_processed_total: events,
        events_accumulated_total: events_accumulated_total.unwrap_or(0),
        events_emitted_total: events_emitted_total.unwrap_or(0),
        errors_total: errors_total.unwrap_or(0),
        errors_by_kind,
        in_flight: in_flight.unwrap_or(0),
        recent_p50_ms: p50_ms.unwrap_or(0),
        recent_p90_ms: p90_ms.unwrap_or(0),
        recent_p95_ms: p95_ms.unwrap_or(0),
        recent_p99_ms: p99_ms.unwrap_or(0),
        recent_p999_ms: p999_ms.unwrap_or(0),
        processing_time_sum_nanos: processing_time_sum_nanos.unwrap_or(0),
        event_loops_total: event_loops_total.unwrap_or(0),
        event_loops_with_work_total: event_loops_with_work_total.unwrap_or(0),
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
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::identity::journal_writer_id::JournalWriterId;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::ChainEvent;
    use obzenflow_core::WriterId;
    use std::sync::{Arc, Mutex};

    /// Minimal in-memory journal for ChainEvent used in tail-read tests.
    ///
    /// Stores envelopes in a Vec and implements `read_last_n` with the contract
    /// expected by `read_latest_runtime_context` (most recent first).
    struct InMemoryChainJournal {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<EventEnvelope<ChainEvent>>>>,
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
            let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
            let mut guard = self.events.lock().unwrap();
            guard.push(envelope);
        }
    }

    struct InMemoryReader {
        events: Vec<EventEnvelope<ChainEvent>>,
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
            _parent: Option<&EventEnvelope<ChainEvent>>,
        ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
            let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
            let mut guard = self.events.lock().unwrap();
            guard.push(envelope.clone());
            Ok(envelope)
        }

        async fn read_causally_ordered(
            &self,
        ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(guard.clone())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &obzenflow_core::EventId,
        ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
            Ok(None)
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(InMemoryReader {
                events: guard.clone(),
                pos: 0,
            }))
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
        ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
            let guard = self.events.lock().unwrap();
            let len = guard.len();
            let start = len.saturating_sub(count);
            // Return most recent first, matching Journal::read_last_n contract.
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl JournalReader<ChainEvent> for InMemoryReader {
        async fn next(&mut self) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
            if self.pos >= self.events.len() {
                Ok(None)
            } else {
                let envelope = self.events.get(self.pos).cloned();
                self.pos += 1;
                Ok(envelope)
            }
        }

        async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
            let start = self.pos as u64;
            self.pos = (self.pos as u64 + n) as usize;
            Ok(self.pos as u64 - start)
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
    ) -> obzenflow_core::event::context::RuntimeContext {
        obzenflow_core::event::context::RuntimeContext {
            in_flight: 0,
            recent_p50_ms: 0,
            recent_p90_ms: 0,
            recent_p95_ms: 0,
            recent_p99_ms: 0,
            recent_p999_ms: 0,
            processing_time_sum_nanos: 0,
            events_processed_total,
            events_accumulated_total: 0,
            events_emitted_total: 0,
            join_reference_since_last_stream: 0,
            errors_total,
            failures_total: 0,
            event_loops_total: 0,
            event_loops_with_work_total: 0,
            errors_by_kind: by_kind.iter().cloned().collect(),
            fsm_state: "Running".to_string(),
            time_in_state_ms: 0,
            reader_seq: 0,
            writer_seq: 0,
            last_consumed_event_id: None,
            last_consumed_writer: None,
            last_consumed_vector_clock: None,
            last_emitted_event_id: None,
            last_emitted_writer: None,
            cb_requests_total: 0,
            cb_successes_total: 0,
            cb_failures_total: 0,
            cb_rejections_total: 0,
            cb_opened_total: 0,
            cb_time_closed_seconds: 0.0,
            cb_time_open_seconds: 0.0,
            cb_time_half_open_seconds: 0.0,
            cb_state: 0.0,
            rl_events_total: 0,
            rl_delayed_total: 0,
            rl_tokens_consumed_total: 0.0,
            rl_delay_seconds_total: 0.0,
            rl_bucket_tokens: 0.0,
            rl_bucket_capacity: 0.0,
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
        data_event = data_event.with_runtime_context(data_ctx);
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
        error_event = error_event.with_runtime_context(error_ctx);
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
        upstream_event = upstream_event.with_runtime_context(upstream_ctx);
        journal_raw.append_raw(upstream_event);

        // Local event with zero errors_total and matching stage_id.
        let local_ctx = runtime_context_with_errors(50, 0, &[]);
        let mut local_event = ChainEventFactory::data_event(
            WriterId::from(local_stage_id),
            "local.data",
            serde_json::json!({"k": "v_local"}),
        );
        local_event.flow_context.stage_id = local_stage_id;
        local_event = local_event.with_runtime_context(local_ctx);
        journal_raw.append_raw(local_event);

        // Generic helper may see either; the stage-aware helper must only see the local snapshot.
        let generic_ctx = read_latest_runtime_context(&journal).await.expect("ctx");
        assert_eq!(generic_ctx.events_processed_total, 50);

        let filtered_ctx = read_latest_runtime_context_for_stage(&journal, local_stage_id)
            .await
            .expect("ctx");
        assert_eq!(filtered_ctx.events_processed_total, 50);
        assert_eq!(filtered_ctx.errors_total, 0);
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
        seeded_event = seeded_event.with_runtime_context(seeded_ctx);
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
        assert_eq!(ctx.events_processed_total, 42);
    }
}
