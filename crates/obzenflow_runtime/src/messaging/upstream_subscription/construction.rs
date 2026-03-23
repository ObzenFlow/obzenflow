// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    ContractTracker, ContractsWiring, DeliveryFilter, SubscriptionState, UpstreamSubscription,
};
use crate::contracts::ContractChain;
use crate::messaging::upstream_subscription_policy::build_policy_stack_for_upstream;
use async_trait::async_trait;
use obzenflow_core::control_middleware::NoControlMiddleware;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::{DeliveryContract, EventEnvelope, Result, StageId, TransportContract};
use std::sync::Arc;

/// Fallback reader used when a real journal reader cannot be created.
///
/// This reader behaves as an always-empty journal (EOF), allowing the
/// subscription machinery to continue operating without failing the FSM.
struct EmptyJournalReader<T: JournalEvent> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: JournalEvent> EmptyJournalReader<T> {
    fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

#[async_trait]
impl<T> JournalReader<T> for EmptyJournalReader<T>
where
    T: JournalEvent + Send + Sync + 'static,
{
    async fn next(&mut self) -> std::result::Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn skip(&mut self, _n: u64) -> std::result::Result<u64, JournalError> {
        Ok(0)
    }

    fn position(&self) -> u64 {
        0
    }

    fn is_at_end(&self) -> bool {
        true
    }
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    /// Create a new subscription from upstream journals
    pub async fn new_with_names(
        owner_label: &str,
        upstream_journals: &[(StageId, String, Arc<dyn Journal<T>>)],
    ) -> Result<Self> {
        // Delegate to the position-aware constructor with all starting
        // positions at 0 (from-beginning semantics).
        let start_positions = vec![0u64; upstream_journals.len()];
        Self::new_with_names_from_positions(owner_label, upstream_journals, &start_positions).await
    }

    /// Create a new subscription from upstream journals, starting each reader
    /// from an explicit position.
    ///
    /// This is used by the metrics aggregator (FLOWIP-059 Phase 6) to
    /// fast-forward readers to the tail while still seeding snapshot metrics
    /// from wide events. Other callers should generally prefer `new_with_names`.
    pub async fn new_with_names_from_positions(
        owner_label: &str,
        upstream_journals: &[(StageId, String, Arc<dyn Journal<T>>)],
        start_positions: &[u64],
    ) -> Result<Self> {
        if upstream_journals.len() != start_positions.len() {
            return Err(format!(
                "start_positions length {} does not match upstream_journals length {}",
                start_positions.len(),
                upstream_journals.len()
            )
            .into());
        }

        let mut readers = Vec::new();

        tracing::debug!(
            "Creating subscription for {} upstream journals",
            upstream_journals.len()
        );

        tracing::debug!(
            target: "flowip-080o",
            owner = owner_label,
            readers = ?upstream_journals
                .iter()
                .map(|(_id, name, journal)| format!("{} ({})", name, journal.id()))
                .collect::<Vec<_>>(),
            "UpstreamSubscription::new_with_names binding readers"
        );

        for ((stage_id, stage_name, journal), position) in
            upstream_journals.iter().zip(start_positions.iter())
        {
            // Get journal ID for debugging
            let journal_id = journal.id();
            tracing::debug!(
                target: "flowip-080o",
                stage_id = ?stage_id,
                stage_name = stage_name,
                journal_id = ?journal_id,
                "Creating reader for upstream journal"
            );
            let reader_result = if *position == 0 {
                journal.reader().await
            } else {
                journal.reader_from(*position).await
            };

            let reader: Box<dyn JournalReader<T>> = match reader_result {
                Ok(reader) => reader,
                Err(e)
                    if crate::runtime_resource_limits::journal_error_is_too_many_open_files(&e) =>
                {
                    return Err(format!(
                        "Too many open files while creating reader for upstream journal (owner={owner_label}, stage_id={stage_id:?}, stage_name={stage_name}, journal_id={journal_id:?}). Increase RLIMIT_NOFILE / `ulimit -n` or reduce pipeline size (for development, disable metrics in obzenflow.toml). Underlying error: {e}"
                    )
                    .into());
                }
                Err(JournalError::Implementation { message, source }) => {
                    // Best-effort: log the failure and use an empty reader so the
                    // FSM can continue operating (upstream treated as having no events).
                    tracing::error!(
                        target: "flowip-080o",
                        stage_id = ?stage_id,
                        stage_name = stage_name,
                        journal_id = ?journal_id,
                        journal_error_message = %message,
                        journal_error_source = %source,
                        "Failed to create reader for upstream journal; using EmptyJournalReader (no events)"
                    );
                    Box::new(EmptyJournalReader::<T>::new()) as Box<dyn JournalReader<T>>
                }
                Err(e) => {
                    return Err(
                        format!("Failed to create reader for stage {stage_id:?}: {e}").into(),
                    );
                }
            };
            readers.push((*stage_id, stage_name.clone(), reader));
        }

        let state = SubscriptionState::new(readers.len());

        Ok(Self {
            delivery_filter: DeliveryFilter::All,
            owner_label: owner_label.to_string(),
            readers,
            state,
            contract_tracker: None,
            contract_chains: Vec::new(),
            contract_policies: Vec::new(),
            control_middleware: Arc::new(NoControlMiddleware),
            last_eof_outcome: None,
            last_delivered_upstream_stage: None,
        })
    }

    /// Configure this subscription to deliver only transport-relevant events to the caller.
    ///
    /// This is intended for *stage runtime* subscriptions where downstream stages should
    /// not be forced to process upstream observability events (e.g. middleware metrics)
    /// as part of normal draining / shutdown.
    pub fn transport_only(mut self) -> Self {
        self.delivery_filter = DeliveryFilter::TransportOnly;
        self
    }

    /// Create a subscription starting from explicit tail positions.
    ///
    /// Readers are treated as logically at EOF for historical data
    /// (baseline_at_tail = true) but will still observe any new
    /// events appended after subscription creation. This is used by
    /// tail-first observers like the metrics aggregator which seed
    /// from tail snapshots and do not need to re-observe historical
    /// EOF control events.
    pub async fn new_at_tail(
        owner_label: &str,
        upstream_journals: &[(StageId, String, Arc<dyn Journal<T>>)],
        tail_positions: &[u64],
    ) -> Result<Self> {
        let mut sub =
            Self::new_with_names_from_positions(owner_label, upstream_journals, tail_positions)
                .await?;

        // Only mark a reader as baseline-at-tail if we're actually skipping historical
        // events (i.e., the computed tail position is non-zero). For fresh/empty
        // journals, setting this baseline would incorrectly allow "logical EOF"
        // termination before new events are observed.
        let mut baseline_count = 0usize;
        for (idx, tail_position) in tail_positions.iter().take(sub.readers.len()).enumerate() {
            if *tail_position > 0 {
                sub.state.mark_reader_baseline_at_tail(idx);
                baseline_count += 1;
            }
        }

        tracing::info!(
            target: "flowip-059d",
            owner = owner_label,
            reader_count = sub.readers.len(),
            baseline_count = baseline_count,
            "Created tail-start upstream subscription"
        );

        Ok(sub)
    }

    /// Backwards-compatible constructor using stage IDs as names
    pub async fn new(upstream_journals: &[(StageId, Arc<dyn Journal<T>>)]) -> Result<Self> {
        let with_names: Vec<(StageId, String, Arc<dyn Journal<T>>)> = upstream_journals
            .iter()
            .map(|(id, journal)| (*id, format!("{id:?}"), journal.clone()))
            .collect();
        Self::new_with_names("unknown_owner", &with_names).await
    }

    /// Enable contract emission for at-least-once delivery guarantees
    pub fn with_contracts(mut self, wiring: ContractsWiring) -> Self {
        let ContractsWiring {
            writer_id,
            contract_journal,
            config,
            system_journal,
            reader_stage,
            control_middleware,
            include_delivery_contract,
            cycle_guard_config,
        } = wiring;

        self.control_middleware = control_middleware.clone();

        self.contract_tracker = Some(ContractTracker {
            config,
            writer_id,
            journal: contract_journal,
            system_journal,
            reader_stage,
            output_events_written: SeqNo(0),
        });

        // Initialize per-reader contract chains using the new Contract framework.
        // For 090c v1, we attach a TransportContract to each upstream edge.
        if !self.readers.is_empty() {
            self.contract_chains = self
                .readers
                .iter()
                .map(|(upstream_stage, _, _)| {
                    let mut chain = ContractChain::new()
                        .with_contract(TransportContract::new())
                        .with_contract(obzenflow_core::SourceContract::new());
                    if include_delivery_contract {
                        chain = chain.with_contract(DeliveryContract::default());
                    }

                    // Divergence detection is attached only for SCC-internal upstreams
                    // when cycle metadata is available (FLOWIP-080r).
                    if let Some(cycle_cfg) = &cycle_guard_config {
                        if cycle_cfg.internal_upstreams.contains(upstream_stage) {
                            let thresholds = obzenflow_core::DivergenceThresholds {
                                max_cycle_depth: cycle_cfg.max_iterations.as_u16(),
                                ..Default::default()
                            };
                            chain = chain.with_contract(
                                obzenflow_core::DivergenceContract::with_thresholds(
                                    cycle_cfg.scc_id,
                                    thresholds,
                                ),
                            );
                        }
                    }
                    Some(chain)
                })
                .collect();

            // Initialize per-reader policy stacks using the upstream stage IDs.
            self.contract_policies = self
                .readers
                .iter()
                .map(|(upstream_stage, _, _)| {
                    let stack =
                        build_policy_stack_for_upstream(*upstream_stage, &control_middleware);
                    Some(stack)
                })
                .collect();
        }

        self
    }
}
