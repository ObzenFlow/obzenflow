// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ContractStatus, ReaderProgress, UpstreamSubscription};
use crate::messaging::upstream_subscription_policy::{
    EdgeContext, EdgeContractDecision, PolicyHints,
};
use obzenflow_core::event::system_event::{
    ContractResultStatusLabel, SystemEvent, SystemEventType,
};
use obzenflow_core::event::types::{
    Count, DurationMs, JournalIndex, JournalPath, SeqNo, ViolationCause as EventViolationCause,
};
use obzenflow_core::event::{
    ChainEventFactory, ConsumptionFinalEventParams, ConsumptionProgressEventParams, JournalEvent,
};
use obzenflow_core::{ContractResult, ViolationCause};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ContractCheckMode {
    Authoritative,
    DiagnosticsOnly,
}

fn contract_result_labels_for_emission(
    result: &ContractResult,
    pending_label: ContractResultStatusLabel,
) -> (String, Option<String>) {
    match result {
        ContractResult::Passed(_) => (ContractResultStatusLabel::Passed.to_string(), None),
        ContractResult::Failed(v) => (
            ContractResultStatusLabel::Failed.to_string(),
            Some(v.cause.cause_label().to_string()),
        ),
        ContractResult::Pending => (pending_label.to_string(), None),
    }
}

impl<T> UpstreamSubscription<T>
where
    T: JournalEvent + 'static,
{
    /// Check contracts and emit progress/stall/final events as needed.
    ///
    /// This is a separate method that the FSM calls when it decides
    /// contract checking is appropriate (e.g., after idle cycles).
    ///
    /// Per-reader contract state is supplied by the caller so that it can live
    /// inside FSM contexts rather than inside the subscription itself.
    pub async fn check_contracts(
        &mut self,
        reader_progress: &mut [ReaderProgress],
    ) -> ContractStatus {
        self.check_contracts_with_mode(reader_progress, ContractCheckMode::Authoritative)
            .await
    }

    pub async fn check_contracts_diagnostics_only(
        &mut self,
        reader_progress: &mut [ReaderProgress],
    ) -> ContractStatus {
        self.check_contracts_with_mode(reader_progress, ContractCheckMode::DiagnosticsOnly)
            .await
    }

    async fn check_contracts_with_mode(
        &mut self,
        reader_progress: &mut [ReaderProgress],
        mode: ContractCheckMode,
    ) -> ContractStatus {
        if self.contract_tracker.is_none() {
            return ContractStatus::Healthy;
        };

        let now = Instant::now();
        let mut status = ContractStatus::Healthy;

        // Check each reader for progress/stalls
        for (index, progress) in reader_progress.iter_mut().enumerate() {
            if progress.final_emitted {
                continue;
            }

            // Diagnostics-only checks are used by stages that intentionally defer
            // authoritative EOF verification (e.g. sinks that flush receipts before
            // running final contract checks). Once a reader has reached terminal EOF,
            // stall detection is meaningless and can produce a spurious
            // `reader_stalled` failure during shutdown.
            if mode == ContractCheckMode::DiagnosticsOnly && self.state.is_reader_eof(index) {
                continue;
            }

            // Continuous contract evaluation (FLOWIP-080r).
            //
            // We run `check_progress` independently of progress emission so that
            // divergence detection and other mid-flight predicates cannot starve
            // behind the `should_emit_progress` gating logic.
            self.check_progress_contracts_for_reader(progress, index, &mut status)
                .await;

            let should_emit_progress = self.should_emit_progress(progress, index, now);

            if should_emit_progress {
                self.emit_progress_for_reader(progress, index, now, &mut status)
                    .await;

                // Check for EOF contract validation
                if mode == ContractCheckMode::Authoritative
                    && self.state.is_reader_eof(index)
                    && !progress.final_emitted
                {
                    self.verify_eof_contracts_for_reader(progress, index, &mut status)
                        .await;
                }
            } else {
                self.check_stall_for_reader(progress, index, now, &mut status)
                    .await;
            }
        }

        status
    }

    async fn check_progress_contracts_for_reader(
        &mut self,
        progress: &mut ReaderProgress,
        index: usize,
        status: &mut ContractStatus,
    ) {
        let Some(tracker) = &self.contract_tracker else {
            return;
        };

        let (Some(reader_stage), Some(chain_slot)) = (
            tracker.reader_stage,
            self.contract_chains.get(index).and_then(|c| c.as_ref()),
        ) else {
            return;
        };

        // Once we've observed EOF for this upstream, EOF verification will run
        // on the same tick (via `should_emit_progress`) and emit definitive
        // pass/fail evidence. Avoid emitting redundant mid-flight heartbeats.
        if self.state.is_reader_eof(index) {
            return;
        }

        // Avoid emitting contract "healthy" heartbeats before we've observed any
        // data events on this edge.
        //
        // In server mode with `startup_mode=manual`, non-source stages are started
        // during materialization and may poll upstreams while sources are still
        // waiting for an external Run. Those stages still emit `ConsumptionProgress`
        // flow signals with `reader_seq=0` (contract mechanism), which can cause
        // downstream subscriptions to observe events even though no data is flowing.
        //
        // Only emitting heartbeats once `reader_seq` has advanced avoids noisy and
        // misleading UI output ("all contracts healthy") when a flow is idle or
        // awaiting manual start.
        let progress_seq = self.progress_seq(progress);

        if progress_seq.0 == 0 {
            return;
        }

        let should_emit_healthy = progress_seq != progress.last_contract_result_seq;

        let results = chain_slot.check_progress_all(progress.stage_id, reader_stage);
        let results_only: Vec<ContractResult> = results.iter().map(|(_, r)| r.clone()).collect();

        // Emit per-contract progress results to the system journal so SSE/UIs can
        // render mid-flight contract health (even when no violations are present).
        //
        // MetricsAggregator also observes ContractResult, so this provides a
        // lightweight heartbeat for long-running flows (e.g. prometheus_100k_demo).
        if let Some(system_journal) = &tracker.system_journal {
            let mut emitted_any = false;
            for (contract_name, result) in &results {
                // Only emit "healthy" heartbeats when we've observed additional
                // data since the last heartbeat. Failed results must always be
                // emitted (and may occur without new data due to control-signal
                // predicates like divergence detection).
                if matches!(result, ContractResult::Pending) && !should_emit_healthy {
                    continue;
                }

                let (status_label, cause_label) =
                    contract_result_labels_for_emission(result, ContractResultStatusLabel::Healthy);

                let result_event = SystemEvent::new(
                    tracker.writer_id,
                    SystemEventType::ContractResult {
                        upstream: progress.stage_id,
                        reader: reader_stage,
                        contract_name: contract_name.clone(),
                        status: status_label,
                        cause: cause_label,
                        reader_seq: Some(progress_seq),
                        advertised_writer_seq: progress.advertised_writer_seq,
                    },
                );
                if let Err(e) = system_journal.append(result_event, None).await {
                    tracing::error!(
                        target: "flowip-105",
                        owner = %self.owner_label,
                        upstream = ?progress.stage_id,
                        reader = ?reader_stage,
                        reader_index = index,
                        contract = %contract_name,
                        error = %e,
                        "Failed to append progress contract result event; skipping emission"
                    );
                } else {
                    emitted_any = true;
                }
            }

            if emitted_any && should_emit_healthy {
                progress.last_contract_result_seq = progress_seq;
            }
        }

        let edge = EdgeContext {
            upstream_stage: progress.stage_id,
            downstream_stage: reader_stage,
            advertised_writer_seq: progress.advertised_writer_seq,
            reader_seq: progress.reader_seq,
        };

        let cb_info = self
            .control_middleware
            .circuit_breaker_contract_info(&progress.stage_id);
        let hints = PolicyHints {
            breaker_mode: cb_info.map(|i| i.mode),
            has_opened_since_registration: cb_info
                .map(|i| i.has_opened_since_registration)
                .unwrap_or(false),
            has_fallback_configured: cb_info.map(|i| i.has_fallback_configured).unwrap_or(false),
        };

        let Some(policy_stack) = self.contract_policies.get(index).and_then(|p| p.as_ref()) else {
            return;
        };

        let raw_failure: Option<(String, ViolationCause)> = results.iter().find_map(|(name, r)| {
            let ContractResult::Failed(v) = r else {
                return None;
            };
            Some((name.clone(), v.cause.clone()))
        });

        let decision = policy_stack.decide(&results_only, &edge, &hints);
        match decision {
            EdgeContractDecision::Pass => {
                // If raw contracts reported a failure but policies overrode it
                // to Pass, emit an override system event.
                if let (Some(system_journal), Some((contract_name, cause))) =
                    (&tracker.system_journal, raw_failure)
                {
                    let override_event = SystemEvent::new(
                        tracker.writer_id,
                        SystemEventType::ContractOverrideByPolicy {
                            upstream: progress.stage_id,
                            reader: reader_stage,
                            contract_name,
                            original_cause: cause,
                            policy: "breaker_aware".to_string(),
                        },
                    );
                    if let Err(e) = system_journal.append(override_event, None).await {
                        tracing::error!(
                            target: "flowip-105",
                            owner = %self.owner_label,
                            upstream = ?progress.stage_id,
                            reader = ?reader_stage,
                            reader_index = index,
                            error = %e,
                            "Failed to append contract override event; skipping emission"
                        );
                    }
                }
            }
            EdgeContractDecision::Fail(cause) => {
                if !matches!(status, ContractStatus::Violated { .. }) {
                    *status = ContractStatus::Violated {
                        upstream: progress.stage_id,
                        cause: cause.clone(),
                    };
                }

                // Emit edge-level contract status to system journal so gating and SSE
                // can react to the violation.
                if let Some(system_journal) = &tracker.system_journal {
                    let status_event = SystemEvent::new(
                        tracker.writer_id,
                        SystemEventType::ContractStatus {
                            upstream: progress.stage_id,
                            reader: reader_stage,
                            pass: false,
                            reader_seq: Some(progress.reader_seq),
                            advertised_writer_seq: progress.advertised_writer_seq,
                            reason: Some(cause.clone()),
                        },
                    );
                    if let Err(e) = system_journal.append(status_event, None).await {
                        tracing::error!(
                            target: "flowip-105",
                            owner = %self.owner_label,
                            upstream = ?progress.stage_id,
                            reader = ?reader_stage,
                            reader_index = index,
                            error = %e,
                            "Failed to append progress contract status; skipping emission"
                        );
                    }
                }

                progress.contract_violated = true;
            }
        }
    }

    async fn emit_progress_for_reader(
        &mut self,
        progress: &mut ReaderProgress,
        index: usize,
        now: Instant,
        status: &mut ContractStatus,
    ) {
        let Some(tracker) = &self.contract_tracker else {
            return;
        };
        let progress_seq = self.progress_seq(progress);
        let progress_last_event_id = self.progress_last_event_id(progress);
        let progress_vector_clock = self.progress_vector_clock(progress);

        // Emit progress event
        let stalled_duration = progress
            .stalled_since
            .map(|s| DurationMs(now.duration_since(s).as_millis() as u64));

        let progress_event = ChainEventFactory::consumption_progress_event(
            tracker.writer_id,
            ConsumptionProgressEventParams {
                reader_seq: progress_seq,
                last_event_id: progress_last_event_id,
                vector_clock: progress_vector_clock.clone(),
                eof_seen: self.state.is_reader_eof(index),
                reader_path: JournalPath(progress.stage_id.to_string()),
                reader_index: JournalIndex(index as u64),
                advertised_writer_seq: progress.advertised_writer_seq,
                advertised_vector_clock: progress_vector_clock,
                stalled_since: stalled_duration,
            },
        );

        match tracker.journal.append(progress_event, None).await {
            Ok(_) => {
                progress.last_progress_seq = progress_seq;
                progress.last_progress_instant = Some(now);
                progress.stalled_since = None;
                progress.consecutive_stall_checks = 0;

                if matches!(status, ContractStatus::Healthy) {
                    *status = ContractStatus::ProgressEmitted;
                }
            }
            Err(e) => {
                tracing::error!(
                    target: "flowip-105",
                    owner = %self.owner_label,
                    upstream = ?progress.stage_id,
                    reader_index = index,
                    error = %e,
                    "Failed to append progress event; skipping state update"
                );
            }
        }
    }

    async fn verify_eof_contracts_for_reader(
        &mut self,
        progress: &mut ReaderProgress,
        index: usize,
        status: &mut ContractStatus,
    ) {
        let Some(tracker) = &self.contract_tracker else {
            return;
        };
        let progress_seq = self.progress_seq(progress);
        let progress_last_event_id = self.progress_last_event_id(progress);
        let progress_vector_clock = self.progress_vector_clock(progress);

        let mut pass = true;
        let mut failure_reason = None;

        // Prefer the new contract framework (TransportContract via
        // ContractChain) when available. This ensures that the
        // same verification logic is used for both runtime gating
        // and contract evidence. Policies are applied on top.
        if let (Some(reader_stage), Some(chain_slot)) = (
            tracker.reader_stage,
            self.contract_chains.get(index).and_then(|c| c.as_ref()),
        ) {
            let results = chain_slot.verify_all(progress.stage_id, reader_stage);
            let results_only: Vec<ContractResult> =
                results.iter().map(|(_, r)| r.clone()).collect();

            // Emit per-contract verification results to the system journal so that
            // MetricsAggregator can derive contract metrics without interfering with
            // pipeline gating (which uses ContractStatus + policies).
            if let Some(system_journal) = &tracker.system_journal {
                for (contract_name, result) in &results {
                    let (status_label, cause_label) = contract_result_labels_for_emission(
                        result,
                        ContractResultStatusLabel::Pending,
                    );

                    let result_event = SystemEvent::new(
                        tracker.writer_id,
                        SystemEventType::ContractResult {
                            upstream: progress.stage_id,
                            reader: reader_stage,
                            contract_name: contract_name.clone(),
                            status: status_label,
                            cause: cause_label,
                            reader_seq: Some(progress_seq),
                            advertised_writer_seq: progress.advertised_writer_seq,
                        },
                    );
                    if let Err(e) = system_journal.append(result_event, None).await {
                        tracing::error!(
                            target: "flowip-105",
                            owner = %self.owner_label,
                            upstream = ?progress.stage_id,
                            reader = ?reader_stage,
                            reader_index = index,
                            contract = %contract_name,
                            error = %e,
                            "Failed to append contract result event; skipping emission"
                        );
                    }
                }
            }

            let edge = EdgeContext {
                upstream_stage: progress.stage_id,
                downstream_stage: reader_stage,
                advertised_writer_seq: progress.advertised_writer_seq,
                reader_seq: progress.reader_seq,
            };

            let cb_info = self
                .control_middleware
                .circuit_breaker_contract_info(&progress.stage_id);
            let hints = PolicyHints {
                breaker_mode: cb_info.map(|i| i.mode),
                has_opened_since_registration: cb_info
                    .map(|i| i.has_opened_since_registration)
                    .unwrap_or(false),
                has_fallback_configured: cb_info
                    .map(|i| i.has_fallback_configured)
                    .unwrap_or(false),
            };

            if let Some(policy_stack) = self.contract_policies.get(index).and_then(|p| p.as_ref()) {
                let raw_failure: Option<(String, ViolationCause)> =
                    results.iter().find_map(|(name, r)| match r {
                        ContractResult::Failed(v) => Some((name.clone(), v.cause.clone())),
                        _ => None,
                    });

                let decision = policy_stack.decide(&results_only, &edge, &hints);

                match decision {
                    EdgeContractDecision::Pass => {
                        pass = true;
                        failure_reason = None;

                        // If raw contracts reported a failure but policies
                        // overrode it to Pass, emit an override system event.
                        if let (
                            Some(system_journal),
                            Some(reader_stage),
                            Some((contract_name, cause)),
                        ) = (&tracker.system_journal, tracker.reader_stage, raw_failure)
                        {
                            let override_event = SystemEvent::new(
                                tracker.writer_id,
                                SystemEventType::ContractOverrideByPolicy {
                                    upstream: progress.stage_id,
                                    reader: reader_stage,
                                    contract_name,
                                    original_cause: cause,
                                    policy: "breaker_aware".to_string(),
                                },
                            );
                            if let Err(e) = system_journal.append(override_event, None).await {
                                tracing::error!(
                                    target: "flowip-105",
                                    owner = %self.owner_label,
                                    upstream = ?progress.stage_id,
                                    reader = ?reader_stage,
                                    reader_index = index,
                                    error = %e,
                                    "Failed to append contract override event; skipping emission"
                                );
                            }
                        }
                    }
                    EdgeContractDecision::Fail(cause) => {
                        pass = false;
                        failure_reason = Some(cause.clone());
                        if !matches!(status, ContractStatus::Violated { .. }) {
                            *status = ContractStatus::Violated {
                                upstream: progress.stage_id,
                                cause: cause.clone(),
                            };
                        }

                        // For transport SeqDivergence, emit a gap event
                        // when we are missing events.
                        if let EventViolationCause::SeqDivergence {
                            advertised: Some(advertised),
                            reader,
                        } = cause
                        {
                            if advertised.0 > reader.0 {
                                let gap_event = ChainEventFactory::consumption_gap_event(
                                    tracker.writer_id,
                                    SeqNo(reader.0 + 1),
                                    advertised,
                                    progress.stage_id,
                                );
                                if let Err(e) = tracker.journal.append(gap_event, None).await {
                                    tracing::error!(
                                        target: "flowip-105",
                                        owner = %self.owner_label,
                                        upstream = ?progress.stage_id,
                                        reader_index = index,
                                        error = %e,
                                        "Failed to append gap event; skipping emission"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        } else if let Some(advertised) = progress.advertised_writer_seq {
            // Legacy fallback: compare advertised vs reader seq.
            if advertised.0 != progress.reader_seq.0 {
                pass = false;
                let cause = EventViolationCause::SeqDivergence {
                    advertised: Some(advertised),
                    reader: progress.reader_seq,
                };
                failure_reason = Some(cause.clone());

                if advertised.0 > progress.reader_seq.0 {
                    // Missing events
                    let gap_event = ChainEventFactory::consumption_gap_event(
                        tracker.writer_id,
                        SeqNo(progress.reader_seq.0 + 1),
                        advertised,
                        progress.stage_id,
                    );
                    if let Err(e) = tracker.journal.append(gap_event, None).await {
                        tracing::error!(
                            target: "flowip-105",
                            owner = %self.owner_label,
                            upstream = ?progress.stage_id,
                            reader_index = index,
                            error = %e,
                            "Failed to append gap event; skipping emission"
                        );
                    }
                }

                if !matches!(status, ContractStatus::Violated { .. }) {
                    *status = ContractStatus::Violated {
                        upstream: progress.stage_id,
                        cause,
                    };
                }
            }
        }

        // Capture reason for downstream system status before moving it into events
        let status_reason = failure_reason.clone();

        // Emit an explicit at-least-once violation event when we detect
        // a SeqDivergence (advertised > reader). This complements the
        // generic ContractStatus system event and makes at-least-once
        // violations first-class in the data journal for observability.
        if !pass {
            if let Some(EventViolationCause::SeqDivergence { advertised, reader }) =
                failure_reason.clone()
            {
                let violation_event = ChainEventFactory::at_least_once_violation_event(
                    tracker.writer_id,
                    progress.stage_id,
                    EventViolationCause::SeqDivergence { advertised, reader },
                    progress.reader_seq,
                    progress.advertised_writer_seq,
                );

                if let Err(e) = tracker.journal.append(violation_event, None).await {
                    tracing::error!(
                        target: "flowip-105",
                        owner = %self.owner_label,
                        upstream = ?progress.stage_id,
                        reader_index = index,
                        error = %e,
                        "Failed to append at_least_once_violation event; skipping emission"
                    );
                }
            }
        }

        // Emit final event
        let final_event = ChainEventFactory::consumption_final_event(
            tracker.writer_id,
            ConsumptionFinalEventParams {
                pass,
                consumed_count: Count(progress_seq.0),
                expected_count: None,
                eof_seen: true,
                last_event_id: progress_last_event_id,
                reader_seq: progress_seq,
                advertised_writer_seq: progress.advertised_writer_seq,
                advertised_vector_clock: progress_vector_clock,
                failure_reason,
            },
        );

        let final_append_ok = match tracker.journal.append(final_event, None).await {
            Ok(_) => true,
            Err(e) => {
                tracing::error!(
                    target: "flowip-105",
                    owner = %self.owner_label,
                    upstream = ?progress.stage_id,
                    reader_index = index,
                    error = %e,
                    "Failed to append final event; skipping state update"
                );
                false
            }
        };

        // Emit contract status to system journal (if available)
        let mut status_append_ok = true;
        if let (Some(system_journal), Some(reader_stage)) =
            (&tracker.system_journal, tracker.reader_stage)
        {
            let status_event = SystemEvent::new(
                tracker.writer_id,
                SystemEventType::ContractStatus {
                    upstream: progress.stage_id,
                    reader: reader_stage,
                    pass,
                    reader_seq: Some(progress_seq),
                    advertised_writer_seq: progress.advertised_writer_seq,
                    reason: status_reason,
                },
            );
            if let Err(e) = system_journal.append(status_event, None).await {
                status_append_ok = false;
                tracing::error!(
                    target: "flowip-105",
                    owner = %self.owner_label,
                    upstream = ?progress.stage_id,
                    reader = ?reader_stage,
                    reader_index = index,
                    error = %e,
                    "Failed to append contract status; skipping state update"
                );
            }
        }

        if final_append_ok && status_append_ok {
            progress.final_emitted = true;
            progress.contract_violated = !pass;
        }
    }

    async fn check_stall_for_reader(
        &mut self,
        progress: &mut ReaderProgress,
        index: usize,
        now: Instant,
        status: &mut ContractStatus,
    ) {
        let Some(tracker) = &self.contract_tracker else {
            return;
        };

        // Check for stalls
        let Some(last) = progress.last_read_instant else {
            return;
        };

        let elapsed = now.duration_since(last).as_millis() as u64;

        if elapsed >= tracker.config.stall_threshold.0 {
            progress.consecutive_stall_checks += 1;

            if progress.consecutive_stall_checks >= tracker.config.stall_checks_before_emit
                && progress.stalled_since.is_none()
            {
                if tracker.config.stall_cooloff.0 > 0 {
                    if let Some(last_emitted) = progress.last_stall_emitted_instant {
                        let cooloff_elapsed =
                            now.duration_since(last_emitted).as_millis() as u64;
                        if cooloff_elapsed < tracker.config.stall_cooloff.0 {
                            if !matches!(status, ContractStatus::Violated { .. }) {
                                *status = ContractStatus::Stalled(progress.stage_id);
                            }
                            return;
                        }
                    }
                }

                let stall_since_candidate = Some(last);
                let stalled_duration = DurationMs(elapsed);

                let stalled_event = ChainEventFactory::reader_stalled_event(
                    tracker.writer_id,
                    progress.stage_id,
                    stalled_duration,
                );

                let stalled_append_ok = match tracker.journal.append(stalled_event, None).await {
                    Ok(_) => true,
                    Err(e) => {
                        tracing::error!(
                            target: "flowip-105",
                            owner = %self.owner_label,
                            upstream = ?progress.stage_id,
                            reader_index = index,
                            error = %e,
                            "Failed to append stalled event; skipping state update"
                        );
                        false
                    }
                };

                // IMPORTANT: A stall is a liveness signal, not a transport contract violation.
                //
                // Historically we emitted `SystemEventType::ContractStatus { pass: false, reason:
                // reader_stalled }` here. PipelineSupervisor treats *any* ContractStatus failure
                // as a gating contract violation and aborts the flow (including during drain),
                // which has proven wildly non-actionable for long/variable-latency stages
                // (AI calls, network jitter) and creates nondeterministic demo failures.
                //
                // We still emit `FlowControlPayload::ReaderStalled` into the stage journal for
                // observability, and we still return `ContractStatus::Stalled` to allow stage
                // supervisors to log warnings, but we must not poison the global contract barrier.

                if !matches!(status, ContractStatus::Violated { .. }) {
                    *status = ContractStatus::Stalled(progress.stage_id);
                }

                if stalled_append_ok {
                    progress.stalled_since = stall_since_candidate;
                    progress.last_stall_emitted_instant = Some(now);
                }
            }
        } else {
            progress.consecutive_stall_checks = 0;
        }
    }

    fn should_emit_progress(&self, progress: &ReaderProgress, index: usize, now: Instant) -> bool {
        let Some(tracker) = &self.contract_tracker else {
            return false;
        };

        let delta_events = self
            .progress_seq(progress)
            .0
            .saturating_sub(progress.last_progress_seq.0);
        let time_elapsed = progress
            .last_progress_instant
            .map(|t| now.duration_since(t).as_millis() as u64)
            .unwrap_or(0);

        delta_events >= tracker.config.progress_min_events.0
            || time_elapsed >= tracker.config.progress_max_interval.0
            || self.state.is_reader_eof(index)
    }

    /// Track that this stage has emitted an output event
    pub fn track_output_event(&mut self) {
        if let Some(tracker) = &mut self.contract_tracker {
            tracker.output_events_written.0 += 1;
            tracing::trace!(
                "Tracked output event, total: {}",
                tracker.output_events_written.0
            );
        }
    }

    /// Hint to FSM about whether contract check might be useful.
    ///
    /// Uses per-reader timestamps from FSM-owned contract state to decide if
    /// enough time has passed since the last check.
    pub fn should_check_contracts(&self, reader_progress: &[ReaderProgress]) -> bool {
        if let Some(tracker) = &self.contract_tracker {
            let now = Instant::now();

            // Check if enough time has passed since last check
            for progress in reader_progress {
                let Some(last) = progress.last_progress_instant else {
                    return true;
                };

                let elapsed = now.duration_since(last).as_millis() as u64;
                if elapsed >= tracker.config.progress_max_interval.0 / 2 {
                    return true;
                }
            }
        }
        false
    }

    /// Convenience method that combines `should_check_contracts` and
    /// `check_contracts` into a single call.
    ///
    /// Returns `None` when the check interval has not elapsed yet (no work
    /// done). Returns `Some(result)` when a check was performed.
    pub async fn maybe_check_contracts(
        &mut self,
        reader_progress: &mut [ReaderProgress],
    ) -> Option<ContractStatus> {
        if self.should_check_contracts(reader_progress) {
            Some(self.check_contracts(reader_progress).await)
        } else {
            None
        }
    }

    /// Supervisor-driven contract checking tick (FLOWIP-080r).
    ///
    /// This avoids starvation under sustained load by allowing supervisors to
    /// call contract checks from the `PollResult::Event` path using a
    /// wall-clock tick that is independent of `PollResult::NoEvents`.
    ///
    /// `last_contract_check` is stored in the supervisor's running-state
    /// context (per subscription).
    pub async fn maybe_check_contracts_tick(
        &mut self,
        reader_progress: &mut [ReaderProgress],
        last_contract_check: &mut Option<Instant>,
    ) -> Option<ContractStatus> {
        let Some(tracker) = &self.contract_tracker else {
            return None;
        };

        let now = Instant::now();
        let tick_ms = (tracker.config.progress_max_interval.0 / 2).max(1);

        let due = match last_contract_check {
            Some(last) => now.duration_since(*last).as_millis() as u64 >= tick_ms,
            None => true,
        };

        if !due {
            return None;
        }

        *last_contract_check = Some(now);
        Some(self.check_contracts(reader_progress).await)
    }

    pub async fn maybe_check_contracts_tick_diagnostics_only(
        &mut self,
        reader_progress: &mut [ReaderProgress],
        last_contract_check: &mut Option<Instant>,
    ) -> Option<ContractStatus> {
        let Some(tracker) = &self.contract_tracker else {
            return None;
        };

        let now = Instant::now();
        let tick_ms = (tracker.config.progress_max_interval.0 / 2).max(1);

        let due = match last_contract_check {
            Some(last) => now.duration_since(*last).as_millis() as u64 >= tick_ms,
            None => true,
        };

        if !due {
            return None;
        }

        *last_contract_check = Some(now);
        Some(self.check_contracts_diagnostics_only(reader_progress).await)
    }
}
