// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime pipeline scenarios composed with Infra memory journals; metrics
//! collector conformance also exercises the canonical disk backend.
//! Lifecycle scenarios observe child acknowledgements and physical termination.

use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::FlowId;
use obzenflow_infra::journal::MemoryJournalFactory;
use obzenflow_runtime::testing::{metrics, pipeline};

fn journals() -> Box<dyn FlowJournalFactory> {
    Box::new(MemoryJournalFactory::new(FlowId::new()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn application_abort_does_not_turn_owned_child_cancellation_into_failure() {
    pipeline::application_abort_does_not_turn_owned_child_cancellation_into_failure(journals).await;
}

#[tokio::test]
async fn controlled_journal_preserves_causality_groups_and_live_readers() {
    pipeline::controlled_journal_preserves_causality_groups_and_live_readers(journals).await;
}

#[tokio::test]
async fn initialization_requires_all_child_acknowledgements() {
    pipeline::initialization_requires_all_child_acknowledgements(journals).await;
}

#[tokio::test]
async fn child_acknowledgement_carries_causality_to_parent_publication() {
    pipeline::child_acknowledgement_carries_causality_to_parent_publication(journals).await;
}

#[tokio::test]
async fn constructed_pipeline_keeps_startup_mode_when_bootstrap_changes() {
    pipeline::constructed_pipeline_keeps_startup_mode_when_bootstrap_changes(journals).await;
}

#[tokio::test]
async fn startup_waits_for_achieved_transitions_with_zero_child_journal_reads() {
    pipeline::startup_waits_for_achieved_transitions_with_zero_child_journal_reads(journals).await;
}

#[tokio::test]
async fn blocked_registration_preserves_cancellation_and_cleanup() {
    pipeline::blocked_registration_preserves_cancellation_and_cleanup(journals).await;
}

#[tokio::test]
async fn blocked_ready_publication_exposes_pending_state_and_preserves_cancellation() {
    pipeline::blocked_ready_publication_exposes_pending_state_and_preserves_cancellation(journals)
        .await;
}

#[tokio::test]
async fn contract_failure_cause_survives_child_observation_order() {
    pipeline::contract_failure_cause_survives_child_observation_order(journals).await;
}

#[tokio::test]
async fn failure_remains_observable_while_child_cleanup_is_blocked() {
    pipeline::failure_remains_observable_while_child_cleanup_is_blocked(journals).await;
}

#[tokio::test]
async fn terminal_publication_is_owned_until_settlement_and_failure_is_retained() {
    pipeline::terminal_publication_is_owned_until_settlement_and_failure_is_retained(journals)
        .await;
}

#[tokio::test]
async fn expired_graceful_stop_aborts_and_joins_without_a_fresh_cleanup_budget() {
    pipeline::expired_graceful_stop_aborts_and_joins_without_a_fresh_cleanup_budget(journals).await;
}

#[tokio::test]
async fn persistent_controls_cannot_starve_acknowledgements_or_child_termination() {
    pipeline::persistent_controls_cannot_starve_acknowledgements_or_child_termination(journals)
        .await;
}

#[tokio::test]
async fn dropping_pipeline_context_cancels_its_metrics_supervisor() {
    pipeline::dropping_pipeline_context_cancels_its_metrics_supervisor(journals).await;
}

#[tokio::test]
async fn parent_panic_retains_metrics_publication_until_repeated_flow_joins_finish() {
    pipeline::parent_panic_retains_metrics_publication_until_repeated_flow_joins_finish(journals)
        .await;
}

#[tokio::test]
async fn metrics_preparation_is_passive_and_cancellation_prevents_late_installation() {
    pipeline::metrics_preparation_is_passive_and_cancellation_prevents_late_installation(journals)
        .await;
}

#[tokio::test]
async fn metrics_budget_starts_after_terminal_publication_without_readback() {
    pipeline::metrics_budget_starts_after_terminal_publication_without_readback(journals).await;
}

#[tokio::test]
async fn drain_metrics_skips_when_metrics_not_started() {
    pipeline::drain_metrics_skips_when_metrics_not_started(journals).await;
}

#[tokio::test]
async fn late_metrics_bootstrap_selects_current_values_without_stage_eof() {
    pipeline::late_metrics_bootstrap_selects_current_values_without_stage_eof(journals).await;
}

#[tokio::test]
async fn stage_cleanup_keeps_metrics_alive_until_the_terminal_fact() {
    pipeline::stage_cleanup_keeps_metrics_alive_until_the_terminal_fact(journals).await;
}

#[tokio::test]
async fn metrics_preparation_failure_joins_every_supplied_stage() {
    pipeline::metrics_preparation_failure_joins_every_supplied_stage(journals).await;
}

async fn metrics_backends<F, Fut>(scenario: F)
where
    F: Fn(Box<dyn FlowJournalFactory>) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    for disk in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let factory: Box<dyn FlowJournalFactory> = if disk {
            Box::new(
                obzenflow_infra::journal::DiskJournalFactory::new(
                    dir.path().to_path_buf(),
                    FlowId::new(),
                )
                .unwrap(),
            )
        } else {
            journals()
        };
        println!("metrics backend: {}", if disk { "disk" } else { "memory" });
        scenario(factory).await;
    }
}

#[tokio::test]
async fn metrics_tail_overwrites_and_preserves_sparse_families() {
    metrics_backends(
        obzenflow_runtime::testing::metrics::metrics_tail_overwrites_and_preserves_sparse_families,
    )
    .await;
}

#[tokio::test]
async fn metrics_refresh_failures_retain_values_and_exports_do_no_reads() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_refresh_failures_retain_values_and_exports_do_no_reads).await;
}

#[tokio::test]
async fn metrics_pending_refresh_does_not_block_publication_or_other_journals() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_pending_refresh_does_not_block_publication_or_other_journals).await;
}

#[tokio::test]
async fn metrics_cancellation_stops_owned_readers_without_drained() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_cancellation_stops_owned_readers_without_drained).await;
}

#[tokio::test]
async fn metrics_tail_identity_and_accounting_are_idempotent() {
    metrics_backends(
        obzenflow_runtime::testing::metrics::metrics_tail_identity_and_accounting_are_idempotent,
    )
    .await;
}

#[tokio::test]
async fn metrics_terminal_accounting_survives_without_optional_packets() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_terminal_accounting_survives_without_optional_packets).await;
}

#[tokio::test]
async fn metrics_exports_do_not_create_observations() {
    metrics_backends(
        obzenflow_runtime::testing::metrics::metrics_exports_do_not_create_observations,
    )
    .await;
}

#[tokio::test]
async fn metrics_optional_measurements_do_not_invent_missing_accounting() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_optional_measurements_do_not_invent_missing_accounting).await;
}

#[tokio::test]
async fn metrics_manual_export_uses_the_live_control_receiver() {
    metrics_backends(
        obzenflow_runtime::testing::metrics::metrics_manual_export_uses_the_live_control_receiver,
    )
    .await;
}

#[tokio::test]
async fn metrics_exports_settle_accepted_requests_in_their_own_journal() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_exports_settle_accepted_requests_in_their_own_journal).await;
}

#[tokio::test]
async fn metrics_drain_during_export_settles_before_final_refresh() {
    metrics_backends(metrics::metrics_drain_during_export_settles_before_final_refresh).await;
}

#[tokio::test]
async fn metrics_folds_check_eligibility_before_causal_incorporation() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_folds_check_eligibility_before_causal_incorporation).await;
}
