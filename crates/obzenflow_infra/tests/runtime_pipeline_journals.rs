// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime pipeline scenarios composed with Infra memory journals; metrics
//! collector conformance also exercises the canonical disk backend.
//! Every scenario remains a separately discovered test with its original name.

use obzenflow_core::FlowId;
use obzenflow_infra::journal::MemoryJournalFactory;
use obzenflow_runtime::journal::FlowJournalFactory;
use obzenflow_runtime::testing::pipeline;

fn journals() -> Box<dyn FlowJournalFactory> {
    Box::new(MemoryJournalFactory::new(FlowId::new()))
}

#[tokio::test]
async fn controlled_journal_preserves_causality_groups_and_live_readers() {
    pipeline::controlled_journal_preserves_causality_groups_and_live_readers(journals).await;
}

#[tokio::test]
async fn internal_input_admission_is_phase_specific_even_with_satisfied_guards() {
    pipeline::internal_input_admission_is_phase_specific_even_with_satisfied_guards(journals).await;
}

#[tokio::test]
async fn settlement_in_an_eligible_phase_still_requires_its_resource_predicate() {
    pipeline::settlement_in_an_eligible_phase_still_requires_its_resource_predicate(journals).await;
}

#[tokio::test]
async fn execution_deadlines_require_admission_and_expiry_without_mutating_on_rejection() {
    pipeline::execution_deadlines_require_admission_and_expiry_without_mutating_on_rejection(
        journals,
    )
    .await;
}

#[tokio::test]
async fn metrics_deadline_admission_and_guards_use_the_actual_acknowledgement() {
    pipeline::metrics_deadline_admission_and_guards_use_the_actual_acknowledgement(journals).await;
}

#[tokio::test]
async fn finished_has_no_outgoing_inputs_including_controls_and_journal_rows() {
    pipeline::finished_has_no_outgoing_inputs_including_controls_and_journal_rows(journals).await;
}

#[tokio::test]
async fn controls_and_operational_failure_follow_the_approved_successor_matrix() {
    pipeline::controls_and_operational_failure_follow_the_approved_successor_matrix(journals).await;
}

#[tokio::test]
async fn unrelated_rows_only_advance_observation_in_every_live_phase() {
    pipeline::unrelated_rows_only_advance_observation_in_every_live_phase(journals).await;
}

#[tokio::test]
async fn declared_contract_feeds_do_not_fall_back_on_unknown_payload_or_role() {
    pipeline::declared_contract_feeds_do_not_fall_back_on_unknown_payload_or_role(journals).await;
}

#[tokio::test]
async fn empty_topology_cannot_announce_or_consume_all_stage_completion() {
    pipeline::empty_topology_cannot_announce_or_consume_all_stage_completion(journals).await;
}

#[tokio::test]
async fn genuine_early_stage_completion_can_settle_without_start_admission() {
    pipeline::genuine_early_stage_completion_can_settle_without_start_admission(journals).await;
}

#[tokio::test]
async fn graceful_stop_during_startup_preserves_running_then_drain_authority() {
    pipeline::graceful_stop_during_startup_preserves_running_then_drain_authority(journals).await;
}

#[tokio::test]
async fn terminal_and_final_marker_require_the_authorised_writer_and_identity() {
    pipeline::terminal_and_final_marker_require_the_authorised_writer_and_identity(journals).await;
}

#[tokio::test]
async fn final_marker_failure_finishes_with_retained_error_without_another_marker() {
    pipeline::final_marker_failure_finishes_with_retained_error_without_another_marker(journals)
        .await;
}

#[test]
fn contract_keys_for_stage_pair_returns_all_matching_logical_feeds() {
    pipeline::contract_keys_for_stage_pair_returns_all_matching_logical_feeds(journals);
}

#[test]
fn contract_keys_for_contract_event_returns_matching_logical_feed() {
    pipeline::contract_keys_for_contract_event_returns_matching_logical_feed(journals);
}

#[test]
fn contract_keys_for_stage_pair_falls_back_for_legacy_stage_pair_status() {
    pipeline::contract_keys_for_stage_pair_falls_back_for_legacy_stage_pair_status(journals);
}

#[tokio::test]
async fn repeated_graceful_controls_have_no_actions_and_cancel_folds_once() {
    pipeline::repeated_graceful_controls_have_no_actions_and_cancel_folds_once(journals).await;
}

#[tokio::test]
async fn repeated_abort_controls_preserve_the_first_failure_without_new_work() {
    pipeline::repeated_abort_controls_preserve_the_first_failure_without_new_work(journals).await;
}

#[tokio::test]
async fn readiness_and_start_consume_committed_pipeline_facts() {
    pipeline::readiness_and_start_consume_committed_pipeline_facts(journals).await;
}

#[tokio::test]
async fn pre_ready_and_duplicate_start_controls_do_not_authorise_sources() {
    pipeline::pre_ready_and_duplicate_start_controls_do_not_authorise_sources(journals).await;
}

#[tokio::test]
async fn readiness_failure_and_cancel_stay_pending_until_settlement() {
    pipeline::readiness_failure_and_cancel_stay_pending_until_settlement(journals).await;
}

#[tokio::test]
async fn every_private_phase_has_a_truthful_public_projection() {
    pipeline::every_private_phase_has_a_truthful_public_projection(journals).await;
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
async fn original_terminal_acknowledgement_expires_metrics_before_delayed_journal_consumption() {
    pipeline::original_terminal_acknowledgement_expires_metrics_before_delayed_journal_consumption(
        journals,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_metrics_skips_when_metrics_not_started() {
    pipeline::drain_metrics_skips_when_metrics_not_started(journals).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn late_metrics_bootstrap_reads_all_physical_inputs_without_stage_eof() {
    pipeline::late_metrics_bootstrap_reads_all_physical_inputs_without_stage_eof(journals).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_cleanup_keeps_metrics_alive_until_the_terminal_fact() {
    pipeline::stage_cleanup_keeps_metrics_alive_until_the_terminal_fact(journals).await;
}

#[tokio::test]
async fn expired_graceful_stop_aborts_and_joins_stalled_stage_without_fresh_cleanup_budget() {
    pipeline::expired_graceful_stop_aborts_and_joins_stalled_stage_without_fresh_cleanup_budget(
        journals,
    )
    .await;
}

#[tokio::test]
async fn abort_publication_rejection_cannot_skip_siblings_or_resume_commands() {
    pipeline::abort_publication_rejection_cannot_skip_siblings_or_resume_commands(journals).await;
}

#[tokio::test]
async fn terminal_publication_retains_its_outcome_while_servicing_graceful_expiry() {
    pipeline::terminal_publication_retains_its_outcome_while_servicing_graceful_expiry(journals)
        .await;
}

#[tokio::test]
async fn supervisor_join_waits_for_terminal_publication_and_propagates_append_failure() {
    pipeline::supervisor_join_waits_for_terminal_publication_and_propagates_append_failure(
        journals,
    )
    .await;
}

#[tokio::test]
async fn unexpected_errors_preserve_failed_outcomes_before_and_during_stop() {
    pipeline::unexpected_errors_preserve_failed_outcomes_before_and_during_stop(journals).await;
}

#[tokio::test]
async fn pre_execution_teardown_is_explicit_and_failures_stay_selected() {
    pipeline::pre_execution_teardown_is_explicit_and_failures_stay_selected(journals).await;
}

#[tokio::test]
async fn cancellation_catches_up_late_producer_failure_before_selecting_terminal() {
    pipeline::cancellation_catches_up_late_producer_failure_before_selecting_terminal(journals)
        .await;
}

#[tokio::test]
async fn final_marker_coalesces_late_controls_without_restarting_finalisation() {
    pipeline::final_marker_coalesces_late_controls_without_restarting_finalisation(journals).await;
}

#[tokio::test]
async fn manual_ready_for_run_publishes_state_and_waits_for_external_run() {
    pipeline::manual_ready_for_run_publishes_state_and_waits_for_external_run(journals).await;
}

#[tokio::test]
async fn auto_ready_for_run_emits_run_and_reaches_running() {
    pipeline::auto_ready_for_run_emits_run_and_reaches_running(journals).await;
}

#[tokio::test]
async fn materializing_stage_count_mismatch_transitions_to_failed_without_panic() {
    pipeline::materializing_stage_count_mismatch_transitions_to_failed_without_panic(journals)
        .await;
}

#[tokio::test]
async fn materialized_to_ready_for_run_publishes_post_transition_state() {
    pipeline::materialized_to_ready_for_run_publishes_post_transition_state(journals).await;
}

#[tokio::test]
async fn running_state_requires_committed_source_running_after_start() {
    pipeline::running_state_requires_committed_source_running_after_start(journals).await;
}

#[tokio::test]
async fn early_run_queued_in_materialized_is_consumed_before_ready_for_run() {
    pipeline::early_run_queued_in_materialized_is_consumed_before_ready_for_run(journals).await;
}

#[tokio::test]
async fn empty_topology_fails_through_the_canonical_fsm() {
    pipeline::empty_topology_fails_through_the_canonical_fsm(journals).await;
}

#[tokio::test]
async fn stage_failures_and_cancellations_before_readiness_use_journal_evidence() {
    pipeline::stage_failures_and_cancellations_before_readiness_use_journal_evidence(journals)
        .await;
}

#[tokio::test]
async fn materialisation_reconsiders_readiness_facts_already_consumed() {
    pipeline::materialisation_reconsiders_readiness_facts_already_consumed(journals).await;
}

#[tokio::test]
async fn graceful_deadline_bounds_a_stalled_source_control_send() {
    pipeline::graceful_deadline_bounds_a_stalled_source_control_send(journals).await;
}

#[tokio::test]
async fn persistent_controls_cannot_starve_command_delivery_or_stage_joins() {
    pipeline::persistent_controls_cannot_starve_command_delivery_or_stage_joins(journals).await;
}

#[tokio::test]
async fn queued_controls_cannot_starve_bootstrap_or_automatic_start() {
    pipeline::queued_controls_cannot_starve_bootstrap_or_automatic_start(journals).await;
}

#[tokio::test]
async fn ready_stage_joins_cannot_starve_other_resource_completions() {
    pipeline::ready_stage_joins_cannot_starve_other_resource_completions(journals).await;
}

#[tokio::test]
async fn completed_action_failure_gateway_does_not_report_the_original_error_again() {
    pipeline::completed_action_failure_gateway_does_not_report_the_original_error_again(journals)
        .await;
}

#[tokio::test]
async fn pending_journal_read_survives_controls_and_gets_bounded_service() {
    pipeline::pending_journal_read_survives_controls_and_gets_bounded_service(journals).await;
}

#[tokio::test]
async fn expired_stop_is_dispatched_before_a_full_external_control_queue() {
    pipeline::expired_stop_is_dispatched_before_a_full_external_control_queue(journals).await;
}

#[tokio::test]
async fn subscription_or_metrics_preparation_failure_joins_every_supplied_stage() {
    pipeline::subscription_or_metrics_preparation_failure_joins_every_supplied_stage(journals)
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_tail_refresh_keeps_counts_current_without_advancing_input_coverage() {
    obzenflow_runtime::testing::metrics::metrics_tail_refresh_keeps_counts_current_without_advancing_input_coverage(journals).await;
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
async fn metrics_cache_bounds_negative_search_and_reuses_examined_heads() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_cache_bounds_negative_search_and_reuses_examined_heads).await;
}

#[tokio::test]
async fn metrics_snapshot_selection_survives_both_refresh_failure_orders() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_snapshot_selection_survives_both_refresh_failure_orders).await;
}

#[tokio::test]
async fn metrics_capped_search_keeps_sequential_selection_and_search_uncertainty() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_capped_search_keeps_sequential_selection_and_search_uncertainty).await;
}

#[tokio::test]
async fn metrics_tail_results_bind_to_the_window_actually_examined() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_tail_results_bind_to_the_window_actually_examined).await;
}

#[tokio::test]
async fn metrics_snapshot_identity_handles_mixed_writers_groups_and_rail_precedence() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_snapshot_identity_handles_mixed_writers_groups_and_rail_precedence).await;
}

#[tokio::test]
async fn metrics_batches_preserve_prefix_errors_and_require_fresh_positive_ends() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_batches_preserve_prefix_errors_and_require_fresh_positive_ends).await;
}

#[tokio::test(start_paused = true)]
async fn metrics_rotation_coalesces_exports_and_spaces_from_acknowledged_publication() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_rotation_coalesces_exports_and_spaces_from_acknowledged_publication).await;
}

#[tokio::test]
async fn metrics_physical_completion_folds_all_rails_through_the_current_terminal() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_physical_completion_folds_all_rails_through_the_current_terminal).await;
}

#[tokio::test]
async fn metrics_final_refresh_inconsistency_fails_without_successful_drained() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_final_refresh_inconsistency_fails_without_successful_drained).await;
}

#[tokio::test]
async fn metrics_pending_read_cancellation_never_publishes_drained() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_pending_read_cancellation_never_publishes_drained).await;
}

#[tokio::test]
async fn metrics_watermarks_exclude_each_forwarded_control_and_error_witness() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_watermarks_exclude_each_forwarded_control_and_error_witness).await;
}

#[tokio::test(start_paused = true)]
async fn metrics_batch_quantum_keeps_pending_reads_and_finalisation_does_not_wait_for_export() {
    metrics_backends(obzenflow_runtime::testing::metrics::metrics_batch_quantum_keeps_pending_reads_and_finalisation_does_not_wait_for_export).await;
}
