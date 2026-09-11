// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Private-FSM journal scenarios, invoked by Infra tests with its real backend.
//! Each factory call creates an isolated flow for a scenario or parameterised case.
//! Only the test entry points are public; lifecycle types remain crate-private.

pub use crate::pipeline::builder::tests::subscription_or_metrics_preparation_failure_joins_every_supplied_stage;
pub use crate::pipeline::tests::admission::{
    controlled_journal_preserves_causality_groups_and_live_readers,
    controls_and_operational_failure_follow_the_approved_successor_matrix,
    declared_contract_feeds_do_not_fall_back_on_unknown_payload_or_role,
    empty_topology_cannot_announce_or_consume_all_stage_completion,
    execution_deadlines_require_admission_and_expiry_without_mutating_on_rejection,
    final_marker_failure_finishes_with_retained_error_without_another_marker,
    finished_has_no_outgoing_inputs_including_controls_and_journal_rows,
    genuine_early_stage_completion_can_settle_without_start_admission,
    graceful_stop_during_startup_preserves_running_then_drain_authority,
    internal_input_admission_is_phase_specific_even_with_satisfied_guards,
    metrics_deadline_admission_and_guards_use_the_actual_acknowledgement,
    settlement_in_an_eligible_phase_still_requires_its_resource_predicate,
    terminal_and_final_marker_require_the_authorised_writer_and_identity,
    unrelated_rows_only_advance_observation_in_every_live_phase,
};
pub use crate::pipeline::tests::fsm::{
    contract_keys_for_contract_event_returns_matching_logical_feed,
    contract_keys_for_stage_pair_falls_back_for_legacy_stage_pair_status,
    contract_keys_for_stage_pair_returns_all_matching_logical_feeds,
    every_private_phase_has_a_truthful_public_projection,
    pre_ready_and_duplicate_start_controls_do_not_authorise_sources,
    readiness_and_start_consume_committed_pipeline_facts,
    readiness_failure_and_cancel_stay_pending_until_settlement,
    repeated_abort_controls_preserve_the_first_failure_without_new_work,
    repeated_graceful_controls_have_no_actions_and_cancel_folds_once,
};
pub use crate::pipeline::tests::metrics::{
    drain_metrics_skips_when_metrics_not_started,
    dropping_pipeline_context_cancels_its_metrics_supervisor,
    late_metrics_bootstrap_reads_all_physical_inputs_without_stage_eof,
    metrics_preparation_is_passive_and_cancellation_prevents_late_installation,
    original_terminal_acknowledgement_expires_metrics_before_delayed_journal_consumption,
    parent_panic_retains_metrics_publication_until_repeated_flow_joins_finish,
    stage_cleanup_keeps_metrics_alive_until_the_terminal_fact,
};
pub use crate::pipeline::tests::shutdown::{
    abort_publication_rejection_cannot_skip_siblings_or_resume_commands,
    cancellation_catches_up_late_producer_failure_before_selecting_terminal,
    expired_graceful_stop_aborts_and_joins_stalled_stage_without_fresh_cleanup_budget,
    final_marker_coalesces_late_controls_without_restarting_finalisation,
    pre_execution_teardown_is_explicit_and_failures_stay_selected,
    supervisor_join_waits_for_terminal_publication_and_propagates_append_failure,
    terminal_publication_retains_its_outcome_while_servicing_graceful_expiry,
    unexpected_errors_preserve_failed_outcomes_before_and_during_stop,
};
pub use crate::pipeline::tests::startup::{
    auto_ready_for_run_emits_run_and_reaches_running,
    early_run_queued_in_materialized_is_consumed_before_ready_for_run,
    empty_topology_fails_through_the_canonical_fsm,
    manual_ready_for_run_publishes_state_and_waits_for_external_run,
    materialisation_reconsiders_readiness_facts_already_consumed,
    materialized_to_ready_for_run_publishes_post_transition_state,
    materializing_stage_count_mismatch_transitions_to_failed_without_panic,
    running_state_requires_committed_source_running_after_start,
    stage_failures_and_cancellations_before_readiness_use_journal_evidence,
};
pub use crate::pipeline::tests::supervisor::{
    completed_action_failure_gateway_does_not_report_the_original_error_again,
    expired_stop_is_dispatched_before_a_full_external_control_queue,
    graceful_deadline_bounds_a_stalled_source_control_send,
    pending_journal_read_survives_controls_and_gets_bounded_service,
    persistent_controls_cannot_starve_command_delivery_or_stage_joins,
    queued_controls_cannot_starve_bootstrap_or_automatic_start,
    ready_stage_joins_cannot_starve_other_resource_completions,
};
