// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime lifecycle scenarios composed with a supplied Infra journal factory.

pub use crate::pipeline::builder::tests::metrics_preparation_failure_joins_every_supplied_stage;
pub use crate::pipeline::tests::admission::{
    child_acknowledgement_carries_causality_to_parent_publication,
    controlled_journal_preserves_causality_groups_and_live_readers,
    initialization_requires_all_child_acknowledgements,
};
pub use crate::pipeline::tests::metrics::{
    drain_metrics_skips_when_metrics_not_started,
    dropping_pipeline_context_cancels_its_metrics_supervisor,
    late_metrics_bootstrap_selects_current_values_without_stage_eof,
    metrics_budget_starts_after_terminal_publication_without_readback,
    metrics_preparation_is_passive_and_cancellation_prevents_late_installation,
    parent_panic_retains_metrics_publication_until_repeated_flow_joins_finish,
    stage_cleanup_keeps_metrics_alive_until_the_terminal_fact,
};
pub use crate::pipeline::tests::shutdown::{
    contract_failure_cause_survives_child_observation_order,
    expired_graceful_stop_aborts_and_joins_without_a_fresh_cleanup_budget,
    failure_remains_observable_while_child_cleanup_is_blocked,
    terminal_publication_is_owned_until_settlement_and_failure_is_retained,
};
pub use crate::pipeline::tests::startup::{
    blocked_ready_publication_exposes_pending_state_and_preserves_cancellation,
    blocked_registration_preserves_cancellation_and_cleanup,
    startup_waits_for_achieved_transitions_with_zero_child_journal_reads,
};
pub use crate::pipeline::tests::supervisor::persistent_controls_cannot_starve_acknowledgements_or_child_termination;
