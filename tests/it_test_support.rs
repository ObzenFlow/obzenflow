// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit {
    mod count_deliveries;
    mod delivered_order;
    mod failures;
    mod journal;
    mod manifest;
    mod ordered_stage_journal;
    mod prefix_stable;
    mod run_dir;
    mod same_delivered_order;
    mod stage_journal;
    mod stage_writer;
    mod telemetry_rows;
    mod transport_signature;
    mod unsynced_application;
    mod unsynced_candidate;
    mod unsynced_config;
    mod unsynced_context;
    mod verdicts;
    pub use self::{
        count_deliveries::*, delivered_order::*, failures::*, journal::*, manifest::*,
        ordered_stage_journal::*, prefix_stable::*, run_dir::*, same_delivered_order::*,
        stage_journal::*, stage_writer::*, telemetry_rows::*, transport_signature::*,
        unsynced_application::*, unsynced_candidate::*, unsynced_context::*, verdicts::*,
    };
}

mod csv_sink_termination_contract_test;
mod cycle_convergence_eof_gating_test;
mod cycle_unified_guard_test;
mod deterministic_fan_in_replay_test;
mod divergence_mid_flight_abort_test;
mod effectful_fan_in_envelope_test;
mod join_error_passthrough_identity_test;
mod join_phase_transition_replay_test;
mod lineage_policy_flow_test;
mod metrics_exporter_integration_test;
mod public_sink_author_fixture_test;
mod rate_limiter_integration_test;
mod replay_truncated_eof_fidelity_test;
mod source_polling_contract_test;
mod source_rate_limiter_interruption_test;
mod stateful_emit_per_window_test;
mod stateful_metrics_integration_test;
