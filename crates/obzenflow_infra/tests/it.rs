// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../../tests/replay_testkit"]
mod replay_testkit {
    mod bootstrap;
    mod count_deliveries;
    mod delivered_order;
    mod discard;
    mod journal;
    mod manifest;
    mod ordered_stage_journal;
    mod prefix_stable;
    mod reattaching_source;
    mod run_dir;
    mod stage_journal;
    mod unsynced_application;
    mod unsynced_candidate;
    mod unsynced_config;
    mod unsynced_context;
    pub use self::{
        bootstrap::*, count_deliveries::*, discard::*, journal::*, manifest::*, prefix_stable::*,
        reattaching_source::*, run_dir::*, stage_journal::*, unsynced_application::*,
        unsynced_candidate::*, unsynced_context::*,
    };
}

#[path = "support/liveness_observations.rs"]
mod liveness_observations;

mod ancestry_fan_in_join;
mod app_settings_build_test;
mod app_settings_effect_identity_test;
mod bootstrap_verb_archive_guard_test;
mod causal_provenance;
mod contract_state_alignment_test;
mod debug_disk_journal;
mod journal_integration;
mod journal_parity;
mod live_reference_join;
mod liveness_fan_out;
mod liveness_hung_handler_cancel;
mod liveness_join_fan_in;
mod liveness_slow_but_healthy;
mod liveness_stalled_transition;
mod metrics_integration;
mod middleware_topology_slot_collision_test;
mod no_topology_dto_drift;
mod observer_interception_non_interference_test;
mod observer_occurrence_semantics_test;
mod replay_determinism;
mod resume_linear_flow_test;
mod resume_mixed_sources_test;
mod resume_of_resume_test;
mod run_substrate_capability_test;
mod runtime_pipeline_journals;
mod seq_ordered_fan_in_test;
mod stateful_emit_interval;
mod stateful_patterns_test;
mod stateful_primitives_test;
mod stateless_simple_test;
mod supervision_journals;
mod topology_wiring;
