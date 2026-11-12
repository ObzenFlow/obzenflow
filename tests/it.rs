// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod replay_testkit {
    mod bootstrap;
    mod count_deliveries;
    mod discard;
    mod eof_clock;
    mod failures;
    mod journal;
    mod manifest;
    mod ordered_journal;
    mod run_dir;
    mod stage_journal;
    mod stage_writer;
    mod unsynced_application;
    mod unsynced_candidate;
    mod unsynced_config;
    mod unsynced_context;
    mod verdicts;
    pub use self::{
        bootstrap::*, count_deliveries::*, discard::*, eof_clock::*, failures::*, journal::*,
        manifest::*, ordered_journal::*, run_dir::*, stage_journal::*, stage_writer::*,
        unsynced_application::*, unsynced_candidate::*, unsynced_context::*, verdicts::*,
    };
}

mod exported_jsonl {
    mod chain;
    #[cfg(all(feature = "web-host", feature = "prometheus"))]
    mod commitment;
    #[cfg(all(feature = "web-host", feature = "prometheus"))]
    mod without_observations;
    pub use self::chain::*;
    #[cfg(all(feature = "web-host", feature = "prometheus"))]
    pub use self::{commitment::*, without_observations::*};
}

#[path = "support/fake_ai.rs"]
pub mod fake_ai;

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod payment_gateway;

#[path = "../examples/csv_demo_support_sla/support.rs"]
pub mod csv_demo;

mod advanced_tests;
mod ai_map_reduce_build_typed_flow_regression;
mod ai_map_reduce_join_composition_test;
pub mod aml_overnight_monitoring_journal_test;
mod archive_manifest_epoch_gate_test;
mod as_of_join_test;
mod async_finite_source_stage_test;
mod async_infinite_source_stage_test;
mod async_transform_surface_source_guard_test;
mod basic_streaming;
mod catch_up_machine_source_guard_test;
mod circuit_breaker_composition_e2e_test;
mod circuit_breaker_metrics_e2e_test;
mod circuit_breaker_sink_delivery_test;
mod concat_merge_test;
mod config_selected_sink_application_test;
mod core_derive_package_boundary_test;
mod csv_connectors_roundtrip_test;
mod csv_sink_application_conformance_test;
mod csv_source_lifecycle_test;
mod cycle_guard_fan_out_test;
mod delivery_contract_wiring_integration_test;
mod dropped_events_test;
mod effect_control_composition_build_test;
mod effect_fact_containment_test;
mod effect_observer_attachment_scope_test;
mod effect_replay_suppression_test;
mod effectful_stateful_boundary_delivery_test;
mod effective_config_manifest_test;
mod flow_level_middleware_slot_removal_source_guard_test;
mod flow_macro_arm_regression_test;
mod flow_materialisation_build_order_test;
mod flow_materialisation_preflight_test;
mod generated_effect_declaration_source_guard_test;
mod handler_path_source_gate_test;
mod handler_shell_retirement_source_guard_test;
mod hn_ai_digest_demo_does_not_import_rig;
mod hn_ai_digest_demo_prompt_regression_test;
mod hn_story_decoder_test;
pub mod http_ingestion_piggy_bank_journal_test;
mod http_pull_facade_helpers_test;
mod http_pull_source_witness_test;
mod input_alignment_topology_test;
mod keyed_accumulator_lineage_test;
mod legacy_topology_retry_tombstone_test;
mod managed_web_host_boundary_test;
mod middleware_attachment_position_source_guard_test;
mod middleware_factory_context_injection_test;
mod middleware_hook_binding_e2e_test;
mod monitoring_architecture_test;
mod monitoring_injection_test;
mod notify_behavior_test;
mod one_shot_inference_effect_journal_test;
mod payment_gateway_retry_journal_test;
mod payment_gateway_validation_journal_test;
mod placeholder_handlers_lifecycle_test;
mod plain_stateful_witness_source_guard_test;
mod policy_middleware_guard_test;
mod postgres_connector_package_boundary_test;
mod product_catalog_enrichment;
mod prometheus_example_test;
mod recorded_order_source_guard_test;
mod replay_provenance_labelling_test;
mod replay_safe_determinism_lint;
mod replay_verification_divergence_test;
mod replay_verification_error_lane_test;
mod replay_verification_golden_path_test;
mod replay_verification_honesty_test;
mod replay_verification_multi_source_test;
mod replay_verification_prefix_test;
mod replay_verification_scale_test;
mod request_effect_authoring_source_gate_test;
mod retired_observer_product_surface_test;
mod root_facade_source_shape_test;
mod sink_observation_retention_test;
mod sink_operation_failure_chain_test;
mod sink_witness_source_guard_test;
mod source_diagnostic_reasons_test;
mod source_failure_contract_test;
mod source_output_ergonomics_guard_test;
mod source_witness_source_guard_test;
mod stage_metrics_test;
mod stall_detection_liveness_test;
mod standalone_ai_source_authority_test;
mod stateful_accumulator_catalog_test;
mod stateful_pre_error_bypass_test;
mod stateful_state_copies_test;
mod stateful_stop_lifecycle_test;
mod supervisor_construction_guard_test;
mod synchronous_transform_witness_authority_test;
mod synchronous_transform_witness_source_guard_test;
mod then_live_join_test;
mod transform_stage_regression_test;
mod typed_join_journal_parity_test;
mod typed_join_witness_source_guard_test;
mod typed_sink_delivery_parity_test;
mod typed_source_journal_parity_test;
mod typed_stateful_journal_parity_test;
mod typed_sync_transform_journal_parity_test;
mod volume_tests;
