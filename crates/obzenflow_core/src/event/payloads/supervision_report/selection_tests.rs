// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::event::{ChainEventFactory, ChainPayload, JournalRecord, SupervisorRecord};
use crate::JournalPayload;
use serde_json::{json, Value};

fn parity(value: Value, selected: bool) {
    let execution: ExecutionPayload =
        serde_json::from_value(value.clone()).unwrap_or_else(|error| panic!("{value}: {error}"));
    assert_eq!(execution.is_supervision_candidate(), selected, "{value}");
    let mut record = JournalRecord::new(
        crate::JournalWriterId::new(),
        ChainEventFactory::data_event(crate::StageId::new().into(), "fixture", json!({})),
    );
    // Projection is independent of storage admission; these are borrowed payload
    // eligibility checks. Storage parity/commitment is exercised by provider tests.
    record.payload = ChainPayload::Execution(execution);
    assert_eq!(record.payload.is_supervision_candidate(), selected);
    assert_eq!(
        SupervisorRecord::from_chain(record).is_some(),
        selected,
        "{value}"
    );
}

#[test]
fn eligibility_matches_projection_for_every_report_family_and_lifecycle() {
    let stage = crate::StageId::new();
    for value in [
        json!({"execution_type":"replay_lifecycle","replay_event":"completed","replayed_count":0,"skipped_count":0,"duration_ms":1}),
        json!({"execution_type":"supervisor_registered","descriptor":{"name":"child","kind":"transform","supervision":"handler_supervised"}}),
        json!({"execution_type":"supervisor_command_discarded","supervisor":"child","terminal_state":"completed","command":"drain","disposition":"obsolete_control"}),
        json!({"execution_type":"source_cleanup_failed","stage_id":stage,"stage_name":"child","error":"failure"}),
        json!({"execution_type":"contract_status","upstream":stage,"reader":stage,"pass":true}),
        json!({"execution_type":"contract_result","upstream":stage,"reader":stage,"contract_name":"sequence","status":"passed"}),
        json!({"execution_type":"ingress_refusal","ingress_key":"key","stage_id":stage,"stage_key":"child","reason":"not_ready","attempt_seq":1,"request_count":1,"event_count":1,"batch_count":0,"http_status":503}),
    ] {
        parity(value, true);
    }
    for state in [
        "running",
        "draining",
        "drained",
        "completed",
        "cancelled",
        "failed",
    ] {
        parity(
            json!({"execution_type":"stage_lifecycle","stage_state":state,"stage_id":stage,"reason":"requested","error":"failure"}),
            true,
        );
    }
}

#[test]
fn middleware_classification_selects_transitions_but_not_ordinary_execution_work() {
    for (value, selected) in [
        (
            json!({"execution_type":"circuit_breaker","action":"opened","cooldown_ms":1,"error_rate":1.0,"failure_count":1,"trigger":"failure_rate","observed_calls":1}),
            true,
        ),
        (
            json!({"execution_type":"circuit_breaker","action":"closed","success_count":1,"recovery_duration_ms":1}),
            true,
        ),
        (
            json!({"execution_type":"circuit_breaker","action":"half_open","test_request_count":1}),
            true,
        ),
        (
            json!({"execution_type":"circuit_breaker","action":"state_changed","from_state":"closed","to_state":"open","timestamp":1}),
            true,
        ),
        (
            json!({"execution_type":"circuit_breaker","action":"rejected"}),
            false,
        ),
        (
            json!({"execution_type":"rate_limiter","action":"mode_change","mode_from":"normal","mode_to":"limiting","limit_rate":1.0}),
            true,
        ),
        (
            json!({"execution_type":"rate_limiter","action":"config_changed","old_rate":1.0,"new_rate":2.0}),
            true,
        ),
        (
            json!({"execution_type":"rate_limiter","action":"delayed","delay_ms":1,"current_rate":2.0,"limit_rate":1.0}),
            false,
        ),
        (
            json!({"execution_type":"metrics_coordination","metrics_event":"ready","exporter_count":1}),
            false,
        ),
        (
            json!({"execution_type":"accumulator_progress","inputs_since_last_report":1}),
            false,
        ),
        (
            json!({"execution_type":"join_reference_progress","reference_inputs_since_last_report":1}),
            false,
        ),
    ] {
        parity(value, selected);
    }
    let fact =
        ChainPayload::Fact(json!({"execution_type":"stage_lifecycle","stage_state":"running"}));
    assert!(
        !fact.is_supervision_candidate(),
        "application JSON cannot set protected classification"
    );
    assert!(
        SystemPayload::MetricsCoordination(crate::event::MetricsCoordinationEvent::Ready)
            .is_supervision_candidate()
    );
}
