// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "test_support/exported_jsonl.rs"]
mod exported_jsonl;
#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod support;

use support::{gateway, proof};

use obzenflow_core::event::payloads::effect_payload::EFFECT_RECORD_EVENT_TYPE;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerHealthClassification, MiddlewareLifecycle,
    ObservabilityPayload,
};
use obzenflow_core::event::{
    ChainEvent, ChainEventContent, EffectFailureCause, EffectOutcomePayload, EffectRecord,
};
use obzenflow_runtime::effects::EffectCursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn only_run(root: &Path) -> PathBuf {
    let runs: Vec<_> = std::fs::read_dir(root.join("flows"))
        .expect("flow journal directory should exist")
        .map(|entry| {
            entry
                .expect("run directory entry should be readable")
                .path()
        })
        .filter(|path| path.is_dir())
        .collect();
    assert_eq!(runs.len(), 1, "the proof root should contain one run");
    runs.into_iter().next().unwrap()
}

fn exported_run(run: &Path, output: &Path) -> String {
    obzenflow_infra::journal::disk::inspect::export_jsonl(run, Some(output))
        .expect("proof journal should export");
    std::fs::read_to_string(output).expect("exported proof journal should be readable")
}

fn exported_events(jsonl: &str) -> impl Iterator<Item = serde_json::Value> + '_ {
    jsonl.lines().map(|line| {
        serde_json::from_str(line).expect("each exported journal row should be valid JSON")
    })
}

fn data_event_count(jsonl: &str, event_type: &str) -> usize {
    exported_events(jsonl)
        .filter(|row| {
            row.pointer("/event/content/content_type")
                .and_then(|value| value.as_str())
                == Some("data")
                && row
                    .pointer("/event/content/event_type")
                    .and_then(|value| value.as_str())
                    == Some(event_type)
        })
        .count()
}

fn payment_effect_outcome_group_count(jsonl: &str) -> usize {
    exported_events(jsonl)
        .filter_map(|row| {
            (row.pointer("/event/effect_provenance/descriptor/effect_type")
                .and_then(|value| value.as_str())
                == Some("payment.authorize"))
            .then(|| {
                row.pointer("/event/effect_provenance/group_id")
                    .and_then(|value| value.as_str())
                    .expect("payment effect fact should carry its outcome group")
                    .to_string()
            })
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn payment_terminal_group_counters(jsonl: &str) -> (u64, u64) {
    exported_events(jsonl)
        .filter(|row| {
            row.pointer("/event/runtime_context/effect_circuit_breakers")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|breakers| {
                    breakers.iter().any(|breaker| {
                        breaker
                            .get("effect_type")
                            .and_then(serde_json::Value::as_str)
                            == Some("payment.authorize")
                    })
                })
        })
        .fold((0, 0), |(committed, failed), row| {
            (
                committed.max(
                    row.pointer("/event/runtime_context/terminal_groups_committed_total")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0),
                ),
                failed.max(
                    row.pointer("/event/runtime_context/terminal_group_commit_failures_total")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0),
                ),
            )
        })
}

fn exported_chain_events(jsonl: &str) -> impl Iterator<Item = ChainEvent> + '_ {
    exported_jsonl::chain_events(jsonl).into_iter()
}

#[test]
#[should_panic(expected = "is neither a ChainEvent nor a SystemEvent")]
fn exported_record_decoder_fails_loud_on_an_unknown_row() {
    let _ = exported_jsonl::chain_events(r#"{"event":{"unknown":"shape"}}"#);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttemptSettlement {
    cursor: EffectCursor,
    attempt: u32,
    health: CircuitBreakerHealthClassification,
}

fn attempt_settlements(jsonl: &str) -> Vec<AttemptSettlement> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::AttemptSettled {
                    cursor,
                    attempt,
                    health_classification,
                    ..
                }),
            )) => Some(AttemptSettlement {
                cursor,
                attempt,
                health: health_classification,
            }),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetrySchedule {
    cursor: EffectCursor,
    next_attempt: u32,
}

fn retry_schedules(jsonl: &str) -> Vec<RetrySchedule> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RetryScheduled {
                    cursor,
                    next_attempt,
                    ..
                }),
            )) => Some(RetrySchedule {
                cursor,
                next_attempt,
            }),
            _ => None,
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetrySuccess {
    cursor: EffectCursor,
    total_attempts: u32,
    terminal_classification: CircuitBreakerHealthClassification,
}

fn retry_successes(jsonl: &str) -> Vec<RetrySuccess> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RetrySucceeded {
                    cursor,
                    total_attempts,
                    terminal_classification,
                }),
            )) => Some(RetrySuccess {
                cursor,
                total_attempts,
                terminal_classification,
            }),
            _ => None,
        })
        .collect()
}

fn retry_terminal_failure_count(jsonl: &str) -> usize {
    exported_chain_events(jsonl)
        .filter(|event| {
            matches!(
                event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::RetryExhausted { .. }
                            | CircuitBreakerEvent::RetryStoppedNonRetryable { .. }
                    )
                ))
            )
        })
        .count()
}

#[derive(Debug, Clone, PartialEq)]
struct TerminalFailure {
    cursor: EffectCursor,
    cause: Option<EffectFailureCause>,
}

fn payment_terminal_failures(jsonl: &str) -> Vec<TerminalFailure> {
    let mut failures: Vec<_> = exported_chain_events(jsonl)
        .filter_map(|event| match event.content {
            ChainEventContent::Data {
                event_type,
                payload,
            } if event_type == EFFECT_RECORD_EVENT_TYPE => {
                let record: EffectRecord = serde_json::from_value(payload)
                    .expect("framework effect-record payload should decode");
                assert_eq!(
                    event
                        .effect_provenance
                        .as_ref()
                        .map(|provenance| &provenance.cursor),
                    Some(&record.cursor),
                    "effect record and provenance must identify the same cursor"
                );
                if record.descriptor.effect_type.as_str() != "payment.authorize" {
                    return None;
                }
                match record.outcome {
                    EffectOutcomePayload::Failed { cause, .. } => Some(TerminalFailure {
                        cursor: record.cursor,
                        cause,
                    }),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect();
    failures.sort_by_key(|failure| {
        (
            failure.cursor.input_seq.get(),
            failure.cursor.effect_ordinal.get(),
        )
    });
    failures
}

#[derive(Debug)]
struct RecoveryCompletion {
    cursor: EffectCursor,
    total_attempts: u32,
    backoff_elapsed_ms: u64,
    recovery_elapsed_ms: u64,
}

fn recovery_completions(jsonl: &str) -> Vec<RecoveryCompletion> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::CircuitBreaker(CircuitBreakerEvent::RecoveryCompleted {
                    cursor,
                    total_attempts,
                    backoff_elapsed_ms,
                    recovery_elapsed_ms,
                }),
            )) => Some(RecoveryCompletion {
                cursor,
                total_attempts,
                backoff_elapsed_ms,
                recovery_elapsed_ms,
            }),
            _ => None,
        })
        .collect()
}

fn payment_outcome_cursors(jsonl: &str) -> std::collections::HashSet<EffectCursor> {
    exported_chain_events(jsonl)
        .filter_map(|event| {
            event
                .effect_provenance
                .filter(|provenance| provenance.descriptor.effect_type == "payment.authorize")
                .map(|provenance| provenance.cursor)
        })
        .collect()
}

fn assert_one_recovery_completion_per_payment_cursor(jsonl: &str) -> Vec<RecoveryCompletion> {
    let completions = recovery_completions(jsonl);
    let completion_cursors: std::collections::HashSet<_> = completions
        .iter()
        .map(|completion| completion.cursor.clone())
        .collect();
    assert_eq!(completion_cursors.len(), completions.len());
    assert_eq!(completion_cursors, payment_outcome_cursors(jsonl));
    completions
}

#[derive(Debug, PartialEq)]
struct BreakerCounts {
    requests: u64,
    successes: u64,
    failures: u64,
    slow: u64,
    rejections: u64,
    opened: u64,
    state: f64,
}

fn last_payment_breaker_counts(jsonl: &str) -> BreakerCounts {
    let mut last = None;
    for row in exported_events(jsonl) {
        let Some(breakers) = row
            .pointer("/event/runtime_context/effect_circuit_breakers")
            .and_then(|value| value.as_array())
        else {
            continue;
        };
        for breaker in breakers {
            if breaker.get("effect_type").and_then(|value| value.as_str())
                == Some("payment.authorize")
            {
                last = Some(BreakerCounts {
                    requests: breaker["cb_requests_total"]
                        .as_u64()
                        .expect("request count"),
                    successes: breaker["cb_successes_total"]
                        .as_u64()
                        .expect("success count"),
                    failures: breaker["cb_failures_total"]
                        .as_u64()
                        .expect("failure count"),
                    slow: breaker["cb_slow_total"].as_u64().expect("slow count"),
                    rejections: breaker["cb_rejections_total"]
                        .as_u64()
                        .expect("rejection count"),
                    opened: breaker["cb_opened_total"].as_u64().expect("open count"),
                    state: breaker["cb_state"].as_f64().expect("breaker state"),
                });
            }
        }
    }
    last.expect("payment breaker metrics should appear in the exported journal")
}

#[derive(Debug, PartialEq)]
struct LimiterCounts {
    events: u64,
    delayed: u64,
    tokens: f64,
    delay_seconds: f64,
}

fn payment_limiter_counts(event: &ChainEvent) -> Option<LimiterCounts> {
    event
        .runtime_context
        .as_ref()?
        .effect_rate_limiters
        .iter()
        .find(|limiter| limiter.effect_type == "payment.authorize")
        .map(|limiter| LimiterCounts {
            events: limiter.rl_events_total,
            delayed: limiter.rl_delayed_total,
            tokens: limiter.rl_tokens_consumed_total,
            delay_seconds: limiter.rl_delay_seconds_total,
        })
}

fn last_payment_limiter_counts(jsonl: &str) -> LimiterCounts {
    let mut last = None;
    for event in exported_chain_events(jsonl) {
        if let Some(snapshot) = payment_limiter_counts(&event) {
            last = Some(snapshot);
        }
    }
    last.expect("payment limiter metrics should appear in the exported journal")
}

fn payment_failure_limiter_snapshots(jsonl: &str) -> Vec<(EffectCursor, LimiterCounts)> {
    exported_chain_events(jsonl)
        .filter_map(|event| {
            let ChainEventContent::Data {
                ref event_type,
                ref payload,
            } = event.content
            else {
                return None;
            };
            if event_type != EFFECT_RECORD_EVENT_TYPE {
                return None;
            }
            let record: EffectRecord = serde_json::from_value(payload.clone())
                .expect("framework effect-record payload should decode");
            if record.descriptor.effect_type.as_str() != "payment.authorize"
                || !matches!(record.outcome, EffectOutcomePayload::Failed { .. })
            {
                return None;
            }
            let snapshot = payment_limiter_counts(&event)
                .expect("live payment failure record should carry its limiter snapshot");
            Some((record.cursor, snapshot))
        })
        .collect()
}

#[test]
fn payment_gateway_retry_journal_test() {
    // Canonical healthy profile: five dependency calls at one per second.
    // The journal proves limiter pacing is real and that fast successes never
    // increment the breaker's slow-call counter.
    let healthy_root = tempfile::tempdir().expect("healthy journal root");
    let healthy_proof = Arc::new(gateway::GatewayRetryProof::healthy(false));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_retry_journal_test"])
        .run_blocking(proof::build_flow_for_profile(
            proof::RetryProofProfile::Healthy,
            Some(healthy_proof.clone()),
            healthy_root.path().to_path_buf(),
        ))
        .expect("healthy proof flow should complete");
    assert_eq!(healthy_proof.calls(), 5);
    let healthy = exported_run(
        &only_run(healthy_root.path()),
        &healthy_root.path().join("healthy.jsonl"),
    );
    assert_eq!(data_event_count(&healthy, "payment.authorized.v1"), 5);
    assert_eq!(
        data_event_count(&healthy, "payment.authorization_unavailable.v1"),
        0
    );
    assert_eq!(payment_effect_outcome_group_count(&healthy), 5);
    assert_eq!(payment_terminal_group_counters(&healthy), (5, 0));
    assert_eq!(
        last_payment_breaker_counts(&healthy),
        BreakerCounts {
            requests: 5,
            successes: 5,
            failures: 0,
            slow: 0,
            rejections: 0,
            opened: 0,
            state: 0.0,
        }
    );
    let healthy_limiter = last_payment_limiter_counts(&healthy);
    assert_eq!(healthy_limiter.events, 5);
    assert_eq!(healthy_limiter.tokens, 5.0);
    assert!(
        healthy_limiter.delayed >= 4,
        "all calls after the initial burst token should be paced: {healthy_limiter:?}"
    );
    assert!(
        healthy_limiter.delay_seconds > 0.0,
        "healthy profile must prove non-zero limiter delay: {healthy_limiter:?}"
    );
    let healthy_attempts = attempt_settlements(&healthy);
    assert_eq!(healthy_attempts.len(), 5);
    assert!(healthy_attempts.iter().all(|attempt| {
        attempt.attempt == 1 && attempt.health == CircuitBreakerHealthClassification::Success
    }));
    let healthy_completions = assert_one_recovery_completion_per_payment_cursor(&healthy);
    assert_eq!(healthy_completions.len(), 5);
    assert!(healthy_completions.iter().all(|completion| {
        completion.total_attempts == 1
            && completion.backoff_elapsed_ms == 0
            && completion.recovery_elapsed_ms >= completion.backoff_elapsed_ms
    }));

    let control_root = tempfile::tempdir().expect("control journal root");
    let control_proof = Arc::new(gateway::GatewayRetryProof::new(false));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_retry_journal_test"])
        .run_blocking(proof::build_flow_for_profile(
            proof::RetryProofProfile::Control,
            Some(control_proof.clone()),
            control_root.path().to_path_buf(),
        ))
        .expect("control proof flow should complete");
    assert_eq!(control_proof.calls(), 1);
    let control = exported_run(
        &only_run(control_root.path()),
        &control_root.path().join("control.jsonl"),
    );
    assert_eq!(
        data_event_count(&control, "payment.authorization_unavailable.v1"),
        1
    );
    assert_eq!(data_event_count(&control, "payment.authorized.v1"), 0);
    assert_eq!(payment_effect_outcome_group_count(&control), 1);
    assert_eq!(payment_terminal_group_counters(&control), (1, 0));
    assert_eq!(
        last_payment_breaker_counts(&control),
        BreakerCounts {
            requests: 1,
            successes: 0,
            failures: 1,
            slow: 0,
            rejections: 0,
            opened: 0,
            state: 0.0,
        }
    );
    assert_eq!(last_payment_limiter_counts(&control).events, 1);
    assert_eq!(last_payment_limiter_counts(&control).tokens, 1.0);
    let control_attempts = attempt_settlements(&control);
    assert_eq!(control_attempts.len(), 1);
    assert_eq!(control_attempts[0].attempt, 1);
    assert_eq!(
        control_attempts[0].health,
        CircuitBreakerHealthClassification::TransientFailure
    );
    assert!(retry_schedules(&control).is_empty());
    assert!(retry_successes(&control).is_empty());
    assert_eq!(retry_terminal_failure_count(&control), 0);
    let control_completions = assert_one_recovery_completion_per_payment_cursor(&control);
    assert_eq!(control_completions.len(), 1);
    assert_eq!(control_completions[0].total_attempts, 1);
    assert_eq!(control_completions[0].backoff_elapsed_ms, 0);

    let treatment_root = tempfile::tempdir().expect("treatment journal root");
    let treatment_proof = Arc::new(gateway::GatewayRetryProof::new(false));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_retry_journal_test"])
        .run_blocking(proof::build_flow_for_profile(
            proof::RetryProofProfile::Treatment,
            Some(treatment_proof.clone()),
            treatment_root.path().to_path_buf(),
        ))
        .expect("treatment proof flow should complete");
    assert_eq!(treatment_proof.calls(), 3);
    let treatment_run = only_run(treatment_root.path());
    let treatment = exported_run(
        &treatment_run,
        &treatment_root.path().join("treatment.jsonl"),
    );
    assert_eq!(data_event_count(&treatment, "payment.authorized.v1"), 1);
    assert_eq!(
        data_event_count(&treatment, "payment.authorization_unavailable.v1"),
        0
    );
    assert_eq!(payment_effect_outcome_group_count(&treatment), 1);
    assert_eq!(payment_terminal_group_counters(&treatment), (1, 0));
    assert_eq!(
        last_payment_breaker_counts(&treatment),
        BreakerCounts {
            requests: 3,
            successes: 1,
            failures: 2,
            slow: 0,
            rejections: 0,
            opened: 0,
            state: 0.0,
        }
    );
    assert_eq!(last_payment_limiter_counts(&treatment).events, 3);
    assert_eq!(last_payment_limiter_counts(&treatment).tokens, 3.0);
    let treatment_attempts = attempt_settlements(&treatment);
    assert_eq!(treatment_attempts.len(), 3);
    let treatment_cursor = treatment_attempts[0].cursor.clone();
    assert!(
        treatment_attempts
            .iter()
            .all(|attempt| attempt.cursor == treatment_cursor),
        "all physical attempts must remain correlated to one logical effect cursor"
    );
    assert_eq!(
        treatment_attempts
            .iter()
            .map(|attempt| (attempt.attempt, attempt.health))
            .collect::<Vec<_>>(),
        vec![
            (1, CircuitBreakerHealthClassification::TransientFailure),
            (2, CircuitBreakerHealthClassification::TransientFailure),
            (3, CircuitBreakerHealthClassification::Success),
        ],
        "breaker health must record failure, failure, success in physical-call order"
    );
    assert_eq!(
        retry_schedules(&treatment),
        vec![
            RetrySchedule {
                cursor: treatment_cursor.clone(),
                next_attempt: 2,
            },
            RetrySchedule {
                cursor: treatment_cursor.clone(),
                next_attempt: 3,
            },
        ]
    );
    assert_eq!(
        retry_successes(&treatment),
        vec![RetrySuccess {
            cursor: treatment_cursor.clone(),
            total_attempts: 3,
            terminal_classification: CircuitBreakerHealthClassification::Success,
        }]
    );
    assert_eq!(retry_terminal_failure_count(&treatment), 0);
    assert!(
        !treatment.contains("gateway_timeout_simulated"),
        "intermediate failures must not become terminal journal records"
    );
    let treatment_completions = assert_one_recovery_completion_per_payment_cursor(&treatment);
    assert_eq!(treatment_completions.len(), 1);
    assert_eq!(treatment_completions[0].cursor, treatment_cursor);
    assert_eq!(treatment_completions[0].total_attempts, 3);
    assert!(treatment_completions[0].backoff_elapsed_ms > 0);
    assert!(
        treatment_completions[0].recovery_elapsed_ms >= treatment_completions[0].backoff_elapsed_ms
    );

    let replay_root = tempfile::tempdir().expect("replay journal root");
    let replay_proof = Arc::new(gateway::GatewayRetryProof::new(true));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args([
            "payment_gateway_retry_journal_test",
            "--replay-from",
            treatment_run
                .to_str()
                .expect("treatment path should be UTF-8"),
            "--verify",
        ])
        .run_blocking(proof::build_flow_for_profile(
            proof::RetryProofProfile::Treatment,
            Some(replay_proof.clone()),
            replay_root.path().to_path_buf(),
        ))
        .expect("strict replay should verify without live gateway I/O");
    assert_eq!(replay_proof.calls(), 0);
    let replay = exported_run(
        &only_run(replay_root.path()),
        &replay_root.path().join("replay.jsonl"),
    );
    assert_eq!(data_event_count(&replay, "payment.authorized.v1"), 1);
    assert_eq!(
        data_event_count(&replay, "payment.authorization_unavailable.v1"),
        0
    );
    assert_eq!(payment_effect_outcome_group_count(&replay), 1);
    assert_eq!(payment_terminal_group_counters(&replay), (0, 0));
    assert!(retry_schedules(&replay).is_empty());
    assert!(retry_successes(&replay).is_empty());
    assert_eq!(retry_terminal_failure_count(&replay), 0);
    assert!(
        attempt_settlements(&replay).is_empty(),
        "strict replay must not emit fresh physical-attempt evidence"
    );
    assert!(recovery_completions(&replay).is_empty());

    // Five failures satisfy the tutorial breaker's count window; the sixth
    // logical effect is rejected without a dependency call or limiter permit.
    let open_root = tempfile::tempdir().expect("open-rejection journal root");
    let open_proof = Arc::new(gateway::GatewayRetryProof::always_fail(false));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_retry_journal_test"])
        .run_blocking(proof::build_flow_for_profile(
            proof::RetryProofProfile::OpenRejection,
            Some(open_proof.clone()),
            open_root.path().to_path_buf(),
        ))
        .expect("open-rejection proof flow should complete");
    assert_eq!(open_proof.calls(), 5);
    let open_run = only_run(open_root.path());
    let open = exported_run(&open_run, &open_root.path().join("open.jsonl"));
    assert_eq!(
        data_event_count(&open, "payment.authorization_unavailable.v1"),
        6
    );
    assert_eq!(data_event_count(&open, "payment.authorized.v1"), 0);
    assert_eq!(payment_terminal_group_counters(&open), (6, 0));
    assert_eq!(
        last_payment_breaker_counts(&open),
        BreakerCounts {
            requests: 5,
            successes: 0,
            failures: 5,
            slow: 0,
            rejections: 1,
            opened: 1,
            state: 1.0,
        }
    );
    assert_eq!(last_payment_limiter_counts(&open).events, 5);
    assert_eq!(last_payment_limiter_counts(&open).tokens, 5.0);
    let open_attempts = attempt_settlements(&open);
    assert_eq!(open_attempts.len(), 5);
    assert!(open_attempts.iter().all(|attempt| {
        attempt.attempt == 1
            && attempt.health == CircuitBreakerHealthClassification::TransientFailure
    }));
    assert!(retry_schedules(&open).is_empty());
    assert!(retry_successes(&open).is_empty());
    assert_eq!(retry_terminal_failure_count(&open), 0);
    let open_failures = payment_terminal_failures(&open);
    assert_eq!(open_failures.len(), 6);
    let rejected_failure = open_failures
        .iter()
        .find(|failure| {
            failure
                .cause
                .as_ref()
                .is_some_and(|cause| cause.code.as_str() == "circuit_open")
        })
        .expect("one failed effect record must carry the circuit_open cause");
    assert_eq!(
        rejected_failure
            .cause
            .as_ref()
            .expect("rejection cause")
            .source
            .as_str(),
        "circuit_breaker"
    );
    assert!(
        open_attempts
            .iter()
            .all(|attempt| attempt.cursor != rejected_failure.cursor),
        "the rejected cursor must have no physical-attempt row"
    );
    let open_completions = assert_one_recovery_completion_per_payment_cursor(&open);
    assert_eq!(open_completions.len(), 6);
    let rejected_completion = open_completions
        .iter()
        .find(|completion| completion.cursor == rejected_failure.cursor)
        .expect("the rejected effect record must share its cursor with recovery completion");
    assert_eq!(rejected_completion.total_attempts, 0);
    assert_eq!(rejected_completion.backoff_elapsed_ms, 0);
    let failure_limiter_snapshots = payment_failure_limiter_snapshots(&open);
    let rejected_snapshot_index = failure_limiter_snapshots
        .iter()
        .position(|(cursor, _)| cursor == &rejected_failure.cursor)
        .expect("the rejected cursor must carry a limiter snapshot");
    let (_, preceding_snapshot) = failure_limiter_snapshots
        .get(
            rejected_snapshot_index
                .checked_sub(1)
                .expect("open rejection must follow the physical failures that opened it"),
        )
        .expect("preceding physical failure snapshot");
    let (_, rejected_snapshot) = &failure_limiter_snapshots[rejected_snapshot_index];
    assert_eq!(
        rejected_snapshot, preceding_snapshot,
        "an open rejection must not mutate any per-effect limiter counter"
    );
    assert_eq!(
        last_payment_limiter_counts(&open).tokens,
        open_attempts.len() as f64,
        "the cursor rejected by the open breaker must not commit a limiter permit"
    );

    let open_replay_root = tempfile::tempdir().expect("open-rejection replay root");
    let open_replay_proof = Arc::new(gateway::GatewayRetryProof::always_fail(true));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args([
            "payment_gateway_retry_journal_test",
            "--replay-from",
            open_run
                .to_str()
                .expect("open-rejection path should be UTF-8"),
            "--verify",
        ])
        .run_blocking(proof::build_flow_for_profile(
            proof::RetryProofProfile::OpenRejection,
            Some(open_replay_proof.clone()),
            open_replay_root.path().to_path_buf(),
        ))
        .expect("open-rejection strict replay should verify without live gateway I/O");
    assert_eq!(open_replay_proof.calls(), 0);
    let open_replay = exported_run(
        &only_run(open_replay_root.path()),
        &open_replay_root.path().join("open-replay.jsonl"),
    );
    assert_eq!(
        data_event_count(&open_replay, "payment.authorization_unavailable.v1"),
        6
    );
    assert!(retry_schedules(&open_replay).is_empty());
    assert!(retry_successes(&open_replay).is_empty());
    assert_eq!(retry_terminal_failure_count(&open_replay), 0);
    assert!(
        attempt_settlements(&open_replay).is_empty(),
        "open-rejection replay must not emit fresh physical-attempt evidence"
    );
    assert!(recovery_completions(&open_replay).is_empty());
    assert_eq!(
        payment_terminal_failures(&open_replay),
        open_failures,
        "strict replay must preserve every terminal failure cause under its recorded cursor"
    );
}
