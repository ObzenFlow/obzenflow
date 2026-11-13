// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::{exported_jsonl, replay_testkit};
#[path = "retry_fixture.rs"]
mod retry_fixture;

use obzenflow_core::config::{ConfigScope, ConfigSource, ConfigSubject, ResolvedForDoc};
use obzenflow_core::event::payloads::execution_payload::{
    BackpressureFact, CircuitBreakerFact, CircuitBreakerHealthClassification, ExecutionPayload,
    RecoveryFact, RetryStopReason,
};
use obzenflow_core::event::{ChainEvent, ChainPayload, EffectFailureCause, EffectOutcomePayload};
use obzenflow_core::journal::archive::manifest::RunManifest;
use obzenflow_runtime::effects::EffectCursor;
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::{
    CandidateSet, ConfigValue, ResolvedRuntimeConfig, ScopedCandidate,
    CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY, CIRCUIT_BREAKER_COUNT_WINDOW_KEY,
    CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY, CIRCUIT_BREAKER_MAX_OPEN_FOR_MS_KEY,
    CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, CIRCUIT_BREAKER_MODE_KEY, CIRCUIT_BREAKER_OPEN_FOR_MS_KEY,
    CIRCUIT_BREAKER_OPEN_FOR_MULTIPLIER_KEY, CIRCUIT_BREAKER_PROBES_KEY,
    CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY, CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
    CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY, RATE_LIMITER_COST_PER_ATTEMPT_KEY,
    RATE_LIMITER_EVENTS_PER_SECOND_KEY, RETRY_ATTEMPT_START_WINDOW_MS_KEY,
    RETRY_FIXED_DELAY_MS_KEY, RETRY_KIND_KEY, RETRY_MAX_ATTEMPTS_KEY, RETRY_MAX_BACKOFF_MS_KEY,
    RETRY_MAX_OUTAGE_WAIT_MS_KEY, RETRY_ON_REFUSAL_KEY,
};
use obzenflow_topology::{
    MiddlewareFamily, MiddlewareSettingInfo, ResolvedSettingInfo, SettingProvenanceInfo,
    SettingSubject, SettingValueInfo,
};
use serde_json::json;
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    assert_eq!(runs.len(), 1, "the witness root should contain one run");
    runs.into_iter().next().unwrap()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Winner {
    Dsl,
    Default,
    FlowFile,
    StageFile,
}

type ReleaseConfig = BTreeMap<String, (serde_json::Value, Winner)>;

fn release_manifest(run: &Path) -> ReleaseConfig {
    let raw = std::fs::read_to_string(run.join("run_manifest.json"))
        .expect("release witness run manifest should be readable");
    let manifest: RunManifest =
        serde_json::from_str(&raw).expect("release witness run manifest should decode");
    let evidence = manifest
        .effective_config
        .expect("release witness must record effective configuration");
    assert_eq!(evidence.schema_version, 2);

    let effect_type = "payment.authorize".to_string();
    let resolved_for = ResolvedForDoc::Effect {
        stage: "authorize_payment".to_string(),
        effect_type: effect_type.clone(),
    };
    let authored_subject = ConfigSubject::Effect {
        effect_type: effect_type.into(),
    };
    let mut actual = BTreeMap::new();
    for row in evidence
        .values
        .into_iter()
        .filter(|row| row.resolved_for.as_ref() == Some(&resolved_for))
    {
        let winner = match (row.source.as_str(), row.scope.as_str(), &row.winner_subject) {
            ("dsl", "stage:authorize_payment", Some(subject)) if *subject == authored_subject => {
                Winner::Dsl
            }
            ("default", "global", Some(ConfigSubject::Unqualified)) => Winner::Default,
            ("file", "flow", Some(ConfigSubject::Unqualified)) => Winner::FlowFile,
            ("file", "stage:authorize_payment", Some(ConfigSubject::Unqualified)) => {
                Winner::StageFile
            }
            (source, scope, subject) => panic!(
                "{} resolved from {source} at {scope} for {subject:?}",
                row.key_path
            ),
        };
        assert!(
            actual
                .insert(row.key_path.clone(), (row.value, winner))
                .is_none(),
            "release effect configuration contains duplicate key {}",
            row.key_path
        );
    }
    actual
}

fn expected_release_config(policy: retry_fixture::ReleasePolicy) -> ReleaseConfig {
    let mut authored = Vec::new();
    if policy == retry_fixture::ReleasePolicy::OutageRecovery {
        authored.extend([
            (CIRCUIT_BREAKER_MODE_KEY, json!("consecutive")),
            (CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY, json!(1)),
            (CIRCUIT_BREAKER_OPEN_FOR_MS_KEY, json!(100)),
            (CIRCUIT_BREAKER_OPEN_FOR_MULTIPLIER_KEY, json!(2.0)),
            (CIRCUIT_BREAKER_MAX_OPEN_FOR_MS_KEY, json!(400)),
            (CIRCUIT_BREAKER_PROBES_KEY, json!(1)),
            (
                CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
                json!(false),
            ),
            (RETRY_KIND_KEY, json!("fixed")),
            (RETRY_FIXED_DELAY_MS_KEY, json!(10)),
            (RETRY_MAX_ATTEMPTS_KEY, json!(3)),
            (RETRY_MAX_BACKOFF_MS_KEY, json!(30_000)),
            (RETRY_ATTEMPT_START_WINDOW_MS_KEY, json!(120_000)),
            (RETRY_MAX_OUTAGE_WAIT_MS_KEY, json!(60_000)),
        ]);
    } else {
        if policy.has_breaker() {
            authored.extend([
                (CIRCUIT_BREAKER_MODE_KEY, json!("rate_based")),
                (CIRCUIT_BREAKER_COUNT_WINDOW_KEY, json!(5)),
                (CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, json!(5)),
                (CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY, json!(0.6)),
                (CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY, json!(250)),
                (CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY, json!(0.5)),
                (CIRCUIT_BREAKER_OPEN_FOR_MS_KEY, json!(5_000)),
                (CIRCUIT_BREAKER_PROBES_KEY, json!(1)),
                (
                    CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
                    json!(false),
                ),
            ]);
        }
        if policy.has_limiter() {
            authored.extend([
                (RATE_LIMITER_EVENTS_PER_SECOND_KEY, json!(1.0)),
                (RATE_LIMITER_COST_PER_ATTEMPT_KEY, json!(1.0)),
            ]);
        }
        if policy.has_retry() {
            authored.extend([
                (RETRY_KIND_KEY, json!("fixed")),
                (RETRY_FIXED_DELAY_MS_KEY, json!(250)),
                (RETRY_MAX_ATTEMPTS_KEY, json!(3)),
                (RETRY_MAX_BACKOFF_MS_KEY, json!(30_000)),
                (RETRY_ATTEMPT_START_WINDOW_MS_KEY, json!(30_000)),
            ]);
        }
    }
    let mut expected: ReleaseConfig = authored
        .into_iter()
        .map(|(key, value)| (key.to_string(), (value, Winner::Dsl)))
        .collect();
    if policy.has_breaker() && policy.has_retry() {
        expected
            .entry(RETRY_ON_REFUSAL_KEY.to_string())
            .or_insert((json!("wait"), Winner::Default));
        expected
            .entry(RETRY_MAX_OUTAGE_WAIT_MS_KEY.to_string())
            .or_insert((json!(120_000), Winner::Default));
    }
    expected
}

fn with_stage_file(mut config: ReleaseConfig, rows: &[(&str, serde_json::Value)]) -> ReleaseConfig {
    for (key, value) in rows {
        config.insert(key.to_string(), (value.clone(), Winner::StageFile));
    }
    config
}

fn run_witness(
    policy: retry_fixture::ReleasePolicy,
    orders: Vec<retry_fixture::ValidatedOrder>,
    gateway: Arc<retry_fixture::ScriptedGateway>,
    journal_root: &Path,
    config: Option<&str>,
    replay_from: Option<&Path>,
) -> Result<PathBuf, String> {
    let mut args = vec!["payment_gateway_retry_journal_test".to_string()];
    if let Some(archive) = replay_from {
        args.extend([
            "--replay-from".to_string(),
            archive
                .to_str()
                .expect("release archive path should be UTF-8")
                .to_string(),
            "--verify".to_string(),
        ]);
    }
    let mut application = replay_testkit::unsynced_application().with_cli_args(args);
    if let Some(config) = config {
        let path = journal_root.join("obzenflow.toml");
        std::fs::write(&path, format!("[journal]\nsync = \"off\"\n\n{config}"))
            .expect("witness configuration should be writable");
        application = application.with_config_file(path);
    }
    application
        .run_blocking(retry_fixture::build_flow(
            policy,
            orders,
            gateway,
            journal_root.to_path_buf(),
        ))
        .map_err(|error| error.to_string())?;
    Ok(only_run(journal_root))
}

fn run_release_witness(
    policy: retry_fixture::ReleasePolicy,
    orders: Vec<retry_fixture::ValidatedOrder>,
    gateway: Arc<retry_fixture::ScriptedGateway>,
    journal_root: &Path,
    replay_from: Option<&Path>,
) -> PathBuf {
    let run = run_witness(policy, orders, gateway, journal_root, None, replay_from)
        .expect("configuration-faithful payment release witness should complete");
    assert_eq!(
        release_manifest(&run),
        expected_release_config(policy),
        "release evidence must exactly match its locked payment policy"
    );
    run
}

fn exported_run(run: &Path, output: &Path) -> String {
    obzenflow_infra::journal::disk::inspect::export_jsonl(run, Some(output))
        .expect("witness journal should export");
    std::fs::read_to_string(output).expect("exported witness journal should be readable")
}

fn exported_events(jsonl: &str) -> impl Iterator<Item = serde_json::Value> + '_ {
    jsonl.lines().map(|line| {
        serde_json::from_str(line).expect("each exported journal row should be valid JSON")
    })
}

fn data_event_count(jsonl: &str, event_type: &str) -> usize {
    exported_events(jsonl)
        .filter(|row| {
            row.pointer("/envelope/provenance/event/event_kind")
                .and_then(|value| value.as_str())
                == Some("fact")
                && row
                    .pointer("/envelope/provenance/event/event_type")
                    .and_then(|value| value.as_str())
                    == Some(event_type)
        })
        .count()
}

fn payment_effect_outcome_group_count(jsonl: &str) -> usize {
    exported_events(jsonl)
        .filter_map(|row| {
            (row.pointer("/envelope/provenance/event/effect_provenance/descriptor/effect_type")
                .and_then(|value| value.as_str())
                == Some("payment.authorize"))
            .then(|| {
                row.pointer("/envelope/provenance/event/effect_provenance/group_id")
                    .and_then(|value| value.as_str())
                    .expect("payment effect fact should carry its outcome group")
                    .to_string()
            })
        })
        .collect::<std::collections::BTreeSet<_>>()
        .len()
}

fn payment_committed_terminal_group_count(jsonl: &str) -> usize {
    exported_events(jsonl)
        .filter(|row| {
            row.pointer("/envelope/provenance/event/flow_context/stage_name")
                .and_then(serde_json::Value::as_str)
                == Some("authorize_payment")
        })
        .filter(|row| {
            // Count physical group starts, including repeated deterministic
            // group IDs. The exporter validates membership against each frame.
            row.pointer("/envelope/provenance/journal/journal_group_member/index")
                .and_then(serde_json::Value::as_u64)
                == Some(0)
        })
        .count()
}

fn payment_outcome_group_members(jsonl: &str) -> Vec<(u64, u64)> {
    exported_events(jsonl)
        .filter(|row| {
            row.pointer("/envelope/provenance/event/event_type")
                .and_then(serde_json::Value::as_str)
                == Some("payment.authorized")
        })
        .map(|row| {
            let member = |field: &str| {
                row.pointer(&format!(
                    "/envelope/provenance/journal/journal_group_member/{field}"
                ))
                .and_then(serde_json::Value::as_u64)
                .expect("a payment outcome is a member of its atomic group")
            };
            (member("index"), member("size"))
        })
        .collect()
}

fn exported_chain_events(jsonl: &str) -> impl Iterator<Item = ChainEvent> + '_ {
    exported_jsonl::chain_events(jsonl).into_iter()
}

#[test]
#[should_panic(expected = "is neither a ChainEvent nor a SystemEvent")]
fn exported_record_decoder_fails_loud_on_an_unknown_row() {
    exported_jsonl::chain_events(r#"{"event":{"unknown":"shape"}}"#);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AttemptSettlement {
    cursor: EffectCursor,
    attempt: u32,
    health: CircuitBreakerHealthClassification,
}

fn attempt_settlements(jsonl: &str) -> Vec<AttemptSettlement> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                CircuitBreakerFact::AttemptSettled {
                    cursor,
                    attempt,
                    health_classification,
                    ..
                },
            )) => Some(AttemptSettlement {
                cursor,
                attempt,
                health: health_classification,
            }),
            _ => None,
        })
        .collect()
}

fn breaker_transition_counts(jsonl: &str) -> (usize, usize, usize) {
    exported_chain_events(jsonl).fold((0, 0, 0), |(opened, half_open, closed), event| match event
        .payload
    {
        ChainPayload::Execution(ExecutionPayload::CircuitBreaker(CircuitBreakerFact::Opened {
            ..
        })) => (opened + 1, half_open, closed),
        ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
            CircuitBreakerFact::HalfOpen { .. },
        )) => (opened, half_open + 1, closed),
        ChainPayload::Execution(ExecutionPayload::CircuitBreaker(CircuitBreakerFact::Closed {
            ..
        })) => (opened, half_open, closed + 1),
        _ => (opened, half_open, closed),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetrySchedule {
    cursor: EffectCursor,
    next_attempt: u32,
}

fn retry_schedules(jsonl: &str) -> Vec<RetrySchedule> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryScheduled {
                cursor,
                next_attempt,
                ..
            })) => Some(RetrySchedule {
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
}

fn retry_successes(jsonl: &str) -> Vec<RetrySuccess> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetrySucceeded {
                cursor,
                total_attempts,
            })) => Some(RetrySuccess {
                cursor,
                total_attempts,
            }),
            _ => None,
        })
        .collect()
}

fn retry_terminal_failure_count(jsonl: &str) -> usize {
    exported_chain_events(jsonl)
        .filter(|event| {
            matches!(
                event.payload,
                ChainPayload::Execution(ExecutionPayload::Recovery(
                    RecoveryFact::RetryExhausted { .. }
                        | RecoveryFact::RetryStoppedNonRetryable { .. }
                ))
            )
        })
        .count()
}

fn retry_exhaustions(jsonl: &str) -> Vec<(u32, RetryStopReason)> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryExhausted {
                total_attempts,
                reason,
                ..
            })) => Some((total_attempts, reason)),
            _ => None,
        })
        .collect()
}

fn opened_cooldowns(jsonl: &str) -> Vec<u64> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::CircuitBreaker(
                CircuitBreakerFact::Opened { cooldown_ms, .. },
            )) => Some(cooldown_ms),
            _ => None,
        })
        .collect()
}

fn payment_observations(jsonl: &str) -> Vec<&'static str> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::EffectRecord(record))
                if record.descriptor.effect_type.as_str() == "payment.authorize" =>
            {
                Some(record.observation.event_type())
            }
            _ => None,
        })
        .collect()
}

fn stall_writers(jsonl: &str) -> Vec<String> {
    exported_chain_events(jsonl)
        .filter(|event| {
            matches!(
                event.payload,
                ChainPayload::Execution(ExecutionPayload::Backpressure(
                    BackpressureFact::Stalled { .. }
                ))
            )
        })
        .map(|event| event.flow_context.stage_name.as_str().to_string())
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
struct TerminalFailure {
    cursor: EffectCursor,
    cause: Option<EffectFailureCause>,
}

fn payment_terminal_failures(jsonl: &str) -> Vec<TerminalFailure> {
    let mut failures: Vec<_> = exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::EffectRecord(record)) => {
                assert_eq!(
                    event
                        .envelope
                        .provenance
                        .event
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecoveryCompletion {
    cursor: EffectCursor,
    total_attempts: u32,
    backoff_elapsed_ms: u64,
    recovery_elapsed_ms: u64,
}

fn recovery_completions(jsonl: &str) -> Vec<RecoveryCompletion> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::RecoveryCompleted {
                    cursor,
                    total_attempts,
                    backoff_elapsed_ms,
                    recovery_elapsed_ms,
                },
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
                .envelope
                .provenance
                .event
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
            .pointer("/envelope/observability/runtime/effect_circuit_breakers")
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
        .envelope
        .observability
        .as_ref()?
        .runtime
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

#[test]
fn payment_gateway_configuration_faithful_release_portfolio() {
    // Breaker-only release witness: five dependency calls at one per second.
    // The journal proves limiter pacing is real and that fast successes never
    // increment the breaker's slow-call counter.
    let healthy_root = tempfile::tempdir().expect("healthy journal root");
    let healthy_gateway = Arc::new(retry_fixture::ScriptedGateway::healthy());
    let healthy_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        retry_fixture::healthy_orders(),
        healthy_gateway.clone(),
        healthy_root.path(),
        None,
    );
    assert_eq!(healthy_gateway.calls(), 5);
    let healthy = exported_run(&healthy_run, &healthy_root.path().join("healthy.jsonl"));
    assert_eq!(data_event_count(&healthy, "payment.authorized"), 5);
    assert_eq!(
        data_event_count(&healthy, "payment.authorization_unavailable"),
        0
    );
    assert_eq!(payment_effect_outcome_group_count(&healthy), 5);
    assert_eq!(payment_committed_terminal_group_count(&healthy), 5);
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

    let healthy_replay_root = tempfile::tempdir().expect("healthy replay journal root");
    let healthy_replay_gateway =
        Arc::new(retry_fixture::ScriptedGateway::healthy().panic_on_live_call());
    let healthy_replay_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        retry_fixture::healthy_orders(),
        healthy_replay_gateway.clone(),
        healthy_replay_root.path(),
        Some(&healthy_run),
    );
    assert_eq!(healthy_replay_gateway.calls(), 0);
    let healthy_replay = exported_run(
        &healthy_replay_run,
        &healthy_replay_root.path().join("healthy-replay.jsonl"),
    );
    assert_eq!(data_event_count(&healthy_replay, "payment.authorized"), 5);
    assert_eq!(
        attempt_settlements(&healthy_replay),
        healthy_attempts,
        "strict replay must rematerialise archived attempt settlements unchanged"
    );
    assert_eq!(
        recovery_completions(&healthy_replay),
        healthy_completions,
        "strict replay must rematerialise archived recovery completions unchanged"
    );

    // Canonical breaker-only control over the same scripted order as the
    // recovery treatment: one physical failure and no retry continuation.
    let breaker_only_root = tempfile::tempdir().expect("breaker-only journal root");
    let breaker_only_gateway = Arc::new(retry_fixture::ScriptedGateway::fail_first(2));
    let breaker_only_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        vec![retry_fixture::retry_order()],
        breaker_only_gateway.clone(),
        breaker_only_root.path(),
        None,
    );
    assert_eq!(breaker_only_gateway.calls(), 1);
    let breaker_only = exported_run(
        &breaker_only_run,
        &breaker_only_root.path().join("breaker-only.jsonl"),
    );
    assert_eq!(
        data_event_count(&breaker_only, "payment.authorization_unavailable"),
        1
    );
    assert_eq!(data_event_count(&breaker_only, "payment.authorized"), 0);
    assert_eq!(payment_effect_outcome_group_count(&breaker_only), 1);
    assert_eq!(payment_committed_terminal_group_count(&breaker_only), 1);
    assert_eq!(
        last_payment_breaker_counts(&breaker_only),
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
    assert_eq!(last_payment_limiter_counts(&breaker_only).events, 1);
    assert_eq!(last_payment_limiter_counts(&breaker_only).tokens, 1.0);
    let breaker_only_attempts = attempt_settlements(&breaker_only);
    assert_eq!(breaker_only_attempts.len(), 1);
    assert_eq!(breaker_only_attempts[0].attempt, 1);
    assert_eq!(
        breaker_only_attempts[0].health,
        CircuitBreakerHealthClassification::TransientFailure
    );
    assert!(retry_schedules(&breaker_only).is_empty());
    assert!(retry_successes(&breaker_only).is_empty());
    assert_eq!(retry_terminal_failure_count(&breaker_only), 0);
    let breaker_only_completions = assert_one_recovery_completion_per_payment_cursor(&breaker_only);
    assert_eq!(breaker_only_completions.len(), 1);
    assert_eq!(breaker_only_completions[0].total_attempts, 1);
    assert_eq!(breaker_only_completions[0].backoff_elapsed_ms, 0);

    // Canonical breaker-recovery release witness: fixed 250 ms retry, three
    // attempts, a 30-second attempt-start window, and one limiter token/second.
    let treatment_root = tempfile::tempdir().expect("treatment journal root");
    let treatment_gateway = Arc::new(retry_fixture::ScriptedGateway::fail_first(2));
    let treatment_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerRecovery,
        vec![retry_fixture::retry_order()],
        treatment_gateway.clone(),
        treatment_root.path(),
        None,
    );
    assert_eq!(treatment_gateway.calls(), 3);
    let order_key = obzenflow_runtime::effects::IdempotencyKey(format!(
        "payment-authorize:{}",
        retry_fixture::retry_order().order_id
    ));
    assert_eq!(
        treatment_gateway.keys(),
        vec![order_key; 3],
        "every retry of one authorization sends its order's key"
    );
    let treatment = exported_run(
        &treatment_run,
        &treatment_root.path().join("treatment.jsonl"),
    );
    assert_eq!(data_event_count(&treatment, "payment.authorized"), 1);
    assert_eq!(
        data_event_count(&treatment, "payment.authorization_unavailable"),
        0
    );
    assert_eq!(payment_effect_outcome_group_count(&treatment), 1);
    assert_eq!(payment_committed_terminal_group_count(&treatment), 1);
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
    let treatment_limiter = last_payment_limiter_counts(&treatment);
    assert_eq!(treatment_limiter.events, 3);
    assert_eq!(treatment_limiter.tokens, 3.0);
    assert!(treatment_limiter.delayed >= 2);
    assert!(treatment_limiter.delay_seconds > 0.0);
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
    let replay_gateway =
        Arc::new(retry_fixture::ScriptedGateway::fail_first(2).panic_on_live_call());
    let replay_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerRecovery,
        vec![retry_fixture::retry_order()],
        replay_gateway.clone(),
        replay_root.path(),
        Some(&treatment_run),
    );
    assert_eq!(replay_gateway.calls(), 0);
    let replay = exported_run(&replay_run, &replay_root.path().join("replay.jsonl"));
    assert_eq!(data_event_count(&replay, "payment.authorized"), 1);
    assert_eq!(
        data_event_count(&replay, "payment.authorization_unavailable"),
        0
    );
    assert_eq!(payment_effect_outcome_group_count(&replay), 1);
    assert_eq!(payment_committed_terminal_group_count(&replay), 1);
    assert_eq!(retry_schedules(&replay), retry_schedules(&treatment));
    assert_eq!(retry_successes(&replay), retry_successes(&treatment));
    assert_eq!(retry_terminal_failure_count(&replay), 0);
    assert_eq!(
        attempt_settlements(&replay),
        treatment_attempts,
        "strict replay must rematerialise archived physical-attempt evidence unchanged"
    );
    assert_eq!(
        recovery_completions(&replay),
        treatment_completions,
        "strict replay must rematerialise the archived recovery completion unchanged"
    );

    // Breaker-only release witness: five one-per-second failures satisfy the
    // count window; the sixth logical effect is rejected without a dependency
    // call or limiter permit.
    let open_root = tempfile::tempdir().expect("open-rejection journal root");
    let open_gateway = Arc::new(retry_fixture::ScriptedGateway::always_fail());
    let open_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        retry_fixture::open_rejection_orders(),
        open_gateway.clone(),
        open_root.path(),
        None,
    );
    assert_eq!(open_gateway.calls(), 5);
    let open = exported_run(&open_run, &open_root.path().join("open.jsonl"));
    assert_eq!(
        data_event_count(&open, "payment.authorization_unavailable"),
        6
    );
    assert_eq!(data_event_count(&open, "payment.authorized"), 0);
    assert_eq!(payment_committed_terminal_group_count(&open), 6);
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
    assert_eq!(breaker_transition_counts(&open), (1, 0, 0));
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
    // FLOWIP-145a: optional snapshots need not accompany each failure. Prove
    // the exact attempted cursor set from the protected outcome/attempt rows,
    // alongside the five dependency calls and retained limiter totals above.
    let physical_failure_cursors: HashSet<_> = open_failures
        .iter()
        .filter(|failure| failure.cursor != rejected_failure.cursor)
        .map(|failure| &failure.cursor)
        .collect();
    assert_eq!(physical_failure_cursors.len(), 5);
    assert_eq!(
        open_attempts
            .iter()
            .map(|attempt| &attempt.cursor)
            .collect::<HashSet<_>>(),
        physical_failure_cursors,
        "only the five dependency failures have physical attempts"
    );
    assert_eq!(
        last_payment_limiter_counts(&open).tokens,
        open_attempts.len() as f64,
        "the cursor rejected by the open breaker must not commit a limiter permit"
    );

    let open_replay_root = tempfile::tempdir().expect("open-rejection replay root");
    let open_replay_gateway =
        Arc::new(retry_fixture::ScriptedGateway::always_fail().panic_on_live_call());
    let open_replay_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        retry_fixture::open_rejection_orders(),
        open_replay_gateway.clone(),
        open_replay_root.path(),
        Some(&open_run),
    );
    assert_eq!(open_replay_gateway.calls(), 0);
    let open_replay = exported_run(
        &open_replay_run,
        &open_replay_root.path().join("open-replay.jsonl"),
    );
    assert_eq!(
        data_event_count(&open_replay, "payment.authorization_unavailable"),
        6
    );
    assert_eq!(payment_committed_terminal_group_count(&open_replay), 6);
    assert!(retry_schedules(&open_replay).is_empty());
    assert!(retry_successes(&open_replay).is_empty());
    assert_eq!(retry_terminal_failure_count(&open_replay), 0);
    assert_eq!(
        attempt_settlements(&open_replay),
        open_attempts,
        "open-rejection replay must rematerialise archived physical-attempt evidence unchanged"
    );
    assert_eq!(
        recovery_completions(&open_replay),
        open_completions,
        "open-rejection replay must rematerialise archived recovery completions unchanged"
    );
    assert_eq!(
        payment_terminal_failures(&open_replay),
        open_failures,
        "strict replay must preserve every terminal failure cause under its recorded cursor"
    );
}

#[test]
fn payment_gateway_half_open_release_witness_uses_the_real_cooldown() {
    // Five failures open the canonical breaker, the sixth logical invocation
    // proves open rejection, and the seventh waits outside policy machinery for
    // the real five-second cooldown before becoming the sole half-open probe.
    let live_root = tempfile::tempdir().expect("half-open journal root");
    let live_gateway = Arc::new(
        retry_fixture::ScriptedGateway::fail_first(5)
            .pause_before_invocation(7, Duration::from_millis(5_250)),
    );
    let live_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        retry_fixture::half_open_recovery_orders(),
        live_gateway.clone(),
        live_root.path(),
        None,
    );
    assert_eq!(live_gateway.calls(), 6);

    let live = exported_run(&live_run, &live_root.path().join("half-open.jsonl"));
    assert_eq!(data_event_count(&live, "payment.authorized"), 1);
    assert_eq!(
        data_event_count(&live, "payment.authorization_unavailable"),
        6
    );
    assert_eq!(payment_committed_terminal_group_count(&live), 7);
    assert_eq!(
        last_payment_breaker_counts(&live),
        BreakerCounts {
            requests: 6,
            successes: 1,
            failures: 5,
            slow: 0,
            rejections: 1,
            opened: 1,
            state: 0.0,
        }
    );
    assert_eq!(breaker_transition_counts(&live), (1, 1, 1));

    let limiter = last_payment_limiter_counts(&live);
    assert_eq!(limiter.events, 6);
    assert_eq!(limiter.tokens, 6.0);
    assert!(limiter.delayed >= 4);
    assert!(limiter.delay_seconds > 0.0);

    let attempts = attempt_settlements(&live);
    assert_eq!(attempts.len(), 6);
    assert!(attempts[..5].iter().all(|attempt| {
        attempt.attempt == 1
            && attempt.health == CircuitBreakerHealthClassification::TransientFailure
    }));
    assert_eq!(attempts[5].attempt, 1);
    assert_eq!(
        attempts[5].health,
        CircuitBreakerHealthClassification::Success
    );
    assert!(retry_schedules(&live).is_empty());
    assert!(retry_successes(&live).is_empty());

    let failures = payment_terminal_failures(&live);
    assert_eq!(failures.len(), 6);
    let rejected = failures
        .iter()
        .find(|failure| {
            failure
                .cause
                .as_ref()
                .is_some_and(|cause| cause.code.as_str() == "circuit_open")
        })
        .expect("half-open release witness must include the pre-cooldown rejection");
    assert!(
        attempts
            .iter()
            .all(|attempt| attempt.cursor != rejected.cursor),
        "the pre-cooldown rejection must have no physical attempt"
    );
    let completions = assert_one_recovery_completion_per_payment_cursor(&live);
    assert_eq!(completions.len(), 7);
    assert_eq!(
        completions
            .iter()
            .find(|completion| completion.cursor == rejected.cursor)
            .expect("rejected cursor must have a recovery completion")
            .total_attempts,
        0
    );

    let replay_root = tempfile::tempdir().expect("half-open replay journal root");
    let replay_gateway = Arc::new(
        retry_fixture::ScriptedGateway::fail_first(5)
            .panic_on_live_call()
            .pause_before_invocation(7, Duration::from_millis(5_250)),
    );
    let replay_run = run_release_witness(
        retry_fixture::ReleasePolicy::BreakerOnly,
        retry_fixture::half_open_recovery_orders(),
        replay_gateway.clone(),
        replay_root.path(),
        Some(&live_run),
    );
    assert_eq!(replay_gateway.calls(), 0);
    let replay = exported_run(
        &replay_run,
        &replay_root.path().join("half-open-replay.jsonl"),
    );
    assert_eq!(data_event_count(&replay, "payment.authorized"), 1);
    assert_eq!(
        data_event_count(&replay, "payment.authorization_unavailable"),
        6
    );
    assert_eq!(
        attempt_settlements(&replay),
        attempts,
        "strict replay must rematerialise the archived half-open attempt evidence unchanged"
    );
    assert_eq!(
        recovery_completions(&replay),
        completions,
        "strict replay must rematerialise the archived half-open recovery evidence unchanged"
    );
    assert_eq!(
        payment_terminal_failures(&replay),
        failures,
        "strict replay must preserve the half-open witness's cursor-to-cause map"
    );
}

#[test]
fn payment_outage_failed_probes_grow_cooldowns() {
    use CircuitBreakerHealthClassification::{Success, TransientFailure};
    let live_root = tempfile::tempdir().expect("outage journal root");
    let live_gateway = Arc::new(retry_fixture::ScriptedGateway::fail_calls(&[1, 2, 3, 4, 6]));
    let live_run = run_release_witness(
        retry_fixture::ReleasePolicy::OutageRecovery,
        retry_fixture::outage_orders(),
        live_gateway.clone(),
        live_root.path(),
        None,
    );
    assert_eq!(live_gateway.calls(), 8);
    let live = exported_run(&live_run, &live_root.path().join("outage.jsonl"));
    assert_eq!(data_event_count(&live, "payment.authorized"), 3);
    assert!(
        payment_terminal_failures(&live).is_empty(),
        "a call that waits out the outage records no rejection"
    );
    assert_eq!(
        opened_cooldowns(&live),
        [100, 200, 400, 400, 100],
        "failed probes double the opening up to its cap, and the next outage starts over"
    );
    assert_eq!(breaker_transition_counts(&live), (5, 5, 2));
    let attempts = attempt_settlements(&live);
    assert_eq!(
        attempts
            .iter()
            .map(|attempt| (attempt.attempt, attempt.health))
            .collect::<Vec<_>>(),
        [
            (1, TransientFailure),
            (2, TransientFailure),
            (3, TransientFailure),
            (4, TransientFailure),
            (5, Success),
            (1, TransientFailure),
            (2, Success),
            (1, Success),
        ],
        "attempt numbers keep counting across waits"
    );
    let (first, second) = (&attempts[0].cursor, &attempts[5].cursor);
    assert_eq!(
        retry_schedules(&live),
        [(first, 2), (first, 3), (first, 4), (first, 5), (second, 2)].map(
            |(cursor, next_attempt)| RetrySchedule {
                cursor: cursor.clone(),
                next_attempt,
            }
        ),
        "each wait records one scheduled row"
    );
    assert_eq!(
        retry_successes(&live),
        [(first, 5), (second, 2)].map(|(cursor, total_attempts)| RetrySuccess {
            cursor: cursor.clone(),
            total_attempts,
        })
    );
    let outcome_members = payment_outcome_group_members(&live);
    assert_eq!(outcome_members.len(), 3);
    assert!(
        outcome_members
            .iter()
            .all(|&(index, size)| size > 1 && index + 1 == size),
        "each outcome closes its group, after its breaker and retry rows: {outcome_members:?}"
    );

    let replay_root = tempfile::tempdir().expect("outage replay journal root");
    let replay_gateway =
        Arc::new(retry_fixture::ScriptedGateway::fail_calls(&[1, 2, 3, 4, 6]).panic_on_live_call());
    let replay_run = run_release_witness(
        retry_fixture::ReleasePolicy::OutageRecovery,
        retry_fixture::outage_orders(),
        replay_gateway.clone(),
        replay_root.path(),
        Some(&live_run),
    );
    assert_eq!(replay_gateway.calls(), 0);
    let replay = exported_run(&replay_run, &replay_root.path().join("outage-replay.jsonl"));
    assert_eq!(data_event_count(&replay, "payment.authorized"), 3);
    assert_eq!(opened_cooldowns(&replay), opened_cooldowns(&live));
    assert_eq!(retry_schedules(&replay), retry_schedules(&live));
    assert_eq!(attempt_settlements(&replay), attempts);
    assert_eq!(payment_outcome_group_members(&replay), outcome_members);
}

#[test]
fn outage_past_bound_records_outage_wait_limit() {
    let policy = retry_fixture::ReleasePolicy::OutageRecovery;
    let curve = "[middleware.stages.authorize_payment.circuit_breaker]\n\
                 open_for_ms = 2000\nopen_for_multiplier = 8.0\nmax_open_for_ms = 16000\n\n";
    let curved = with_stage_file(
        expected_release_config(policy),
        &[
            (CIRCUIT_BREAKER_OPEN_FOR_MS_KEY, json!(2_000)),
            (CIRCUIT_BREAKER_OPEN_FOR_MULTIPLIER_KEY, json!(8.0)),
            (CIRCUIT_BREAKER_MAX_OPEN_FOR_MS_KEY, json!(16_000)),
        ],
    );
    let failed_then_refused = [
        "effect.execution_failed",
        "effect.execution_rejected",
        "effect.execution_rejected",
    ];

    let control_root = tempfile::tempdir().expect("fail-fast journal root");
    let control_gateway = Arc::new(retry_fixture::ScriptedGateway::always_fail());
    let control_run = run_witness(
        policy,
        retry_fixture::outage_orders(),
        control_gateway.clone(),
        control_root.path(),
        Some(&format!(
            "{curve}[middleware.stages.authorize_payment.retry]\non_refusal = \"fail\"\n"
        )),
        None,
    )
    .expect("the fail-fast night completes");
    assert_eq!(control_gateway.calls(), 1);
    assert_eq!(
        release_manifest(&control_run),
        with_stage_file(curved.clone(), &[(RETRY_ON_REFUSAL_KEY, json!("fail"))])
    );
    let control = exported_run(&control_run, &control_root.path().join("fail-fast.jsonl"));
    assert_eq!(payment_observations(&control), failed_then_refused);
    assert_eq!(
        retry_exhaustions(&control),
        [(1, RetryStopReason::CircuitNoLongerClosed)]
    );
    assert!(retry_schedules(&control).is_empty());

    let treatment_root = tempfile::tempdir().expect("bounded-wait journal root");
    let treatment_gateway = Arc::new(retry_fixture::ScriptedGateway::always_fail());
    let treatment_run = run_witness(
        policy,
        retry_fixture::outage_orders(),
        treatment_gateway.clone(),
        treatment_root.path(),
        Some(&format!(
            "{curve}[middleware.stages.authorize_payment.retry]\n\
             on_refusal = \"wait\"\nmax_outage_wait_ms = 3000\n"
        )),
        None,
    )
    .expect("the bounded-wait night completes");
    assert_eq!(
        treatment_gateway.calls(),
        2,
        "the first order is sent once more when the breaker reopens, and the refused orders never"
    );
    assert_eq!(
        release_manifest(&treatment_run),
        with_stage_file(
            curved,
            &[
                (RETRY_ON_REFUSAL_KEY, json!("wait")),
                (RETRY_MAX_OUTAGE_WAIT_MS_KEY, json!(3_000)),
            ]
        )
    );
    let treatment = exported_run(
        &treatment_run,
        &treatment_root.path().join("bounded-wait.jsonl"),
    );
    assert_eq!(opened_cooldowns(&treatment), [2_000, 16_000]);
    assert_eq!(
        retry_schedules(&treatment)
            .iter()
            .map(|schedule| schedule.next_attempt)
            .collect::<Vec<_>>(),
        [2]
    );
    assert_eq!(payment_observations(&treatment), failed_then_refused);
    assert_eq!(
        retry_exhaustions(&treatment),
        [
            (2, RetryStopReason::OutageWaitLimit),
            (0, RetryStopReason::OutageWaitLimit),
            (0, RetryStopReason::OutageWaitLimit),
        ],
        "the call that gives up and every refusal after it name the outage bound"
    );
}

#[test]
fn enforced_edge_outage_completes_without_stall() {
    let policy = retry_fixture::ReleasePolicy::OutageRecovery;
    let config = "[runtime.backpressure.flow]\nmode = \"enforce\"\nwindow = 1\n\
                  stall_timeout_ms = 2000\n\n\
                  [middleware.stages.authorize_payment.circuit_breaker]\n\
                  open_for_ms = 3000\nmax_open_for_ms = 6000\n";

    let held_root = tempfile::tempdir().expect("held-handler journal root");
    run_witness(
        policy,
        retry_fixture::outage_orders(),
        Arc::new(
            retry_fixture::ScriptedGateway::healthy()
                .pause_before_invocation(1, Duration::from_millis(3_500)),
        ),
        held_root.path(),
        Some(config),
        None,
    )
    .expect_err("a handler held past the stall ceiling fails the night");
    let held = exported_run(
        &only_run(held_root.path()),
        &held_root.path().join("held.jsonl"),
    );
    assert_eq!(stall_writers(&held), ["orders"]);

    let waiting_root = tempfile::tempdir().expect("waiting journal root");
    let waiting_gateway = Arc::new(retry_fixture::ScriptedGateway::fail_first(1));
    let waiting_run = run_witness(
        policy,
        retry_fixture::outage_orders(),
        waiting_gateway.clone(),
        waiting_root.path(),
        Some(config),
        None,
    )
    .expect("a night that waits past the stall ceiling completes");
    assert_eq!(waiting_gateway.calls(), 4);
    assert_eq!(
        release_manifest(&waiting_run),
        with_stage_file(
            expected_release_config(policy),
            &[
                (CIRCUIT_BREAKER_OPEN_FOR_MS_KEY, json!(3_000)),
                (CIRCUIT_BREAKER_MAX_OPEN_FOR_MS_KEY, json!(6_000)),
            ]
        )
    );
    let waiting = exported_run(&waiting_run, &waiting_root.path().join("waiting.jsonl"));
    assert_eq!(opened_cooldowns(&waiting), [3_000]);
    assert_eq!(data_event_count(&waiting, "payment.authorized"), 3);
    assert!(
        stall_writers(&waiting).is_empty(),
        "renewed holds keep the edge alive while the call waits"
    );
}

fn retry_rows(
    handle: &obzenflow_runtime::prelude::FlowHandle,
) -> BTreeMap<String, MiddlewareSettingInfo> {
    let topology = handle
        .topology()
        .expect("a built flow carries its topology");
    let attachment = topology
        .stages()
        .filter_map(|stage| stage.middleware.as_ref())
        .flat_map(|middleware| &middleware.attachments)
        .find(|attachment| attachment.family() == MiddlewareFamily::Retry)
        .expect("the plan retries its effect");
    attachment.details.settings().settings().clone()
}

fn unqualified(
    value: SettingValueInfo,
    source: &str,
    scope: &str,
) -> ResolvedSettingInfo<SettingValueInfo> {
    ResolvedSettingInfo {
        value,
        provenance: SettingProvenanceInfo {
            source: source.to_string(),
            scope: scope.to_string(),
            winner_subject: SettingSubject::Unqualified,
        },
    }
}

#[test]
fn independent_retry_plans_preserve_terminal_groups_and_strict_replay() {
    use retry_fixture::ReleasePolicy;

    let mut candidates = CandidateSet::default();
    candidates
        .admit(ScopedCandidate::unqualified(
            RETRY_ON_REFUSAL_KEY,
            ConfigScope::Flow,
            ConfigSource::File,
            ConfigValue::Text("fail".to_string()),
        ))
        .expect("a flow-scope on_refusal admits");
    replay_testkit::admit_unsynced(&mut candidates);
    let flow_scope = Arc::new(ResolvedRuntimeConfig::new(candidates));
    let runtime = tokio::runtime::Runtime::new().expect("build runtime");
    for policy in [
        ReleasePolicy::BreakerRecovery,
        ReleasePolicy::RetryOnly,
        ReleasePolicy::RetryWithLimiter,
        ReleasePolicy::RetryWithLimiterPermuted,
        ReleasePolicy::RetryWithLimiterSplit,
    ] {
        let root = tempfile::tempdir().unwrap();
        let handle = runtime
            .block_on(
                retry_fixture::build_flow(
                    policy,
                    Vec::new(),
                    Arc::new(retry_fixture::ScriptedGateway::healthy()),
                    root.path().to_path_buf(),
                )
                .build(FlowBuildContext::new(flow_scope.clone())),
            )
            .unwrap_or_else(|error| {
                panic!("{policy:?} builds under a flow-scope on_refusal: {error}")
            });
        let rows = retry_rows(&handle);
        let mut expected = expected_release_config(policy);
        if policy.has_breaker() {
            assert_eq!(
                rows[RETRY_ON_REFUSAL_KEY],
                MiddlewareSettingInfo::Active {
                    setting: unqualified(SettingValueInfo::Text("fail".into()), "file", "flow"),
                    display: "Fail".into(),
                }
            );
            assert_eq!(
                rows[RETRY_MAX_OUTAGE_WAIT_MS_KEY],
                MiddlewareSettingInfo::Inactive {
                    setting: unqualified(SettingValueInfo::U64(120_000), "default", "global"),
                    display: "120000 ms".into(),
                }
            );
            expected.insert(
                RETRY_ON_REFUSAL_KEY.to_string(),
                (json!("fail"), Winner::FlowFile),
            );
        } else {
            assert!(
                !rows.contains_key(RETRY_ON_REFUSAL_KEY)
                    && !rows.contains_key(RETRY_MAX_OUTAGE_WAIT_MS_KEY),
                "{policy:?} leaves the outage settings to protected retries: {rows:?}"
            );
        }
        assert_eq!(
            release_manifest(&only_run(root.path())),
            expected,
            "{policy:?}"
        );
    }

    for policy in [
        ReleasePolicy::RetryOnly,
        ReleasePolicy::RetryWithLimiter,
        ReleasePolicy::RetryWithLimiterPermuted,
        ReleasePolicy::RetryWithLimiterSplit,
    ] {
        let live_root = tempfile::tempdir().unwrap();
        let gateway = Arc::new(retry_fixture::ScriptedGateway::fail_first(2));
        let live_run = run_release_witness(
            policy,
            vec![retry_fixture::retry_order()],
            gateway.clone(),
            live_root.path(),
            None,
        );
        assert_eq!(gateway.calls(), 3, "{policy:?}");
        assert_eq!(
            gateway.invocations(),
            1,
            "retry must not repeat the handler"
        );
        let live = exported_run(&live_run, &live_root.path().join("live.jsonl"));
        assert_eq!(data_event_count(&live, "payment.authorized"), 1);
        assert_eq!(
            data_event_count(&live, "payment.authorization_unavailable"),
            0
        );
        assert_eq!(payment_effect_outcome_group_count(&live), 1);
        assert_eq!(payment_committed_terminal_group_count(&live), 1);
        assert_eq!(retry_schedules(&live).len(), 2);
        assert_eq!(retry_successes(&live)[0].total_attempts, 3);
        let completions = assert_one_recovery_completion_per_payment_cursor(&live);
        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].total_attempts, 3);
        let attempts = neutral_attempts(&live);
        assert_eq!(attempts.len(), 3);
        assert_eq!(
            attempts
                .iter()
                .map(|(_, attempt)| *attempt)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(attempts
            .iter()
            .all(|(cursor, _)| cursor == &completions[0].cursor));
        assert_no_breaker_evidence(&live);
        if policy.has_limiter() {
            let limiter = last_payment_limiter_counts(&live);
            assert_eq!(limiter.events, 3);
            assert_eq!(limiter.tokens, 3.0);
            assert!(limiter.delayed >= 2);
        } else {
            assert!(
                exported_chain_events(&live).all(|event| payment_limiter_counts(&event).is_none())
            );
        }

        let replay_root = tempfile::tempdir().unwrap();
        let replay_gateway =
            Arc::new(retry_fixture::ScriptedGateway::healthy().panic_on_live_call());
        let replay_run = run_release_witness(
            policy,
            vec![retry_fixture::retry_order()],
            replay_gateway.clone(),
            replay_root.path(),
            Some(&live_run),
        );
        assert_eq!(replay_gateway.calls(), 0);
        let replay = exported_run(&replay_run, &replay_root.path().join("replay.jsonl"));
        assert_eq!(data_event_count(&replay, "payment.authorized"), 1);
        assert_eq!(payment_committed_terminal_group_count(&replay), 1);
        assert_eq!(neutral_attempts(&replay), attempts);
        assert_eq!(recovery_completions(&replay), completions);
        assert_eq!(retry_schedules(&replay), retry_schedules(&live));
        assert_no_breaker_evidence(&replay);
    }
}

fn neutral_attempts(jsonl: &str) -> Vec<(EffectCursor, u32)> {
    exported_chain_events(jsonl)
        .filter_map(|event| match event.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(
                RecoveryFact::AttemptCompleted {
                    cursor, attempt, ..
                },
            )) => Some((cursor, attempt)),
            _ => None,
        })
        .collect()
}

fn assert_no_breaker_evidence(jsonl: &str) {
    for event in exported_chain_events(jsonl) {
        assert!(
            !matches!(
                event.payload,
                ChainPayload::Execution(ExecutionPayload::CircuitBreaker(_))
            ),
            "an omitted breaker must not author health or state facts"
        );
        if let Some(runtime) = event
            .envelope
            .observability
            .as_ref()
            .and_then(|observation| observation.runtime.as_ref())
        {
            assert!(
                runtime.effect_circuit_breakers.is_empty(),
                "an omitted breaker must not register metrics"
            );
        }
    }
}
