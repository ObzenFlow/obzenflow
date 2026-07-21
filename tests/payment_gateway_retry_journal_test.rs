// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod support;

use support::{gateway, proof};

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

fn occurrences(haystack: &str, needle: &str) -> usize {
    haystack.match_indices(needle).count()
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

fn last_payment_limiter_counts(jsonl: &str) -> LimiterCounts {
    let mut last = None;
    for row in exported_events(jsonl) {
        let Some(limiters) = row
            .pointer("/event/runtime_context/effect_rate_limiters")
            .and_then(|value| value.as_array())
        else {
            continue;
        };
        for limiter in limiters {
            if limiter.get("effect_type").and_then(|value| value.as_str())
                == Some("payment.authorize")
            {
                last = Some(LimiterCounts {
                    events: limiter["rl_events_total"]
                        .as_u64()
                        .expect("admitted attempt count"),
                    delayed: limiter["rl_delayed_total"]
                        .as_u64()
                        .expect("delayed attempt count"),
                    tokens: limiter["rl_tokens_consumed_total"]
                        .as_f64()
                        .expect("committed permit count"),
                    delay_seconds: limiter["rl_delay_seconds_total"]
                        .as_f64()
                        .expect("cumulative delay"),
                });
            }
        }
    }
    last.expect("payment limiter metrics should appear in the exported journal")
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
    assert_eq!(occurrences(&healthy, "\"action\":\"attempt_settled\""), 5);

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
    assert_eq!(occurrences(&control, "\"action\":\"attempt_settled\""), 1);
    assert!(!control.contains("\"action\":\"retry_scheduled\""));
    assert!(!control.contains("\"action\":\"retry_succeeded\""));
    assert!(!control.contains("\"action\":\"retry_exhausted\""));

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
    assert_eq!(occurrences(&treatment, "\"action\":\"attempt_settled\""), 3);
    assert_eq!(occurrences(&treatment, "\"attempt\":1"), 1);
    assert_eq!(occurrences(&treatment, "\"attempt\":2"), 1);
    assert_eq!(occurrences(&treatment, "\"attempt\":3"), 1);
    assert_eq!(occurrences(&treatment, "\"action\":\"retry_scheduled\""), 2);
    assert_eq!(occurrences(&treatment, "\"action\":\"retry_succeeded\""), 1);
    assert!(!treatment.contains("\"action\":\"retry_exhausted\""));
    assert_eq!(occurrences(&treatment, "\"next_attempt\":2"), 1);
    assert_eq!(occurrences(&treatment, "\"next_attempt\":3"), 1);
    assert!(treatment.contains("\"total_attempts\":3"));
    assert!(treatment.contains("\"terminal_classification\":\"success\""));
    assert!(
        !treatment.contains("gateway_timeout_simulated"),
        "intermediate failures must not become terminal journal records"
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
    assert!(
        !replay.contains("\"action\":\"retry_"),
        "strict replay must not emit fresh circuit-breaker retry evidence"
    );
    assert!(
        !replay.contains("\"action\":\"attempt_settled\""),
        "strict replay must not emit fresh physical-attempt evidence"
    );

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
    assert_eq!(occurrences(&open, "\"action\":\"attempt_settled\""), 5);
    assert!(
        open.contains("\"code\":\"circuit_open\""),
        "open rejection must carry the stable machine-readable cause code"
    );
    assert!(!open.contains("\"action\":\"retry_"));

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
    assert!(!open_replay.contains("\"action\":\"retry_"));
    assert!(
        !open_replay.contains("\"action\":\"attempt_settled\""),
        "open-rejection replay must not emit fresh physical-attempt evidence"
    );
}
