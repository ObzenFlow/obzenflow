// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod support;

use support::{flow, gateway};

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

fn last_payment_breaker_counts(jsonl: &str) -> (u64, u64, u64) {
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
                last = Some((
                    breaker["cb_requests_total"]
                        .as_u64()
                        .expect("request count"),
                    breaker["cb_successes_total"]
                        .as_u64()
                        .expect("success count"),
                    breaker["cb_failures_total"]
                        .as_u64()
                        .expect("failure count"),
                ));
            }
        }
    }
    last.expect("payment breaker metrics should appear in the exported journal")
}

#[test]
fn payment_gateway_retry_journal_test() {
    let control_root = tempfile::tempdir().expect("control journal root");
    let control_proof = Arc::new(gateway::GatewayRetryProof::new(false));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_retry_journal_test"])
        .run_blocking(flow::build_flow_for_profile(
            Some(flow::RetryProofProfile::Control),
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
    assert_eq!(last_payment_breaker_counts(&control), (1, 0, 1));
    assert!(!control.contains("\"action\":\"retry_scheduled\""));
    assert!(!control.contains("\"action\":\"retry_succeeded\""));
    assert!(!control.contains("\"action\":\"retry_exhausted\""));

    let treatment_root = tempfile::tempdir().expect("treatment journal root");
    let treatment_proof = Arc::new(gateway::GatewayRetryProof::new(false));
    obzenflow_infra::application::FlowApplication::builder()
        .with_cli_args(["payment_gateway_retry_journal_test"])
        .run_blocking(flow::build_flow_for_profile(
            Some(flow::RetryProofProfile::Treatment),
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
    assert_eq!(last_payment_breaker_counts(&treatment), (1, 1, 0));
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
        .run_blocking(flow::build_flow_for_profile(
            Some(flow::RetryProofProfile::Treatment),
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
}
