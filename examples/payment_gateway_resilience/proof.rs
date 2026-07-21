// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Circuit-breaker acceptance profiles.
//!
//! Control and treatment compare one scripted order against a gateway that
//! fails twice and then succeeds. Healthy proves ordinary one-call-per-second
//! pacing and zero slow classifications. Open rejection proves five physical
//! failures open the breaker and the sixth logical effect fails fast. Everything
//! profile-shaped lives here so `flow.rs` stays the tutorial; both entry points
//! share `flow::assemble_flow`.

use super::fixtures;
use super::flow;
use super::gateway::{GatewayRetryProof, GatewayTransform};
use obzenflow_adapters::middleware::Retry;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryProofProfile {
    Control,
    Treatment,
    Healthy,
    OpenRejection,
}

fn retry_proof_profile() -> Option<RetryProofProfile> {
    let profile = std::env::var("PAYMENT_DEMO_RETRY_PROOF"); // allow-replay-ambient: build-time acceptance-test profile only
    match profile {
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("PAYMENT_DEMO_RETRY_PROOF could not be read: {error}"),
        Ok(value) if value == "control" => Some(RetryProofProfile::Control),
        Ok(value) if value == "treatment" => Some(RetryProofProfile::Treatment),
        Ok(value) if value == "healthy" => Some(RetryProofProfile::Healthy),
        Ok(value) if value == "open-rejection" => Some(RetryProofProfile::OpenRejection),
        Ok(value) => panic!(
            "unknown PAYMENT_DEMO_RETRY_PROOF value '{value}'; expected 'control', 'treatment', 'healthy', or 'open-rejection'"
        ),
    }
}

/// The binary's flow entry point: the tutorial flow, or the retry-proof rig
/// when `PAYMENT_DEMO_RETRY_PROOF` selects a profile.
pub fn build_flow() -> obzenflow_dsl::FlowDefinition {
    match retry_proof_profile() {
        None => flow::build_flow(),
        Some(profile) => {
            let replay_requested = std::env::args().any(|argument| argument == "--replay-from");
            let proof = match profile {
                RetryProofProfile::Control | RetryProofProfile::Treatment => {
                    GatewayRetryProof::new(replay_requested)
                }
                RetryProofProfile::Healthy => GatewayRetryProof::healthy(replay_requested),
                RetryProofProfile::OpenRejection => {
                    GatewayRetryProof::always_fail(replay_requested)
                }
            };
            build_flow_for_profile(
                profile,
                Some(Arc::new(proof)),
                std::path::PathBuf::from(flow::DEMO_JOURNAL_ROOT),
            )
        }
    }
}

/// Build the proof flow for one profile. The acceptance test calls this
/// directly with its own journal root and call-counting proof handle.
pub fn build_flow_for_profile(
    profile: RetryProofProfile,
    retry_proof: Option<Arc<GatewayRetryProof>>,
    journal_root: std::path::PathBuf,
) -> obzenflow_dsl::FlowDefinition {
    let gateway_transform = retry_proof
        .map(GatewayTransform::with_retry_proof)
        .unwrap_or_default();
    let gateway_retry = match profile {
        RetryProofProfile::Control
        | RetryProofProfile::Healthy
        | RetryProofProfile::OpenRejection => None,
        RetryProofProfile::Treatment => Some(
            Retry::fixed(Duration::from_millis(1))
                .max_attempts(3)
                .max_backoff(Duration::from_millis(10))
                .attempt_start_window(Duration::from_secs(1)),
        ),
    };
    let orders = match profile {
        RetryProofProfile::Control | RetryProofProfile::Treatment => {
            vec![fixtures::retry_proof_order()]
        }
        RetryProofProfile::Healthy => fixtures::healthy_proof_orders(),
        RetryProofProfile::OpenRejection => fixtures::open_rejection_proof_orders(),
    };
    let calls_per_second = match profile {
        RetryProofProfile::Healthy => 1.0,
        RetryProofProfile::Control
        | RetryProofProfile::Treatment
        | RetryProofProfile::OpenRejection => 1_000.0,
    };
    flow::assemble_flow(
        orders,
        Vec::new(),
        gateway_transform,
        gateway_retry,
        calls_per_second,
        journal_root,
    )
}
