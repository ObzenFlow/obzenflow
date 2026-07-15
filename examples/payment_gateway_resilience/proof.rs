// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115h acceptance rigging: the retry-proof profiles.
//!
//! The proof runs one scripted order against a gateway whose backend fails
//! twice and then succeeds. The control profile disables breaker retry (one
//! call, unavailable outcome); the treatment profile enables it (three calls,
//! authorized outcome). Everything profile-shaped lives here so `flow.rs`
//! stays the tutorial; both entry points share `flow::assemble_flow`.

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
}

fn retry_proof_profile() -> Option<RetryProofProfile> {
    let profile = std::env::var("PAYMENT_DEMO_RETRY_PROOF"); // allow-replay-ambient: build-time acceptance-test profile only
    match profile {
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("PAYMENT_DEMO_RETRY_PROOF could not be read: {error}"),
        Ok(value) if value == "control" => Some(RetryProofProfile::Control),
        Ok(value) if value == "treatment" => Some(RetryProofProfile::Treatment),
        Ok(value) => panic!(
            "unknown PAYMENT_DEMO_RETRY_PROOF value '{value}'; expected 'control' or 'treatment'"
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
            build_flow_for_profile(
                profile,
                Some(Arc::new(GatewayRetryProof::new(replay_requested))),
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
        RetryProofProfile::Control => None,
        RetryProofProfile::Treatment => Some(
            Retry::fixed(Duration::from_millis(1))
                .attempts(3)
                .max_delay(Duration::from_millis(10))
                .start_window(Duration::from_secs(1)),
        ),
    };
    flow::assemble_flow(
        vec![fixtures::retry_proof_order()],
        Vec::new(),
        gateway_transform,
        gateway_retry,
        // The proof asserts on journal contents, not pacing; run the gateway
        // effectively unthrottled so the acceptance test finishes quickly.
        1_000.0,
        journal_root,
    )
}
