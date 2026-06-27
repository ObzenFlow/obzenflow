// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Payment Gateway Resilience: a durable-execution tutorial.
//!
//! The gateway call is modelled as a replay-suppressed effect, so a recorded run
//! can be replayed with zero gateway calls and identical output. A circuit
//! breaker on the same stage is the second, live-run layer of protection.
//!
//! Run it:
//!
//! ```text
//! cargo run -p obzenflow --example payment_gateway_resilience
//! ```
//!
//! Then replay the recorded run without re-calling the gateway:
//!
//! ```text
//! cargo run -p obzenflow --example payment_gateway_resilience -- \
//!     --replay-from target/payment-gateway-logs/flows/<flow_id>
//! ```
//!
//! See `README.md` for the full walkthrough.

mod domain;
mod fixtures;
mod flow;
mod gateway;
mod sinks;
mod validation;

use obzenflow_infra::application::{
    Banner, FlowApplication, Footer, LogLevel, Presentation, RunMode, RunPresentationOutcome,
};

/// Startup copy branches on the resolved run mode (FLOWIP-120i): the live
/// banner teaches the resilience behaviour, the replay banner says what a
/// strict replay reconstructs and what it suppresses, so the transcript never
/// implies the breaker or the gateway ran again.
fn banner_for(mode: &RunMode) -> Banner {
    match mode {
        RunMode::Replay(ctx) => Banner::new("Payment Gateway Resilience Demo (strict replay)")
            .description("Reconstructing a recorded run from its journals; nothing external runs.")
            .bullets(
                "What this replay does",
                [
                    format!("Source archive: {}", ctx.source_label()),
                    "Source config and env vars are ignored; recorded order events are re-admitted".to_string(),
                    "Gateway calls are suppressed; recorded effect outcomes are reused as facts".to_string(),
                    "Circuit breaker and rate limiter are configured for topology validation only; no live counters move".to_string(),
                    "Sink lines below are archived outcomes re-emitted deterministically, marked [replay]".to_string(),
                ],
            )
            .config("journal_dir", "target/payment-gateway-logs"),
        _ => Banner::new("Payment Gateway Resilience Demo")
            .description("The gateway call is a replay-suppressed effect; a circuit breaker is the second layer.")
            .bullets(
                "What to watch",
                [
                    "validate_order classifies once and authors multiple named facts; invalid orders and gateway declines converge on the cancelled-orders delivery",
                    "InvalidOrder and PaymentDeclined are journal-recorded provenance facts with no sink; OrderCancelled carries the order's fate",
                    "Gateway outages open the circuit breaker (obzenflow_circuit_breaker_*); once open it emits authorization-unavailable events, which do not cancel",
                    "Re-run with --replay-from target/payment-gateway-logs/flows/<flow_id> to replay with zero gateway calls",
                ],
            )
            .config("flow_name", "payment_gateway_resilience_demo")
            .config("journal_dir", "target/payment-gateway-logs"),
    }
}

/// Completion copy branches the same way: live runs are pointed at replay and
/// live metrics; replays are pointed at verification (FLOWIP-095j) and
/// replay-of-replay, never at metrics the replay did not touch.
fn footer_for(outcome: RunPresentationOutcome) -> Footer {
    let next_step = match outcome.run_mode() {
        RunMode::Replay(_) => {
            "Next: add --verify to make the runtime prove the reconstruction (\"output matched the original run, 0 differences\"), or replay the replay."
        }
        _ => "Next: replay this run with --replay-from, or scrape /metrics for obzenflow_circuit_breaker_*.",
    };
    // The raw .log files are internal framed storage; the supported way to read a
    // run is the journal subcommands (FLOWIP-120q).
    let inspect_step = "Inspect the run with `obzenflow journal inspect <run-dir>`, or `obzenflow journal export-jsonl <run-dir>` for one JSON object per committed record.";
    outcome
        .into_footer()
        .paragraph(next_step)
        .paragraph(inspect_step)
}

/// Exit codes follow the FLOWIP-095j verification contract when `--verify`
/// is passed: 0 certified match, 1 divergence, 2 uncertified remainder,
/// 3 refused or skipped. Every other failure exits 1.
fn main() -> std::process::ExitCode {
    let presentation = Presentation::for_mode(banner_for).with_footer(footer_for);

    let result = FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .run_blocking(flow::build_flow());

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            std::process::ExitCode::from(err.process_exit_code())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_infra::application::ReplayRunContext;
    use std::path::PathBuf;

    fn replay_mode() -> RunMode {
        RunMode::Replay(ReplayRunContext {
            archive_path: PathBuf::from("target/payment-gateway-logs/flows/flow_01SOURCE"),
            archive_flow_id: Some("flow_01SOURCE".to_string()),
        })
    }

    /// FLOWIP-120i: the replay transcript must not contain live-only guidance,
    /// and must name the source archive and the suppressions.
    #[test]
    fn replay_banner_drops_live_guidance_and_names_the_archive() {
        let live = banner_for(&RunMode::Live).render_for_stdout().text;
        let replay = banner_for(&replay_mode()).render_for_stdout().text;

        assert!(live.contains("Gateway outages open the circuit breaker"));
        assert!(!replay.contains("Gateway outages open the circuit breaker"));
        assert!(replay.contains("strict replay"));
        assert!(replay.contains("flow_01SOURCE"));
        assert!(replay.contains("Gateway calls are suppressed"));
        assert!(replay.contains("topology validation only"));
    }

    #[test]
    fn replay_footer_offers_comparison_not_live_metrics() {
        let live = footer_for(RunPresentationOutcome::Completed {
            flow_name: "payment_gateway_resilience_demo".to_string(),
            run_dir: Some(PathBuf::from(
                "target/payment-gateway-logs/flows/flow_01REPLAY",
            )),
            run_mode: RunMode::Live,
        })
        .finish();
        let replay = footer_for(RunPresentationOutcome::Completed {
            flow_name: "payment_gateway_resilience_demo".to_string(),
            run_dir: Some(PathBuf::from(
                "target/payment-gateway-logs/flows/flow_01REPLAY",
            )),
            run_mode: replay_mode(),
        })
        .finish();

        assert!(live.contains("scrape /metrics"));
        assert!(!replay.contains("scrape /metrics"));
        assert!(replay.contains("strict replay of flow_01SOURCE"));
        assert!(
            replay.contains("--verify"),
            "replay footer must teach the verification verb (FLOWIP-095j): {replay}"
        );
        assert!(
            replay.contains("--replay-from target/payment-gateway-logs/flows/flow_01REPLAY"),
            "replay footer must offer replay-of-replay on the new journal: {replay}"
        );
    }
}
