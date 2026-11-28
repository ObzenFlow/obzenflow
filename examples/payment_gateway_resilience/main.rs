// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Payment gateway resilience. Run from the repository root:
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

mod console;
mod deliveries;
mod domain;
mod fixtures;
mod flow;
mod gateway;
mod validation;

use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation, RunMode};

fn banner_for(mode: &RunMode) -> Banner {
    match mode {
        RunMode::Replay(_) => Banner::new("Payment Gateway Resilience Demo (strict replay)")
            .description("Reconstructing recorded outcomes with gateway calls suppressed.")
            .bullets(
                "What this replay does",
                [
                    "Retry, circuit breaker, and rate limiter are configured for topology validation only; no live state moves",
                    "Replay prints no sink lines, since it makes no sink calls",
                ],
            )
            .config("journal_dir", "target/payment-gateway-logs"),
        _ => Banner::new("Payment Gateway Resilience Demo")
            .description("The gateway call is a replay-suppressed effect; retry, a circuit breaker, and a per-attempt limiter form the second layer.")
            .bullets(
                "What to watch",
                [
                    "validate_order classifies once and authors multiple named facts; InvalidOrder goes to invalid_orders for intake review and CancelledOrder carries the order's fate",
                    "Gateway declines are payment outcomes, not cancellations; PaymentDeclined reaches its own delivery",
                    "Transient gateway failures receive bounded retry; sustained outages open the circuit breaker (obzenflow_circuit_breaker_*), and terminal unavailability does not cancel",
                    "Re-run with --replay-from target/payment-gateway-logs/flows/<flow_id> to replay with zero gateway calls",
                ],
            )
            .config("flow_name", "payment_gateway_resilience_demo")
            .config("journal_dir", "target/payment-gateway-logs"),
    }
}

fn main() -> std::process::ExitCode {
    let presentation = Presentation::for_mode(banner_for);

    let config_file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/payment_gateway_resilience/obzenflow.toml"
    );

    let result = FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .with_config_file(config_file)
        .run_blocking(flow::build_flow());

    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            std::process::ExitCode::from(err.process_exit_code())
        }
    }
}
