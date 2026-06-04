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
mod sources;

use anyhow::Result;
use obzenflow_infra::application::{Banner, FlowApplication, LogLevel, Presentation};

fn main() -> Result<()> {
    let banner = Banner::new("Payment Gateway Resilience Demo")
        .description("The gateway call is a replay-suppressed effect; a circuit breaker is the second layer.")
        .bullets(
            "What to watch",
            [
                "Local validation errors still show up in obzenflow_errors_total",
                "Gateway outages open the circuit breaker (obzenflow_circuit_breaker_*) and emit degraded authorizations",
                "Re-run with --replay-from target/payment-gateway-logs/flows/<flow_id> to replay with zero gateway calls",
            ],
        )
        .config("flow_name", "payment_gateway_resilience_demo")
        .config("journal_dir", "target/payment-gateway-logs");

    let presentation = Presentation::new(banner).with_footer(|outcome| {
        outcome.into_footer().paragraph(
            "Next: replay this run with --replay-from, or scrape /metrics for obzenflow_circuit_breaker_*.",
        )
    });

    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_presentation(presentation)
        .run_blocking(flow::build_flow())?;

    Ok(())
}
