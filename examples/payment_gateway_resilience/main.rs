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

use obzenflow::application::{Banner, FlowApplication, LogLevel, Presentation};

fn main() -> std::process::ExitCode {
    let presentation = Presentation::new(
        Banner::new("Payment Gateway Resilience Demo")
            .description(
                "authorize_payment calls a simulated payment gateway. A circuit breaker, a retry \
                 and a rate limiter control each call.",
            )
            .bullets(
                "Outcomes",
                [
                    "For an order that fails validation, validate_order sends InvalidOrder to invalid_orders and CancelledOrder to cancelled_orders",
                    "When the gateway authorizes a payment, authorize_payment sends PaymentAuthorized to paid_orders",
                    "When the gateway declines a payment, authorize_payment sends PaymentDeclined to declined_orders and does not cancel the order",
                    "When a gateway call fails, authorize_payment sends PaymentAuthorizationUnavailable to manual_review and does not cancel the order",
                ],
            )
            .config("flow_name", "payment_gateway_resilience_demo")
            .config("journal_dir", "target/payment-gateway-logs"),
    );

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
