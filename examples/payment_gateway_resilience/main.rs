// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

mod support;

#[cfg(test)]
fn main() -> anyhow::Result<()> {
    support::flow::run_example_in_tests()
}

#[cfg(not(test))]
fn main() -> anyhow::Result<()> {
    use obzenflow_infra::application::{Banner, Presentation};

    let config = support::DemoConfig::from_env()?;

    let mut banner = Banner::new("Payment Gateway Resilience Demo")
        .description("Circuit breakers and rate limits protecting an unreliable dependency.")
        .bullets(
            "Highlights",
            [
                "Local validation errors still show up in obzenflow_errors_total",
                "The payments source simulates a semi-reliable upstream feed (MQTT/IOT style)\n  When it glitches, the source circuit breaker opens and you can watch:\n  obzenflow_circuit_breaker_*{stage=\"payments\",...}",
                "Gateway outages open the circuit breaker and increase:\n  obzenflow_circuit_breaker_state\n  obzenflow_circuit_breaker_rejection_rate\n  obzenflow_circuit_breaker_consecutive_failures",
                "Once open, the breaker stops hammering the gateway",
            ],
        );

    if let Some(total_events) = config.total_events {
        let payments_src = if config.use_async_source {
            "async (086f)"
        } else {
            "sync"
        };
        banner = banner
            .config("mode", "high-volume glitchy")
            .config("enabled_by", config.glitchy_reason.unwrap_or("unknown"))
            .config("payments_src", payments_src);

        if config.use_async_source {
            let poll_timeout = match config.async_poll_timeout {
                Some(d) => format!("{}s", d.as_secs()),
                None => "disabled".to_string(),
            };
            banner = banner.config("poll_timeout", poll_timeout);
        }

        banner = banner
            .config("total_events", total_events)
            .config(
                "rate_limit",
                format!("{} events/sec", config.rate_limit_events_per_sec),
            )
            .config(
                "cycle",
                format!(
                    "warmup={}, outage={}, recovery={}",
                    config.warmup_events, config.outage_events, config.recovery_events
                ),
            )
            .config("flow_name", "payment_gateway_resilience_glitchy_demo")
            .config("journal_dir", "target/payment-gateway-logs-glitchy")
            .config(
                "progress_log",
                format!("every {} events", config.summary_progress_every),
            );
    } else {
        banner = banner
            .config("flow_name", "payment_gateway_resilience_demo")
            .config("journal_dir", "target/payment-gateway-logs");
    }

    let presentation = Presentation::new(banner).with_footer(|outcome| {
        outcome
            .into_footer()
            .paragraph("Next step: scrape /metrics for obzenflow_circuit_breaker_* and obzenflow_errors_total.")
    });

    support::run_example(config, presentation)
}
