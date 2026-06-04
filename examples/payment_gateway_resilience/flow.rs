// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The payment-gateway resilience flow.
//!
//! Four stages, one durable-execution lesson:
//!
//! ```text
//! payments (source) -> validation (transform) -> gateway (effectful) -> summary (sink)
//! ```
//!
//! The gateway stage performs the `AuthorizePayment` effect (see `gateway.rs`).
//! On a live run the effect executes once and the runtime journals its result;
//! on a replay the runtime returns that recorded authorization without calling
//! the gateway again.
//!
//! The circuit breaker on the gateway stage is the second, independent layer:
//! it watches the live effect boundary and, once the dependency looks unhealthy,
//! short-circuits to a typed fallback instead of hammering it. See `README.md`.

use super::domain::{AuthorizedPayment, PaymentCommand, ValidatedPayment};
use super::gateway::{AuthorizePayment, GatewayAuthorization, GatewayTransform, ValidationTransform};
use super::sinks::PaymentSummarySink;
use super::sources::PaymentCommandSource;
use obzenflow_adapters::middleware::circuit_breaker::{HalfOpenPolicy, OpenPolicy};
use obzenflow_adapters::middleware::{backpressure, CircuitBreakerBuilder, RateLimiterBuilder};
use obzenflow_core::CircuitBreakerContractMode;
use obzenflow_dsl::{effectful_transform, flow, sink, source, transform};
use obzenflow_infra::journal::disk_journals;
use std::num::NonZeroU32;

const BACKPRESSURE_WINDOW: u64 = 1_000;

/// Build the demo flow.
///
/// A gentle flow-level rate limit keeps logs and metrics readable, so you can
/// watch the stage metrics and circuit-breaker gauges change over time.
pub fn build_flow() -> obzenflow_dsl::FlowDefinition {
    flow! {
        name: "payment_gateway_resilience_demo",
        journals: disk_journals(std::path::PathBuf::from("target/payment-gateway-logs")),

        middleware: [
            RateLimiterBuilder::new(1.0).build()
        ],

        stages: {
            // Source: a scripted stream of payment commands across three phases
            // (warmup, outage, recovery).
            payments = source!(PaymentCommand => PaymentCommandSource::new(), [
                backpressure(BACKPRESSURE_WINDOW)
            ]);

            // Local validation: deterministic checks with no external I/O.
            // Failures are tagged as errors and still emitted, so they count
            // toward obzenflow_errors_total and never reach the gateway.
            validated = transform!(name: "validation", PaymentCommand -> ValidatedPayment => ValidationTransform, [
                backpressure(BACKPRESSURE_WINDOW)
            ]);

            // Gateway: the only stage that touches the outside world, expressed
            // as the `AuthorizePayment` effect. The runtime runs it once, records
            // the result, and suppresses it on replay.
            //
            // The circuit breaker is the live-run safety layer on top:
            //   - opens when >= 60% of the last 5 gateway calls fail,
            //   - also counts calls slower than 250ms toward opening,
            //   - while open, emits a typed degraded authorization instead of
            //     calling the gateway (EmitFallback), with a single half-open probe.
            // BreakerAware keeps transport contracts green while the breaker is open.
            gateway = effectful_transform!(
                ValidatedPayment -> AuthorizedPayment => GatewayTransform,
                effects: [AuthorizePayment],
                middleware: [
                    CircuitBreakerBuilder::new(3)
                        .cooldown(std::time::Duration::from_secs(5))
                        .rate_based_over_last_n_calls(5, 0.6)
                        .slow_call(std::time::Duration::from_millis(250), 0.5)
                        .with_typed_fallback::<ValidatedPayment, GatewayAuthorization, _>(|_validated| GatewayAuthorization {
                            authorization_id: AuthorizedPayment::AUTHORIZATION_ID_FALLBACK_CB_OPEN.to_string(),
                        })
                        .open_policy(OpenPolicy::EmitFallback)
                        .half_open_policy(HalfOpenPolicy::new(
                            NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
                            OpenPolicy::EmitFallback,
                        ))
                        .with_contract_mode(CircuitBreakerContractMode::BreakerAware)
                        .build()
                ]
            );

            // Sink: prints a running commentary and a final summary so you can
            // read off what validation rejected and what the breaker protected.
            summary = sink!(AuthorizedPayment => PaymentSummarySink::new());
        },

        topology: {
            payments |> validated;
            validated |> gateway;
            gateway |> summary;
        }
    }
}
