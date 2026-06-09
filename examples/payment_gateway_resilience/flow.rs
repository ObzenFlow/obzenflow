// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The payment-gateway resilience flow.
//!
//! One upstream event stream, four business outcome channels:
//!
//! ```text
//! orders (source) -> valid orders -> gateway -> paid-order sink
//!                 |                       \-> gateway-decline sink
//!                 |                       \-> unavailable-authorization sink
//!                 \-> invalid orders -> order-status sink
//! ```
//!
//! Validation lives in `validation.rs`; the gateway stage performs the
//! `AuthorizePayment` effect (see `gateway.rs`).
//! On a live run the effect executes once and the runtime journals its result;
//! on a replay the runtime returns that recorded gateway decision without calling
//! the gateway again. The gateway then emits the named payment fact that happened.
//!
//! The circuit breaker on the gateway stage is the second, independent layer:
//! it watches the live effect boundary and, once the dependency looks unhealthy,
//! short-circuits to a typed unavailable outcome instead of hammering it. See
//! `README.md`.

use super::domain::{
    CustomerOrderPlaced, GatewayPaymentDecision, InvalidOrder, PaymentAuthorizationUnavailable,
    PaymentAuthorized, PaymentDeclined, ValidatedOrder,
};
use super::fixtures;
use super::gateway::{self, AuthorizePayment, GatewayTransform};
use super::sinks;
use super::validation;
use obzenflow::typed::{sources as typed_sources, transforms as typed_transforms};
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
    let scripted_orders = fixtures::scripted_orders();

    flow! {
        name: "payment_gateway_resilience_demo",
        journals: disk_journals(std::path::PathBuf::from("target/payment-gateway-logs")),

        middleware: [
            RateLimiterBuilder::new(1.0).build()
        ],

        stages: {
            // Source: a scripted stream of upstream customer-order events
            // across three phases (warmup, outage, recovery).
            //
            // The flow reacts to these facts. On replay the runtime injects
            // journaled source events instead of polling this source again.
            orders = source!(CustomerOrderPlaced => typed_sources::finite_from_fn(move |index| {
                scripted_orders.get(index).cloned()
            }), [
                backpressure(BACKPRESSURE_WINDOW)
            ]);

            // Local validation: deterministic checks with no external I/O.
            // This is typed business classification, not exception handling:
            // valid orders go to the gateway, invalid orders go to their own
            // domain-event channel.
            valid_orders = transform!(CustomerOrderPlaced -> ValidatedOrder => typed_transforms::filter_map(validation::valid_order), [
                backpressure(BACKPRESSURE_WINDOW)
            ]);
            invalid_orders = transform!(CustomerOrderPlaced -> InvalidOrder => typed_transforms::filter_map(validation::invalid_order), [
                backpressure(BACKPRESSURE_WINDOW)
            ]);

            // Gateway: the only stage that touches the outside world, expressed
            // as the `AuthorizePayment` effect. The runtime runs it once, records
            // the result, and suppresses it on replay.
            //
            // The circuit breaker is the live-run safety layer on top:
            //   - opens when >= 60% of the last 5 gateway calls fail,
            //   - also counts calls slower than 250ms toward opening,
            //   - while open, emits a typed authorization-unavailable outcome
            //     instead of calling the gateway (EmitFallback), with a single
            //     half-open probe.
            // BreakerAware keeps transport contracts green while the breaker is open.
            gateway = effectful_transform!(
                ValidatedOrder -> {
                    GatewayPaymentDecision,
                    PaymentAuthorized,
                    PaymentDeclined,
                    PaymentAuthorizationUnavailable
                } => GatewayTransform,
                effects: [AuthorizePayment],
                middleware: [
                    CircuitBreakerBuilder::new(3)
                        .cooldown(std::time::Duration::from_secs(5))
                        .rate_based_over_last_n_calls(5, 0.6)
                        .slow_call(std::time::Duration::from_millis(250), 0.5)
                        .with_typed_fallback::<ValidatedOrder, GatewayPaymentDecision, _>(|_order| GatewayPaymentDecision::AuthorizationUnavailable {
                            reason: "circuit breaker open".to_string(),
                        })
                        .with_failure_classifier(gateway::simulated_gateway_unavailability_counts_as_failure)
                        .open_policy(OpenPolicy::EmitFallback)
                        .half_open_policy(HalfOpenPolicy::new(
                            NonZeroU32::new(1).expect("permitted_probes must be non-zero"),
                            OpenPolicy::EmitFallback,
                        ))
                        .with_contract_mode(CircuitBreakerContractMode::BreakerAware)
                        .build()
                ]
            );

            // Paid-order sink: in production this is the boundary a shipping
            // system would subscribe to.
            paid_orders = sink!(|authorized: PaymentAuthorized| {
                sinks::send_to_shipping(authorized);
            });

            // Invalid-order sink: local validation failures. These are normal
            // business events, not framework errors.
            invalid_order_events = sink!(|invalid: InvalidOrder| {
                sinks::record_invalid_order(invalid);
            });

            // Gateway-decline sink: material payment declines from the remote
            // authorization boundary, separate from local validation failures.
            declined_payments = sink!(|declined: PaymentDeclined| {
                sinks::record_gateway_decline(declined);
            });

            // Unavailable-authorization sink: failed gateway call or breaker
            // fallback. This means no payment decision was reached.
            manual_review = sink!(|unavailable: PaymentAuthorizationUnavailable| {
                sinks::record_authorization_unavailable(unavailable);
            });
        },

        topology: {
            orders |> valid_orders;
            orders |> invalid_orders;
            valid_orders |> gateway;
            gateway |> paid_orders;
            invalid_orders |> invalid_order_events;
            gateway |> declined_payments;
            gateway |> manual_review;
        }
    }
}
